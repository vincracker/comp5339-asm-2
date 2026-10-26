"""Task 4: event storage (src/storage/event_store.py) and dashboard queries (src/storage/queries.py).

Uses a minimal stand-in for the Assignment 1 charger table, so no generated data is needed.
"""
import json
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

from src.storage import queries
from src.storage.event_store import EventStore, ingest, init_db, wrap

EXAMPLE = Path(__file__).resolve().parents[1] / "src" / "MQTT_publishing" / "example.json"
T = datetime.fromisoformat


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "ev.duckdb"
    with duckdb.connect(str(path)) as con:
        con.execute("""CREATE TABLE charger (charger_record_id VARCHAR PRIMARY KEY, number_of_plugs INTEGER,
                                             charger_type VARCHAR)""")
        con.execute("INSERT INTO charger VALUES ('chr_example', 2, 'DC'), ('chr_other', 1, 'DC')")
    return path


@pytest.fixture
def con(db_path):
    con = duckdb.connect(str(db_path))
    init_db(con)
    yield con
    con.close()


def batch(sequence, events, replay="r1", start="2026-10-01T01:00:00", end="2026-10-01T01:00:10"):
    return {"replay_id": replay, "sequence": sequence, "window_start": start, "window_end": end, "events": events}


def store(con, *batches):
    return ingest(con, [wrap(json.dumps(b)) for b in batches])


def counts(con):
    return {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
            for t in ("stream_batch", "price_event", "charging_session", "meter_event", "unmatched_event")}


def price(station, ts, p):
    return {"event_id": f"price_update:{station}:{ts}", "type": "price_update", "ts": ts, "station_id": station,
            "price_per_kwh": p, "wholesale_per_kwh": 0.1}


def start(session, ts, station="chr_example", connector=1):
    return {"event_id": f"session_start:{session}", "type": "session_start", "ts": ts, "station_id": station,
            "session_id": session, "connector_id": connector, "site_type": "urban", "vehicle_model": "MG4",
            "start_ts": ts, "price_per_kwh": 0.6}


def meter(session, ts, energy, station="chr_example", connector=1):
    return {"event_id": f"meter:{session}:{ts}", "type": "meter", "ts": ts, "station_id": station,
            "session_id": session, "connector_id": connector, "voltage_v": 400.0, "current_a": 50.0,
            "power_kw": 20.0, "energy_kwh": energy, "soc_pct": 50.0}


def end(session, start_ts, end_ts, energy, station="chr_example", connector=1, minutes=60.0):
    return {"event_id": f"session_end:{session}", "type": "session_end", "ts": end_ts, "station_id": station,
            "session_id": session, "connector_id": connector, "site_type": "urban", "vehicle_model": "MG4",
            "start_ts": start_ts, "charge_end_ts": end_ts, "end_ts": end_ts, "charge_minutes": minutes,
            "energy_kwh": energy, "avg_kw": 20.0, "peak_kw": 25.0, "price_per_kwh": 0.6, "cost_aud": energy * 0.6}


def test_example_batch_is_stored_by_type(con):
    replay, clock = store(con, json.loads(EXAMPLE.read_text()))
    assert (replay, clock) == ("example-replay", T("2026-10-01T01:00:10"))
    assert counts(con) == {"stream_batch": 1, "price_event": 1, "charging_session": 2, "meter_event": 1,
                           "unmatched_event": 0}
    ended, started = con.execute("""SELECT session_id, end_ts, energy_kwh, start_seen FROM charging_session
                                    ORDER BY session_id""").fetchall()
    # The ended session was first seen through its meter reading; session_end completed it.
    assert ended[1:] == (T("2026-10-01T01:00:00"), 7.5, True)
    assert started[1:] == (None, None, True)


def test_redelivery_and_second_replay_store_nothing_twice(con):
    b = json.loads(EXAMPLE.read_text())
    store(con, b, b)  # QoS 1 redelivery inside one flush
    store(con, b)     # ... and in a later flush
    store(con, {**b, "replay_id": "second-pass"})
    c = counts(con)
    assert c["stream_batch"] == 2  # one row per distinct MQTT message
    assert (c["price_event"], c["charging_session"], c["meter_event"]) == (1, 2, 1)


def test_joined_mid_session_then_completed(con):
    store(con, batch(1, [meter("s1", "2026-10-01T01:00:00", 3.0)]))
    assert con.execute("SELECT start_ts, start_seen FROM charging_session").fetchone() == (T("2026-10-01T01:00:00"), False)
    store(con, batch(2, [end("s1", "2026-10-01T00:30:00", "2026-10-01T01:05:00", 5.0)]))
    assert con.execute("SELECT start_ts, end_ts, start_seen FROM charging_session").fetchone() == (
        T("2026-10-01T00:30:00"), T("2026-10-01T01:05:00"), True)


def test_unknown_charger_is_set_aside_without_losing_the_rest(con):
    store(con, batch(1, [price("chr_missing", "2026-10-01T01:00:00", 0.5), price("chr_example", "2026-10-01T01:00:00", 0.5)]))
    c = counts(con)
    assert (c["unmatched_event"], c["price_event"]) == (1, 1)


def test_metrics_are_computed_as_of_the_simulated_clock(con):
    store(con, batch(1, [
        price("chr_example", "2026-10-01T00:00:00", 0.50), price("chr_example", "2026-10-01T01:00:00", 0.70),
        start("a", "2026-10-01T00:00:00"), end("a", "2026-10-01T00:00:00", "2026-10-01T01:00:00", 30.0),
        start("b", "2026-10-01T01:30:00", connector=2), end("b", "2026-10-01T01:30:00", "2026-10-01T02:00:00", 10.0,
                                                           connector=2, minutes=30.0),
    ], start="2026-10-01T00:00:00", end="2026-10-01T02:00:10"))
    # At 01:45 session b is open and a finished 45 min ago; the 01:00 price is in force.
    now = queries.charger_current(con, "chr_example", T("2026-10-01T01:45:00"))
    assert (now["active_sessions"], float(now["price_per_kwh"]), now["price_since"]) == (1, 0.70, T("2026-10-01T01:00:00"))
    # Over 00:00-02:00: 60 + 30 connector-minutes of 2 connectors x 120 min = 37.5 %.
    usage = con.execute("FROM charger_usage(TIMESTAMP '2026-10-01 00:00', TIMESTAMP '2026-10-01 02:00')").fetchone()
    assert usage[1:3] == (2, 40.0) and usage[5] == pytest.approx(0.375)
    hist = queries.charger_history(con, "chr_example", T("2026-10-01T02:00:00"))
    assert (float(hist["price_range"]["low"]), float(hist["price_range"]["high"])) == (0.50, 0.70)
    assert hist["day_from"] == T("2026-10-01T00:00:00")  # clamped to the first stored batch
    hours = queries.charger_hourly_utilisation(con, "chr_example", T("2026-10-01T02:00:00"))
    assert len(hours) == 24 and [round(u, 3) for _, u in hours[-3:]] == [0.0, 0.5, 0.25]
    net = queries.network_summary(con, T("2026-10-01T02:00:00"))
    assert (net["sessions"], net["energy_kwh"], net["active_sessions"]) == (2, 40.0, 0)


def test_store_thread_commits_and_skips_a_rejected_message(db_path):
    s = EventStore(db_path, flush_interval=60)  # flushed by close()
    s.submit(json.dumps(batch(1, [price("chr_example", "2026-10-01T01:00:00", 0.5)])).encode())
    s.submit(json.dumps(batch(2, [price("chr_example", "2026-10-01T01:00:05", -1.0)])))  # violates a CHECK
    s.submit(json.dumps(batch(3, [price("chr_example", "2026-10-01T01:00:09", 0.6)], end="2026-10-01T01:00:20")))
    s.close()
    assert (s.stored_batches, s.failed_batches) == (2, 1)
    assert s.clock == T("2026-10-01T01:00:20")
    with duckdb.connect(str(db_path), read_only=True) as con:
        assert con.execute("SELECT count(*) FROM price_event").fetchone()[0] == 2
