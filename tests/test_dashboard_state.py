import json
from pathlib import Path

from src.dashboard.state import LiveState, status

EXAMPLE = Path(__file__).resolve().parents[1] / "src" / "MQTT_publishing" / "example.json"
CHARGERS = {"chr_example": {"station_id": "chr_example", "name": "Example", "address": "1 Test St",
                            "operator": "Op", "connectors": 2, "lat": -33.9, "lon": 151.2}}


def batch(sequence, events, replay="r1", start="2026-10-01T01:00:00", end="2026-10-01T01:00:10"):
    return {"replay_id": replay, "sequence": sequence, "window_start": start, "window_end": end, "events": events}


def meter(session, ts, energy, connector=1):
    return {"event_id": f"meter:{session}:{ts}", "type": "meter", "ts": ts, "station_id": "chr_example",
            "session_id": session, "connector_id": connector, "voltage_v": 400.0, "current_a": 50.0,
            "power_kw": 20.0, "energy_kwh": energy, "soc_pct": 50.0}


def start(session, ts, connector=1):
    return {"event_id": f"session_start:{session}", "type": "session_start", "ts": ts,
            "station_id": "chr_example", "session_id": session, "connector_id": connector,
            "site_type": "urban", "vehicle_model": "MG4", "start_ts": ts, "price_per_kwh": 0.6}


def test_example_batch_mid_session_join_then_end_and_new_start():
    state = LiveState(CHARGERS)
    assert state.apply_batch(json.loads(EXAMPLE.read_text()))
    d = state.station_detail("chr_example")
    assert d["price"] == 0.65
    assert d["completed"] == 1 and d["last_session"]["cost_aud"] == 4.5
    # The ended session (joined from its meter reading) is gone; the new one is active.
    assert [s["session_id"] for s in d["sessions"]] == ["chr_example-1-202610010100"]
    assert d["sessions"][0]["joined_mid_session"] is False
    assert state.summary()["active_sessions"] == 1


def test_meter_for_unseen_session_opens_it():
    state = LiveState(CHARGERS)
    state.apply_batch(batch(1, [meter("s1", "2026-10-01T01:00:00", 3.0)]))
    (s,) = state.station_detail("chr_example")["sessions"]
    assert s["joined_mid_session"] and [r["energy_kwh"] for r in s["readings"]] == [3.0]


def test_readings_accumulate_into_the_curve():
    state = LiveState(CHARGERS)
    state.apply_batch(batch(1, [start("s1", "2026-10-01T01:00:00"), meter("s1", "2026-10-01T01:00:00", 0.3)]))
    state.apply_batch(batch(2, [meter("s1", "2026-10-01T01:01:00", 0.9)], end="2026-10-01T01:01:10"))
    (s,) = state.station_detail("chr_example")["sessions"]
    assert [r["energy_kwh"] for r in s["readings"]] == [0.3, 0.9]
    assert s["vehicle_model"] == "MG4" and s["price"] == 0.6


def test_duplicate_sequence_is_skipped():
    state = LiveState(CHARGERS)
    b = batch(1, [meter("s1", "2026-10-01T01:00:00", 3.0)])
    assert state.apply_batch(b)
    assert not state.apply_batch(b)
    assert state.duplicates == 1
    assert len(state.station_detail("chr_example")["sessions"][0]["readings"]) == 1


def test_sequence_gap_is_counted():
    state = LiveState(CHARGERS)
    state.apply_batch(batch(5, []))
    state.apply_batch(batch(8, []))
    assert state.summary()["gaps"] == 2


def test_new_replay_resets_state():
    state = LiveState(CHARGERS)
    state.apply_batch(batch(100, [meter("s1", "2026-10-14T23:00:00", 3.0)]))
    state.apply_batch(batch(1, [], replay="r2", start="2026-10-01T00:00:00", end="2026-10-01T00:00:10"))
    summary = state.summary()
    assert summary["replay_id"] == "r2" and summary["on_map"] == 0 and summary["active_sessions"] == 0
    assert summary["gaps"] == 0


def test_new_session_on_same_connector_replaces_unfinished_one():
    state = LiveState(CHARGERS)
    state.apply_batch(batch(1, [meter("old", "2026-10-01T01:00:00", 3.0), start("new", "2026-10-01T01:00:05")]))
    assert [s["session_id"] for s in state.station_detail("chr_example")["sessions"]] == ["new"]


def test_session_without_end_is_dropped_when_stale():
    state = LiveState(CHARGERS)
    state.apply_batch(batch(1, [meter("s1", "2026-10-01T01:00:00", 3.0)]))
    state.apply_batch(batch(2, [], start="2026-10-01T08:00:00", end="2026-10-01T08:00:10"))
    assert state.summary()["active_sessions"] == 0


def test_unknown_station_is_ignored():
    state = LiveState(CHARGERS)
    e = {**meter("s1", "2026-10-01T01:00:00", 3.0), "station_id": "chr_missing"}
    state.apply_batch(batch(1, [e]))
    assert state.summary()["unknown_events"] == 1 and state.markers() == []


def test_marker_status():
    assert status(0, 4, None) == "unknown"
    assert status(0, 4, 0.5) == "free"
    assert status(1, 4, 0.5) == "busy"
    assert status(4, 4, None) == "full"
