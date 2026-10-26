"""Task 4 event store utilities.

    python -m src.storage check              # row counts and integrity checks of the stored events
    python -m src.storage load               # store a full replay WITHOUT MQTT (testing / offline)

`load` builds the exact batches the publisher would send (src/MQTT_publishing/publisher.py,
iter_batches) and passes them through the same ingest SQL the live subscriber uses, so the
schema and queries can be tested in ~2 minutes instead of a 3.5 hour MQTT replay. Batches
stored this way carry replay_id "offline-<uuid>". Events recorded live arrive through
`python -m src.dashboard`, which stores everything it receives.
"""
import argparse
import itertools
import json
import time
from pathlib import Path
from uuid import uuid4

import duckdb

from .event_store import DB_PATH, ingest, init_db, wrap

CHECKS = {
    # name: SQL returning a single count that must be 0
    "events for unknown chargers": "SELECT count(*) FROM unmatched_event",
    "sessions with a connector the charger lacks": """
        SELECT count(*) FROM charging_session s JOIN charger c ON c.charger_record_id = s.station_id
        WHERE s.connector_id > c.number_of_plugs""",
    "overlapping sessions on one connector": """
        SELECT count(*) FROM charging_session a JOIN charging_session b
          ON a.station_id = b.station_id AND a.connector_id = b.connector_id AND a.session_id < b.session_id
         AND a.start_ts < coalesce(b.end_ts, b.start_ts) AND b.start_ts < coalesce(a.end_ts, a.start_ts)""",
    "meter readings outside their session": """
        SELECT count(*) FROM meter_event m JOIN charging_session s USING (session_id)
        WHERE m.ts < s.start_ts OR m.ts > s.end_ts""",
    "meter readings without a session": """
        SELECT count(*) FROM meter_event m ANTI JOIN charging_session s USING (session_id)""",
    "duplicate meter readings": """
        SELECT count(*) - count(DISTINCT (session_id, ts)) FROM meter_event""",
    "completed sessions whose final meter kWh != session kWh": """
        SELECT count(*) FROM charging_session s
        JOIN (SELECT session_id, max(energy_kwh) AS kwh FROM meter_event GROUP BY 1) m USING (session_id)
        WHERE s.end_ts IS NOT NULL AND s.start_seen AND abs(m.kwh - s.energy_kwh) > 0.01""",
}


def load(db: Path, data_dir: Path | None, window_seconds: float, chunk: int) -> None:
    from src.MQTT_publishing.publisher import DATA_DIR, iter_batches

    con = duckdb.connect(str(db))
    init_db(con)
    replay_id = f"offline-{uuid4()}"
    batches = iter_batches(data_dir or DATA_DIR, window_seconds, replay_id=replay_id)
    start, n = time.perf_counter(), 0
    while group := list(itertools.islice(batches, chunk)):
        ingest(con, [wrap(json.dumps(b, separators=(",", ":"))) for b in group])
        n += len(group)
        if n % (chunk * 24) == 0:  # once per simulated day at the defaults
            print(f"  {n:,} batches, simulated time {group[-1]['window_end']}")
    con.execute("CHECKPOINT")
    print(f"Stored {n:,} batches ({replay_id}) in {time.perf_counter() - start:.0f} s -> {db}")
    con.close()


def check(db: Path) -> bool:
    con = duckdb.connect(str(db), read_only=True)
    con.execute("LOAD spatial")
    for table in ("stream_batch", "price_event", "charging_session", "meter_event", "unmatched_event"):
        print(f"{table:18} {con.execute(f'SELECT count(*) FROM {table}').fetchone()[0]:>12,}")
    open_, partial, chargers = con.execute("""
        SELECT count(*) FILTER (WHERE end_ts IS NULL), count(*) FILTER (WHERE NOT start_seen),
               count(DISTINCT station_id) FROM charging_session""").fetchone()
    print(f"sessions without end: {open_:,}; opened from a meter reading: {partial:,}; chargers with sessions: {chargers}")
    ok = True
    for name, sql in CHECKS.items():
        bad = con.execute(sql).fetchone()[0]
        ok &= bad == 0
        print(f"[{'ok' if bad == 0 else 'FAIL'}] {name}: {bad:,}")
    con.close()
    return ok


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["load", "check"])
    ap.add_argument("--db", type=Path, default=DB_PATH)
    ap.add_argument("--data-dir", type=Path, help="generated CSVs (default data/generated)")
    ap.add_argument("--window-seconds", type=float, default=10, help="simulated seconds per batch, as published")
    ap.add_argument("--chunk", type=int, default=360, help="batches per transaction (360 = 1 simulated hour)")
    args = ap.parse_args(argv)
    if args.command == "load":
        load(args.db, args.data_dir, args.window_seconds, args.chunk)
    elif not check(args.db):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
