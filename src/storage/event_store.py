"""Stateful storage of the MQTT event stream in DuckDB. Task 4.

Output: the Task 4 tables in data/processed/ev_charging.duckdb (DDL: sql/events.sql), next
to the corrected Assignment 1 tables they reference.

The MQTT thread must never wait on the database, so `submit` only queues the raw message
payload. A writer thread commits everything queued once a second in a single transaction.
The batches are parsed and split by event type inside DuckDB (set-based INSERT ... SELECT
over the JSON); row-by-row executemany measured ~2.5 ms per row, far too slow for 2.3M meter rows.

Idempotent: every table is keyed on the events' natural keys, so a QoS 1 redelivery, a
second replay or a re-run inserts nothing twice. Dash callbacks read through `cursor()`;
DuckDB connections from one process share the database, so readers see each committed
flush and never a half-written one.
"""
import logging
import queue
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "data" / "processed" / "ev_charging.duckdb"
DDL = ROOT / "sql" / "events.sql"

log = logging.getLogger(__name__)

# Shape of one published batch (README, Task 2). Events stay JSON here and are typed per
# event type below, because the four types carry different fields.
BATCH_TYPE = ('{"received_at": "TIMESTAMPTZ", "batch": {"replay_id": "VARCHAR", "sequence": "BIGINT", '
              '"window_start": "TIMESTAMP", "window_end": "TIMESTAMP", "events": ["JSON"]}}')

# Run in order, in one transaction, after the queued payloads are bound as one JSON array.
INGEST = [
    # 1. Unpack: one row per batch, then one row per event with its fields extracted.
    f"""CREATE OR REPLACE TEMP TABLE _batch AS
        SELECT m.received_at, unnest(m.batch) FROM (SELECT unnest(from_json($payloads::JSON, '[{BATCH_TYPE}]')) AS m)""",
    """CREATE OR REPLACE TEMP TABLE _event AS
       SELECT DISTINCT ON (event_id) * FROM (
           SELECT b.replay_id, b.sequence, e->>'event_id' AS event_id, e->>'type' AS type,
                  e->>'station_id' AS station_id, (e->>'ts')::TIMESTAMP AS ts, e->>'session_id' AS session_id,
                  (e->>'connector_id')::INTEGER AS connector_id, e AS payload,
                  c.charger_record_id IS NOT NULL AS known_charger
           FROM (SELECT replay_id, sequence, unnest(events) AS e FROM _batch) b
           LEFT JOIN charger c ON c.charger_record_id = (b.e->>'station_id'))
       ORDER BY replay_id, sequence""",
    # 2. Stream audit (one row per MQTT message).
    """INSERT OR IGNORE INTO stream_batch
       SELECT DISTINCT ON (replay_id, sequence) replay_id, sequence, window_start, window_end,
              len(events), received_at
       FROM _batch""",
    # 3. Events that do not match an Assignment 1 charger are set aside, not dropped.
    """INSERT OR IGNORE INTO unmatched_event
       SELECT event_id, type, station_id, ts, payload, replay_id FROM _event WHERE NOT known_charger""",
    # 4. Prices.
    """INSERT OR IGNORE INTO price_event
       SELECT station_id, ts, (payload->>'price_per_kwh')::DOUBLE, (payload->>'wholesale_per_kwh')::DOUBLE, replay_id
       FROM _event WHERE known_charger AND type = 'price_update'""",
    # 5. Sessions: start creates the row; a start arriving after a meter-opened partial row fills it in.
    """INSERT INTO charging_session (session_id, station_id, connector_id, site_type, vehicle_model,
                                    start_ts, price_per_kwh, start_seen, replay_id)
       SELECT session_id, station_id, connector_id, payload->>'site_type', payload->>'vehicle_model',
              (payload->>'start_ts')::TIMESTAMP, (payload->>'price_per_kwh')::DOUBLE, true, replay_id
       FROM _event WHERE known_charger AND type = 'session_start'
       ON CONFLICT (session_id) DO UPDATE SET
           site_type = excluded.site_type, vehicle_model = excluded.vehicle_model,
           start_ts = excluded.start_ts, price_per_kwh = excluded.price_per_kwh, start_seen = true
       WHERE NOT charging_session.start_seen""",
    # 6. session_end carries the full session row, so it completes (or creates) it.
    """INSERT INTO charging_session
       SELECT session_id, station_id, connector_id, payload->>'site_type', payload->>'vehicle_model',
              (payload->>'start_ts')::TIMESTAMP, (payload->>'charge_end_ts')::TIMESTAMP,
              (payload->>'end_ts')::TIMESTAMP, (payload->>'charge_minutes')::DOUBLE,
              (payload->>'energy_kwh')::DOUBLE, (payload->>'avg_kw')::DOUBLE, (payload->>'peak_kw')::DOUBLE,
              (payload->>'price_per_kwh')::DOUBLE, (payload->>'cost_aud')::DOUBLE, true, replay_id
       FROM _event WHERE known_charger AND type = 'session_end'
       ON CONFLICT (session_id) DO UPDATE SET
           site_type = excluded.site_type, vehicle_model = excluded.vehicle_model,
           start_ts = excluded.start_ts, charge_end_ts = excluded.charge_end_ts, end_ts = excluded.end_ts,
           charge_minutes = excluded.charge_minutes, energy_kwh = excluded.energy_kwh,
           avg_kw = excluded.avg_kw, peak_kw = excluded.peak_kw,
           price_per_kwh = excluded.price_per_kwh, cost_aud = excluded.cost_aud, start_seen = true""",
    # 7. A meter reading for a session never seen to start (subscriber joined mid-session)
    #    opens a partial row, so the reading still has a session to belong to.
    """INSERT OR IGNORE INTO charging_session (session_id, station_id, connector_id, start_ts, start_seen, replay_id)
       SELECT session_id, any_value(station_id), any_value(connector_id), min(ts), false, any_value(replay_id)
       FROM _event WHERE known_charger AND type = 'meter'
       GROUP BY session_id""",
    # 8. Meter readings, skipping any already stored. $lo/$hi bound the readings in this
    #    flush, so the existing-row scan only touches row groups whose ts zone map overlaps.
    """INSERT INTO meter_event
       SELECT e.session_id, e.ts, (payload->>'voltage_v')::REAL, (payload->>'current_a')::REAL,
              (payload->>'power_kw')::REAL, (payload->>'energy_kwh')::REAL, (payload->>'soc_pct')::REAL
       FROM _event e
       ANTI JOIN (SELECT session_id, ts FROM meter_event WHERE ts BETWEEN $lo AND $hi) m
            ON m.session_id = e.session_id AND m.ts = e.ts
       WHERE e.known_charger AND e.type = 'meter'""",
]


def init_db(con: duckdb.DuckDBPyConnection) -> None:
    """Create the Task 4 tables and macros if missing (sql/events.sql is idempotent)."""
    # Assignment 1 put RTREE indexes on charger/sa4_region geometries: without the spatial
    # extension loaded, DuckDB cannot checkpoint (write the WAL into) this database file.
    con.execute("LOAD spatial")  # installed by `python main.py corrections`
    if not con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'charger'").fetchone()[0]:
        raise RuntimeError("charger table missing; run `python main.py corrections` first")
    con.execute(DDL.read_text())


def ingest(con: duckdb.DuckDBPyConnection, payloads: list[str]) -> tuple[str, datetime]:
    """Store raw MQTT payloads in one transaction. Each item is a wrapped JSON object
    {"received_at": ..., "batch": <payload>} (see `wrap`). Returns the replay_id and
    window_end of the last batch: the simulated time the stored data now reaches."""
    con.execute("BEGIN")
    try:
        for sql in INGEST:
            if "$payloads" in sql:
                con.execute(sql, {"payloads": "[" + ",".join(payloads) + "]"})
            elif "$lo" in sql:
                lo, hi = con.execute("SELECT min(ts), max(ts) FROM _event WHERE type = 'meter'").fetchone()
                if lo is not None:
                    con.execute(sql, {"lo": lo, "hi": hi})
            else:
                con.execute(sql)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return con.execute("SELECT replay_id, window_end FROM _batch ORDER BY received_at DESC, sequence DESC LIMIT 1").fetchone()


def wrap(payload: bytes | str, received_at: datetime | None = None) -> str:
    """Attach the receive time to a raw payload without re-serialising it."""
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    received_at = received_at or datetime.now(timezone.utc)
    return f'{{"received_at":"{received_at.isoformat()}","batch":{payload}}}'


class EventStore:
    """Owns the read-write connection; buffers payloads and flushes them on a thread."""

    def __init__(self, db_path: Path = DB_PATH, flush_interval: float = 1.0):
        if not Path(db_path).exists():
            raise FileNotFoundError(f"{db_path} not found; run `python main.py corrections` first")
        self.con = duckdb.connect(str(db_path))
        init_db(self.con)
        # A DuckDB connection object must not be used by two threads at once: the writer
        # gets its own, and readers each take a fresh cursor (created under a lock).
        self._writer = self.con.cursor()
        self._cursor_lock = threading.Lock()
        self.flush_interval = flush_interval
        self._queue: queue.Queue[str] = queue.Queue()
        self._stop = threading.Event()
        self.stored_batches = 0
        self.failed_batches = 0
        self.replay_id: str | None = None   # replay of the last stored batch
        self.clock: datetime | None = None  # its window_end: metrics are computed as of this
        self.last_flush_ms = 0.0
        self._thread = threading.Thread(target=self._run, name="event-store", daemon=True)
        self._thread.start()

    def submit(self, payload: bytes | str) -> None:
        """Called from the MQTT thread with a message that parsed as JSON."""
        self._queue.put(wrap(payload))

    def cursor(self) -> duckdb.DuckDBPyConnection:
        """A connection for one reader thread (Dash callback)."""
        with self._cursor_lock:
            return self.con.cursor()

    def _drain(self) -> list[str]:
        items = []
        while True:
            try:
                items.append(self._queue.get_nowait())
            except queue.Empty:
                return items

    def flush(self) -> None:
        items = self._drain()
        if not items:
            return
        start = time.perf_counter()
        try:
            self.replay_id, self.clock = ingest(self._writer, items)
            self.stored_batches += len(items)
        except duckdb.Error as exc:
            # One bad message must not lose the others: retry them one at a time.
            log.warning("Batch insert failed (%s); retrying %d messages singly", exc, len(items))
            for item in items:
                try:
                    self.replay_id, self.clock = ingest(self._writer, [item])
                    self.stored_batches += 1
                except duckdb.Error as exc_one:
                    self.failed_batches += 1
                    log.warning("Skipped message the store rejected: %s", exc_one)
        self.last_flush_ms = (time.perf_counter() - start) * 1000

    def _run(self) -> None:
        while not self._stop.wait(self.flush_interval):
            self.flush()

    def close(self) -> None:
        self._stop.set()
        self._thread.join()
        self.flush()  # whatever arrived after the last tick
        self._writer.close()
        self.con.execute("CHECKPOINT")
        self.con.close()
