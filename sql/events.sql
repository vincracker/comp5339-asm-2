-- Assignment 2 schema extension 2 (Task 4): MQTT event store.
-- Applied to data/processed/ev_charging.duckdb by src/storage/event_store.py when the
-- dashboard starts (every statement is IF NOT EXISTS / OR REPLACE, so re-running is safe).
--
-- price_event and charging_session reference the Assignment 1 `charger` table through
-- station_id, which the generator copied from charger.charger_record_id
-- (src/generation/stations.py); meter_event links to charger through its session. Events
-- naming an unknown charger are kept in unmatched_event instead of being dropped silently.
--
-- Keys are the natural keys of the events, so a QoS 1 redelivery or a second replay of
-- the same data inserts nothing (INSERT OR IGNORE / ON CONFLICT). The MQTT event_id is
-- derivable from them: price_update:<station_id>:<ts>, session_start|session_end:<session_id>,
-- meter:<session_id>:<ts>.
--
-- Physical design (see README, Task 4):
--   * Events arrive ordered by simulated time and are appended in that order, so DuckDB's
--     per-row-group min/max zone maps prune the time-window filters (ts, start_ts, end_ts)
--     the dashboard uses without an extra index.
--   * The small, frequently queried tables keep their keys (each backed by an ART index):
--     charging_session (76k rows/replay) and price_event (101k rows/replay).
--   * meter_event (2.3M rows/replay, write-heavy, read one session at a time) carries no
--     index at all; see its comment.
--   * station_id / connector_id are stored once per session, not on every meter reading
--     (they depend on session_id only).

-- One row per MQTT message (a batch of simulated time). Audit trail for the stream:
-- duplicates, gaps (missing sequence numbers) and lag (received_at vs window_end).
CREATE TABLE IF NOT EXISTS stream_batch (
    replay_id    VARCHAR   NOT NULL,
    sequence     BIGINT    NOT NULL CHECK (sequence >= 1),
    window_start TIMESTAMP NOT NULL,
    window_end   TIMESTAMP NOT NULL,
    event_count  INTEGER   NOT NULL CHECK (event_count >= 0),
    received_at  TIMESTAMPTZ NOT NULL,          -- wall clock when the subscriber got it
    PRIMARY KEY (replay_id, sequence),
    CHECK (window_end > window_start)
);

-- Retail price changes. Only actual changes are published (Task 2), so a charger's price at
-- time t is the latest row with ts <= t.
CREATE TABLE IF NOT EXISTS price_event (
    station_id        VARCHAR   NOT NULL REFERENCES charger(charger_record_id),
    ts                TIMESTAMP NOT NULL,
    price_per_kwh     DECIMAL(6, 4) NOT NULL CHECK (price_per_kwh >= 0),   -- AUD/kWh
    wholesale_per_kwh DECIMAL(8, 5),                                     -- AUD/kWh, may be < 0
    replay_id         VARCHAR   NOT NULL,       -- replay in which it was first stored
    PRIMARY KEY (station_id, ts)
);

-- One row per charging session, built up by its events: session_start creates it,
-- session_end completes it (upsert), and a meter reading for a session whose start was
-- missed (subscriber joined mid-session) creates a partial row that later events fill in.
CREATE TABLE IF NOT EXISTS charging_session (
    session_id     VARCHAR   PRIMARY KEY,
    station_id     VARCHAR   NOT NULL REFERENCES charger(charger_record_id),
    connector_id   INTEGER   NOT NULL CHECK (connector_id >= 1),
    site_type      VARCHAR   CHECK (site_type IN ('urban', 'regional')),
    vehicle_model  VARCHAR,
    start_ts       TIMESTAMP NOT NULL,          -- first meter ts if the start was missed
    charge_end_ts  TIMESTAMP,                   -- NULL until session_end
    end_ts         TIMESTAMP,                   -- NULL while the session is open
    charge_minutes DOUBLE    CHECK (charge_minutes >= 0),
    energy_kwh     DOUBLE    CHECK (energy_kwh >= 0),
    avg_kw         DOUBLE    CHECK (avg_kw >= 0),
    peak_kw        DOUBLE    CHECK (peak_kw >= 0),
    price_per_kwh  DECIMAL(6, 4) CHECK (price_per_kwh >= 0),
    cost_aud       DECIMAL(10, 2) CHECK (cost_aud >= 0),
    start_seen     BOOLEAN   NOT NULL,          -- false: opened from a meter reading
    replay_id      VARCHAR   NOT NULL,
    CHECK (end_ts IS NULL OR end_ts >= start_ts),
    CHECK ((end_ts IS NULL) = (energy_kwh IS NULL))
);

-- Per-minute meter readings: the session progress curves. energy_kwh is cumulative.
-- Deliberately WITHOUT primary/foreign keys: DuckDB backs every key with an ART index, and
-- on ~2.3M rows per replay those indexes measured ~250 MB of a 366 MB database (8x the table
-- data). Uniqueness of (session_id, ts) is enforced at ingest instead (anti-join limited to
-- the flush's time range, so zone maps skip all older row groups), the parent session row is
-- always written first, and `python -m src.storage check` verifies both.
CREATE TABLE IF NOT EXISTS meter_event (
    session_id   VARCHAR   NOT NULL,            -- -> charging_session.session_id
    ts           TIMESTAMP NOT NULL,
    voltage_v    REAL,
    current_a    REAL,
    power_kw     REAL      CHECK (power_kw >= 0),
    energy_kwh   REAL      CHECK (energy_kwh >= 0),
    soc_pct      REAL      CHECK (soc_pct BETWEEN 0 AND 100)
);

-- Events whose station_id is not an Assignment 1 charger (should stay empty).
CREATE TABLE IF NOT EXISTS unmatched_event (
    event_id   VARCHAR PRIMARY KEY,
    type       VARCHAR NOT NULL,
    station_id VARCHAR,
    ts         TIMESTAMP,
    payload    JSON    NOT NULL,
    replay_id  VARCHAR NOT NULL
);

-- ---- Analytics, parameterised by the simulated time t ("as of t") ----------------------
-- The stream can be replayed (each pass restarts on 1 October), so every metric is computed
-- as of the simulated clock rather than from "the latest rows".

-- Current state of every charger at t: price in force, open sessions, idle time.
-- A session with no session_end for 6 simulated hours lost it (longest session is 3.3 h).
CREATE OR REPLACE MACRO charger_state_at(t) AS TABLE
WITH price AS (
    SELECT station_id, arg_max(price_per_kwh, ts) AS price_per_kwh, max(ts) AS price_since
    FROM price_event WHERE ts <= t GROUP BY station_id
), sess AS (
    SELECT station_id,
           count(*) FILTER (WHERE start_ts <= t AND (end_ts > t OR (end_ts IS NULL AND start_ts > t - INTERVAL 6 HOUR))) AS active_sessions,
           max(end_ts) FILTER (WHERE end_ts <= t) AS last_session_end
    FROM charging_session WHERE start_ts <= t GROUP BY station_id
)
SELECT c.charger_record_id AS station_id, c.number_of_plugs AS connectors,
       p.price_per_kwh, p.price_since,
       coalesce(s.active_sessions, 0) AS active_sessions,
       s.last_session_end
FROM charger c
LEFT JOIN price p ON p.station_id = c.charger_record_id
LEFT JOIN sess s ON s.station_id = c.charger_record_id
WHERE c.charger_type = 'DC';

-- Usage of every charger in the window (t0, t1]: completed sessions, energy, revenue, mean
-- duration and utilisation (share of connector-time occupied, counting the overlap of every
-- session with the window, including ones still open at t1).
CREATE OR REPLACE MACRO charger_usage(t0, t1) AS TABLE
SELECT s.station_id,
       count(*) FILTER (WHERE s.end_ts > t0 AND s.end_ts <= t1) AS sessions_completed,
       sum(s.energy_kwh) FILTER (WHERE s.end_ts > t0 AND s.end_ts <= t1) AS energy_kwh,
       sum(s.cost_aud) FILTER (WHERE s.end_ts > t0 AND s.end_ts <= t1) AS revenue_aud,
       avg(s.charge_minutes) FILTER (WHERE s.end_ts > t0 AND s.end_ts <= t1) AS avg_charge_minutes,
       sum(epoch(least(coalesce(s.end_ts, t1), t1) - greatest(s.start_ts, t0)))
           / (epoch(t1 - t0) * any_value(c.number_of_plugs)) AS utilisation
FROM charging_session s JOIN charger c ON c.charger_record_id = s.station_id
WHERE s.start_ts < t1
  AND (s.end_ts > t0 OR (s.end_ts IS NULL AND s.start_ts > t1 - INTERVAL 6 HOUR))
GROUP BY s.station_id;
