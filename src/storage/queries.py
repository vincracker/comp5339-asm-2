"""Dashboard metrics derived from the stored events. Task 4.

Every query takes the simulated clock `t` (the last stored window_end of the current replay),
so the answers are the same whenever the replay is watched, and a second replay of the
same data shows what the first did at that point rather than its totals so far. The table
macros charger_state_at(t) and charger_usage(t0, t1) are defined in sql/events.sql.

Typical latency on a full 14-day replay (76k sessions, 2.3M meter readings): a few
milliseconds per call, see README (Task 4).
"""
from datetime import datetime, timedelta

import duckdb

DAY, WEEK = timedelta(days=1), timedelta(days=7)


def _row(cur: duckdb.DuckDBPyConnection, sql: str, params: list) -> dict | None:
    cur.execute(sql, params)
    row = cur.fetchone()
    return dict(zip([d[0] for d in cur.description], row)) if row else None


def charger_current(cur, station_id: str, t: datetime) -> dict | None:
    """Current state (as of t): price in force and since when, open sessions, idle time."""
    return _row(cur, "FROM charger_state_at(?::TIMESTAMP) WHERE station_id = ?", [t, station_id])


def charger_history(cur, station_id: str, t: datetime) -> dict:
    """Historical usage over the last 24 h and 7 days before t, plus the 24 h price range.

    A window reaching back before the first stored batch starts at that batch instead, so a
    7-day utilisation is not diluted by days the subscriber never saw (`<label>_from`)."""
    cur.execute("SELECT min(window_start) FROM stream_batch")
    recorded_from = cur.fetchone()[0]
    out = {}
    for label, span in (("day", DAY), ("week", WEEK)):
        t0 = max(t - span, recorded_from) if recorded_from else t - span
        out[label + "_from"] = t0
        out[label] = _row(cur, "FROM charger_usage(?::TIMESTAMP, ?::TIMESTAMP) WHERE station_id = ?",
                          [t0, t, station_id]) or {"sessions_completed": 0}
    # The price in force at the window start counts too, so take the last change before it.
    out["price_range"] = _row(cur, """
        SELECT min(price_per_kwh) AS low, max(price_per_kwh) AS high FROM price_event
        WHERE station_id = $sid AND ts <= $t
          AND ts >= coalesce((SELECT max(ts) FROM price_event WHERE station_id = $sid AND ts <= $t0), $t0)
    """, {"sid": station_id, "t": t, "t0": t - DAY})
    return out


def charger_hourly_utilisation(cur, station_id: str, t: datetime) -> list[tuple[datetime, float]]:
    """Share of connector-time in use for each of the 24 hours up to t (current hour partial)."""
    cur.execute("""
        WITH hour AS (
            SELECT unnest(generate_series(date_trunc('hour', $t::TIMESTAMP - INTERVAL 1 MICROSECOND) - INTERVAL 23 HOUR,
                                          date_trunc('hour', $t::TIMESTAMP - INTERVAL 1 MICROSECOND), INTERVAL 1 HOUR)) AS h0
        ), s AS (
            SELECT start_ts, coalesce(least(end_ts, $t), $t) AS end_ts FROM charging_session
            WHERE station_id = $sid AND start_ts < $t AND (end_ts > $t - INTERVAL 24 HOUR
                  OR (end_ts IS NULL AND start_ts > $t - INTERVAL 6 HOUR))
        )
        -- FILTER: least/greatest skip NULLs, so an hour with no session would count as full.
        SELECT h0, coalesce(sum(epoch(least(s.end_ts, h0 + INTERVAL 1 HOUR) - greatest(s.start_ts, h0)))
                            FILTER (WHERE s.start_ts IS NOT NULL), 0)
                   / (epoch(least(h0 + INTERVAL 1 HOUR, $t) - h0) * $plugs) AS utilisation
        FROM hour LEFT JOIN s ON s.start_ts < h0 + INTERVAL 1 HOUR AND s.end_ts > h0
        WHERE h0 < $t
        GROUP BY h0 ORDER BY h0
    """, {"sid": station_id, "t": t, "plugs": _plugs(cur, station_id)})
    return cur.fetchall()


def network_summary(cur, t: datetime) -> dict:
    """Whole network: sessions open at t (current state) and the last 24 h of usage."""
    current = _row(cur, """
        SELECT sum(active_sessions)::INTEGER AS active_sessions,
               count(*) FILTER (WHERE active_sessions > 0) AS busy_chargers
        FROM charger_state_at(?::TIMESTAMP)""", [t])
    day = _row(cur, """
        SELECT coalesce(sum(sessions_completed), 0)::INTEGER AS sessions, coalesce(sum(energy_kwh), 0) AS energy_kwh,
               coalesce(sum(revenue_aud), 0) AS revenue_aud
        FROM charger_usage(?::TIMESTAMP, ?::TIMESTAMP)""", [t - DAY, t])
    return {**current, **day}


def stored_counts(cur) -> dict:
    """MQTT messages and events received (each message once), across all replays."""
    return _row(cur, "SELECT count(*) AS batches, coalesce(sum(event_count), 0)::BIGINT AS events FROM stream_batch", [])


def _plugs(cur, station_id: str) -> int:
    cur.execute("SELECT number_of_plugs FROM charger WHERE charger_record_id = ?", [station_id])
    return cur.fetchone()[0]
