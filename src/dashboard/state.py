"""In-memory live state built from the MQTT batches published in Task 2. Task 3.

The MQTT thread calls `apply_batch`; Dash callbacks read through the snapshot methods.
One lock guards everything, and snapshots return plain copies, so callbacks never see a
half-applied batch.

Stream rules (see the Task 2 section of the README):
  * Batches arrive in order on one topic. A QoS 1 redelivery repeats a (replay_id, sequence)
    already applied, so any sequence <= the last applied one is skipped.
  * A new replay_id means the publisher restarted from the first timestamp. The state is
    cleared, so the map shows that pass only and never mixes two passes.
  * The dashboard may connect mid-replay. A meter reading for a session it never saw start
    opens that session from the reading, flagged `joined_mid_session`.
"""
import threading
from datetime import datetime, timedelta

# A session longer than this past its last update lost its session_end (e.g. a broker
# disconnect). The longest generated session is 3.3 h, so 6 h never drops a real one.
STALE_AFTER = timedelta(hours=6)


class LiveState:
    def __init__(self, chargers: dict[str, dict]):
        self.chargers = chargers
        self._lock = threading.Lock()
        self.connected = False
        self.connection_note = "connecting"
        self.duplicates = 0
        self._reset(None)

    def _reset(self, replay_id: str | None) -> None:
        self.replay_id = replay_id
        self.last_sequence = 0
        self.gaps = 0               # batches missed while disconnected (sequence jumps)
        self.clock = None           # simulated time: window_end of the last batch
        self.batches = 0
        self.events = 0
        self.unknown_events = 0     # events for a station_id not in Assignment 1
        self.stations: dict[str, dict] = {}   # chargers that have appeared on the map
        self.sessions: dict[str, dict] = {}   # active sessions by session_id
        self.version = getattr(self, "version", 0) + 1

    # ---- writes (MQTT thread) -------------------------------------------------------------

    def set_connection(self, connected: bool, note: str) -> None:
        with self._lock:
            self.connected, self.connection_note = connected, note
            self.version += 1

    def apply_batch(self, batch: dict) -> bool:
        """Apply one published batch. Returns False if it was a duplicate."""
        with self._lock:
            if batch["replay_id"] != self.replay_id:
                self._reset(batch["replay_id"])
            elif batch["sequence"] <= self.last_sequence:
                self.duplicates += 1
                return False
            if self.last_sequence and batch["sequence"] != self.last_sequence + 1:
                self.gaps += batch["sequence"] - self.last_sequence - 1
            self.last_sequence = batch["sequence"]
            for event in batch["events"]:
                self._apply(event)
            self.clock = datetime.fromisoformat(batch["window_end"])
            self._drop_stale()
            self.batches += 1
            self.events += len(batch["events"])
            self.version += 1
            return True

    def _station(self, station_id: str, ts: str) -> dict | None:
        if station_id not in self.chargers:
            self.unknown_events += 1
            return None
        if station_id not in self.stations:  # first event: the marker is added to the map
            self.stations[station_id] = {
                "first_seen": ts, "price": None, "wholesale": None, "price_ts": None,
                "previous_price": None, "active": set(), "completed": 0, "last_session": None,
            }
        return self.stations[station_id]

    def _apply(self, e: dict) -> None:
        station = self._station(e["station_id"], e["ts"])
        if station is None:
            return
        kind = e["type"]
        if kind == "price_update":
            station["previous_price"] = station["price"]
            station.update(price=e["price_per_kwh"], wholesale=e["wholesale_per_kwh"], price_ts=e["ts"])
        elif kind == "session_start":
            self._open_session(station, e, start_ts=e["start_ts"], joined_mid=False)
            self.sessions[e["session_id"]]["price"] = e["price_per_kwh"]
        elif kind == "meter":
            session = self.sessions.get(e["session_id"])
            if session is None:
                session = self._open_session(station, e, start_ts=None, joined_mid=True)
            session["readings"].append({
                "ts": e["ts"], "energy_kwh": e["energy_kwh"], "power_kw": e["power_kw"],
                "soc_pct": e["soc_pct"], "voltage_v": e["voltage_v"], "current_a": e["current_a"],
            })
            session["updated"] = e["ts"]
        elif kind == "session_end":
            self.sessions.pop(e["session_id"], None)
            station["active"].discard(e["session_id"])
            station["completed"] += 1
            station["last_session"] = {k: e[k] for k in (
                "session_id", "connector_id", "vehicle_model", "start_ts", "end_ts",
                "charge_minutes", "energy_kwh", "avg_kw", "peak_kw", "price_per_kwh", "cost_aud")}

    def _open_session(self, station: dict, e: dict, start_ts: str | None, joined_mid: bool) -> dict:
        # A connector holds one session at a time, so a new one replaces any it still shows.
        for sid in [s for s in station["active"] if self.sessions[s]["connector_id"] == e["connector_id"]]:
            station["active"].discard(sid)
            self.sessions.pop(sid)
        session = {
            "session_id": e["session_id"], "station_id": e["station_id"],
            "connector_id": e["connector_id"], "vehicle_model": e.get("vehicle_model"),
            "start_ts": start_ts or e["ts"], "price": station["price"],
            "joined_mid_session": joined_mid, "readings": [], "updated": e["ts"],
        }
        self.sessions[e["session_id"]] = session
        station["active"].add(e["session_id"])
        return session

    def _drop_stale(self) -> None:
        for sid, s in list(self.sessions.items()):
            if self.clock - datetime.fromisoformat(s["updated"]) > STALE_AFTER:
                self.sessions.pop(sid)
                self.stations[s["station_id"]]["active"].discard(sid)

    # ---- reads (Dash callbacks) -----------------------------------------------------------

    def summary(self) -> dict:
        with self._lock:
            return {
                "version": self.version, "connected": self.connected, "note": self.connection_note,
                "replay_id": self.replay_id, "clock": self.clock.isoformat() if self.clock else None,
                "batches": self.batches, "events": self.events, "gaps": self.gaps,
                "duplicates": self.duplicates, "unknown_events": self.unknown_events,
                "on_map": len(self.stations), "total_chargers": len(self.chargers),
                "active_sessions": len(self.sessions),
                "busy_chargers": sum(1 for s in self.stations.values() if s["active"]),
            }

    def markers(self) -> list[dict]:
        """One entry per charger seen so far, with the values the marker displays."""
        with self._lock:
            out = []
            for sid, s in self.stations.items():
                c = self.chargers[sid]
                in_use = len(s["active"])
                out.append({
                    "station_id": sid, "lat": c["lat"], "lon": c["lon"], "name": c["name"],
                    "operator": c["operator"], "connectors": c["connectors"], "in_use": in_use,
                    "price": s["price"], "status": status(in_use, c["connectors"], s["price"]),
                })
            return out

    def station_detail(self, station_id: str) -> dict | None:
        with self._lock:
            s = self.stations.get(station_id)
            if s is None:
                return None
            sessions = sorted((self.sessions[sid] for sid in s["active"]), key=lambda x: x["connector_id"])
            return {
                **self.chargers[station_id],
                **{k: v for k, v in s.items() if k != "active"},
                "in_use": len(sessions), "clock": self.clock.isoformat() if self.clock else None,
                "sessions": [{**x, "readings": list(x["readings"])} for x in sessions],
            }


def status(in_use: int, connectors: int, price: float | None) -> str:
    if in_use >= connectors:
        return "full"
    if in_use:
        return "busy"
    return "free" if price is not None else "unknown"
