"""Replay EV charging CSVs as ordered MQTT batches of simulated time.

    python -m src.MQTT_publishing.publisher --cycles 1
    python -m src.MQTT_publishing.publisher --dry-run

The default maps ten simulated seconds to at least 0.1 real seconds (100x). Source
timestamps are simulation times without timezone offsets and stay unchanged.
"""

import argparse
import csv
import heapq
import json
import math
import threading
import time
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4


MQTT_SERVER = "154.36.178.77"
MQTT_PORT = 1883
TOPIC = "COMP5339/EV_charging"
DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "generated"
MIN_INTERVAL = 0.1
EVENT_PRIORITY = {"price_update": 0, "meter": 1, "session_end": 2, "session_start": 3}
PRICE_FIELDS = ("ts", "station_id", "price_per_kwh", "wholesale_per_kwh")
SESSION_FIELDS = (
    "session_id", "station_id", "connector_id", "site_type", "vehicle_model",
    "start_ts", "charge_end_ts", "end_ts", "charge_minutes", "energy_kwh",
    "avg_kw", "peak_kw", "price_per_kwh", "cost_aud",
)
METER_FIELDS = (
    "ts", "type", "station_id", "connector_id", "session_id", "voltage_v",
    "current_a", "power_kw", "energy_kwh", "soc_pct",
)
START_FIELDS = (
    "session_id", "station_id", "connector_id", "site_type", "vehicle_model",
    "start_ts", "price_per_kwh",
)
NUMBER_FIELDS = {
    "price_per_kwh", "wholesale_per_kwh", "charge_minutes", "energy_kwh",
    "avg_kw", "peak_kw", "cost_aud", "voltage_v", "current_a", "power_kw", "soc_pct",
}


def _read_rows(path, fields):
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        missing = set(fields) - set(reader.fieldnames or ())
        if missing:
            raise ValueError(f"{path}: missing columns {sorted(missing)}")
        for row in reader:
            # Select the documented source columns; do not infer types from IDs.
            typed = {field: row[field] for field in fields}
            for field in NUMBER_FIELDS.intersection(fields):
                value = float(typed[field])
                if not math.isfinite(value):
                    raise ValueError(f"{path}: {field} must be finite")
                typed[field] = value
            if "connector_id" in typed:
                typed["connector_id"] = int(typed["connector_id"])
            yield typed


def _event(row, event_type, ts):
    identity = row.get("session_id", row["station_id"])
    event_id = f"{event_type}:{identity}"
    if event_type in ("price_update", "meter"):
        event_id += f":{ts}"
    return {**row, "event_id": event_id, "type": event_type, "ts": ts}


def _event_key(event):
    return (
        datetime.fromisoformat(event["ts"]), EVENT_PRIORITY[event["type"]],
        event["station_id"], event.get("session_id", ""),
    )


def _price_events(data_dir):
    rows = sorted(
        _read_rows(data_dir / "prices_hourly.csv", PRICE_FIELDS),
        key=lambda row: (datetime.fromisoformat(row["ts"]), row["station_id"]),
    )
    last_price = {}
    for row in rows:
        station, price = row["station_id"], row["price_per_kwh"]
        if station not in last_price or price != last_price[station]:
            yield _event(row, "price_update", row["ts"])
        last_price[station] = price


def _session_events(data_dir):
    events = []
    for row in _read_rows(data_dir / "sessions.csv", SESSION_FIELDS):
        start = {field: row[field] for field in START_FIELDS}
        events.append(_event(start, "session_start", row["start_ts"]))
        events.append(_event(row, "session_end", row["end_ts"]))
    return sorted(events, key=_event_key)


def _meter_events(data_dir):
    # The course generator sorts meters by (ts, session_id). Buffer just one
    # timestamp's readings to also provide deterministic ties for custom files.
    previous_time = None
    simultaneous = []
    for row in _read_rows(data_dir / "session_metering.csv", METER_FIELDS):
        event_time = datetime.fromisoformat(row["ts"])
        if previous_time is not None and event_time < previous_time:
            raise ValueError("session_metering.csv must be ordered by timestamp")
        if previous_time is not None and event_time != previous_time:
            yield from sorted(simultaneous, key=_event_key)
            simultaneous = []
        simultaneous.append(_event(row, "meter", row["ts"]))
        previous_time = event_time
    yield from sorted(simultaneous, key=_event_key)


def iter_events(data_dir=DATA_DIR):
    """Merge all sources in event-time order, filtering unchanged retail prices.

    Prices and sessions are sorted in memory; the much larger metering CSV is
    streamed and must already be chronological, as produced by the generator.
    """
    data_dir = Path(data_dir)
    meters = _meter_events(data_dir)
    try:
        yield from heapq.merge(
            _price_events(data_dir), _session_events(data_dir), meters, key=_event_key,
        )
    finally:
        meters.close()


def iter_batches(data_dir=DATA_DIR, window_seconds=10, replay_id=None):
    """Yield consecutive [window_start, window_end) batches, including empty ones."""
    if not math.isfinite(window_seconds) or window_seconds <= 0:
        raise ValueError("window_seconds must be positive and finite")
    window = timedelta(seconds=window_seconds)
    if not window:
        raise ValueError("The simulated window must be at least one microsecond")
    replay_id = replay_id or str(uuid4())
    events = iter_events(data_dir)
    try:
        event = next(events, None)
        if event is None:
            raise ValueError("No events found in the input CSVs")
        window_start = datetime.fromisoformat(event["ts"])
        sequence = 1
        while event is not None:
            window_end = window_start + window
            batch_events = []
            while event is not None and datetime.fromisoformat(event["ts"]) < window_end:
                batch_events.append(event)
                event = next(events, None)
            yield {
                "replay_id": replay_id, "sequence": sequence,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(), "events": batch_events,
            }
            window_start = window_end
            sequence += 1
    finally:
        events.close()


def _payload(batch):
    return json.dumps(batch, separators=(",", ":"), allow_nan=False)


def dry_run(data_dir=DATA_DIR, window_seconds=10, interval=MIN_INTERVAL):
    """Validate and summarise one full pass without importing MQTT or sleeping."""
    counts = Counter()
    batch_count = max_payload_bytes = 0
    for batch in iter_batches(data_dir, window_seconds):
        counts.update(event["type"] for event in batch["events"])
        batch_count += 1
        max_payload_bytes = max(max_payload_bytes, len(_payload(batch).encode("utf-8")))
    print(f"Dry run: {batch_count:,} batches; {sum(counts.values()):,} events")
    print("Events: " + ", ".join(f"{name}={counts[name]:,}" for name in EVENT_PRIORITY))
    print(f"Minimum replay duration: {batch_count * interval:.1f}s "
          f"({timedelta(seconds=batch_count * interval)}); "
          f"largest payload: {max_payload_bytes:,} bytes")


def publish_events(data_dir=DATA_DIR, server=MQTT_SERVER, topic=TOPIC,
                   interval=MIN_INTERVAL, window_seconds=10, cycles=0, port=MQTT_PORT):
    """Publish acknowledged QoS 1 batches. Zero cycles repeats until interrupted."""
    if not math.isfinite(interval) or interval < MIN_INTERVAL:
        raise ValueError(f"interval must be finite and at least {MIN_INTERVAL} seconds")
    if cycles < 0:
        raise ValueError("cycles must be non-negative (0 means repeat forever)")

    # Import only for live publishing, so inspection and dry runs need no broker
    # or MQTT library. Paho 2's callback version matches the five-argument callback.
    import paho.mqtt.client as mqtt

    connected = threading.Event()
    connection_result = []

    def on_connect(client, userdata, connect_flags, reason_code, properties):
        connection_result.append(reason_code)
        connected.set()

    publisher = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    publisher.on_connect = on_connect
    publisher.connect_timeout = 10
    loop_started = False
    try:
        publisher.connect(server, port, 60)
        publisher.loop_start()
        loop_started = True
        if not connected.wait(timeout=10):
            raise TimeoutError("The MQTT connection was not accepted within 10 seconds")
        if connection_result[0] != 0:
            raise ConnectionError(f"MQTT connection failed: {connection_result[0]}")

        cycle = 0
        next_publish = time.monotonic()
        while cycles == 0 or cycle < cycles:
            cycle += 1
            batch_count = event_count = 0
            batches = iter_batches(data_dir, window_seconds)
            try:
                for batch in batches:
                    payload = _payload(batch)
                    delay = next_publish - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                    message = publisher.publish(topic, payload, qos=1, retain=False)
                    # Never catch up by sending bursts after a slow publish.
                    next_publish = time.monotonic() + interval
                    if message.rc != mqtt.MQTT_ERR_SUCCESS:
                        raise RuntimeError(f"MQTT publish failed: {mqtt.error_string(message.rc)}")
                    message.wait_for_publish(timeout=10)
                    if not message.is_published():
                        raise TimeoutError("The MQTT message was not acknowledged within 10 seconds")
                    batch_count += 1
                    event_count += len(batch["events"])
                    print(f"Replay {cycle}, batch {batch['sequence']}: "
                          f"[{batch['window_start']}, {batch['window_end']}) "
                          f"{len(batch['events']):,} events")
            finally:
                batches.close()
            print(f"Replay {cycle} complete: {batch_count:,} batches, {event_count:,} events")
            # Let the final window elapse too, including at a replay boundary.
            delay = next_publish - time.monotonic()
            if delay > 0:
                time.sleep(delay)
    finally:
        try:
            publisher.disconnect()
        finally:
            if loop_started:
                publisher.loop_stop()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default=MQTT_SERVER)
    parser.add_argument("--port", type=int, default=MQTT_PORT)
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--window-seconds", type=float, default=10,
                        help="simulated seconds per batch (default: 10; 100x speed at --interval 0.1)")
    parser.add_argument("--interval", type=float, default=MIN_INTERVAL,
                        help="minimum real seconds between batches (at least 0.1)")
    parser.add_argument("--cycles", type=int, default=0,
                        help="number of full replays; 0 repeats until interrupted (default)")
    parser.add_argument("--dry-run", action="store_true",
                        help="validate one replay and print totals without publishing or sleeping")
    args = parser.parse_args(argv)
    if not math.isfinite(args.interval) or args.interval < MIN_INTERVAL:
        parser.error("--interval must be finite and at least 0.1 seconds")
    if not math.isfinite(args.window_seconds) or args.window_seconds <= 0:
        parser.error("--window-seconds must be positive and finite")
    if args.cycles < 0:
        parser.error("--cycles must be non-negative")
    try:
        if args.dry_run:
            dry_run(args.data_dir, args.window_seconds, args.interval)
        else:
            publish_events(args.data_dir, args.server, args.topic, args.interval,
                           args.window_seconds, args.cycles, args.port)
    except KeyboardInterrupt:
        print("Publishing stopped.")


if __name__ == "__main__":
    main()
