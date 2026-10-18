"""Check MQTT batches live, or check example.json offline with --file PATH."""

import argparse
import hashlib
import json
import math
from collections import Counter
from datetime import datetime
from pathlib import Path

if __package__:
    from . import publisher
else:
    import publisher


EVENT_FIELDS = {
    "price_update": publisher.PRICE_FIELDS,
    "session_start": publisher.START_FIELDS,
    "meter": publisher.METER_FIELDS,
    "session_end": publisher.SESSION_FIELDS,
}


def _timestamp(value):
    if not isinstance(value, str):
        raise ValueError("Timestamps must be ISO strings")
    result = datetime.fromisoformat(value)
    if result.tzinfo is not None:
        raise ValueError("Expected source simulation timestamps without UTC offsets")
    return result


class StreamVerifier:
    """Keep checks bounded to the current replay; the first batch may be midstream."""

    def __init__(self):
        self.replay_id = None
        self.batches = self.duplicates = 0
        self.counts = Counter()

    def check(self, batch):
        if not isinstance(batch, dict):
            raise ValueError("Each MQTT message must contain a JSON object")
        replay_id, sequence = batch.get("replay_id"), batch.get("sequence")
        if not isinstance(replay_id, str) or not replay_id:
            raise ValueError("replay_id must be a nonempty string")
        if type(sequence) is not int or sequence < 1:
            raise ValueError("sequence must be a positive integer")
        if not isinstance(batch.get("events"), list):
            raise ValueError("events must be an array")
        start, end = _timestamp(batch.get("window_start")), _timestamp(batch.get("window_end"))
        if start >= end:
            raise ValueError("window_start must precede window_end")
        fingerprint = hashlib.sha256(json.dumps(batch, sort_keys=True, allow_nan=False).encode()).digest()

        if replay_id != self.replay_id:
            if self.replay_id is not None and sequence != 1:
                raise ValueError("A new replay must start at sequence 1")
            self.replay_id = replay_id
            self.sequence, self.window_end, self.last_key = None, None, None
            self.window_size = end - start
            self.seen, self.prices, self.meters = {}, {}, {}
        if sequence in self.seen:
            if fingerprint != self.seen[sequence]:
                raise ValueError("A repeated batch sequence contains different data")
            self.duplicates += 1
            return False
        if self.sequence is not None:
            if sequence != self.sequence + 1:
                raise ValueError("Missing or out-of-order batch sequence")
            if start != self.window_end or end - start != self.window_size:
                raise ValueError("Batch windows must be consecutive and have a fixed duration")

        event_ids = set()
        for event in batch["events"]:
            if (not isinstance(event, dict) or not isinstance(event.get("type"), str)
                    or event["type"] not in EVENT_FIELDS):
                raise ValueError("Unknown or missing event type")
            kind = event["type"]
            fields = set(EVENT_FIELDS[kind]) | {"event_id", "type", "ts", "station_id"}
            for field in fields:
                value = event.get(field)
                if field == "connector_id":
                    valid = type(value) is int and value > 0
                elif field in publisher.NUMBER_FIELDS:
                    valid = type(value) is int or (type(value) is float and math.isfinite(value))
                else:
                    valid = isinstance(value, str) and bool(value)
                if not valid:
                    raise ValueError(f"{kind}: missing or invalid {field}")
                if field == "ts" or field.endswith("_ts"):
                    _timestamp(value)
            ts = _timestamp(event["ts"])
            if not start <= ts < end:
                raise ValueError("Event timestamp is outside its batch window")
            key = (ts, publisher.EVENT_PRIORITY[kind], event["station_id"], event.get("session_id", ""))
            if self.last_key is not None and key < self.last_key:
                raise ValueError("Events are out of order")
            if event["event_id"] in event_ids:
                raise ValueError("Duplicate event_id within a batch")
            event_ids.add(event["event_id"])
            self.last_key = key
            if kind == "price_update":
                station, price = event["station_id"], event["price_per_kwh"]
                if station in self.prices and self.prices[station] == price:
                    raise ValueError("price_update repeats an unchanged retail price")
                self.prices[station] = price
            elif kind == "meter":
                session, energy = event["session_id"], event["energy_kwh"]
                previous = self.meters.get(session)
                if previous is not None and (ts <= previous[0] or energy < previous[1]):
                    raise ValueError("Meter timestamps must increase and cumulative energy must not decrease")
                self.meters[session] = (ts, energy)
            elif ts != _timestamp(event["start_ts" if kind == "session_start" else "end_ts"]):
                raise ValueError(f"{kind}: ts does not match its source timestamp")

        self.seen[sequence] = fingerprint
        self.sequence, self.window_end = sequence, end
        self.batches += 1
        self.counts.update(event["type"] for event in batch["events"])
        return True


def listen(verifier, server, port, topic, messages):
    import paho.mqtt.client as mqtt

    failure = []

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code != 0:
            failure.append(f"MQTT connection failed: {reason_code}")
            client.disconnect()
            return
        result, _ = client.subscribe(topic, qos=1)
        if result != mqtt.MQTT_ERR_SUCCESS:
            failure.append(f"MQTT subscription failed: {mqtt.error_string(result)}")
            client.disconnect()

    def on_subscribe(client, userdata, mid, reason_codes, properties):
        if any(reason.is_failure for reason in reason_codes):
            failure.append("The MQTT broker rejected the subscription")
            client.disconnect()
        else:
            print(f"Subscribed to {topic}")

    def on_message(client, userdata, message):
        try:
            batch = json.loads(message.payload)
            if verifier.check(batch):
                print(f"OK {batch['replay_id']} batch {batch['sequence']}: "
                      f"{len(batch['events'])} events, "
                      f"[{batch['window_start']}, {batch['window_end']})")
            else:
                print(f"Duplicate batch {batch['sequence']} skipped")
            if messages and verifier.batches >= messages:
                client.disconnect()
        except (ValueError, TypeError) as error:
            failure.append(str(error))
            client.disconnect()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect, client.on_message = on_connect, on_message
    client.on_subscribe = on_subscribe
    client.connect_timeout = 10
    try:
        client.connect(server, port, 60)
        print(f"Listening on {server}:{port}, topic {topic}; Ctrl+C stops verification")
        client.loop_forever()
    finally:
        client.disconnect()
    if failure:
        raise ValueError(failure[0])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default=publisher.MQTT_SERVER)
    parser.add_argument("--port", type=int, default=publisher.MQTT_PORT)
    parser.add_argument("--topic", default=publisher.TOPIC)
    parser.add_argument("--messages", type=int, default=0,
                        help="stop after N valid batches; 0 listens until interrupted")
    parser.add_argument("--file", type=Path, help="check a batch JSON object or array offline")
    args = parser.parse_args(argv)
    if args.messages < 0:
        parser.error("--messages must be non-negative")
    verifier = StreamVerifier()
    try:
        if args.file:
            with args.file.open(encoding="utf-8") as stream:
                batches = json.load(stream)
            for batch in batches if isinstance(batches, list) else [batches]:
                verifier.check(batch)
            if not verifier.batches:
                raise ValueError("The file contains no batches")
        else:
            listen(verifier, args.server, args.port, args.topic, args.messages)
    except KeyboardInterrupt:
        pass
    except (OSError, ValueError, TypeError) as error:
        print(f"Verification failed: {error}")
        return 1
    print(f"Verified {verifier.batches:,} batches, {sum(verifier.counts.values()):,} events; "
          f"{verifier.duplicates:,} duplicate batches skipped")
    print("Events: " + ", ".join(f"{kind}={verifier.counts[kind]:,}" for kind in EVENT_FIELDS))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
