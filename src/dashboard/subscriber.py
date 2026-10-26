"""MQTT subscriber feeding LiveState from the Task 2 topic. Task 3.

Runs on paho's network thread next to the Dash server. `connect_async` plus paho's
automatic reconnect keep the dashboard up while the broker is unreachable; the header shows
the connection state. With an EventStore (Task 4), every message that parses is also queued
for DuckDB; the store's writer thread commits it, so this thread never waits on the database.
"""
import json
import logging
from uuid import uuid4

import paho.mqtt.client as mqtt

from .state import LiveState
from ..storage.event_store import EventStore

MQTT_SERVER = "154.36.178.77"
MQTT_PORT = 1883
TOPIC = "COMP5339/EV_charging"

log = logging.getLogger(__name__)


def start(state: LiveState, server: str = MQTT_SERVER, port: int = MQTT_PORT,
          topic: str = TOPIC, store: EventStore | None = None) -> mqtt.Client:
    # A unique client id, so several dashboards (one per team member) can subscribe at once.
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"comp5339-dashboard-{uuid4().hex[:8]}")

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            state.set_connection(False, f"refused: {reason_code}")
            return
        client.subscribe(topic, qos=1)
        state.set_connection(True, f"{server}:{port} {topic}")

    def on_disconnect(client, userdata, flags, reason_code, properties):
        state.set_connection(False, f"disconnected ({reason_code}); retrying")

    def on_connect_fail(client, userdata):
        state.set_connection(False, f"{server}:{port} unreachable; retrying")

    def on_message(client, userdata, message):
        try:
            state.apply_batch(json.loads(message.payload))
        except (ValueError, KeyError, TypeError) as exc:  # a malformed batch must not kill the thread
            log.warning("Skipped malformed message: %s", exc)
            return
        if store is not None:  # duplicates included: the store's keys make them no-ops
            store.submit(message.payload)

    client.on_connect, client.on_disconnect, client.on_message = on_connect, on_disconnect, on_message
    client.on_connect_fail = on_connect_fail
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    client.connect_async(server, port, keepalive=60)
    client.loop_start()
    return client
