"""Task 3: live EV charger map subscribed to the Task 2 MQTT stream.

    python -m src.dashboard                          # course broker, http://127.0.0.1:8050
    python -m src.dashboard --server 127.0.0.1       # a local broker

Needs data/processed/ev_charging.duckdb (python main.py corrections) for charger details.
Every received event is also stored in that database (Task 4); --no-store turns this off.
"""
import argparse
import logging
from pathlib import Path

from . import subscriber
from .app import create_app
from .chargers import DB_PATH, load_chargers
from .state import LiveState
from ..storage.event_store import EventStore


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--server", default=subscriber.MQTT_SERVER, help="MQTT broker host")
    ap.add_argument("--port", type=int, default=subscriber.MQTT_PORT, help="MQTT broker port")
    ap.add_argument("--topic", default=subscriber.TOPIC)
    ap.add_argument("--db", type=Path, default=DB_PATH,
                    help="corrected Assignment 1 database; received events are stored in it too")
    ap.add_argument("--no-store", action="store_true", help="do not store events in DuckDB (Task 3 only)")
    ap.add_argument("--host", default="127.0.0.1", help="address the web server listens on")
    ap.add_argument("--http-port", type=int, default=8050)
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("werkzeug").setLevel(logging.WARNING)  # not one log line per 1 s poll
    print(f"Dashboard: http://{args.host}:{args.http_port}  (MQTT {args.server}:{args.port} {args.topic})")
    state = LiveState(load_chargers(args.db))  # read-only connection, closed before the store opens
    # One process owns the database file (DuckDB allows a single writing process), so the
    # store lives here and the Dash callbacks read through its connection.
    store = None if args.no_store else EventStore(args.db)
    client = subscriber.start(state, args.server, args.port, args.topic, store)
    try:
        # No debug reloader: it would start a second process with its own MQTT subscriber.
        create_app(state, store).run(host=args.host, port=args.http_port, debug=False)
    finally:
        client.loop_stop()
        client.disconnect()
        if store is not None:
            store.close()  # commits what is still queued


if __name__ == "__main__":
    main()
