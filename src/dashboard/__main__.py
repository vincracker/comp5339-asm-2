"""Task 3: live EV charger map subscribed to the Task 2 MQTT stream.

    python -m src.dashboard                          # course broker, http://127.0.0.1:8050
    python -m src.dashboard --server 127.0.0.1       # a local broker

Needs data/processed/ev_charging.duckdb (python main.py corrections) for charger details.
"""
import argparse
import logging
from pathlib import Path

from . import subscriber
from .app import create_app
from .chargers import DB_PATH, load_chargers
from .state import LiveState


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--server", default=subscriber.MQTT_SERVER, help="MQTT broker host")
    ap.add_argument("--port", type=int, default=subscriber.MQTT_PORT, help="MQTT broker port")
    ap.add_argument("--topic", default=subscriber.TOPIC)
    ap.add_argument("--db", type=Path, default=DB_PATH, help="corrected Assignment 1 database")
    ap.add_argument("--host", default="127.0.0.1", help="address the web server listens on")
    ap.add_argument("--http-port", type=int, default=8050)
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("werkzeug").setLevel(logging.WARNING)  # not one log line per 1 s poll
    print(f"Dashboard: http://{args.host}:{args.http_port}  (MQTT {args.server}:{args.port} {args.topic})")
    state = LiveState(load_chargers(args.db))
    client = subscriber.start(state, args.server, args.port, args.topic)
    try:
        # No debug reloader: it would start a second process with its own MQTT subscriber.
        create_app(state).run(host=args.host, port=args.http_port, debug=False)
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
