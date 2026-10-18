"""Task 1 end to end: export Assignment 1 chargers, then run the provided generator on them.

    python -m src.generation [--start 2026-10-01] [--days 14] [extra generator flags...]

Unknown flags are passed through to generate_dataset.py unchanged (e.g. --meter-every 2).
"""
import argparse
import subprocess
import sys
from pathlib import Path

from . import stations

GENERATOR = Path(__file__).with_name("generate_dataset.py")
OUT_DIR = stations.OUT_CSV.parent


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default="2026-10-01", help="first day of AEMO prices (inclusive)")
    ap.add_argument("--days", type=int, default=14)
    args, passthrough = ap.parse_known_args([] if __name__ != "__main__" else None)

    stations.run()
    cmd = [sys.executable, str(GENERATOR), "--stations", str(stations.OUT_CSV),
           "--start", args.start, "--days", str(args.days), "--out", str(OUT_DIR), *passthrough]
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
