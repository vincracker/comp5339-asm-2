"""Run the Assignment 2 pipeline stages in order.

    python main.py                 # all stages
    python main.py corrections     # one stage

Stages:
  corrections  copy the Assignment 1 DB to data/processed/ and fix scrambled coordinates
  generation   Task 1: export chargers and run the course dataset generator (1-14 Oct 2026)
"""
import sys

from src.corrections import coordinates
from src.generation.__main__ import main as generate

STAGES = {"corrections": coordinates.run, "generation": generate}


def main(argv: list[str]) -> None:
    selected = argv or list(STAGES)
    unknown = [s for s in selected if s not in STAGES]
    if unknown:
        sys.exit(f"Unknown stage(s) {unknown}; choose from {list(STAGES)}")
    for name in selected:
        print(f"== {name}")
        STAGES[name]()


if __name__ == "__main__":
    main(sys.argv[1:])
