"""Export the Assignment 1 DC chargers as the station list for the dataset generator. Task 1.

Input:  data/processed/ev_charging.duckdb     (Assignment 1 database with corrected coordinates)
Output: data/generated/stations_input.csv     (columns expected by generate_dataset.py)

Every Assignment 1 charger becomes one generator station, and `station_id` is the
Assignment 1 primary key `charger_record_id`. That keeps every generated price, session and
meter event joinable to the Assignment 1 schema without a lookup table.
"""
import csv
import re
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
A1_DB = ROOT / "data" / "processed" / "ev_charging.duckdb"  # Assignment 1 DB after src.corrections
OUT_CSV = ROOT / "data" / "generated" / "stations_input.csv"

FIELDS = ("station_id", "name", "lat", "lon", "site_type", "connectors", "max_kw")

# SA4 regions treated as urban: the Greater Sydney GCCSA (every "Sydney - *" SA4 plus
# Central Coast) and the two other NSW Major Cities, Newcastle and Wollongong (Illawarra).
# Everything else is regional, matching the ARENA urban/regional split the generator calibrates on.
URBAN_SA4 = {"Central Coast", "Newcastle and Lake Macquarie", "Illawarra"}

QUERY = """
SELECT charger_record_id, station_name, station_address, sa4_name,
       number_of_plugs, rating_kw, rating_text, latitude, longitude
FROM charger_analysis
WHERE charger_type = 'DC'
ORDER BY charger_record_id
"""


def site_type(sa4_name: str | None) -> str:
    if sa4_name and (sa4_name.startswith("Sydney - ") or sa4_name in URBAN_SA4):
        return "urban"
    return "regional"


def max_kw(rating_kw: float | None, rating_text: str | None) -> float:
    """Rated kW; when Assignment 1 left it NULL, take the highest kW figure in the rating text.

    The NULL rows are mixed-power sites such as "2x350kW & 2x175kW", so the maximum is the
    power the fastest connector can deliver.
    """
    if rating_kw is not None:
        return float(rating_kw)
    figures = [float(x) for x in re.findall(r"(\d+(?:\.\d+)?)\s*kW", rating_text or "", re.I)]
    if not figures:
        raise ValueError(f"No kW rating in {rating_text!r}")
    return max(figures)


def to_station(row: tuple) -> dict:
    record_id, name, address, sa4, plugs, rating_kw, rating_text, lat, lon = row
    if not plugs or plugs < 1:
        raise ValueError(f"{record_id}: number_of_plugs must be >= 1, got {plugs!r}")
    return {
        "station_id": record_id,
        "name": name or address,  # station_name is mostly NULL in the TfNSW source
        "lat": lat,
        "lon": lon,
        "site_type": site_type(sa4),
        "connectors": int(plugs),
        "max_kw": max_kw(rating_kw, rating_text),
    }


def load_stations(db_path: Path = A1_DB) -> list[dict]:
    with duckdb.connect(str(db_path), read_only=True) as con:
        con.execute("INSTALL spatial; LOAD spatial")  # the charger table has GEOMETRY columns
        return [to_station(r) for r in con.execute(QUERY).fetchall()]


def run(db_path: Path = A1_DB, out_csv: Path = OUT_CSV) -> list[dict]:
    stations = load_stations(db_path)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(stations)
    urban = sum(s["site_type"] == "urban" for s in stations)
    print(f"{len(stations)} stations ({urban} urban, {len(stations) - urban} regional) -> {out_csv}")
    return stations


if __name__ == "__main__":
    run()
