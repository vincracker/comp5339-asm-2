"""Static charger details for the map, read from the corrected Assignment 1 database. Task 3.

Input: data/processed/ev_charging.duckdb (built by `python main.py corrections`)

Events carry only `station_id`, which is the Assignment 1 primary key `charger_record_id`
(see src/generation/stations.py), so every marker's location, name, address, operator and
plug types come straight from Assignment 1 with no lookup table.
"""
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "data" / "processed" / "ev_charging.duckdb"

# Geometry columns are not selected, so the spatial extension is not needed here.
# Plug types come from the OCM/OSM station matched in Assignment 1; TfNSW itself lists none.
QUERY = """
SELECT ca.charger_record_id, ca.station_name, ca.station_address, ca.operator_name,
       ca.number_of_plugs, ca.rating_text, ca.rating_kw, ca.latitude, ca.longitude,
       ca.lga_name, ca.sa4_name, ca.raw_cost,
       list(DISTINCT cc.connector_name ORDER BY cc.connector_name)
           FILTER (WHERE cc.connector_name IS NOT NULL) AS plug_types
FROM charger_analysis ca
LEFT JOIN charger_connector cc USING (charger_record_id)
WHERE ca.charger_type = 'DC'
GROUP BY ALL
ORDER BY ca.charger_record_id
"""


def to_charger(row: tuple) -> dict:
    (record_id, name, address, operator, plugs, rating_text, rating_kw, lat, lon,
     lga, sa4, raw_cost, plug_types) = row
    return {
        "station_id": record_id,
        "name": name or address,  # station_name is NULL for most TfNSW records
        "address": address,
        "operator": operator,
        "connectors": int(plugs),
        "rating": rating_text or (f"{rating_kw:g} kW" if rating_kw else None),
        "lat": lat,
        "lon": lon,
        "lga": lga,
        "sa4": sa4,
        "listed_cost": raw_cost,  # free-text tariff from OCM/OSM, shown for reference only
        "plug_types": plug_types or [],
    }


def load_chargers(db_path: Path = DB_PATH) -> dict[str, dict]:
    if not Path(db_path).exists():
        raise FileNotFoundError(f"{db_path} not found; run `python main.py corrections` first")
    with duckdb.connect(str(db_path), read_only=True) as con:
        return {r[0]: to_charger(r) for r in con.execute(QUERY).fetchall()}
