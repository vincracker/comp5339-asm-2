"""Fix the scrambled TfNSW Regional charger coordinates in a copy of the Assignment 1 database.

Input:  data/assignment1/ev_chargers.duckdb              (left untouched)
        data/corrections/tfnsw_regional_coordinates.csv  (reviewed correction table)
Output: data/processed/ev_charging.duckdb                (Assignment 2 working database)

Every 'TfNSW Regional' row in the source CSV carries the coordinates and PCODE of another
row in that group, e.g. the Wilcannia address sits at Armidale. The correction table gives
each address its real coordinates, with an OCM/OSM charger as evidence (see sql/corrections.sql
for the methods). Applying it updates the coordinates, postcode, SA4 region and external
match, and records old and new values in charger_coordinate_correction.

The output is rebuilt from the Assignment 1 file on every run, so the step is idempotent.
"""
import shutil
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
A1_DB = ROOT / "data" / "assignment1" / "ev_chargers.duckdb"
OUT_DB = ROOT / "data" / "processed" / "ev_charging.duckdb"
CORRECTIONS_CSV = ROOT / "data" / "corrections" / "tfnsw_regional_coordinates.csv"
DDL = ROOT / "sql" / "corrections.sql"

SCRAMBLED_SOURCE = "TfNSW Regional"
MATCH_RADIUS_M = 200  # Assignment 1 proximity-match radius
MAX_EVIDENCE_M = 250  # corrected point must be this close to its evidence charger

DIST = "ST_Distance_Sphere(ST_FlipCoordinates({a}), ST_FlipCoordinates({b}))"  # geoms are lon/lat

NEAREST_EXTERNAL = f"""
SELECT e.external_station_id, {DIST.format(a='c.geom', b='e.geom')} AS d
FROM external_station e
WHERE e.source = '{{source}}' AND {DIST.format(a='c.geom', b='e.geom')} <= {MATCH_RADIUS_M}
ORDER BY d LIMIT 1
"""


def load_corrections(con: duckdb.DuckDBPyConnection, csv_path: Path) -> None:
    con.execute(f"""
        CREATE TEMP TABLE fix AS
        SELECT * FROM read_csv(?, header = true, columns = {{
            'charger_record_id': 'VARCHAR', 'town': 'VARCHAR', 'station_address': 'VARCHAR',
            'postcode': 'VARCHAR', 'latitude': 'DOUBLE', 'longitude': 'DOUBLE', 'method': 'VARCHAR',
            'evidence_ref': 'VARCHAR', 'evidence_distance_m': 'DOUBLE'}})
    """, [str(csv_path)])


def validate(con: duckdb.DuckDBPyConnection) -> None:
    """Fail before changing anything if the correction table does not fit the database."""
    scrambled, fixes, joined, distinct_points = con.execute(f"""
        SELECT (SELECT count(*) FROM charger WHERE (raw_record->>'Source') = '{SCRAMBLED_SOURCE}'),
               (SELECT count(*) FROM fix),
               (SELECT count(*) FROM fix JOIN charger c USING (charger_record_id)
                WHERE (c.raw_record->>'Source') = '{SCRAMBLED_SOURCE}'),
               (SELECT count(DISTINCT (latitude, longitude)) FROM fix)
    """).fetchone()
    if not scrambled == fixes == joined == distinct_points:
        raise ValueError(f"Correction table mismatch: {scrambled} scrambled chargers, {fixes} fixes, "
                         f"{joined} matching ids, {distinct_points} distinct points")
    far = con.execute(f"SELECT town, evidence_distance_m FROM fix WHERE evidence_distance_m > {MAX_EVIDENCE_M}").fetchall()
    if far:
        raise ValueError(f"Corrected points too far from their evidence: {far}")


def snapshot_old_values(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TEMP TABLE old AS
        SELECT c.charger_record_id, c.latitude, c.longitude, c.postcode, c.sa4_code, c.external_station_id
        FROM charger c JOIN fix USING (charger_record_id)
    """)


def update_location(con: duckdb.DuckDBPyConnection) -> None:
    # The RTREE index is rebuilt around the update so geom can change.
    con.execute("DROP INDEX charger_geom_idx")
    con.execute("""
        UPDATE charger c SET latitude = f.latitude, longitude = f.longitude, postcode = f.postcode,
                             geom = ST_Point(f.longitude, f.latitude)
        FROM fix f WHERE c.charger_record_id = f.charger_record_id
    """)
    con.execute("CREATE INDEX charger_geom_idx ON charger USING RTREE (geom)")


def update_sa4(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        UPDATE charger c SET sa4_match_count = s.n, sa4_code = CASE WHEN s.n = 1 THEN s.code END
        FROM (SELECT f.charger_record_id, count(r.sa4_code) AS n, any_value(r.sa4_code) AS code
              FROM fix f LEFT JOIN sa4_region r ON ST_Contains(r.geom, ST_Point(f.longitude, f.latitude))
              GROUP BY 1) s
        WHERE c.charger_record_id = s.charger_record_id
    """)


def update_external_match(con: duckdb.DuckDBPyConnection) -> None:
    """Re-run Assignment 1's proximity rule (OCM first, then OSM) at the corrected location.

    Only stations already in external_station are candidates, and the address-corroborated
    OCM rule is not repeated because external_station keeps no street address.
    """
    con.execute(f"""
        CREATE TEMP TABLE rematch AS
        SELECT c.charger_record_id,
               coalesce(ocm.external_station_id, osm.external_station_id) AS external_station_id,
               round(coalesce(ocm.d, osm.d), 1) AS distance_m,
               (SELECT count(*) FROM external_station e WHERE e.source = 'ocm'
                  AND {DIST.format(a='c.geom', b='e.geom')} <= {MATCH_RADIUS_M}) > 1 AS ambiguous
        FROM charger c JOIN fix USING (charger_record_id)
        LEFT JOIN LATERAL ({NEAREST_EXTERNAL.format(source='ocm')}) ocm ON true
        LEFT JOIN LATERAL ({NEAREST_EXTERNAL.format(source='osm')}) osm ON true
    """)
    con.execute("""
        UPDATE charger c SET external_station_id = m.external_station_id,
                             matched = m.external_station_id IS NOT NULL,
                             match_method = CASE WHEN m.external_station_id IS NULL THEN 'unmatched' ELSE 'proximity' END,
                             match_distance_m = m.distance_m,
                             match_is_ambiguous = m.ambiguous
        FROM rematch m WHERE c.charger_record_id = m.charger_record_id
    """)


def record_audit(con: duckdb.DuckDBPyConnection) -> None:
    # Created after the charger updates: DuckDB rejects UPDATEs on a table referenced by a FK.
    con.execute(DDL.read_text())
    con.execute("""
        INSERT INTO charger_coordinate_correction
        SELECT f.charger_record_id, f.method, f.evidence_ref, f.evidence_distance_m,
               o.latitude, o.longitude, o.postcode, o.sa4_code, o.external_station_id,
               f.latitude, f.longitude, f.postcode
        FROM fix f JOIN old o USING (charger_record_id)
    """)


def summarise(con: duckdb.DuckDBPyConnection) -> tuple:
    return con.execute("""
        SELECT count(*),
               count(*) FILTER (WHERE c.sa4_code IS DISTINCT FROM a.old_sa4_code),
               count(*) FILTER (WHERE c.external_station_id IS DISTINCT FROM a.old_external_station_id),
               count(*) FILTER (WHERE NOT c.matched)
        FROM charger_coordinate_correction a JOIN charger c USING (charger_record_id)
    """).fetchone()


def run(a1_db: Path = A1_DB, out_db: Path = OUT_DB, csv_path: Path = CORRECTIONS_CSV) -> tuple:
    out_db.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_db.with_suffix(".tmp.duckdb")
    shutil.copyfile(a1_db, tmp)
    try:
        with duckdb.connect(str(tmp)) as con:
            con.execute("INSTALL spatial; LOAD spatial")
            load_corrections(con, csv_path)
            validate(con)
            con.execute("BEGIN")
            snapshot_old_values(con)
            update_location(con)
            update_sa4(con)
            update_external_match(con)
            record_audit(con)
            con.execute("COMMIT")
            fixed, sa4_changed, match_changed, unmatched = summarise(con)
            con.execute("CHECKPOINT")
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
    tmp.replace(out_db)  # only a fully corrected database ever appears at out_db
    print(f"{fixed} chargers relocated: {sa4_changed} changed SA4 region, {match_changed} changed "
          f"external match, {unmatched} now unmatched -> {out_db}")
    return fixed, sa4_changed, match_changed, unmatched


if __name__ == "__main__":
    run()
