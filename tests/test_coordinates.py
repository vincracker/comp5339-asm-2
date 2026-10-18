import csv

import duckdb
import pytest

from src.corrections import coordinates


@pytest.fixture(scope="module")
def corrected_db(tmp_path_factory):
    out = tmp_path_factory.mktemp("db") / "ev.duckdb"
    coordinates.run(out_db=out)
    con = duckdb.connect(str(out), read_only=True)
    con.execute("LOAD spatial")
    yield con
    con.close()


def test_every_scrambled_charger_is_audited(corrected_db):
    n = corrected_db.execute("SELECT count(*) FROM charger_coordinate_correction").fetchone()[0]
    assert n == 23


def test_wilcannia_moved_to_far_west(corrected_db):
    lat, lon, sa4 = corrected_db.execute("""
        SELECT c.latitude, c.longitude, r.sa4_name FROM charger c JOIN sa4_region r USING (sa4_code)
        WHERE c.station_address LIKE '%Wilcannia%'
    """).fetchone()
    assert (round(lat, 2), round(lon, 2)) == (-31.56, 143.39)
    assert sa4 == "Far West and Orana"


def test_geom_and_index_follow_coordinates(corrected_db):
    stale = corrected_db.execute("""
        SELECT count(*) FROM charger
        WHERE ST_X(geom) <> longitude OR ST_Y(geom) <> latitude
    """).fetchone()[0]
    assert stale == 0
    assert corrected_db.execute(
        "SELECT count(*) FROM duckdb_indexes() WHERE index_name = 'charger_geom_idx'").fetchone()[0] == 1


def test_only_scrambled_rows_change(corrected_db):
    corrected_db.execute(f"ATTACH '{coordinates.A1_DB}' AS a1 (READ_ONLY)")
    changed = corrected_db.execute("""
        SELECT count(*) FROM charger c JOIN a1.charger o USING (charger_record_id)
        WHERE (c.latitude, c.longitude, c.external_station_id) IS DISTINCT FROM (o.latitude, o.longitude, o.external_station_id)
          AND (o.raw_record->>'Source') <> 'TfNSW Regional'
    """).fetchone()[0]
    corrected_db.execute("DETACH a1")
    assert changed == 0


def test_bad_correction_table_leaves_no_output(tmp_path):
    with open(coordinates.CORRECTIONS_CSV) as f:
        rows = list(csv.DictReader(f))
    bad = tmp_path / "bad.csv"
    with open(bad, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows[:-1])  # one scrambled charger missing
    out = tmp_path / "ev.duckdb"
    with pytest.raises(ValueError, match="Correction table mismatch"):
        coordinates.run(out_db=out, csv_path=bad)
    assert not out.exists()
    assert not out.with_suffix(".tmp.duckdb").exists()
