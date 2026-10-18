-- Assignment 2 schema extension 1: coordinate corrections to Assignment 1 chargers.
-- Applied to data/processed/ev_charging.duckdb by src/corrections/coordinates.py.
--
-- The 23 'TfNSW Regional' rows in the TfNSW source carry each other's coordinates and
-- PCODE (a permutation within that group), so their markers, SA4 region and external
-- match were all wrong. charger is corrected in place; this table keeps the before/after
-- values and the evidence for every corrected row.

CREATE TABLE charger_coordinate_correction (
    charger_record_id VARCHAR PRIMARY KEY REFERENCES charger(charger_record_id),
    method VARCHAR NOT NULL CHECK (method IN (
        'tfnsw_pair_near_evidence',   -- another row's TfNSW pair, confirmed by an OCM/OSM charger nearby
        'tfnsw_pair_by_elimination',  -- the only unassigned TfNSW pair, in the right town
        'external_location')),        -- no TfNSW pair fits; OCM/OSM charger location used
    evidence_ref VARCHAR,             -- ocm:<id> / osm:<id> from the Assignment 1 caches
    evidence_distance_m DOUBLE CHECK (evidence_distance_m >= 0),
    old_latitude DOUBLE NOT NULL,
    old_longitude DOUBLE NOT NULL,
    old_postcode VARCHAR,
    old_sa4_code VARCHAR,
    old_external_station_id VARCHAR,
    new_latitude DOUBLE NOT NULL CHECK (new_latitude BETWEEN -38 AND -28),   -- NSW extent
    new_longitude DOUBLE NOT NULL CHECK (new_longitude BETWEEN 140 AND 154),
    new_postcode VARCHAR NOT NULL,
    CHECK ((method = 'tfnsw_pair_by_elimination') = (evidence_ref IS NULL))
);
