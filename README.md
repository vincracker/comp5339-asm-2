# COMP5339 Assignment 2 — EV Charging Data Streaming and Analysis

TODO(all): group name, members (name + SID), one-paragraph summary.

## Project structure

```
code/
├── main.py                      # runs all stages: python main.py [stage ...]
├── src/
│   ├── corrections/
│   │   └── coordinates.py       # fixes scrambled Assignment 1 coordinates
│   └── generation/              # Task 1 — synthetic dataset generation
│       ├── stations.py          # Assignment 1 chargers -> generator station list
│       ├── generate_dataset.py  # provided course generator, unmodified copy
│       └── __main__.py          # runs both steps: python -m src.generation
├── sql/
│   └── corrections.sql          # DDL: charger_coordinate_correction audit table
├── data/
│   ├── assignment1/ev_chargers.duckdb   # Assignment 1 database (input, never modified)
│   ├── corrections/tfnsw_regional_coordinates.csv   # reviewed coordinate fixes
│   ├── processed/ev_charging.duckdb     # Assignment 2 working database
│   └── generated/                       # Task 1 outputs (see below)
└── tests/                       # pytest
```

## Setup

Python 3.13+.

**Team (uv):**

```
git clone https://github.com/vincracker/comp5339-asm-2.git && cd comp5339-asm-2
uv sync                  # creates .venv with exact versions from uv.lock, incl. pytest
uv run python main.py    # builds the database and the dataset (~30 s)
```

**Marker / plain pip:**

```
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

The generated dataset (`data/generated/*.csv`, ~500 MB) and the working database
(`data/processed/`) are **not in git**. `python main.py` rebuilds them, and the output is
identical on every machine. The AEMO price file is committed in `data/generated/aemo_cache/`,
so prices never depend on when you run it. The first run needs internet once, so DuckDB can
download its `spatial` extension.

After changing a dependency, regenerate `requirements.txt` with
`uv export --no-hashes --no-dev --format requirements-txt -o requirements.txt`.

## Execution

```
python main.py                  # all stages, ~30 s
python main.py corrections      # a single stage
uv run pytest                   # tests (pip users: pip install pytest first)
```

### Coordinate corrections

`python main.py corrections` copies the Assignment 1 database to
`data/processed/ev_charging.duckdb` and fixes 23 chargers whose coordinates are wrong in
the TfNSW source (see Assumptions). It rebuilds the copy from scratch each run, and fails
without writing anything if the correction table does not fit the database.

### Task 1 — dataset generation

```
python -m src.generation --start 2026-10-01 --days 14
```

Runs on the corrected database. This (1) exports every charger to
`data/generated/stations_input.csv`, then (2) runs the course generator on it. Any other
generator flag is passed through, e.g. `--meter-every 2`. Output is deterministic
(seed 5339).

### Task 2 — MQTT event publishing

```bash
# Check the example batch offline without connecting to MQTT.
python -m src.MQTT_publishing.verify --file src/MQTT_publishing/example.json

# In a separate terminal, start the subscriber before publishing.
# Stop after 20 valid unique batches; omit --messages to keep listening.
python -m src.MQTT_publishing.verify --messages 20

# Validate every source event and batch without connecting to MQTT or waiting.
python -m src.MQTT_publishing.publisher --dry-run --cycles 1

# Publish one complete replay; omit --cycles to repeat continuously.
python -m src.MQTT_publishing.publisher --cycles 1

# Explicit 100x speed: 10 simulated seconds per 0.1 real seconds.
python -m src.MQTT_publishing.publisher --window-seconds 10 --interval 0.1 --cycles 1
```

The publisher reads `prices_hourly.csv`, `sessions.csv`, and `session_metering.csv`
from `--data-dir` (default `data/generated`). Configure the broker with `--server`
(default `154.36.178.77`), `--port` (1883), and `--topic`
(`COMP5339/EV_charging`). `--cycles 0`, the default, repeats indefinitely; a positive
value limits the number of complete replays.

Each MQTT message represents one fixed simulated-time interval. `--window-seconds`
defaults to 10 and `--interval` defaults to 0.1 seconds, with a minimum of 0.1.
Thus the default mapping is 10 simulated seconds per 0.1 real seconds, or 100×
speed. The full generated dataset needs about 3 hours 22 minutes at this speed
before processing and MQTT acknowledgement overhead; sessions finishing after the
nominal fourteen-day generation period are included. Setting `--interval 0.2`
gives 50× speed. The publisher waits at least the configured interval between
messages, including across replay boundaries.

Messages contain JSON numbers for numeric CSV values and use this envelope.
[`example.json`](src/MQTT_publishing/example.json) shows a complete batch with all
four event types, including a final meter reading, session completion, and reuse
of the same connector at one timestamp.

```json
{
  "replay_id": "unique-id-for-this-pass",
  "sequence": 1,
  "window_start": "2026-10-01T00:00:00",
  "window_end": "2026-10-01T00:00:10",
  "events": [
    {
      "event_id": "price_update:chr_example:2026-10-01T00:00:00",
      "type": "price_update",
      "ts": "2026-10-01T00:00:00",
      "station_id": "chr_example",
      "price_per_kwh": 0.66,
      "wholesale_per_kwh": 0.1032
    }
  ]
}
```

Windows start at the earliest event and cover half-open intervals
`[window_start, window_end)`: an event at `window_end` belongs to the next batch.
Empty windows publish an empty `events` array to preserve the time mapping.
Events are globally ordered by timestamp within and between batches; ties use
`price_update`, `meter`, `session_end`, then `session_start`. Prices and session
records are sorted internally. The meter file is streamed in its generated order,
and an out-of-order timestamp is rejected.

Every event has `event_id`, `type`, `ts`, and `station_id`. Remaining fields retain
the CSV names:

| Event | Source timestamp | Additional content |
|---|---|---|
| `price_update` | Price row's `ts` | `price_per_kwh`, `wholesale_per_kwh`; publish the first price for each station, then only actual changes to its retail `price_per_kwh` |
| `session_start` | `start_ts` | `session_id`, `connector_id`, `site_type`, `vehicle_model`, `start_ts`, `price_per_kwh`; future completion times and totals are omitted |
| `meter` | Meter row's `ts` | `session_id`, `connector_id`, `voltage_v`, `current_a`, `power_kw`, `energy_kwh`, `soc_pct` |
| `session_end` | `end_ts` | All session-row fields, including `charge_end_ts`, `end_ts`, charging duration, energy, average/peak power, and cost |

Meter `energy_kwh` is cumulative within its session. Prices are AUD/kWh and
`cost_aud` is AUD. Source timestamps are preserved without assigning a timezone:
the CSV timestamps have no UTC offset. Wholesale-only changes do not trigger a
price event.

Delivery uses MQTT QoS 1. Event IDs are stable across replays, and each pass gets
a new `replay_id`; consumers can deduplicate redelivery using the pair
`(replay_id, event_id)`. Sequence numbers restart at 1, price baselines reset, and
original event timestamps repeat on each pass. The dry run validates a complete
replay and prints a per-cycle summary without publishing or sleeping.

The small `verify.py` subscriber uses the same `--server`, `--port`, and `--topic`
defaults as the publisher. It checks field types, event window membership, event
order, sequence and window continuity, changed retail prices, and cumulative
meter energy. It skips QoS duplicate batches identified by `(replay_id, sequence)`
and prints batch and event counts; invalid messages cause an exit status of 1.
It can attach partway through a replay: its first batch establishes a baseline,
and subsequent gaps are flagged. `--file` accepts one batch object or an array
of batches for offline validation.

## Assumptions

**Scrambled TfNSW coordinates.** All 23 rows with `Source = 'TfNSW Regional'` in the TfNSW
CSV carry the coordinates and `PCODE` of a *different* row in that group. For example, the
Wilcannia address sits at Armidale and the Broken Hill row holds Wilcannia's coordinates.
The address text and `LGANAME` are correct. Assignment 1 took the coordinates as given, so
these chargers also got the wrong SA4 region and were matched to another town's OCM/OSM
station.

`data/corrections/tfnsw_regional_coordinates.csv` assigns each address its real location:

| Method | Rows | Rule |
|---|---|---|
| `tfnsw_pair_near_evidence` | 21 | Another row's TfNSW coordinates, confirmed by an OCM/OSM charger (same postcode/street) within 210 m |
| `tfnsw_pair_by_elimination` | 1 | Gilgandra: no OCM/OSM record; the one unassigned pair lies in Gilgandra |
| `external_location` | 1 | Moree: no TfNSW pair is in Moree (the spare pair is in Jerilderie); the NRMA charger's OSM location is used |

The correction step then recomputes `geom`, `postcode` (from the address), `sa4_code`, and
the external match using Assignment 1's rule (nearest OCM within 200 m, else nearest OSM
within 200 m), against stations already in `external_station`. Results: 19 chargers changed
SA4 region. 21 are matched again, 19 of them to their evidence record. Gilgandra and Moree are
now unmatched, because their real stations were never loaded into `external_station`. Old and
new values for every row are kept in `charger_coordinate_correction`.

**Charger → generator station mapping** (`src/generation/stations.py`):

| Generator column | Assignment 1 source | Rule |
|---|---|---|
| `station_id` | `charger.charger_record_id` | Same primary key, so all events join to Assignment 1 directly |
| `name` | `station_name`, else `station_address` | `station_name` is NULL for most TfNSW records |
| `lat`, `lon` | `latitude`, `longitude` | |
| `site_type` | `sa4_region.sa4_name` | Urban = Greater Sydney GCCSA (`Sydney - *` SA4s + Central Coast) + Newcastle and Lake Macquarie + Illawarra; all other SA4s regional |
| `connectors` | `number_of_plugs` | Each plug is treated as an independently usable connector |
| `max_kw` | `rating_kw` | 5 NULL ratings are mixed sites ("2x350kW & 2x175kW"); the highest figure (350) is used |

All 432 Assignment 1 chargers are included (all DC; Assignment 1 kept DC only), with the
corrected coordinates.

**Prices** use the generator's hypothetical dynamic tariff driven by real AEMO NSW1 wholesale
prices for 1–14 Oct 2026 (all 336 hours present in the AEMO file, so no hour falls back to the
period mean).

## Outputs

`data/generated/` (1–14 Oct 2026, 432 stations). Column-by-column guide: [`data/generated/README.md`](data/generated/README.md).

| File | Rows | Content |
|---|---|---|
| `stations_input.csv` | 432 | Generator input built from Assignment 1 |
| `stations.csv` | 432 | Generator's station echo, plus the drawn `base_price` |
| `prices_hourly.csv` | 145,152 | Hourly price per station; 100,656 rows are actual changes |
| `sessions.csv` | 76,330 | One row per charging session |
| `session_metering.csv` | 2,305,283 | Per-minute meter readings (avg 30 per session) |
| `aemo_cache/` | — | Raw AEMO price file the prices are derived from |

Checked after generation: every `station_id` matches an Assignment 1 `charger_record_id` and
vice versa; no overlapping sessions on any connector; every session has meter readings;
session IDs are unique.
