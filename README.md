# COMP5339 Assignment 2 — EV Charging Data Streaming and Analysis

TODO(all): group name, members (name + SID), one-paragraph summary.

## Project structure

```
code/
├── main.py                      # runs all stages: python main.py [stage ...]
├── src/
│   ├── corrections/
│   │   └── coordinates.py       # fixes scrambled Assignment 1 coordinates
│   ├── generation/              # Task 1 — synthetic dataset generation
│   │   ├── stations.py          # Assignment 1 chargers -> generator station list
│   │   ├── generate_dataset.py  # provided course generator, unmodified copy
│   │   └── __main__.py          # runs both steps: python -m src.generation
│   ├── MQTT_publishing/         # Task 2 — ordered MQTT replay (publisher.py, verify.py)
│   ├── dashboard/               # Task 3 — live map: python -m src.dashboard
│   │   ├── chargers.py          # charger details from the corrected Assignment 1 DB
│   │   ├── state.py             # live state built from the MQTT batches
│   │   ├── subscriber.py        # paho MQTT client feeding the state (and the event store)
│   │   ├── app.py               # Dash layout and callbacks
│   │   └── assets/              # marker layer (JS) and styles (CSS)
│   └── storage/                 # Task 4 — event store in DuckDB
│       ├── event_store.py       # buffered writer: MQTT payloads -> event tables
│       ├── queries.py           # current-state and historical metrics for the dashboard
│       └── __main__.py          # python -m src.storage check | load
├── sql/
│   ├── corrections.sql          # DDL: charger_coordinate_correction audit table
│   └── events.sql               # DDL: Task 4 event tables and analytics macros
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

### Task 3 — live dashboard

```bash
python -m src.dashboard                      # course broker; open http://127.0.0.1:8050
python -m src.dashboard --server 127.0.0.1   # a local broker (e.g. for testing)
```

Flags: `--server`, `--port`, `--topic` (same defaults as the publisher), `--db` (default
`data/processed/ev_charging.duckdb`, so run `python main.py corrections` first), `--host` and
`--http-port` (default `127.0.0.1:8050`). The dashboard can be started before or after the
publisher, and it reconnects on its own if the broker drops out.

A Dash app with a Leaflet map (dash-leaflet). An MQTT subscriber thread applies each batch
to an in-memory `LiveState` (`src/dashboard/state.py`), and the page polls that state once
a second.

- **Markers appear dynamically.** A charger's marker is added the first time any event names
  its `station_id`. That `station_id` is the Assignment 1 `charger_record_id`, so its location,
  name, address, operator and plug types come from Assignment 1, with the corrected coordinates.
- **Each marker shows two live values.** It displays the latest price per kWh, and its colour
  shows occupancy: green = all connectors free, amber = some in use (the badge shows "in use/total"),
  red = all in use, grey = no price received yet. Below zoom 12 markers shrink to coloured dots,
  so all of NSW stays readable. Hovering shows a tooltip.
- **Clicking a marker opens the side panel.** It shows the name, address, operator, plug types,
  rating, region, latest price (with the time it was set, the wholesale component and a ▲/▼
  trend), occupancy and the last completed session.
- **Live session curves.** Every active session at the selected charger gets one line,
  built from its `meter` events and redrawn each second. A toggle switches the curve between
  energy delivered, power and state of charge. Below the chart, each session lists its connector,
  vehicle, start time, kWh, kW, SoC and the estimated cost so far.
- **Header.** Shows the stream status, the simulated time (`window_end` of the last batch),
  chargers on the map and current sessions. There is also an optional operator filter.

Stream handling (matches the Task 2 envelope):
- QoS 1 redeliveries are skipped by `(replay_id, sequence)`.
- Missed sequence numbers are counted and shown in the header.
- A new `replay_id` clears the state, because each pass restarts at 1 October.
- If the dashboard joins mid-replay, a `meter` event for a session it never saw start still
  opens that session, marked "joined mid-session". Markers then fill in as chargers report
  events, and their prices appear as each one next changes.
- A session that never receives its `session_end` (e.g. lost during a disconnect) is dropped
  after 6 simulated hours. The longest generated session is 3.3 h.

The browser loads the map tiles from OpenStreetMap (no API key needed), so viewing the map needs internet.

### Task 4 — event storage and charging analytics

```bash
python -m src.dashboard --server 127.0.0.1   # stores every received event while it runs
python -m src.storage check                  # row counts + integrity checks of what is stored
python -m src.storage load                   # testing only: store a full replay without MQTT (~2 min)
```

The dashboard stores every MQTT message it receives in `data/processed/ev_charging.duckdb`,
the same database that holds the corrected Assignment 1 tables, so events join to
`charger` directly. `--no-store` turns storage off. `python main.py corrections` rebuilds
that file from Assignment 1, which also deletes stored events.

**How it is wired.** One process owns the database, because DuckDB allows a single writing
process: the dashboard. The MQTT thread only queues each raw payload (`EventStore.submit`).
A writer thread commits everything queued once a second, in one transaction. The batches
are unpacked inside DuckDB with `from_json` and split into the event tables by set-based
`INSERT ... SELECT`. Row-by-row `executemany` was far too slow for this (7.7 s for 3,000
rows in a test); set-based ingest stores a whole replay of 2.56M events in about 1.5 minutes. Dash callbacks read through their own cursors, so they
see each committed flush and never half of one.

**Schema extension** ([`sql/events.sql`](sql/events.sql), applied automatically and idempotent):

| Table | Key | Linked to Assignment 1 | Content |
|---|---|---|---|
| `stream_batch` | `(replay_id, sequence)` | — | One row per MQTT message: window, event count, receive time (audit, gaps, lag) |
| `price_event` | `(station_id, ts)` | `station_id` → `charger` | Retail and wholesale price changes |
| `charging_session` | `session_id` | `station_id` → `charger` | One row per session: start fields, then completed by `session_end` (upsert) |
| `meter_event` | `(session_id, ts)`, enforced at ingest | via `charging_session` | Per-minute readings (session progress curves) |
| `unmatched_event` | `event_id` | — | Events naming no Assignment 1 charger (kept, not dropped; 0 so far) |

Design decisions:
- **Natural keys make storage idempotent.** A QoS 1 redelivery, a second replay or a
  re-run inserts nothing twice (`INSERT OR IGNORE` / `ON CONFLICT`). A full second replay
  adds only its 121,318 `stream_batch` rows.
- **Sessions are built up from events.** A meter reading for a session whose start was missed
  (dashboard joined mid-session) opens a partial row (`start_seen = false`), and the
  later `session_end`, which carries the full session, completes it.
- **`station_id` and `connector_id` are stored once per session**, not on every meter reading,
  because they depend on `session_id` alone.
- **Unknown chargers do not abort a flush.** Events are checked against `charger` before
  insertion, so a bad `station_id` cannot violate a foreign key and roll back the other
  events. If a flush still fails (e.g. a CHECK constraint), its messages are retried one at
  a time and only the bad one is skipped.

**Physical design** (full 14-day replay: 121k messages, 101k prices, 76k sessions, 2.3M meter readings):
- `meter_event` has **no index**. DuckDB backs every primary and foreign key with an ART
  index. With the keys declared, the database was 366 MB. Without them, it is 132 MB, and
  the 53 MB Assignment 1 file is part of both figures. Uniqueness is enforced at ingest
  instead, by an anti-join against the stored rows in the flush's time range.
  `python -m src.storage check` verifies the uniqueness and the parent session rows.
- **Insertion order is time order.** Events arrive sorted by simulated time, so DuckDB's
  per-row-group min/max zone maps act as a time index for free. A 100-second `ts` range of
  `meter_event` scans in 0.7 ms. The same filter written so the zone maps cannot be used
  (`strftime(ts)`) takes 14.5 ms. The ingest anti-join and every dashboard query filter on
  plain timestamp ranges for this reason.
- The small, frequently queried tables (`charging_session`, `price_event`) keep their keys.
- Prices and costs are `DECIMAL`; meter readings are `REAL` (4 bytes), which is ample for
  sensor-style values.

Ingest cost at the default publishing speed: a one-second flush (10 batches) takes a median
of 47 ms, and the p95 is 67 ms. A full replay loads offline in about 1.5 minutes.

Tested end to end over a local MQTT broker: one full replay published with
`--window-seconds 600` was stored completely, 2,022 messages and 2,558,599 events, and
`python -m src.storage check` passed every check.

**Analytics** ([`src/storage/queries.py`](src/storage/queries.py)). The table macros
`charger_state_at(t)` and `charger_usage(t0, t1)` live in `sql/events.sql`. Every metric
is computed *as of* the simulated time of the last stored batch, not from "the latest rows".
A replay therefore shows what the network looked like at that moment, even when the store
already holds later data from an earlier pass. Each dashboard query takes 1–11 ms on the
full replay.

| Where | Metric | Kind |
|---|---|---|
| Header, "Stored" | Events and MQTT messages stored | store status |
| Header, "Last 24 h" | Network sessions completed and MWh delivered in the last 24 simulated hours | historical |
| Charger panel, "Now" | Connectors charging, or how long the charger has been idle | current state |
| Charger panel, "Price" | Price in force, and for how long | current state |
| Charger panel, table | Sessions, energy, revenue, average charge time, utilisation: last 24 h vs last 7 days | historical |
| Charger panel, chart | Connector utilisation for each of the last 24 hours | historical |

Utilisation is the connector-time in use divided by the connector-time available, with
sessions clipped to the window. A window that reaches back before the first stored message
starts at that message (the heading then reads "Since …"). A session that never received
its `session_end` stops counting as active after 6 simulated hours, the same rule as the
live state (Task 3).

`python -m src.storage load` replays the generated CSVs through the publisher's own batching
(`iter_batches`) and the same ingest SQL, without MQTT. It is for tests and benchmarks;
its batches are tagged `replay_id = offline-…`.

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
