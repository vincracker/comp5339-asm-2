# Generated dataset (Task 1)

Synthetic EV charging data for **all 432 DC chargers from Assignment 1**, covering
**1–14 October 2026** (Sydney local time, no timezone in the timestamps). Prices follow real
AEMO NSW wholesale electricity prices for those two weeks.

Do not edit these files by hand. Rebuild them from `code/` with:

```
python main.py
```

The output is deterministic (generator seed 5339), so every teammate gets identical files.

---

## Files at a glance

| File | Rows | Size | One row is… | Sorted by |
|---|---|---|---|---|
| `stations.csv` | 432 | 64 KB | a charger | `station_id` |
| `prices_hourly.csv` | 145,152 | 14 MB | a charger's price for one hour | `station_id`, then `ts` |
| `sessions.csv` | 76,330 | 19 MB | a charging session | `station_id`, then start time |
| `session_metering.csv` | 2,305,283 | 464 MB | a 1-minute meter reading | `ts`, then `session_id` |
| `stations_input.csv` | 432 | 62 KB | generator input (see "How it was made") | `station_id` |
| `aemo_cache/` | 1 file | 221 KB | raw AEMO 5-minute wholesale prices | time |

**Watch out:** only `session_metering.csv` is in time order. To publish an ordered event
stream (Task 2), merge all files and sort by event time first.

## How the files link together

```
stations.station_id ──┬── prices_hourly.station_id
  (= Assignment 1     ├── sessions.station_id
   charger_record_id) └── session_metering.station_id

sessions.session_id ───── session_metering.session_id
```

`station_id` **is** the Assignment 1 primary key `charger.charger_record_id`
(e.g. `chr_001f6e5e…`). Join straight to the database, with no lookup table:

```sql
SELECT * FROM 'data/generated/sessions.csv' s
JOIN charger c ON c.charger_record_id = s.station_id;
```

Use `data/processed/ev_charging.duckdb` for this. It has the corrected coordinates.

## From files to the four MQTT event types

The assignment's four event types are not stored as four files. They come from the files
like this:

| Event type | Source | Event time | One per… |
|---|---|---|---|
| `price_update` | `prices_hourly.csv` | `ts` | hour **in which the price changed** (skip a row whose `price_per_kwh` equals the station's previous hour). Leaves 100,656 of 145,152 rows. |
| `session_start` | `sessions.csv` | `start_ts` | session (76,330) |
| `meter` | `session_metering.csv` | `ts` | reading (2,305,283) |
| `session_end` | `sessions.csv` | `end_ts` | session (76,330); the totals (`energy_kwh`, `cost_aud`, …) are only final at this point |

This makes about **2.56 million events** in total. Because messages must be at least
0.1 s apart, publishing all of them would take about 71 hours. Plan to replay a time
window.

---

## Column reference

### `stations.csv`

| Column | Type | Meaning |
|---|---|---|
| `station_id` | text | Assignment 1 `charger_record_id` |
| `name` | text | Assignment 1 `station_name`, or `station_address` when the name is empty (most rows) |
| `lat`, `lon` | float | WGS84 coordinates (corrected; see "How it was made") |
| `site_type` | `urban` / `regional` | 282 urban, 150 regional; drives session volume |
| `connectors` | int | Connectors that can charge at the same time (= Assignment 1 `number_of_plugs`); 1,652 in total |
| `max_kw` | float | Charger rating in kW |
| `base_price` | $/kWh | The station's normal price, drawn at random by the generator ($0.45–0.79). Hourly prices move around it. |

### `prices_hourly.csv`

| Column | Type | Meaning |
|---|---|---|
| `ts` | timestamp | Start of the hour (`2026-10-01T00:00:00` … `2026-10-14T23:00:00`) |
| `station_id` | text | → `stations` |
| `price_per_kwh` | $/kWh | Price the driver pays this hour. Range $0.28–1.20, mean $0.63. |
| `wholesale_per_kwh` | $/kWh | Real AEMO NSW1 wholesale price, averaged over the hour. Can be **negative** (−0.008) or spike (up to 9.52). |

The price is calculated as:

```
price = base_price + (wholesale − mean wholesale over the 2 weeks) + $0.10 if 4pm–9pm
        then clipped to $0.20–1.20 and rounded to the cent
```

Rounding to the cent is why many hours repeat the previous price. Those are not new events.

### `sessions.csv`

| Column | Type | Meaning |
|---|---|---|
| `session_id` | text | `<station_id>-<connector_id>-<YYYYMMDDHHMM of start>`; unique |
| `station_id` | text | → `stations` |
| `connector_id` | int | 1 … `connectors` of that station |
| `site_type` | text | Copied from the station |
| `vehicle_model` | text | E.g. Tesla Model Y (30%), Tesla Model 3, BYD Atto 3, MG4, … (10 models) |
| `start_ts` | timestamp | Car plugs in and charging starts |
| `charge_end_ts` | timestamp | Charging stops (target battery level reached) |
| `end_ts` | timestamp | Car unplugs; on average 3 minutes after `charge_end_ts`. **The connector is busy until here.** |
| `charge_minutes` | float | `charge_end_ts − start_ts` |
| `energy_kwh` | float | Energy delivered |
| `avg_kw`, `peak_kw` | float | Average and peak charging power |
| `price_per_kwh` | $/kWh | The station's price in the hour the session started; fixed for the whole session |
| `cost_aud` | $ | `energy_kwh × price_per_kwh` |

Averages: urban 31.7 min / 25.3 kWh / $15.94, regional 27.6 min / 25.3 kWh / $16.80.
A connector never has two overlapping sessions.

### `session_metering.csv`

| Column | Type | Meaning |
|---|---|---|
| `ts` | timestamp | Reading time; every minute while charging |
| `type` | text | Always `meter` |
| `station_id`, `connector_id`, `session_id` | | → `stations`, `sessions` |
| `voltage_v` | V | Battery pack voltage (rises as it fills; 800 V cars ≈ 650–770 V) |
| `current_a` | A | Charging current |
| `power_kw` | kW | Charging power; ramps up in the first minutes, tapers above 50% battery |
| `energy_kwh` | kWh | **Running total** since the session started; use it for the live charging curve |
| `soc_pct` | % | Battery state of charge |

Guaranteed relationships, all checked:
- The first reading is exactly 1 minute after `start_ts`.
- The last reading is at `charge_end_ts`, and its `energy_kwh` equals the session's
  `energy_kwh`.
- No readings fall between `charge_end_ts` and `end_ts`, because the car is plugged in but idle.

### `aemo_cache/PRICE_AND_DEMAND_202610_NSW1.csv`

AEMO's published NSW data for October 2026, one row per 5 minutes: `REGION`,
`SETTLEMENTDATE` (end of the 5-minute period), `TOTALDEMAND` (MW), `RRP` ($/MWh),
`PERIODTYPE`. The generator downloads it once and reuses the saved copy. Keep it: it is the
evidence that prices come from real AEMO data, and AEMO keeps revising the current month.

### `stations_input.csv`

Same as `stations.csv` without `base_price`. Built from the Assignment 1 database and fed
to the generator. You won't normally need it.

---

## Time range edge cases

- Prices cover 1 Oct 00:00 to 14 Oct 23:00 (336 hours × 432 stations).
- Sessions **start** up to 14 Oct 23:59 but can **end** up to 15 Oct 00:59, so the last meter
  readings fall after the last price hour.

## How it was made

1. **Coordinate fix.** All 23 "TfNSW Regional" chargers in the TfNSW source had their
   coordinates shuffled among themselves (the Wilcannia charger was placed at Armidale).
   `src/corrections/coordinates.py` fixes them in `data/processed/ev_charging.duckdb`.
   The details are in the main `code/README.md`.
2. **Station list.** `src/generation/stations.py` exports every charger from that database
   as `stations_input.csv`:
   - `site_type` is urban for Greater Sydney, Newcastle and Wollongong, otherwise regional.
   - The 5 chargers with no rating ("2x350kW & 2x175kW") get 350 kW.
3. **Generator.** The provided course script, `src/generation/generate_dataset.py` (an
   unmodified copy), runs with `--start 2026-10-01 --days 14` and default settings.
   - Sessions arrive at random, mostly at midday, with an extra weekday-evening peak at
     urban sites. Urban connectors average about 4 sessions a day and regional ones about 3.
   - Fewer drivers come when the price is high. A driver who finds the connector busy
     leaves.
   - Charging is simulated minute by minute from each car's battery and charging limits.
