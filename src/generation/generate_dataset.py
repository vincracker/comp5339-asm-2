#!/usr/bin/env python3
"""
Synthetic EV fast-charging dataset generator for NSW (COMP5339 Assignment 2).

Produces three time-series files for a set of charging stations:
  prices_hourly.csv      hourly $/kWh price per station (dynamic, driven by real AEMO NSW1 wholesale prices)
  sessions.csv           one row per charging session (start, end, kWh, peak/avg kW, vehicle, ...)
  session_metering.csv   one row per meter event of a charging session (session, voltage, current, kW, kWh, SoC%)
  
Calibration sources (see README.md):
  - ARENA / Energeia "Insights from Ultra-Fast Charging Network Data" (Aug 2023), Evie + Chargefox data:
      avg session 2023: urban 27.3 min / 27.0 kWh, regional 25.9 min / 24.5 kWh;
      majority of starts mid-day; urban weekday evening (6-10pm) bump; weekends ~ weekdays.
  - NRMA published tariffs: ~$0.54-0.60/kWh (<=150 kW), $0.59-0.65 (>=175 kW), $0.41-0.45 AC;
      location-based prices since Apr 2025 up to ~$0.79/kWh.
  - Chargefox/Tesla time-of-use examples: $0.12-1.20/kWh (SA trial), $0.43-0.69 (Tesla).

Only the Python standard library is used.

Example:
  python generate_dataset.py --stations demo_stations.csv --start 2026-10-01 --days 14 --out data
"""
import argparse
import csv
import io
import json
import math
import os
import random
import urllib.request
from datetime import datetime, timedelta

AEMO_URL = "https://www.aemo.com.au/aemo/data/nem/priceanddemand/PRICE_AND_DEMAND_{ym}_{region}.csv"

# Share of session starts per hour of day (index 0 = 00:00-01:00). Shape approximated from the
# ARENA 2023 charts (values read off charts, not published as numbers): low overnight, ramp from
# 7am, plateau 10am-4pm, shoulder into the evening.
BASE_START_PROFILE = [0.6, 0.4, 0.3, 0.3, 0.4, 0.8, 1.6, 3.0, 4.5, 5.6, 6.3, 6.8,
                      7.0, 7.0, 6.8, 6.3, 5.8, 5.4, 5.0, 4.4, 3.6, 2.6, 1.7, 1.0]
# Urban weekday commuter bump 6-10pm (ARENA: "higher utilisation for weekdays during 6-10pm").
URBAN_WEEKDAY_EVENING = {18: 1.35, 19: 1.4, 20: 1.35, 21: 1.25}

# Approximate AU fleet mix: (model, usable battery kWh, max DC kW, nominal pack V, share)
VEHICLES = [
    ("Tesla Model Y",    75, 250, 360, 0.30),
    ("Tesla Model 3",    60, 170, 360, 0.18),
    ("BYD Atto 3",       60,  88, 400, 0.10),
    ("BYD Seal",         82, 150, 400, 0.06),
    ("MG4",              64, 140, 400, 0.08),
    ("Hyundai Ioniq 5",  77, 230, 700, 0.06),
    ("Kia EV6",          77, 230, 700, 0.05),
    ("Polestar 2",       78, 155, 400, 0.05),
    ("Nissan Leaf",      40,  46, 360, 0.04),
    ("BYD Dolphin",      60,  88, 400, 0.08),
]

# Charger current limit (A) by rated power.
def charger_max_amps(max_kw):
    if max_kw <= 50:
        return 125
    if max_kw <= 75:
        return 200
    if max_kw <= 150:
        return 375
    return 500


def poisson(lam, rng):
    # Knuth; lam is small here (< ~3 per hour per connector).
    if lam <= 0:
        return 0
    L, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= L:
            return k
        k += 1


def month_range(start, end):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield f"{y:04d}{m:02d}"
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def load_wholesale_hourly(start, end, region, cache_dir):
    """Return {hour_start_datetime: $/kWh} from AEMO 5-minute RRP (period-ending timestamps)."""
    os.makedirs(cache_dir, exist_ok=True)
    buckets = {}
    for ym in month_range(start, end):
        path = os.path.join(cache_dir, f"PRICE_AND_DEMAND_{ym}_{region}.csv")
        if not os.path.exists(path):
            req = urllib.request.Request(AEMO_URL.format(ym=ym, region=region),
                                         headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=60) as r, open(path, "wb") as f:
                f.write(r.read())
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                ts = datetime.strptime(row["SETTLEMENTDATE"], "%Y/%m/%d %H:%M:%S")
                hour = (ts - timedelta(minutes=5)).replace(minute=0, second=0)  # period-ending -> start
                buckets.setdefault(hour, []).append(float(row["RRP"]) / 1000.0)
    return {h: sum(v) / len(v) for h, v in buckets.items()}


def station_base_price(st, rng):
    """NRMA-style location-based flat price for the station (non-member, $/kWh)."""
    if st["max_kw"] >= 175:
        return round(rng.uniform(0.65, 0.79), 2)
    if st["max_kw"] >= 50:
        return round(rng.uniform(0.60, 0.75), 2)
    return 0.45


def dynamic_price(base, wholesale, wholesale_mean, hour, args):
    """Hypothetical dynamic tariff: base + pass-through of wholesale deviation + evening peak adder."""
    p = base + args.pass_through * (wholesale - wholesale_mean)
    if 16 <= hour < 21:
        p += args.peak_adder
    return round(min(max(p, args.price_floor), args.price_cap), 2)


def simulate_session(rng, st, conn_id, sess_id, t0, price, args):
    """Simulate one charging session minute by minute. Returns (session_row, meter_events, end_time)."""
    model, batt, veh_kw, nom_v, _ = rng.choices(VEHICLES, weights=[v[4] for v in VEHICLES])[0]
    soc = rng.uniform(0.20, 0.55)
    target = min(0.98, max(soc + 0.12, rng.gauss(0.74, 0.08)))
    derate = rng.uniform(0.40, 0.80)  # cold pack, shared power cabinet, older charger firmware
    max_a = charger_max_amps(st["max_kw"])
    t, kwh, peak_kw, meters = t0, 0.0, 0.0, []
    while soc < target and (t - t0).total_seconds() < 4 * 3600:
        volts = nom_v * (0.88 + 0.22 * soc)                       # pack voltage rises with SoC
        taper = 1.0 if soc < 0.5 else max(0.15, 1.0 - 2.2 * (soc - 0.5))
        ramp = min(1.0, 0.4 + 0.3 * (t - t0).total_seconds() / 60)  # handshake/ramp-up first minutes
        kw = min(st["max_kw"], veh_kw * taper * derate, volts * max_a / 1000.0) * ramp
        kw *= rng.uniform(0.97, 1.02)
        amps = kw * 1000.0 / volts
        step_kwh = kw / 60.0
        kwh += step_kwh
        soc = min(1.0, soc + step_kwh / batt)
        peak_kw = max(peak_kw, kw)
        t += timedelta(minutes=1)
        if args.meter_every and int((t - t0).total_seconds() // 60) % args.meter_every == 0:
            meters.append({"ts": t.isoformat(), "type": "meter", "station_id": st["station_id"],
                           "connector_id": conn_id, "session_id": sess_id,
                           "voltage_v": round(volts, 1), "current_a": round(amps, 1),
                           "power_kw": round(kw, 2), "energy_kwh": round(kwh, 3), "soc_pct": round(soc * 100, 1)})
    charge_end = t
    unplug = charge_end + timedelta(seconds=round(60 * rng.expovariate(1 / args.mean_idle_min)))
    minutes = (charge_end - t0).total_seconds() / 60
    row = {"session_id": sess_id, "station_id": st["station_id"], "connector_id": conn_id,
           "site_type": st["site_type"], "vehicle_model": model,
           "start_ts": t0.isoformat(), "charge_end_ts": charge_end.isoformat(), "end_ts": unplug.isoformat(),
           "charge_minutes": round(minutes, 1), "energy_kwh": round(kwh, 2),
           "avg_kw": round(kwh / (minutes / 60), 1) if minutes else 0, "peak_kw": round(peak_kw, 1),
           "price_per_kwh": price, "cost_aud": round(kwh * price, 2)}
    return row, meters, unplug


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stations", default="demo_stations.csv")
    ap.add_argument("--start", default="2026-10-01")
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--region", default="NSW1")
    ap.add_argument("--out", default="data")
    ap.add_argument("--seed", type=int, default=5339)
    ap.add_argument("--sessions-per-connector-day", type=float, nargs=2, default=[4.0, 3.0],
                    metavar=("URBAN", "REGIONAL"))
    ap.add_argument("--meter-every", type=int, default=1, help="minutes between meter readings (0 = none)")
    ap.add_argument("--mean-idle-min", type=float, default=3.0, help="mean minutes plugged-in after charging")
    ap.add_argument("--pass-through", type=float, default=1.0, help="share of wholesale deviation passed on")
    ap.add_argument("--peak-adder", type=float, default=0.10, help="$/kWh added 4pm-9pm")
    ap.add_argument("--price-floor", type=float, default=0.20)
    ap.add_argument("--price-cap", type=float, default=1.20)
    ap.add_argument("--elasticity", type=float, default=-0.5, help="demand response to price/base ratio")
    ap.add_argument("--static-prices", action="store_true", help="flat NRMA-style prices, no wholesale link")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    start = datetime.fromisoformat(args.start)
    end = start + timedelta(days=args.days)
    os.makedirs(args.out, exist_ok=True)

    with open(args.stations, newline="") as f:
        stations = [dict(r, lat=float(r["lat"]), lon=float(r["lon"]), connectors=int(r["connectors"]),
                         max_kw=float(r["max_kw"])) for r in csv.DictReader(f)]
    for st in stations:
        st["base_price"] = station_base_price(st, rng)

    wholesale = {} if args.static_prices else load_wholesale_hourly(start, end, args.region,
                                                                   os.path.join(args.out, "aemo_cache"))
    in_range = [v for h, v in wholesale.items() if start <= h < end]
    w_mean = sum(in_range) / len(in_range) if in_range else 0.0

    sessions, meter_events, price_rows = [], [], []
    hours = [start + timedelta(hours=i) for i in range(args.days * 24)]
    prof_total = sum(BASE_START_PROFILE)

    for st in stations:
        busy_until = [start] * st["connectors"]
        per_day = args.sessions_per_connector_day[0 if st["site_type"] == "urban" else 1]
        for h in hours:
            w = wholesale.get(h, w_mean)
            price = st["base_price"] if args.static_prices else dynamic_price(st["base_price"], w, w_mean, h.hour, args)
            price_rows.append({"ts": h.isoformat(), "station_id": st["station_id"], "price_per_kwh": price,
                               "wholesale_per_kwh": round(w, 4)})

            weight = BASE_START_PROFILE[h.hour] / prof_total
            if st["site_type"] == "urban" and h.weekday() < 5:
                weight *= URBAN_WEEKDAY_EVENING.get(h.hour, 1.0)
            lam = per_day * weight * (price / st["base_price"]) ** args.elasticity
            for c in range(st["connectors"]):
                for _ in range(poisson(lam, rng)):
                    t0 = (h + timedelta(seconds=rng.uniform(0, 3600))).replace(second=0, microsecond=0)
                    if t0 < busy_until[c]:
                        continue  # connector occupied -> driver goes elsewhere
                    sid = f"{st['station_id']}-{c + 1}-{t0:%Y%m%d%H%M}"
                    row, meters, unplug = simulate_session(rng, st, c + 1, sid, t0, price, args)
                    busy_until[c] = unplug
                    sessions.append(row)
                    meter_events.extend(meters)

    meter_events.sort(key=lambda e: (e["ts"], e["session_id"]))

    def write_csv(name, rows):
        with open(os.path.join(args.out, name), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)

    write_csv("stations.csv", [{k: st[k] for k in ("station_id", "name", "lat", "lon", "site_type",
                                                   "connectors", "max_kw", "base_price")} for st in stations])
    write_csv("prices_hourly.csv", price_rows)
    write_csv("sessions.csv", sessions)
    write_csv("session_metering.csv", meter_events)

    # Output of generated data and calibration check against ARENA 2023 averages.
    print(f"{len(stations)} stations, {len(price_rows)} hourly prices, {len(sessions)} charging-sessions, {len(meter_events)} meter-events -> {args.out}/")
    for kind, ref in (("urban", (27.3, 27.0)), ("regional", (25.9, 24.5))):
        s = [r for r in sessions if r["site_type"] == kind]
        if s:
            m = sum(r["charge_minutes"] for r in s) / len(s)
            e = sum(r["energy_kwh"] for r in s) / len(s)
            print(f"  {kind:8s} mean {m:5.1f} min / {e:5.1f} kWh   (ARENA 2023: {ref[0]} min / {ref[1]} kWh)")
    ps = [r["price_per_kwh"] for r in price_rows]
    print(f"  price $/kWh min {min(ps):.2f}  mean {sum(ps) / len(ps):.2f}  max {max(ps):.2f}")


if __name__ == "__main__":
    main()
