"""Dash layout and callbacks for the live EV charger map. Task 3.

The page polls LiveState once a second (dcc.Interval). Marker data is only resent when the
state version changes, i.e. a new batch arrived; assets/dashboard.js then updates just the
markers that changed. Each marker shows the charger's latest price, coloured by how many of
its connectors are in use.

With an EventStore (Task 4), the header and the charger panel also show metrics queried from
the stored events (src/storage/queries.py): the current state of the charger and its usage
over the last 24 hours and 7 days, as of the last stored simulated time.
"""
from datetime import datetime, timedelta
from html import escape
from pathlib import Path

import dash_leaflet as dl
import plotly.graph_objects as go
from dash import ClientsideFunction, Dash, Input, Output, State, dcc, html, no_update

from .state import LiveState, status
from ..storage import queries
from ..storage.event_store import EventStore

NSW_CENTER = (-33.2, 148.6)
# Categorical slots for one line per concurrent session (fixed order, see dataviz palette).
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
STATUS_LABELS = {"free": "Available", "busy": "Partly in use", "full": "All connectors in use",
                 "unknown": "Price not received yet"}
METRICS = {"energy_kwh": "Energy delivered (kWh)", "power_kw": "Power (kW)", "soc_pct": "State of charge (%)"}


def create_app(state: LiveState, store: EventStore | None = None) -> Dash:
    app = Dash(__name__, title="NSW EV Charging Live", assets_folder=str(Path(__file__).with_name("assets")))
    operators = sorted({c["operator"] for c in state.chargers.values() if c["operator"]})
    app.layout = html.Div(className="page", children=[
        html.Header(className="topbar", children=[
            html.Div(className="brand", children=[
                html.Span("⚡", className="brand-icon"),
                html.Div([html.H1("NSW EV Charging Live"),
                          html.P("Simulated prices and charging sessions over MQTT")]),
            ]),
            html.Div(id="stats", className="stats"),
        ]),
        html.Main(className="body", children=[
            html.Section(className="map-wrap", children=[
                dl.Map(id="map", center=NSW_CENTER, zoom=6, className="map", children=[
                    dl.TileLayer(
                        url="https://tile.openstreetmap.org/{z}/{x}/{y}.png", maxZoom=19,
                        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
                                    ' contributors'),
                ]),
                html.Div(className="map-controls", children=[
                    dcc.Dropdown(id="operator-filter", options=operators, multi=True,
                                 placeholder="All operators", className="operator-filter"),
                ]),
                html.Div(className="legend", children=[
                    html.Div([html.Span(className=f"dot dot-{k}"), v]) for k, v in STATUS_LABELS.items()
                ]),
            ]),
            html.Aside(className="panel", children=[
                html.Div(id="panel-info"),
                # Kept on the page permanently, so the metric choice and graph zoom survive ticks.
                html.Div(id="live-section", style={"display": "none"}, children=[
                    html.H3("Active charging sessions"),
                    dcc.RadioItems(id="metric", value="energy_kwh", inline=True, className="metric",
                                   options=[{"label": v.split(" (")[0], "value": k} for k, v in METRICS.items()]),
                    dcc.Graph(id="session-graph", config={"displayModeBar": False}, className="graph"),
                    html.Div(id="session-cards"),
                ]),
                html.Div(id="panel-footer"),
                # Task 4: metrics from the event store; kept on the page like the live graph.
                html.Div(id="history-section", style={"display": "none"}, children=[
                    html.Div(id="history-info"),
                    html.Div("Connector utilisation by hour, last 24 h", className="muted small chart-title"),
                    dcc.Graph(id="history-graph", config={"displayModeBar": False}, className="graph"),
                ]),
            ]),
        ]),
        dcc.Interval(id="tick", interval=1000),
        dcc.Store(id="selected"),
        dcc.Store(id="map-version"),
        dcc.Store(id="marker-data"),
        dcc.Store(id="marker-count"),
        dcc.Store(id="history-key"),
    ])

    @app.callback(Output("marker-data", "data"), Output("stats", "children"), Output("map-version", "data"),
                  Input("tick", "n_intervals"), Input("operator-filter", "value"),
                  State("map-version", "data"))
    def refresh_map(_, operator_filter, seen):
        summary = state.summary()
        key = [summary["version"], operator_filter]
        if key == seen:
            return no_update, no_update, no_update
        markers = state.markers()
        if operator_filter:
            markers = [m for m in markers if m["operator"] in operator_filter]
        return [_marker(m) for m in markers], _stats(summary) + _store_stats(store), key

    # Drawn in the browser (assets/dashboard.js), which also sets "selected" on marker click.
    app.clientside_callback(ClientsideFunction("ev", "updateMarkers"), Output("marker-count", "data"),
                            Input("marker-data", "data"), Input("selected", "data"))

    @app.callback(Output("panel-info", "children"), Output("live-section", "style"),
                  Output("session-cards", "children"), Output("session-graph", "figure"),
                  Output("panel-footer", "children"),
                  Input("tick", "n_intervals"), Input("selected", "data"), Input("metric", "value"))
    def refresh_panel(_, station_id, metric):
        detail = state.station_detail(station_id) if station_id else None
        if detail is None:
            hidden = {"display": "none"}
            if station_id in state.chargers:  # selected, then cleared by a new replay
                c = state.chargers[station_id]
                info = [html.H2(c["name"]),
                        html.P("Waiting for this charger's first event in the current replay.", className="muted")]
            else:
                info = _intro()
            return info, hidden, [], _figure([], metric), []
        sessions = detail["sessions"]
        cards = [_session_card(s, i, detail["price"]) for i, s in enumerate(sessions)]
        return (_info(detail), {} if sessions else {"display": "none"}, cards,
                _figure(sessions, metric), _footer(detail))

    @app.callback(Output("history-section", "style"), Output("history-info", "children"),
                  Output("history-graph", "figure"), Output("history-key", "data"),
                  Input("tick", "n_intervals"), Input("selected", "data"), State("history-key", "data"))
    def refresh_history(_, station_id, seen):
        if store is None or store.clock is None or station_id not in state.chargers:
            # Hide once; the layout starts hidden, so nothing to send while it stays that way.
            return ({"display": "none"}, [], _history_figure([]), None) if seen else (no_update,) * 4
        key = [station_id, store.clock.isoformat()]
        if key == seen:  # nothing new stored since this browser's last tick
            return no_update, no_update, no_update, no_update
        cur = store.cursor()
        try:
            current = queries.charger_current(cur, station_id, store.clock)
            history = queries.charger_history(cur, station_id, store.clock)
            hourly = queries.charger_hourly_utilisation(cur, station_id, store.clock)
        finally:
            cur.close()
        return {}, _history(current, history, store.clock), _history_figure(hourly), key

    return app


def _marker(m: dict) -> dict:
    price = f"${m['price']:.2f}" if m["price"] is not None else "–"
    return {
        "id": m["station_id"], "lat": m["lat"], "lon": m["lon"], "status": m["status"], "label": price,
        "in_use": m["in_use"], "connectors": m["connectors"],
        "tooltip": f"<b>{escape(m['name'])}</b><br>{escape(m['operator'] or 'Unknown operator')}<br>"
                   f"{price}/kWh · {m['in_use']} of {m['connectors']} connectors in use",
    }


def _stats(s: dict) -> list:
    clock = datetime.fromisoformat(s["clock"]).strftime("%a %d %b %Y, %H:%M:%S") if s["clock"] else "waiting…"
    link = html.Span([html.Span(className="dot " + ("dot-free" if s["connected"] else "dot-full")),
                      "Live" if s["connected"] else "Offline"], title=s["note"])
    tiles = [
        ("Stream", link), ("Simulated time", clock),
        ("Chargers on map", f"{s['on_map']} / {s['total_chargers']}"),
        ("Charging now", f"{s['active_sessions']} sessions at {s['busy_chargers']} chargers"),
    ]
    out = [html.Div(className="stat", children=[html.Span(k, className="stat-label"),
                                                 html.Span(v, className="stat-value")]) for k, v in tiles]
    if s["gaps"]:
        out.append(html.Div(f"{s['gaps']} batches missed", className="stat warn", title="Sequence gaps in this replay"))
    return out


def _row(label, value):
    return html.Div(className="row", children=[html.Span(label, className="row-label"),
                                               html.Span(value if value not in (None, "") else "Not recorded")])


def _intro() -> list:
    return [html.Div(className="empty", children=[
        html.H2("Select a charger"),
        html.P("Markers appear as the stream reports each charger. Each shows the latest price per kWh, "
               "coloured by connector occupancy. Click one for details and live session curves."),
    ])]


def _info(d: dict) -> list:
    if d["price"] is not None:
        trend = ""
        if d["previous_price"] is not None and d["previous_price"] != d["price"]:
            trend = "▲ " if d["price"] > d["previous_price"] else "▼ "
        price = html.Div(className="price", children=[
            html.Span(f"${d['price']:.2f}", className="price-value"), html.Span(" /kWh", className="muted"),
            html.Div(f"{trend}set {_time(d['price_ts'])} · wholesale ${d['wholesale']:.4f}/kWh",
                     className="muted small"),
        ])
    else:
        price = html.P("No price update received yet in this replay.", className="muted")
    occ = status(d["in_use"], d["connectors"], d["price"])
    return [
        html.H2(d["name"]),
        price,
        html.Div(className="occupancy", children=[
            html.Span(className=f"dot dot-{occ}"), f"{d['in_use']} of {d['connectors']} connectors in use"]),
        html.Div(className="details", children=[
            _row("Address", d["address"]),
            _row("Operator", d["operator"]),
            _row("Plug types", ", ".join(d["plug_types"])),
            _row("Rating", d["rating"]),
            _row("Region", f"{d['lga']}, {d['sa4']}" if d["lga"] else d["sa4"]),
            _row("Sessions completed", f"{d['completed']} since the dashboard joined this replay"),
        ]),
    ]


def _footer(d: dict) -> list:
    out = []
    if not d["sessions"]:
        out += [html.H3("No active session"),
                html.P("A live curve appears here when a vehicle starts charging.", className="muted")]
    if d["last_session"]:
        last = d["last_session"]
        out += [html.H3("Last completed session"),
                html.P(f"{last['vehicle_model']} on connector {last['connector_id']}: {last['energy_kwh']:.1f} kWh "
                       f"in {last['charge_minutes']:.0f} min, ${last['cost_aud']:.2f} at "
                       f"${last['price_per_kwh']:.2f}/kWh (ended {_time(last['end_ts'])})")]
    return out


def _session_card(s: dict, i: int, station_price: float | None) -> html.Div:
    last = s["readings"][-1] if s["readings"] else None
    price = s["price"] if s["price"] is not None else station_price
    started = (f"joined at {_time(s['start_ts'])} (session already running)" if s["joined_mid_session"]
               else f"started {_time(s['start_ts'])}")
    figures = []
    if last:
        figures = [f"{last['energy_kwh']:.1f} kWh", f"{last['power_kw']:.0f} kW", f"{last['soc_pct']:.0f}% SoC"]
        if price is not None and not s["joined_mid_session"]:
            figures.append(f"≈ ${last['energy_kwh'] * price:.2f}")
    return html.Div(className="session", children=[
        html.Span(className="swatch", style={"background": SERIES[i % len(SERIES)]}),
        html.Div([
            html.Div(f"Connector {s['connector_id']} · {s['vehicle_model'] or 'Unknown vehicle'}", className="session-title"),
            html.Div(started, className="muted small"),
            html.Div(" · ".join(figures) if figures else "Waiting for the first meter reading", className="small"),
        ]),
    ])


def _figure(sessions: list[dict], metric: str) -> go.Figure:
    fig = go.Figure()
    for i, s in enumerate(sessions):
        r = s["readings"]
        fig.add_trace(go.Scatter(
            x=[p["ts"] for p in r], y=[p[metric] for p in r], mode="lines+markers",
            name=f"Connector {s['connector_id']}", line={"width": 2, "color": SERIES[i % len(SERIES)]},
            marker={"size": 4}, hovertemplate="%{x|%H:%M}<br>%{y:.1f}<extra>%{fullData.name}</extra>"))
    fig.update_layout(
        margin={"l": 44, "r": 12, "t": 8, "b": 36}, height=230, paper_bgcolor="#fcfcfb", plot_bgcolor="#fcfcfb",
        font={"family": "system-ui, -apple-system, Segoe UI, sans-serif", "size": 11, "color": "#52514e"},
        showlegend=len(sessions) > 1, legend={"orientation": "h", "y": -0.25},
        hovermode="x unified", uirevision=metric,
        yaxis={"title": METRICS[metric], "gridcolor": "#e1e0d9", "zeroline": False, "rangemode": "tozero"},
        xaxis={"gridcolor": "#e1e0d9", "tickformat": "%H:%M", "linecolor": "#c3c2b7"},
    )
    return fig


def _time(ts: str | None) -> str:
    return datetime.fromisoformat(ts).strftime("%d %b %H:%M") if ts else "?"


# ---- Task 4: metrics from the event store ---------------------------------------------------

def _store_stats(store: EventStore | None) -> list:
    """Header tiles: what the store holds, and network-wide usage over the last 24 h."""
    if store is None or store.clock is None:
        return []
    cur = store.cursor()
    try:
        counts = queries.stored_counts(cur)
        net = queries.network_summary(cur, store.clock)
    finally:
        cur.close()
    stored = html.Span(f"{counts['events']:,} events",
                       title=f"{counts['batches']:,} MQTT messages in DuckDB; last write {store.last_flush_ms:.0f} ms")
    return [html.Div(className="stat", children=[html.Span(k, className="stat-label"), html.Span(v, className="stat-value")])
            for k, v in (("Stored", stored),
                         ("Last 24 h", f"{net['sessions']:,} sessions · {net['energy_kwh'] / 1000:,.1f} MWh"))]


def _history(cur: dict | None, hist: dict, clock: datetime) -> list:
    out = [html.H3("From the event store"),
           html.P(f"As of {clock:%d %b %H:%M:%S} (simulated), from the events stored in DuckDB.",
                  className="muted small")]
    if cur:
        if cur["active_sessions"]:
            idle = f"{cur['active_sessions']} of {cur['connectors']} connectors charging"
        elif cur["last_session_end"]:
            idle = f"Idle for {_minutes((clock - cur['last_session_end']).total_seconds() / 60)}"
        else:
            idle = "No session stored yet"
        price = (f"${cur['price_per_kwh']:.2f}/kWh for {_minutes((clock - cur['price_since']).total_seconds() / 60)}"
                 if cur["price_per_kwh"] is not None else None)
        out.append(html.Div(className="details", children=[_row("Now", idle), _row("Price", price)]))
    day, week = hist["day"], hist["week"]
    rows = [("Sessions", "sessions_completed", lambda v: f"{v:,}"),
            ("Energy", "energy_kwh", lambda v: f"{v:,.0f} kWh"),
            ("Revenue", "revenue_aud", lambda v: f"${v:,.0f}"),
            ("Avg charge", "avg_charge_minutes", lambda v: f"{v:.0f} min"),
            ("Utilisation", "utilisation", lambda v: f"{v:.0%}")]

    def cell(d, key, fmt):
        v = d.get(key)
        return fmt(v) if v is not None else "–"

    table = html.Table(className="usage", children=[
        html.Thead(html.Tr([html.Th(""), html.Th(_window(hist["day_from"], clock, timedelta(days=1), "Last 24 h")),
                            html.Th(_window(hist["week_from"], clock, timedelta(days=7), "Last 7 days"))])),
        html.Tbody([html.Tr([html.Td(label), html.Td(cell(day, k, f)), html.Td(cell(week, k, f))])
                    for label, k, f in rows]),
    ])
    out.append(table)
    pr = hist["price_range"]
    if pr and pr["low"] is not None:
        span = _window(hist["day_from"], clock, timedelta(days=1), "last 24 h")
        out.append(html.P(f"Price range, {span[0].lower() + span[1:]}: ${pr['low']:.2f} – ${pr['high']:.2f}/kWh",
                          className="muted small"))
    return out


def _history_figure(hourly: list) -> go.Figure:
    fig = go.Figure(go.Bar(x=[h for h, _ in hourly], y=[u * 100 for _, u in hourly], marker_color=SERIES[0],
                           hovertemplate="%{x|%d %b %H:00}<br>%{y:.0f}% of connector time<extra></extra>"))
    fig.update_layout(
        margin={"l": 44, "r": 12, "t": 8, "b": 36}, height=170, paper_bgcolor="#fcfcfb", plot_bgcolor="#fcfcfb",
        font={"family": "system-ui, -apple-system, Segoe UI, sans-serif", "size": 11, "color": "#52514e"},
        bargap=0.15, showlegend=False,
        yaxis={"title": "In use (%)", "gridcolor": "#e1e0d9", "zeroline": False, "rangemode": "tozero"},
        xaxis={"tickformat": "%H:00", "linecolor": "#c3c2b7"},
    )
    return fig


def _window(t0: datetime, clock: datetime, span: timedelta, label: str) -> str:
    """Column heading; names the start when storage began within the window."""
    return label if clock - t0 >= span else f"Since {t0:%d %b %H:%M}"


def _minutes(m: float) -> str:
    return f"{m:.0f} min" if m < 90 else f"{m / 60:.1f} h"
