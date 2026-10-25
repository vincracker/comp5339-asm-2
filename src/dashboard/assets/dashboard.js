// Live charger markers. Dash loads every file in assets/ automatically.
//
// The server sends the full marker list (Store "marker-data") whenever a batch changes it.
// Instead of redrawing all ~430 markers each second, updateMarkers diffs that list against
// the markers already on the map: new chargers are added, changed ones get a new icon or
// tooltip, and the rest are left alone, so tooltips and clicks are not interrupted.
const LABEL_ZOOM = 12;  // below this zoom, markers shrink to coloured dots so NSW stays readable
const STATUS_Z = {unknown: 0, free: 1000, busy: 2000, full: 3000};  // busy chargers draw on top

window.evDashboard = {map: null, layer: null, markers: {}, pending: null};

// Capture the Leaflet map dash-leaflet creates, and switch pin/dot mode on zoom.
L.Map.addInitHook(function () {
    const map = this;
    window.evDashboard.map = map;
    window.evDashboard.layer = L.layerGroup().addTo(map);
    window.evDashboard.markers = {};
    const update = () => map.getContainer().classList.toggle("zoomed-out", map.getZoom() < LABEL_ZOOM);
    map.on("zoomend", update);
    map.whenReady(() => {
        update();
        const pending = window.evDashboard.pending;  // data that arrived before the map existed
        if (pending) {
            window.dash_clientside.ev.updateMarkers(...pending);
        }
    });
});

function iconFor(m, selected) {
    const busy = m.in_use > 0 ? `<span class="pin-use">${m.in_use}/${m.connectors}</span>` : "";
    return L.divIcon({
        className: "pin-wrap",
        html: `<div class="marker marker-${m.status}${selected ? " marker-selected" : ""}">` +
              `<div class="pin">⚡${m.label}${busy}</div><div class="spot"></div></div>`,
        iconSize: null,
        iconAnchor: [0, 0],
    });
}

window.dash_clientside = Object.assign({}, window.dash_clientside, {
    ev: {
        updateMarkers: function (data, selected) {
            const ev = window.evDashboard;
            if (!ev.map || !data) {
                ev.pending = data ? [data, selected] : null;
                return window.dash_clientside.no_update;
            }
            ev.pending = null;
            const seen = new Set();
            for (const m of data) {
                seen.add(m.id);
                const isSelected = m.id === selected;
                const key = `${m.status}|${m.label}|${m.in_use}|${isSelected}`;
                let entry = ev.markers[m.id];
                if (!entry) {
                    const marker = L.marker([m.lat, m.lon], {icon: iconFor(m, isSelected), riseOnHover: true});
                    marker.bindTooltip(m.tooltip, {direction: "top", offset: [0, -28]});
                    marker.on("click", () => window.dash_clientside.set_props("selected", {data: m.id}));
                    marker.addTo(ev.layer);
                    entry = ev.markers[m.id] = {marker: marker, key: key, tooltip: m.tooltip};
                } else if (entry.key !== key) {
                    entry.marker.setIcon(iconFor(m, isSelected));
                    entry.key = key;
                }
                if (entry.tooltip !== m.tooltip) {
                    entry.marker.setTooltipContent(m.tooltip);
                    entry.tooltip = m.tooltip;
                }
                entry.marker.setZIndexOffset(isSelected ? 10000 : STATUS_Z[m.status]);
            }
            for (const id of Object.keys(ev.markers)) {  // filtered out, or cleared by a new replay
                if (!seen.has(id)) {
                    ev.layer.removeLayer(ev.markers[id].marker);
                    delete ev.markers[id];
                }
            }
            return data.length;
        },
    },
});
