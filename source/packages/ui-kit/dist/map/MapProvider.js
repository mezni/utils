import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { MapContainer, TileLayer, useMapEvents } from "react-leaflet";
import { useEffect, useRef } from "react";
import L from "leaflet";
import styles from "./MapProvider.module.css";
function ViewportListener({ onViewportChange }) {
    const prevRef = useRef(null);
    useMapEvents({
        moveend(e) {
            if (!onViewportChange)
                return;
            const map = e.target;
            const c = map.getCenter();
            const center = [c.lat, c.lng];
            const zoom = map.getZoom();
            const prev = prevRef.current;
            if (!prev || prev.center[0] !== center[0] || prev.center[1] !== center[1] || prev.zoom !== zoom) {
                prevRef.current = { center, zoom };
                onViewportChange(center, zoom);
            }
        },
    });
    return null;
}
function FixLeafletIcons() {
    useEffect(() => {
        const icon = L.Icon.Default;
        delete icon.prototype._getIconUrl;
        icon.mergeOptions({
            iconRetinaUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png",
            iconUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png",
            shadowUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png",
        });
    }, []);
    return null;
}
export function MapProvider({ center, zoom, children, onViewportChange }) {
    return (_jsx("div", { className: styles.wrapper, "data-testid": "map-provider", children: _jsxs(MapContainer, { center: center, zoom: zoom, className: styles.map, scrollWheelZoom: true, children: [_jsx(FixLeafletIcons, {}), _jsx(TileLayer, { attribution: '\u00A9 <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>', url: "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" }), _jsx(ViewportListener, { onViewportChange: onViewportChange }), children] }) }));
}
//# sourceMappingURL=MapProvider.js.map