import { useEffect, useRef } from "react";
import { useMap } from "react-leaflet";
import L from "leaflet";
import "leaflet.markercluster";
export function StationMarkerLayer({ stations }) {
    const map = useMap();
    const clusterGroupRef = useRef(null);
    useEffect(() => {
        if (stations.length === 0)
            return;
        if (!clusterGroupRef.current) {
            clusterGroupRef.current = L.markerClusterGroup({
                maxClusterRadius: 50,
                spiderfyOnMaxZoom: true,
                disableClusteringAtZoom: 10,
                chunkedLoading: true,
            });
            map.addLayer(clusterGroupRef.current);
        }
        const group = clusterGroupRef.current;
        group.clearLayers();
        stations.forEach((s) => {
            const marker = L.marker([s.lat, s.lon]);
            marker.bindTooltip(s.name ?? "Unnamed Station", {
                direction: "top",
                offset: L.point(0, -10),
            });
            marker.bindPopup(`
        <div style="font-family: Inter, sans-serif; font-size: 13px; line-height: 1.5;">
          <strong>${s.name ?? "Unnamed Station"}</strong><br/>
          ID: ${s.station_id}<br/>
          ${s.distance_km.toFixed(2)} km away
        </div>
      `);
            group.addLayer(marker);
        });
        return () => {
            if (clusterGroupRef.current) {
                map.removeLayer(clusterGroupRef.current);
                clusterGroupRef.current = null;
            }
        };
    }, [stations, map]);
    return null;
}
//# sourceMappingURL=StationMarkerLayer.js.map