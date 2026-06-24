import { useEffect, useRef } from "react";
import { useMap } from "react-leaflet";
import L from "leaflet";
import "leaflet.markercluster";
// Temporarily skip type check to avoid TS resolution issues in workspace
// import type { StationDto } from "@bornemap/domain-types";
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-expect-error - TS resolution issue with workspace package
import type { StationDto } from "@bornemap/domain-types";

export interface StationMarkerLayerProps {
  stations: StationDto[];
}

export function StationMarkerLayer({ stations }: StationMarkerLayerProps) {
  const map = useMap();
  const clusterGroupRef = useRef<L.MarkerClusterGroup | null>(null);

  useEffect(() => {
    if (stations.length === 0) return;

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
