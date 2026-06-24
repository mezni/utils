import { MapContainer, TileLayer, useMapEvents } from "react-leaflet";
import type { ReactNode } from "react";
import { useEffect, useRef } from "react";
import L from "leaflet";
import styles from "./MapProvider.module.css";

export interface MapProviderProps {
  center: [number, number];
  zoom: number;
  children: ReactNode;
  onViewportChange?: (center: [number, number], zoom: number) => void;
}

function ViewportListener({ onViewportChange }: { onViewportChange?: MapProviderProps["onViewportChange"] }) {
  const prevRef = useRef<{ center: [number, number]; zoom: number } | null>(null);

  useMapEvents({
    moveend(e) {
      if (!onViewportChange) return;
      const map = e.target;
      const c = map.getCenter();
      const center: [number, number] = [c.lat, c.lng];
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
    const icon = L.Icon.Default as any;
    delete icon.prototype._getIconUrl;
    icon.mergeOptions({
      iconRetinaUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png",
      iconUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png",
      shadowUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png",
    });
  }, []);
  return null;
}

export function MapProvider({ center, zoom, children, onViewportChange }: MapProviderProps) {
  return (
    <div className={styles.wrapper} data-testid="map-provider">
      <MapContainer
        center={center}
        zoom={zoom}
        className={styles.map}
        scrollWheelZoom={true}
      >
        <FixLeafletIcons />
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        <ViewportListener onViewportChange={onViewportChange} />
        {children}
      </MapContainer>
    </div>
  );
}
