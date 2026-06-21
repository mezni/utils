import { MapContainer, TileLayer, Marker, Popup } from "react-leaflet";
import "leaflet/dist/leaflet.css";

interface Station {
  station_id: string;
  name: string;
  distance_meters: number;
}

interface MapViewProps {
  stations: Station[];
  center: [number, number];
  zoom?: number;
}

export function MapView({ stations, center, zoom = 7 }: MapViewProps) {
  return (
    <MapContainer
      center={center}
      zoom={zoom}
      className="h-[600px] w-full rounded-lg"
      scrollWheelZoom={true}
    >
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      {stations.map((s) => (
        <Marker key={s.station_id} position={center as [number, number]}>
          <Popup>{s.name}</Popup>
        </Marker>
      ))}
    </MapContainer>
  );
}
