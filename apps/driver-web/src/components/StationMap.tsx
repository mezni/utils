import { MapContainer, TileLayer, Marker, Popup } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import L from 'leaflet';
import { useStations } from '../hooks/useStations';

// Fix default marker icon issue with bundlers
import iconUrl from 'leaflet/dist/images/marker-icon.png';
import iconRetinaUrl from 'leaflet/dist/images/marker-icon-2x.png';
import shadowUrl from 'leaflet/dist/images/marker-shadow.png';

L.Icon.Default.mergeOptions({
  iconUrl,
  iconRetinaUrl,
  shadowUrl,
});

function haversineDistance(
  lat1: number,
  lng1: number,
  lat2: number,
  lng2: number
): number {
  const R = 6371;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLng = ((lng2 - lng1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

export default function StationMap() {
  const { stations, loading, error } = useStations();

  return (
    <div className="h-screen w-full relative">
      {loading && (
        <div className="absolute top-4 left-1/2 -translate-x-1/2 z-[1000] bg-white px-4 py-2 rounded shadow text-sm">
          Loading stations...
        </div>
      )}
      {error && (
        <div className="absolute top-4 left-1/2 -translate-x-1/2 z-[1000] bg-red-50 border border-red-200 px-4 py-2 rounded shadow text-sm text-red-700">
          {error}
        </div>
      )}
      <MapContainer
        center={[34.0, 9.0]}
        zoom={7}
        className="h-full w-full"
        scrollWheelZoom
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}{r}.png"
        />
        {stations.map((station) => (
          <Marker
            key={station.id}
            position={[station.latitude, station.longitude]}
          >
            <Popup>
              <div className="text-sm">
                <strong>{station.name}</strong>
                <br />
                {station.address}
                <br />
                <span className="text-green-700">
                  {station.available_chargers}/{station.total_chargers} available
                </span>
                <br />
                <span className="text-gray-500">
                  {haversineDistance(
                    34.0,
                    9.0,
                    station.latitude,
                    station.longitude
                  ).toFixed(1)}{' '}
                  km from center
                </span>
              </div>
            </Popup>
          </Marker>
        ))}
      </MapContainer>
    </div>
  );
}
