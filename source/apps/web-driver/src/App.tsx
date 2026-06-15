import { useState, useCallback, useRef, useEffect } from 'react';
import { MapContainer, TileLayer, Marker, useMapEvents } from 'react-leaflet';
import { fetchNearbyStations, NearbyStationDto } from './api';

const TUNIS_CENTER: [number, number] = [36.8065, 10.1815];
const TUNIS_ZOOM = 10;

function StationMarkers({ stations }: { stations: NearbyStationDto[] }) {
  return (
    <>
      {stations.map((station) => (
        <Marker
          key={station.station_id}
          position={[station.latitude, station.longitude]}
        />
      ))}
    </>
  );
}

function MapController({
  onMoveEnd,
}: {
  onMoveEnd: (center: [number, number]) => void;
}) {
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useMapEvents({
    moveend(e) {
      if (debounceRef.current) {
        clearTimeout(debounceRef.current);
      }
      const center = e.target.getCenter();
      debounceRef.current = setTimeout(() => {
        onMoveEnd([center.lat, center.lng]);
      }, 300);
    },
  });

  return null;
}

export function App() {
  const [stations, setStations] = useState<NearbyStationDto[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadStations = useCallback(async (center: [number, number]) => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchNearbyStations(center[0], center[1]);
      setStations(data.stations);
    } catch (e) {
      setError('Failed to load stations');
      console.error(e);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadStations(TUNIS_CENTER);
  }, [loadStations]);

  const handleMoveEnd = useCallback(
    (center: [number, number]) => {
      loadStations(center);
    },
    [loadStations]
  );

  return (
    <div style={{ width: '100vw', height: '100vh', position: 'relative' }}>
      <MapContainer
        center={TUNIS_CENTER}
        zoom={TUNIS_ZOOM}
        style={{ width: '100%', height: '100%' }}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        <MapController onMoveEnd={handleMoveEnd} />
        <StationMarkers stations={stations} />
      </MapContainer>
      {loading && (
        <div
          style={{
            position: 'absolute',
            top: 16,
            right: 16,
            background: 'white',
            padding: '8px 16px',
            borderRadius: 8,
            boxShadow: '0 2px 4px rgba(0,0,0,0.2)',
          }}
        >
          Loading...
        </div>
      )}
      {error && (
        <div
          style={{
            position: 'absolute',
            top: 16,
            left: 16,
            right: 16,
            background: '#ff4444',
            color: 'white',
            padding: 12,
            borderRadius: 8,
            textAlign: 'center',
            fontWeight: 600,
          }}
        >
          {error}
        </div>
      )}
    </div>
  );
}
