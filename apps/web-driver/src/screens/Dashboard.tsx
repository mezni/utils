import React, { useState } from 'react';
import { MapContainer, TileLayer, useMapEvents } from 'react-leaflet';
import type { Station } from '@bornemap/shared-types';
import { useNearbyStations } from '@bornemap/shared-hooks';
import { StationMarker } from '../components/StationMarker';
import { useClustering } from '../hooks/useClustering';

const TUNISIA_CENTER = { lat: 33.8869, lon: 9.5375 };

function MapEvents({ onMove }: { onMove: (lat: number, lon: number, zoom: number) => void }) {
  useMapEvents({
    moveend: (e) => {
      const center = e.target.getCenter();
      onMove(center.lat, center.lng, e.target.getZoom());
    },
  });
  return null;
}

export const Dashboard: React.FC = () => {
  const [selectedLocation, setSelectedLocation] = useState(TUNISIA_CENTER);
  const [zoom, setZoom] = useState(13);

  const { stations, error, loading, count } = useNearbyStations(
    selectedLocation,
    { radius_m: 5000, max_results: 50 }
  );

  const { clusters } = useClustering(stations, zoom);

  const handleMove = (lat: number, lon: number, z: number) => {
    setSelectedLocation({ lat, lon });
    setZoom(z);
  };

  return (
    <div style={styles.container}>
      <MapContainer
        center={[TUNISIA_CENTER.lat, TUNISIA_CENTER.lon]}
        zoom={13}
        style={styles.map}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        <MapEvents onMove={handleMove} />
        {clusters.map((cluster) =>
          cluster.stations.map((station) => (
            <StationMarker key={station.id} station={station} />
          ))
        )}
      </MapContainer>

      <div style={styles.overlay}>
        <div style={styles.stats}>
          <span style={styles.statsText}>
            {loading ? 'Loading...' : `${count} stations nearby`}
          </span>
        </div>

        {error && (
          <div style={styles.errorBanner}>
            <div style={styles.errorText}>{error.error.message}</div>
            <button style={styles.retryButton} onClick={() => window.location.reload()}>
              Retry
            </button>
          </div>
        )}
      </div>
    </div>
  );
};

const styles: Record<string, React.CSSProperties> = {
  container: { width: '100vw', height: '100vh', position: 'relative' },
  map: { width: '100%', height: '100%' },
  overlay: { position: 'absolute', top: 20, left: 10, right: 10, zIndex: 1000 },
  stats: {
    backgroundColor: 'rgba(255,255,255,0.95)', padding: '10px 20px',
    borderRadius: 8, display: 'inline-block', boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
    fontSize: 14, fontWeight: 600, color: '#333',
  },
  errorBanner: {
    backgroundColor: '#FFE6E6', padding: 12, borderRadius: 8,
    border: '1px solid #FF0000', marginTop: 10,
  },
  errorText: { fontSize: 12, color: '#CC0000', marginBottom: 8 },
  retryButton: {
    backgroundColor: '#FF0000', color: 'white', border: 'none',
    padding: '6px 16px', borderRadius: 6, fontSize: 12, fontWeight: 600, cursor: 'pointer',
  },
};
