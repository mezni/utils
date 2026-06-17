import React, { useState } from 'react';
import { MapboxMap, MapboxGL } from '@rnmapbox/maps';
import type { MapboxMapProps, LocationHit } from '@rnmapbox/maps';
import type { Station } from '@bornemap/shared-types';
import { useNearbyStations } from '@bornemap/shared-hooks';
import { StationMarker } from '../components/StationMarker';
import { useClustering } from '../hooks/useClustering';

MapboxGL.setAccessToken(process.env.REACT_APP_MAPBOX_TOKEN || '');

const TUNISIA_CENTER = {
  latitude: 33.8869,
  longitude: 9.5375,
};

export const Dashboard: React.FC = () => {
  const [selectedLocation, setSelectedLocation] = useState<{
    latitude: number;
    longitude: number;
  }>(TUNISIA_CENTER);
  const [zoom, setZoom] = useState(13);

  const { stations, error, loading, count } = useNearbyStations(
    selectedLocation,
    { radius_m: 5000, max_results: 50 }
  );

  const { clusters } = useClustering(stations, zoom);

  const handleRegionChange = (location: LocationHit) => {
    setSelectedLocation({
      latitude: location.coordinates[1],
      longitude: location.coordinates[0],
    });
  };

  return (
    <div style={styles.container}>
      <MapboxMap
        style={styles.map}
        initialCamera={{
          centerCoordinate: [TUNISIA_CENTER.longitude, TUNISIA_CENTER.latitude],
          zoom: 13,
        }}
        onRegionChangeComplete={(location) => {
          handleRegionChange(location);
          setZoom(location.zoom || 13);
        }}
        showsUserLocation={true}
        styleAtmosphere={styles.atmosphere}
      >
        {clusters.map((cluster) =>
          cluster.stations.map((station) => (
            <StationMarker
              key={station.id}
              station={station}
            />
          ))
        )}
      </MapboxMap>

      <div style={styles.overlay}>
        <div style={styles.stats}>
          <span style={styles.statsText}>
            {loading ? 'Loading...' : `${count} stations nearby`}
          </span>
        </div>

        {error && (
          <div style={styles.errorBanner}>
            <div style={styles.errorText}>
              {error.error.message}
            </div>
            <button
              style={styles.retryButton}
              onClick={() => setSelectedLocation(selectedLocation)}
            >
              Retry
            </button>
          </div>
        )}
      </div>
    </div>
  );
};

const styles = {
  container: {
    width: '100vw',
    height: '100vh',
    position: 'relative' as const,
  },
  map: {
    width: '100%',
    height: '100%',
  },
  overlay: {
    position: 'absolute' as const,
    top: 20,
    left: 10,
    right: 10,
    zIndex: 1000,
  },
  stats: {
    backgroundColor: 'rgba(255, 255, 255, 0.95)',
    padding: '10px 20px',
    borderRadius: 8,
    display: 'inline-block',
    boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
    fontSize: 14,
    fontWeight: '600',
    color: '#333',
  },
  errorBanner: {
    backgroundColor: '#FFE6E6',
    padding: '12px',
    borderRadius: 8,
    borderWidth: 1,
    borderColor: '#FF0000',
    marginTop: 10,
  },
  errorText: {
    fontSize: 12,
    color: '#CC0000',
    marginBottom: 8,
  },
  retryButton: {
    backgroundColor: '#FF0000',
    color: 'white',
    border: 'none',
    padding: '6px 16px',
    borderRadius: 6,
    fontSize: 12,
    fontWeight: '600',
    cursor: 'pointer',
  },
  atmosphere: {
    backgroundColor: '#E8F4F8',
  },
} as const;
