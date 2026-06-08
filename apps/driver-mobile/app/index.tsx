import { View, StyleSheet, ActivityIndicator, Text } from 'react-native';
import MapView from 'react-native-maps';
import { useState, useEffect } from 'react';
import { useLocation } from '../hooks/useLocation';
import { fetchStationsNearby, type Station } from '../services/api';
import StationMarker from '../components/StationMarker';

const DEFAULT_REGION = {
  latitude: 36.8065,
  longitude: 10.1815,
  latitudeDelta: 8,
  longitudeDelta: 8,
};

export default function MapScreen() {
  const { coordinates } = useLocation();
  const [stations, setStations] = useState<Station[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        setLoading(true);
        const data = await fetchStationsNearby(36.8065, 10.1815, 200);
        if (!cancelled) {
          setStations(data);
          setLoading(false);
        }
      } catch {
        if (!cancelled) {
          setError('Unable to load stations');
          setLoading(false);
        }
      }
    }

    load();

    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <View style={styles.container}>
      {loading && (
        <View style={styles.overlay}>
          <ActivityIndicator size="small" color="#166534" />
          <Text style={styles.loadingText}>Loading stations...</Text>
        </View>
      )}
      {error && (
        <View style={styles.overlay}>
          <Text style={styles.errorText}>{error}</Text>
        </View>
      )}
      <MapView
        style={styles.map}
        initialRegion={{
          ...DEFAULT_REGION,
          latitude: coordinates.latitude,
          longitude: coordinates.longitude,
        }}
        showsUserLocation
      >
        {stations.map((station) => (
          <StationMarker key={station.id} station={station} />
        ))}
      </MapView>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
  map: {
    flex: 1,
  },
  overlay: {
    position: 'absolute',
    top: 60,
    left: 0,
    right: 0,
    alignItems: 'center',
    zIndex: 100,
  },
  loadingText: {
    marginTop: 4,
    fontSize: 12,
    color: '#166534',
  },
  errorText: {
    fontSize: 12,
    color: '#dc2626',
  },
});
