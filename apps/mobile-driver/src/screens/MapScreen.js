import React, { useState, useEffect, useRef, Component } from 'react';
import { StyleSheet, View, Text, ActivityIndicator, TouchableOpacity } from 'react-native';
import MapView, { Marker, PROVIDER_DEFAULT } from 'react-native-maps';
import { fetchNearbyStations } from '../services/api';
import StationCard from '../components/StationCard';

const TUNISIA_CENTER = {
  latitude: 36.8065,
  longitude: 10.1815,
  latitudeDelta: 0.08,
  longitudeDelta: 0.04,
};

class MapErrorBoundary extends Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, errorMessage: '' };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, errorMessage: error.message || 'Map failed to initialize' };
  }

  render() {
    if (this.state.hasError) {
      return (
        <View style={styles.centered}>
          <Text style={styles.errorText}>Map Unavailable</Text>
          <Text style={styles.infoText}>{this.state.errorMessage}</Text>
        </View>
      );
    }
    return this.props.children;
  }
}

export default function MapScreen() {
  const [stations, setStations] = useState([]);
  const [selectedStation, setSelectedStation] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const abortRef = useRef(null);

  const loadData = () => {
    if (abortRef.current) abortRef.current.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    setLoading(true);
    setError(false);
    setSelectedStation(null);

    fetchNearbyStations(controller.signal)
      .then((data) => {
        if (!data) {
          setLoading(false);
          return;
        }
        setStations(data);
        setLoading(false);
      })
      .catch(() => {
        setError(true);
        setLoading(false);
      });
  };

  useEffect(() => {
    loadData();
    return () => {
      if (abortRef.current) abortRef.current.abort();
    };
  }, []);

  if (loading) {
    return (
      <View style={styles.centered}>
        <ActivityIndicator size="large" color="#007AFF" />
        <Text style={styles.infoText}>Connecting to api-service...</Text>
      </View>
    );
  }

  if (error) {
    return (
      <View style={styles.centered}>
        <Text style={styles.errorText}>🚨 Unable to connect to backend service</Text>
        <Text style={styles.debugText}>Check that api-service is running on port 8080 and accessible from your device</Text>
        <TouchableOpacity style={styles.retryButton} onPress={loadData}>
          <Text style={styles.retryText}>Retry Connection</Text>
        </TouchableOpacity>
      </View>
    );
  }

  return (
    <View style={styles.container}>
      <MapErrorBoundary>
        <MapView provider={PROVIDER_DEFAULT} style={styles.map} initialRegion={TUNISIA_CENTER}>
          {stations.map((station) => (
            <Marker
              key={station.id}
              coordinate={{ latitude: station.latitude, longitude: station.longitude }}
              onPress={() => setSelectedStation(station)}
              pinColor={station.status === 'Available' ? '#4CAF50' : '#F44336'}
            />
          ))}
        </MapView>
      </MapErrorBoundary>

      {selectedStation && (
        <View style={styles.drawer}>
          <StationCard station={selectedStation} />
        </View>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  map: { ...StyleSheet.absoluteFillObject },
  centered: { flex: 1, justifyContent: 'center', alignItems: 'center', backgroundColor: '#FFFFFF' },
  infoText: { marginTop: 10, color: '#555555', fontSize: 14 },
  errorText: { color: '#D32F2F', fontSize: 14, fontWeight: '500', marginBottom: 8 },
  debugText: { color: '#888888', fontSize: 12, marginBottom: 16, marginHorizontal: 24, textAlign: 'center' },
  retryButton: { backgroundColor: '#007AFF', paddingHorizontal: 16, paddingVertical: 8, borderRadius: 8 },
  retryText: { color: '#FFFFFF', fontWeight: '600' },
  drawer: { position: 'absolute', bottom: 24, left: 16, right: 16, backgroundColor: '#FFFFFF', borderRadius: 16, padding: 16, elevation: 5 }
});
