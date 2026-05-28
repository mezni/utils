import React, { useState, useEffect, useRef } from 'react';
import { StyleSheet, View, Text, ActivityIndicator, TouchableOpacity, Platform } from 'react-native';
import MapView from '../components/MapView';
import { fetchNearbyStations } from '../services/api';
import StationCard from '../components/StationCard';

const TUNISIA_CENTER = {
  latitude: 36.8065,
  longitude: 10.1815,
  latitudeDelta: 0.08,
  longitudeDelta: 0.04,
};

class MapErrorBoundary extends React.Component {
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

    fetchNearbyStations({
      lat: TUNISIA_CENTER.latitude,
      lng: TUNISIA_CENTER.longitude,
      showStaged: true,
      signal: controller.signal,
    })
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

  return (
    <View style={styles.container}>
      <MapErrorBoundary>
        <MapView style={styles.map} initialRegion={TUNISIA_CENTER} stations={stations} onMarkerPress={setSelectedStation} />
      </MapErrorBoundary>

      {loading && (
        <View style={styles.loadingOverlay}>
          <ActivityIndicator size="large" color="#007AFF" />
          <Text style={styles.loadingText}>Loading stations...</Text>
        </View>
      )}

      {error && (
        <View style={styles.errorBanner}>
          <Text style={styles.errorBannerText}>Unable to connect to backend service</Text>
          <TouchableOpacity style={styles.retryButton} onPress={loadData}>
            <Text style={styles.retryText}>Retry</Text>
          </TouchableOpacity>
        </View>
      )}

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
  loadingOverlay: { position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, justifyContent: 'center', alignItems: 'center', backgroundColor: 'rgba(255,255,255,0.8)', zIndex: 50 },
  loadingText: { marginTop: 10, color: '#555555', fontSize: 14, fontWeight: '500' },
  errorBanner: { position: 'absolute', top: Platform.OS === 'ios' ? 90 : 60, left: 16, right: 16, backgroundColor: '#D32F2F', borderRadius: 12, padding: 12, flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', zIndex: 60, elevation: 6 },
  errorBannerText: { color: '#FFFFFF', fontSize: 13, fontWeight: '500', flex: 1 },
  retryButton: { backgroundColor: '#FFFFFF', paddingHorizontal: 14, paddingVertical: 6, borderRadius: 8, marginLeft: 12 },
  retryText: { color: '#D32F2F', fontWeight: '700', fontSize: 13 },
  drawer: { position: 'absolute', bottom: 24, left: 16, right: 16, backgroundColor: '#FFFFFF', borderRadius: 16, padding: 16, elevation: 5 }
});
