import React, { useState, useEffect } from 'react';
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

export default function MapScreen() {
  const [stations, setStations] = useState([]);
  const [selectedStation, setSelectedStation] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  const loadData = () => {
    setLoading(true);
    setError(false);
    fetchNearbyStations()
      .then((data) => {
        setStations(data);
        if (data.length > 0) setSelectedStation(data[0]);
        setLoading(false);
      })
      .catch(() => {
        setError(true);
        setLoading(false);
      });
  };

  useEffect(() => {
    loadData();
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
        <TouchableOpacity style={styles.retryButton} onPress={loadData}>
          <Text style={styles.retryText}>Retry Connection</Text>
        </TouchableOpacity>
      </View>
    );
  }

  return (
    <View style={styles.container}>
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
  errorText: { color: '#D32F2F', fontSize: 14, fontWeight: '500', marginBottom: 12 },
  retryButton: { backgroundColor: '#007AFF', paddingHorizontal: 16, paddingVertical: 8, borderRadius: 8 },
  retryText: { color: '#FFFFFF', fontWeight: '600' },
  drawer: { position: 'absolute', bottom: 24, left: 16, right: 16, backgroundColor: '#FFFFFF', borderRadius: 16, padding: 16, elevation: 5 }
});
