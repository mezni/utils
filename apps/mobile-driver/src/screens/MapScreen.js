import React, { useState, useEffect, useRef, useCallback } from 'react';
import { StyleSheet, View, Text, ScrollView, TouchableOpacity, ActivityIndicator, TextInput } from 'react-native';
import MapView, { Marker, PROVIDER_DEFAULT } from 'react-native-maps';
import StationCard from '../components/StationCard';
import { fetchNearbyStations } from '../services/api';

const TUNIS_REGION = {
  latitude: 36.8065,
  longitude: 10.1815,
  latitudeDelta: 0.05,
  longitudeDelta: 0.05,
};

const REFRESH_INTERVAL = 30000;
const SLOW_API_TIMEOUT = 10000;

export default function MapScreen() {
  const [stations, setStations] = useState([]);
  const [selectedStation, setSelectedStation] = useState(null);
  const [activeFilter, setActiveFilter] = useState('All');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [offline, setOffline] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [initialLoadTimeout, setInitialLoadTimeout] = useState(false);
  const mapRef = useRef(null);
  const refreshTimerRef = useRef(null);
  const timeoutRef = useRef(null);

  const loadStations = useCallback(async (lat, lng) => {
    try {
      setError(null);
      const data = await fetchNearbyStations(lat, lng);
      setStations(data);
      if (data.length > 0) setSelectedStation(data[0]);
      if (data.length === 0) setSelectedStation(null);
    } catch (err) {
      setError('Unable to load stations. Please check your connection and try again.');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    setLoading(true);
    setInitialLoadTimeout(false);

    timeoutRef.current = setTimeout(() => {
      if (loading) setInitialLoadTimeout(true);
    }, SLOW_API_TIMEOUT);

    loadStations(TUNIS_REGION.latitude, TUNIS_REGION.longitude);

    refreshTimerRef.current = setInterval(() => {
      if (mapRef.current) {
        mapRef.current.getCamera().then((cam) => {
          loadStations(cam.center.latitude, cam.center.longitude);
        });
      }
    }, REFRESH_INTERVAL);

    return () => {
      clearInterval(refreshTimerRef.current);
      clearTimeout(timeoutRef.current);
    };
  }, []);

  const onRegionChangeComplete = useCallback((region) => {
    loadStations(region.latitude, region.longitude);
  }, [loadStations]);

  const onRefresh = useCallback(() => {
    setLoading(true);
    if (mapRef.current) {
      mapRef.current.getCamera().then((cam) => {
        loadStations(cam.center.latitude, cam.center.longitude);
      });
    }
  }, [loadStations]);

  const onSearch = useCallback(() => {
    if (!searchQuery.trim()) return;
    fetch(`https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(searchQuery)}`)
      .then((res) => res.json())
      .then((data) => {
        if (data.length > 0) {
          const { lat, lon } = data[0];
          mapRef.current?.animateToRegion({
            latitude: parseFloat(lat),
            longitude: parseFloat(lon),
            latitudeDelta: 0.05,
            longitudeDelta: 0.05,
          });
          loadStations(parseFloat(lat), parseFloat(lon));
        }
      })
      .catch(() => {});
  }, [searchQuery, loadStations]);

  const filteredStations = stations.filter((s) => {
    if (activeFilter === 'Available') return s.status === 'Available';
    return true;
  });

  if (loading && initialLoadTimeout) {
    return (
      <View style={styles.center}>
        <Text style={styles.errorText}>Request timed out. Check your connection.</Text>
        <TouchableOpacity style={styles.retryButton} onPress={() => { setInitialLoadTimeout(false); setLoading(true); loadStations(TUNIS_REGION.latitude, TUNIS_REGION.longitude); }}>
          <Text style={styles.retryText}>Retry</Text>
        </TouchableOpacity>
      </View>
    );
  }

  if (loading) {
    return (
      <View style={styles.center}>
        <ActivityIndicator size="large" color="#007AFF" />
        <Text style={styles.loadingText}>Fetching stations from api-service...</Text>
      </View>
    );
  }

  return (
    <View style={styles.container}>
      {offline && (
        <View style={styles.offlineBanner}>
          <Text style={styles.offlineText}>No internet connection</Text>
        </View>
      )}

      <View style={styles.searchContainer}>
        <TextInput
          style={styles.searchInput}
          placeholder="Search place..."
          placeholderTextColor="#999"
          value={searchQuery}
          onChangeText={setSearchQuery}
          onSubmitEditing={onSearch}
          returnKeyType="search"
        />
      </View>

      <MapView
        ref={mapRef}
        provider={PROVIDER_DEFAULT}
        style={styles.map}
        initialRegion={TUNIS_REGION}
        onRegionChangeComplete={onRegionChangeComplete}
        onPanDrag={onRefresh}
      >
        {filteredStations.map((station) => (
          <Marker
            key={station.id}
            coordinate={{ latitude: station.latitude, longitude: station.longitude }}
            onPress={() => setSelectedStation(station)}
            pinColor={station.status === 'Available' ? '#00C853' : '#FF3D00'}
          />
        ))}
      </MapView>

      {error && (
        <View style={styles.errorOverlay}>
          <Text style={styles.errorText}>{error}</Text>
          <TouchableOpacity style={styles.retryButton} onPress={onRefresh}>
            <Text style={styles.retryText}>Retry</Text>
          </TouchableOpacity>
        </View>
      )}

      {stations.length === 0 && !error && (
        <View style={styles.emptyOverlay}>
          <Text style={styles.emptyText}>No stations found in this area</Text>
          <Text style={styles.emptySubtext}>Try panning to a different location</Text>
        </View>
      )}

      <View style={styles.filterContainer}>
        <ScrollView horizontal showsHorizontalScrollIndicator={false}>
          {['All', 'Available'].map((filter) => (
            <TouchableOpacity
              key={filter}
              style={[styles.filterButton, activeFilter === filter && styles.activeFilterButton]}
              onPress={() => setActiveFilter(filter)}
            >
              <Text style={[styles.filterText, activeFilter === filter && styles.activeFilterText]}>{filter}</Text>
            </TouchableOpacity>
          ))}
        </ScrollView>
      </View>

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
  center: { flex: 1, justifyContent: 'center', alignItems: 'center' },
  loadingText: { marginTop: 8, color: '#555' },
  searchContainer: { position: 'absolute', top: 60, left: 16, right: 16, zIndex: 10 },
  searchInput: { backgroundColor: '#FFFFFF', borderRadius: 12, paddingHorizontal: 16, paddingVertical: 10, elevation: 3, fontSize: 15 },
  filterContainer: { position: 'absolute', top: 110, left: 0, right: 0, paddingHorizontal: 16, flexDirection: 'row' },
  filterButton: { backgroundColor: '#FFFFFF', paddingHorizontal: 16, paddingVertical: 8, borderRadius: 20, marginRight: 8, elevation: 3 },
  activeFilterButton: { backgroundColor: '#007AFF' },
  filterText: { color: '#333', fontWeight: '600' },
  activeFilterText: { color: '#FFFFFF' },
  drawer: { position: 'absolute', bottom: 24, left: 16, right: 16 },
  offlineBanner: { position: 'absolute', top: 0, left: 0, right: 0, backgroundColor: '#FF6B6B', paddingVertical: 6, zIndex: 20, alignItems: 'center' },
  offlineText: { color: '#FFF', fontSize: 13, fontWeight: '600' },
  errorOverlay: { position: 'absolute', top: 150, left: 16, right: 16, backgroundColor: '#FFFFFF', borderRadius: 12, padding: 20, elevation: 5, alignItems: 'center', zIndex: 15 },
  errorText: { color: '#D32F2F', fontSize: 14, textAlign: 'center', marginBottom: 12 },
  retryButton: { backgroundColor: '#007AFF', borderRadius: 8, paddingHorizontal: 20, paddingVertical: 8 },
  retryText: { color: '#FFF', fontWeight: '600' },
  emptyOverlay: { position: 'absolute', top: 150, left: 16, right: 16, backgroundColor: '#FFFFFF', borderRadius: 12, padding: 20, elevation: 5, alignItems: 'center', zIndex: 15 },
  emptyText: { fontSize: 15, fontWeight: '600', color: '#555', marginBottom: 4 },
  emptySubtext: { fontSize: 13, color: '#999' },
});
