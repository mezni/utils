import React, { useState, useCallback, useRef, useEffect } from 'react';
import { View, StyleSheet, ActivityIndicator, Text } from 'react-native';
import MapView, { Region } from 'react-native-maps';
import { TUNIS_INITIAL_REGION } from '../../../../shared-mobile/src';
import { NearbyStationDto } from '../../../../shared-mobile/src';
import { StationMarker } from '../components/StationMarker';
import { fetchNearbyStations } from '../../services/api';

export function MapScreen() {
  const [stations, setStations] = useState<NearbyStationDto[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const regionRef = useRef<Region>(TUNIS_INITIAL_REGION);

  const loadStations = useCallback(async (region: Region) => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchNearbyStations(
        region.latitude,
        region.longitude
      );
      setStations(data.stations);
    } catch (e) {
      setError('Failed to load stations');
      console.error(e);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadStations(TUNIS_INITIAL_REGION);
  }, [loadStations]);

  const handleRegionChangeComplete = useCallback(
    (region: Region) => {
      regionRef.current = region;
      if (debounceRef.current) {
        clearTimeout(debounceRef.current);
      }
      debounceRef.current = setTimeout(() => {
        loadStations(region);
      }, 300);
    },
    [loadStations]
  );

  return (
    <View style={styles.container}>
      <MapView
        style={styles.map}
        initialRegion={TUNIS_INITIAL_REGION}
        onRegionChangeComplete={handleRegionChangeComplete}
      >
        {stations.map((station) => (
          <StationMarker key={station.station_id} station={station} />
        ))}
      </MapView>
      {loading && (
        <View style={styles.loadingOverlay}>
          <ActivityIndicator size="small" color="#0000ff" />
        </View>
      )}
      {error && (
        <View style={styles.errorBanner}>
          <Text style={styles.errorText}>{error}</Text>
        </View>
      )}
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
  loadingOverlay: {
    position: 'absolute',
    top: 50,
    right: 16,
    backgroundColor: 'white',
    borderRadius: 20,
    padding: 8,
    elevation: 3,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.2,
    shadowRadius: 4,
  },
  errorBanner: {
    position: 'absolute',
    top: 50,
    left: 16,
    right: 16,
    backgroundColor: '#ff4444',
    padding: 12,
    borderRadius: 8,
    alignItems: 'center',
  },
  errorText: {
    color: 'white',
    fontWeight: '600',
  },
});
