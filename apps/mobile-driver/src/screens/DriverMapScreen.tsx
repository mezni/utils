import React, { useState, useEffect } from 'react';
import { View, StyleSheet, ActivityIndicator, Text, ScrollView, TouchableOpacity } from 'react-native';
import { MapView, MapViewProps, Marker, Callout } from 'react-native-maps';
import type { Station } from '@bornemap/shared-types';
import { useNearbyStations } from '@bornemap/shared-hooks';
import { StationMarker } from '../components/StationMarker';
import { useClustering } from '../hooks/useClustering';
import { VisibilityFilter } from '../components/VisibilityFilter';

const TUNISIA_CENTER = {
  latitude: 33.8869,
  longitude: 9.5375,
};

export const DriverMapScreen: React.FC = () => {
  const [selectedLocation, setSelectedLocation] = useState<{
    latitude: number;
    longitude: number;
  }>(TUNISIA_CENTER);
  const [zoom, setZoom] = useState(13);
  const [selectedVisibility, setSelectedVisibility] = useState('all');

  const { stations, error, loading, count } = useNearbyStations(
    selectedLocation,
    { radius_m: 5000, max_results: 50, visibility: selectedVisibility }
  );

  const { clusters } = useClustering(stations, zoom);

  const handleRegionChange = (region: MapViewProps['onRegionChange'] extends Function ? Parameters<ReturnType<MapViewProps['onRegionChange']>>[0] : any) => {
    setSelectedLocation({
      latitude: region.latitude,
      longitude: region.longitude,
    });
  };

  return (
    <View style={styles.container}>
      <MapView
        style={styles.map}
        initialRegion={{
          latitude: TUNISIA_CENTER.latitude,
          longitude: TUNISIA_CENTER.longitude,
          latitudeDelta: 0.1,
          longitudeDelta: 0.1,
        }}
        onRegionChange={handleRegionChange}
        onRegionChangeComplete={(region) => setZoom(region.latitudeDelta)}
        showsUserLocation
        showsMyLocationButton
      >
        {clusters.map((cluster) => (
          cluster.stations.map((station) => (
            <StationMarker
              key={station.id}
              station={station}
            />
          ))
        ))}
      </MapView>

      <View style={styles.overlay}>
        <VisibilityFilter
          selectedVisibility={selectedVisibility}
          onSelectVisibility={setSelectedVisibility}
          stations={stations}
        />

        <View style={styles.stats}>
          <Text style={styles.statsText}>
            {loading ? 'Loading...' : `${count} stations nearby`}
          </Text>
        </View>

        {error && (
          <View style={styles.errorBanner}>
            <Text style={styles.errorText}>
              {error.error.message}
            </Text>
            <TouchableOpacity style={styles.retryButton} onPress={() => setSelectedLocation(selectedLocation)}>
              <Text style={styles.retryButtonText}>Retry</Text>
            </TouchableOpacity>
          </View>
        )}
      </View>
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
  map: {
    flex: 1,
  },
  overlay: {
    position: 'absolute',
    top: 20,
    left: 10,
    right: 10,
    zIndex: 1,
  },
  stats: {
    backgroundColor: 'rgba(255, 255, 255, 0.9)',
    padding: 10,
    borderRadius: 8,
    alignItems: 'center',
    marginBottom: 10,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.25,
    shadowRadius: 3.84,
    elevation: 5,
  },
  statsText: {
    fontSize: 14,
    fontWeight: '600',
    color: '#333',
  },
  errorBanner: {
    backgroundColor: '#FFE6E6',
    padding: 12,
    borderRadius: 8,
    borderWidth: 1,
    borderColor: '#FF0000',
  },
  errorText: {
    fontSize: 12,
    color: '#CC0000',
    marginBottom: 8,
  },
  retryButton: {
    backgroundColor: '#FF0000',
    paddingHorizontal: 16,
    paddingVertical: 8,
    borderRadius: 6,
  },
  retryButtonText: {
    color: 'white',
    fontSize: 12,
    fontWeight: '600',
  },
});
