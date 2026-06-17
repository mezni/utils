import React, { useState } from 'react';
import { View, StyleSheet, Text, TouchableOpacity } from 'react-native';
import MapView, { Region } from 'react-native-maps';
import { useNearbyStations } from '@bornemap/shared-hooks';
import { StationMarker } from '../components/StationMarker';
import { useClustering } from '../hooks/useClustering';
import { VisibilityFilter } from '../components/VisibilityFilter';

const TUNISIA_CENTER = { lat: 33.8869, lon: 9.5375 };

export const DriverMapScreen: React.FC = () => {
  const [selectedLocation, setSelectedLocation] = useState(TUNISIA_CENTER);
  const [zoom, setZoom] = useState(13);
  const [selectedVisibility, setSelectedVisibility] = useState('all');

  const { stations, error, loading, count } = useNearbyStations(
    selectedLocation,
    { radius_m: 5000, max_results: 50, visibility: selectedVisibility }
  );

  const { clusters } = useClustering(stations, zoom);

  const handleRegionChangeComplete = (region: Region) => {
    setSelectedLocation({ lat: region.latitude, lon: region.longitude });
    const latDelta = region.latitudeDelta;
    const z = Math.round(Math.log(360 / latDelta) / Math.LN2);
    setZoom(z);
  };

  return (
    <View style={styles.container}>
      <MapView
        style={styles.map}
        initialRegion={{
          latitude: TUNISIA_CENTER.lat,
          longitude: TUNISIA_CENTER.lon,
          latitudeDelta: 0.1,
          longitudeDelta: 0.1,
        }}
        onRegionChangeComplete={handleRegionChangeComplete}
        showsUserLocation
        showsMyLocationButton
      >
        {clusters.map((cluster) =>
          cluster.stations.map((station) => (
            <StationMarker key={station.id} station={station} />
          ))
        )}
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
            <Text style={styles.errorText}>{error.error.message}</Text>
          </View>
        )}
      </View>
    </View>
  );
};

const styles = StyleSheet.create({
  container: { flex: 1 },
  map: { flex: 1 },
  overlay: { position: 'absolute', top: 20, left: 10, right: 10, zIndex: 1 },
  stats: {
    backgroundColor: 'rgba(255,255,255,0.9)', padding: 10, borderRadius: 8,
    alignItems: 'center', marginBottom: 10,
    shadowColor: '#000', shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.25, shadowRadius: 3.84, elevation: 5,
  },
  statsText: { fontSize: 14, fontWeight: '600', color: '#333' },
  errorBanner: {
    backgroundColor: '#FFE6E6', padding: 12, borderRadius: 8,
    borderWidth: 1, borderColor: '#FF0000',
  },
  errorText: { fontSize: 12, color: '#CC0000' },
});
