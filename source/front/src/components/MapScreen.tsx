import { useEffect, useCallback, useState, useRef } from 'react';
import { View, StyleSheet } from 'react-native';
import MapView, { Region } from 'react-native-maps';
import { Skeleton } from '@borne/design-system';
import { useLocation } from '../hooks/useLocation';
import { useNearbyStations } from '../hooks/useNearbyStations';
import { useClickstream } from '../hooks/useClickstream';
import { MapRegion } from '../types';
import { StationMarker } from './StationMarker';
import { StationBottomSheet } from './StationBottomSheet';
import { MapErrorState } from './MapErrorState';

const TUNIS_FALLBACK: MapRegion = {
  latitude: 36.8065,
  longitude: 10.1815,
  latitudeDelta: 0.05,
  longitudeDelta: 0.05,
};

export default function MapScreen() {
  const { location, permissionDenied } = useLocation();
  const { stations, loading, error, refetch } = useNearbyStations();
  const { track } = useClickstream();
  const [selectedStationId, setSelectedStationId] = useState<string | null>(
    null,
  );
  const hasTrackedOpen = useRef(false);
  const prevRegionRef = useRef<MapRegion | null>(null);

  const initialRegion: MapRegion = permissionDenied
    ? TUNIS_FALLBACK
    : location
      ? {
          ...TUNIS_FALLBACK,
          latitude: location.latitude,
          longitude: location.longitude,
        }
      : TUNIS_FALLBACK;

  const onRegionChangeComplete = useCallback(
    (region: Region) => {
      const mapRegion: MapRegion = {
        latitude: region.latitude,
        longitude: region.longitude,
        latitudeDelta: region.latitudeDelta,
        longitudeDelta: region.longitudeDelta,
      };

      const prev = prevRegionRef.current;
      const zoomChanged =
        prev &&
        Math.abs(region.latitudeDelta - prev.latitudeDelta) /
          prev.latitudeDelta >
          0.1;

      refetch(mapRegion);

      track({
        event_type: 'nearby_search',
        timestamp: new Date().toISOString(),
        latitude: region.latitude,
        longitude: region.longitude,
        radius_m: Math.round(
          Math.max(region.latitudeDelta, region.longitudeDelta) *
            111_320 *
            0.5,
        ),
      });

      if (zoomChanged) {
        track({
          event_type: 'map_zoom',
          timestamp: new Date().toISOString(),
          latitude: region.latitude,
          longitude: region.longitude,
        });
      }

      track({
        event_type: 'map_pan',
        timestamp: new Date().toISOString(),
        latitude: region.latitude,
        longitude: region.longitude,
      });

      prevRegionRef.current = mapRegion;
    },
    [refetch, track],
  );

  useEffect(() => {
    if (!hasTrackedOpen.current && initialRegion) {
      track({
        event_type: 'map_open',
        timestamp: new Date().toISOString(),
      });
      hasTrackedOpen.current = true;
      onRegionChangeComplete(initialRegion);
    }
  }, [initialRegion, track, onRegionChangeComplete]);

  const handleRetry = useCallback(() => {
    refetch(initialRegion);
  }, [refetch, initialRegion]);

  const handleMarkerPress = useCallback(
    (stationId: string) => {
      setSelectedStationId(stationId);
      track({
        event_type: 'station_click',
        timestamp: new Date().toISOString(),
        station_id: stationId,
      });
    },
    [track],
  );

  if (loading && stations.length === 0) {
    return <Skeleton variant="map" />;
  }

  return (
    <View style={styles.container}>
      <MapView
        style={styles.map}
        initialRegion={initialRegion}
        onRegionChangeComplete={onRegionChangeComplete}
        showsUserLocation
        showsMyLocationButton
      >
        {stations.map((s) => (
          <StationMarker key={s.id} station={s} onPress={handleMarkerPress} />
        ))}
      </MapView>

      <MapErrorState
        error={error}
        isEmpty={stations.length === 0 && !loading}
        loading={loading}
        onRetry={handleRetry}
      />

      <StationBottomSheet
        stationId={selectedStationId}
        onClose={() => setSelectedStationId(null)}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
  map: {
    ...StyleSheet.absoluteFillObject,
  },
});
