import { useEffect, useState, useCallback } from 'react';
import { View, Text, ActivityIndicator, TouchableOpacity, StyleSheet, Platform } from 'react-native';
import MapView, { Marker, Callout, Region } from 'react-native-maps';
import { useQuery } from '@tanstack/react-query';
import * as Location from 'expo-location';
import { useNavigation } from '@react-navigation/native';
import type { NativeStackNavigationProp } from '@react-navigation/native-stack';
import { list, type Partner, type Station, type Charger, type VisibleStation } from '../api/client';
import type { RootStackParamList } from '../navigation/AppNavigator';

const TUNISIA_REGION: Region = {
  latitude: 33.8869,
  longitude: 9.5375,
  latitudeDelta: 8,
  longitudeDelta: 8,
};

type Nav = NativeStackNavigationProp<RootStackParamList, 'Map'>;

export function MapScreen() {
  const navigation = useNavigation<Nav>();
  const [region, setRegion] = useState<Region>(TUNISIA_REGION);
  const [locationGranted, setLocationGranted] = useState<boolean | null>(null);

  useEffect(() => {
    (async () => {
      const { status } = await Location.requestForegroundPermissionsAsync();
      const granted = status === 'granted';
      setLocationGranted(granted);
      if (granted) {
        try {
          const pos = await Location.getCurrentPositionAsync({});
          setRegion({
            latitude: pos.coords.latitude,
            longitude: pos.coords.longitude,
            latitudeDelta: 0.05,
            longitudeDelta: 0.05,
          });
        } catch {
          setRegion(TUNISIA_REGION);
        }
      }
    })();
  }, []);

  const partnersQuery = useQuery({
    queryKey: ['partners'],
    queryFn: () => list<Partner>('partners'),
  });

  const stationsQuery = useQuery({
    queryKey: ['stations'],
    queryFn: () => list<Station>('stations'),
  });

  const chargersQuery = useQuery({
    queryKey: ['chargers'],
    queryFn: () => list<Charger>('chargers'),
  });

  const visibleStations: VisibleStation[] = (() => {
    const partners = partnersQuery.data ?? [];
    const stations = stationsQuery.data ?? [];
    const chargers = chargersQuery.data ?? [];

    const visiblePartnerIds = new Set(
      partners.filter(p => p.is_verified && p.is_live && p.is_active).map(p => p.id)
    );

    const chargerCountMap: Record<string, { total: number; available: number }> = {};
    for (const ch of chargers) {
      if (!chargerCountMap[ch.station_id]) {
        chargerCountMap[ch.station_id] = { total: 0, available: 0 };
      }
      chargerCountMap[ch.station_id].total++;
      if (ch.status === 'available') {
        chargerCountMap[ch.station_id].available++;
      }
    }

    return stations
      .filter(s => visiblePartnerIds.has(s.partner_id))
      .map(s => ({
        ...s,
        availableCount: chargerCountMap[s.id]?.available || 0,
        totalChargers: chargerCountMap[s.id]?.total || 0,
      }));
  })();

  const isLoading = partnersQuery.isLoading || stationsQuery.isLoading || chargersQuery.isLoading;
  const isError = partnersQuery.isError || stationsQuery.isError || chargersQuery.isError;

  const handleMarkerPress = useCallback(
    (stationId: string) => {
      navigation.navigate('StationDetail', { stationId });
    },
    [navigation]
  );

  if (isError) {
    return (
      <View style={styles.centered}>
        <Text style={styles.errorText}>Failed to load stations. Please try again.</Text>
        <TouchableOpacity
          style={styles.retryButton}
          onPress={() => {
            partnersQuery.refetch();
            stationsQuery.refetch();
            chargersQuery.refetch();
          }}
        >
          <Text style={styles.retryText}>Retry</Text>
        </TouchableOpacity>
      </View>
    );
  }

  return (
    <View style={styles.container}>
      <View style={styles.header}>
        <Text style={styles.headerTitle}>BorneMap</Text>
        {isLoading && <ActivityIndicator size="small" color="#007AFF" style={styles.headerLoader} />}
      </View>

      <MapView
        style={styles.map}
        region={region}
        onRegionChangeComplete={setRegion}
      >
        {visibleStations.map(station => (
          <Marker
            key={station.id}
            coordinate={{
              latitude: station.latitude,
              longitude: station.longitude,
            }}
            pinColor={station.availableCount > 0 ? '#00E676' : '#EF4444'}
            onPress={() => handleMarkerPress(station.id)}
          >
            <Callout onPress={() => handleMarkerPress(station.id)}>
              <View style={styles.calloutContainer}>
                <Text style={styles.calloutTitle}>{station.name}</Text>
                <Text style={styles.calloutSubtitle}>
                  {station.availableCount}/{station.totalChargers} available
                </Text>
              </View>
            </Callout>
          </Marker>
        ))}
      </MapView>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  header: {
    position: 'absolute',
    top: 0,
    left: 0,
    right: 0,
    zIndex: 10,
    flexDirection: 'row',
    alignItems: 'center',
    paddingTop: Platform.OS === 'ios' ? 54 : 24,
    paddingBottom: 12,
    paddingHorizontal: 16,
    backgroundColor: '#FFFFFF',
    ...Platform.select({
      ios: { shadowColor: '#000', shadowOffset: { width: 0, height: 1 }, shadowOpacity: 0.1, shadowRadius: 3 },
      android: { elevation: 4 },
    }),
  },
  headerTitle: {
    fontSize: 20,
    fontWeight: '700',
    color: '#007AFF',
  },
  headerLoader: { marginLeft: 10 },
  map: { flex: 1 },
  centered: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
    padding: 24,
    backgroundColor: '#FFFFFF',
  },
  errorText: {
    fontSize: 14,
    color: '#9E9E9E',
    textAlign: 'center',
    marginBottom: 12,
  },
  retryButton: {
    backgroundColor: '#007AFF',
    paddingHorizontal: 16,
    paddingVertical: 8,
    borderRadius: 8,
  },
  retryText: {
    color: '#FFFFFF',
    fontSize: 14,
    fontWeight: '600',
  },
  calloutContainer: {
    minWidth: 160,
    padding: 4,
  },
  calloutTitle: {
    fontSize: 14,
    fontWeight: '600',
    color: '#000000',
  },
  calloutSubtitle: {
    fontSize: 12,
    color: '#666666',
    marginTop: 2,
  },
});
