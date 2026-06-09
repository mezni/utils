import { View, Text, FlatList, ActivityIndicator, TouchableOpacity, StyleSheet } from 'react-native';
import { useQuery } from '@tanstack/react-query';
import { useRoute, useNavigation } from '@react-navigation/native';
import type { RouteProp } from '@react-navigation/native';
import { get, list, type Station, type Charger } from '../api/client';
import type { RootStackParamList } from '../navigation/AppNavigator';
import { ChargerRow } from '../components/ChargerRow';

type DetailRoute = RouteProp<RootStackParamList, 'StationDetail'>;

export function StationDetailScreen() {
  const route = useRoute<DetailRoute>();
  const navigation = useNavigation();
  const { stationId } = route.params;

  const stationQuery = useQuery({
    queryKey: ['station', stationId],
    queryFn: () => get<Station>('stations', stationId),
  });

  const chargersQuery = useQuery({
    queryKey: ['chargers', stationId],
    queryFn: () => list<Charger>('chargers', { station_id: stationId }),
  });

  const isLoading = stationQuery.isLoading || chargersQuery.isLoading;
  const isError = stationQuery.isError || chargersQuery.isError;

  if (isLoading) {
    return (
      <View style={styles.centered}>
        <ActivityIndicator size="large" color="#007AFF" />
      </View>
    );
  }

  if (isError) {
    return (
      <View style={styles.centered}>
        <Text style={styles.errorText}>Failed to load station details. Please try again.</Text>
        <TouchableOpacity
          style={styles.retryButton}
          onPress={() => {
            stationQuery.refetch();
            chargersQuery.refetch();
          }}
        >
          <Text style={styles.retryText}>Retry</Text>
        </TouchableOpacity>
      </View>
    );
  }

  const station = stationQuery.data!;
  const chargers = chargersQuery.data ?? [];

  return (
    <View style={styles.container}>
      <View style={styles.header}>
        <TouchableOpacity onPress={() => navigation.goBack()} style={styles.backButton}>
          <Text style={styles.backText}>Back</Text>
        </TouchableOpacity>
        <Text style={styles.headerTitle}>Station Detail</Text>
        <View style={styles.backButton} />
      </View>

      <View style={styles.stationInfo}>
        <Text style={styles.stationName}>{station.name}</Text>
        <Text style={styles.stationAddress}>{station.address}</Text>
      </View>

      <View style={styles.chargersSection}>
        <Text style={styles.sectionTitle}>Chargers</Text>
        {chargers.length === 0 ? (
          <Text style={styles.emptyText}>No chargers at this station.</Text>
        ) : (
          <FlatList
            data={chargers}
            keyExtractor={item => item.id}
            renderItem={({ item }) => <ChargerRow charger={item} />}
          />
        )}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: '#FFFFFF' },
  header: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    paddingTop: 54,
    paddingBottom: 12,
    paddingHorizontal: 16,
    backgroundColor: '#FFFFFF',
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: '#E0E0E0',
  },
  backButton: { width: 50 },
  backText: { fontSize: 16, color: '#007AFF', fontWeight: '500' },
  headerTitle: { fontSize: 17, fontWeight: '600', color: '#000000' },
  stationInfo: { padding: 16, borderBottomWidth: StyleSheet.hairlineWidth, borderBottomColor: '#E0E0E0' },
  stationName: { fontSize: 22, fontWeight: '700', color: '#000000' },
  stationAddress: { fontSize: 14, color: '#666666', marginTop: 4 },
  chargersSection: { flex: 1, paddingTop: 8 },
  sectionTitle: { fontSize: 16, fontWeight: '600', color: '#000000', paddingHorizontal: 16, marginBottom: 4 },
  emptyText: { fontSize: 14, color: '#9E9E9E', textAlign: 'center', marginTop: 24 },
  centered: { flex: 1, justifyContent: 'center', alignItems: 'center', padding: 24, backgroundColor: '#FFFFFF' },
  errorText: { fontSize: 14, color: '#9E9E9E', textAlign: 'center', marginBottom: 12 },
  retryButton: { backgroundColor: '#007AFF', paddingHorizontal: 16, paddingVertical: 8, borderRadius: 8 },
  retryText: { color: '#FFFFFF', fontSize: 14, fontWeight: '600' },
});
