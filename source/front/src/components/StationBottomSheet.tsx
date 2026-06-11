import { useEffect, useCallback } from 'react';
import { View, Text, StyleSheet } from 'react-native';
import { BottomSheet, Skeleton, ErrorState } from '@borne/design-system';
import { useStationDetail } from '../hooks/useStationDetail';
import { useClickstream } from '../hooks/useClickstream';
import { ChargerList } from './ChargerList';

interface StationBottomSheetProps {
  stationId: string | null;
  onClose: () => void;
}

export function StationBottomSheet({
  stationId,
  onClose,
}: StationBottomSheetProps) {
  const { station, loading, error, refetch } = useStationDetail();
  const { track } = useClickstream();

  useEffect(() => {
    if (stationId) {
      refetch(stationId);
      track({
        event_type: 'station_view',
        timestamp: new Date().toISOString(),
        station_id: stationId,
      });
    }
  }, [stationId, refetch, track]);

  const handleRetry = useCallback(() => {
    if (stationId) refetch(stationId);
  }, [stationId, refetch]);

  return (
    <BottomSheet isOpen={stationId !== null} onClose={onClose}>
      {loading && <Skeleton variant="list" rows={3} />}

      {error && <ErrorState message={error} onRetry={handleRetry} />}

      {station && (
        <View style={styles.content}>
          <Text style={styles.name}>{station.name}</Text>
          {station.address && (
            <Text style={styles.address}>{station.address}</Text>
          )}
          {station.distance_m !== null && (
            <Text style={styles.distance}>
              {Math.round(station.distance_m)}m away
            </Text>
          )}

          <View style={styles.divider} />

          <Text style={styles.sectionTitle}>Chargers</Text>
          <ChargerList chargers={station.chargers} />
        </View>
      )}
    </BottomSheet>
  );
}

const styles = StyleSheet.create({
  content: {
    paddingBottom: 24,
  },
  name: {
    fontSize: 20,
    fontWeight: '700',
    marginBottom: 4,
  },
  address: {
    fontSize: 14,
    color: '#6b7280',
    marginBottom: 2,
  },
  distance: {
    fontSize: 13,
    color: '#9ca3af',
  },
  divider: {
    height: 1,
    backgroundColor: '#e5e7eb',
    marginVertical: 16,
  },
  sectionTitle: {
    fontSize: 16,
    fontWeight: '600',
    marginBottom: 12,
  },
});
