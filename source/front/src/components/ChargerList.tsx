import { View, Text, StyleSheet } from 'react-native';
import { Charger } from '../types';

const STATUS_COLORS: Record<Charger['status'], string> = {
  available: '#22c55e',
  occupied: '#f59e0b',
  offline: '#ef4444',
};

const CONNECTOR_LABELS: Record<Charger['connector_type'], string> = {
  type2: 'Type 2',
  ccs: 'CCS',
  chademo: 'CHAdeMO',
  wall: 'Wall',
};

interface ChargerListProps {
  chargers: Charger[];
}

function ChargerRow({ charger }: { charger: Charger }) {
  return (
    <View style={styles.chargerRow}>
      <View style={styles.chargerInfo}>
        <Text style={styles.chargerType}>
          {CONNECTOR_LABELS[charger.connector_type]}
        </Text>
        <Text style={styles.chargerPower}>{charger.power_kw} kW</Text>
      </View>
      <View
        style={[
          styles.statusBadge,
          { backgroundColor: STATUS_COLORS[charger.status] + '20' },
        ]}
      >
        <View
          style={[
            styles.statusDot,
            { backgroundColor: STATUS_COLORS[charger.status] },
          ]}
        />
        <Text
          style={[
            styles.statusText,
            { color: STATUS_COLORS[charger.status] },
          ]}
        >
          {charger.status.charAt(0).toUpperCase() + charger.status.slice(1)}
        </Text>
      </View>
    </View>
  );
}

export function ChargerList({ chargers }: ChargerListProps) {
  if (chargers.length === 0) {
    return <Text style={styles.empty}>No chargers available</Text>;
  }

  return (
    <View style={styles.container}>
      {chargers.map((c) => (
        <ChargerRow key={c.id} charger={c} />
      ))}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    gap: 0,
  },
  chargerRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    paddingVertical: 10,
    borderBottomWidth: 1,
    borderBottomColor: '#f3f4f6',
  },
  chargerInfo: {
    flexDirection: 'row',
    alignItems: 'baseline',
    gap: 8,
  },
  chargerType: {
    fontSize: 15,
    fontWeight: '500',
  },
  chargerPower: {
    fontSize: 13,
    color: '#6b7280',
  },
  statusBadge: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 12,
  },
  statusDot: {
    width: 8,
    height: 8,
    borderRadius: 4,
  },
  statusText: {
    fontSize: 13,
    fontWeight: '500',
  },
  empty: {
    fontSize: 14,
    color: '#9ca3af',
    fontStyle: 'italic',
  },
});
