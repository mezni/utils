import { View, Text, StyleSheet } from 'react-native';
import type { Charger } from '../api/client';

const STATUS_COLORS: Record<string, string> = {
  available: '#00E676',
  in_use: '#FF9800',
  maintenance: '#9E9E9E',
  offline: '#EF4444',
};

interface ChargerRowProps {
  charger: Charger;
}

export function ChargerRow({ charger }: ChargerRowProps) {
  const statusColor = STATUS_COLORS[charger.status] || '#9E9E9E';

  return (
    <View style={styles.row}>
      <View style={styles.left}>
        <Text style={styles.connector}>{charger.connector_type}</Text>
        <Text style={styles.power}>{charger.power_kw} kW</Text>
      </View>
      <Text style={[styles.status, { color: statusColor }]}>
        {charger.status}
      </Text>
    </View>
  );
}

const styles = StyleSheet.create({
  row: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    paddingVertical: 12,
    paddingHorizontal: 16,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: '#E0E0E0',
  },
  left: {
    flexDirection: 'column',
  },
  connector: {
    fontSize: 15,
    fontWeight: '500',
    color: '#000000',
  },
  power: {
    fontSize: 12,
    color: '#666666',
    marginTop: 2,
  },
  status: {
    fontSize: 14,
    fontWeight: '600',
    textTransform: 'capitalize',
  },
});
