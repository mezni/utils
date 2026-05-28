import React from 'react';
import { StyleSheet, View, Text } from 'react-native';

export default function StationCard({ station }) {
  return (
    <View style={styles.card}>
      <Text style={styles.title}>{station.name}</Text>
      <Text style={styles.provider}>{station.provider_name}</Text>

      <View style={styles.badgeContainer}>
        <View style={[styles.statusBadge, station.status === 'Available' ? styles.bgAvailable : styles.bgOccupied]}>
          <Text style={styles.badgeText}>{station.status}</Text>
        </View>
      </View>

      <Text style={styles.chargerHeader}>Connectors:</Text>
      {station.chargers.map((charger) => (
        <View key={charger.id} style={styles.chargerRow}>
          <Text style={styles.chargerText}>⚡ {charger.plug_type} ({charger.power_output} kW)</Text>
          <Text style={[styles.chargerStatus, charger.status === 'Available' ? styles.txtAvailable : styles.txtOccupied]}>
            {charger.status}
          </Text>
        </View>
      ))}
    </View>
  );
}

const styles = StyleSheet.create({
  card: { backgroundColor: '#FFFFFF', padding: 4 },
  title: { fontSize: 16, fontWeight: 'bold', color: '#111111' },
  provider: { fontSize: 12, color: '#666666', marginTop: 2, textTransform: 'uppercase', letterSpacing: 0.5 },
  badgeContainer: { flexDirection: 'row', marginTop: 8 },
  statusBadge: { paddingHorizontal: 10, paddingVertical: 4, borderRadius: 12 },
  bgAvailable: { backgroundColor: '#E8F5E9' },
  bgOccupied: { backgroundColor: '#FFEBEE' },
  badgeText: { fontSize: 12, fontWeight: '600' },
  chargerHeader: { fontSize: 13, fontWeight: '600', color: '#444444', marginTop: 14, marginBottom: 4 },
  chargerRow: { flexDirection: 'row', justifyContent: 'space-between', paddingVertical: 4, borderBottomWidth: 0.5, borderBottomColor: '#EEEEEE' },
  chargerText: { fontSize: 13, color: '#333333' },
  chargerStatus: { fontSize: 12, fontWeight: '500' },
  txtAvailable: { color: '#2E7D32' },
  txtOccupied: { color: '#C62828' }
});
