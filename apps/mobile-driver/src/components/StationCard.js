import React from 'react';
import { View, Text, TouchableOpacity, StyleSheet, Linking, Platform } from 'react-native';

export default function StationCard({ station }) {
  const openNavigation = () => {
    const { latitude, longitude } = station;
    const url = Platform.select({
      ios: `maps://app?daddr=${latitude},${longitude}`,
      android: `geo:${latitude},${longitude}?q=${latitude},${longitude}`,
      default: `https://www.google.com/maps/dir/?api=1&destination=${latitude},${longitude}`,
    });
    Linking.openURL(url);
  };

  return (
    <View style={styles.card}>
      <View style={styles.header}>
        <Text style={styles.name}>{station.name}</Text>
        <View style={[styles.badge, { backgroundColor: station.status === 'Available' ? '#00C853' : '#FF3D00' }]}>
          <Text style={styles.badgeText}>{station.status}</Text>
        </View>
      </View>
      <Text style={styles.provider}>{station.provider_name}</Text>
      {station.chargers.length > 0 ? (
        station.chargers.map((charger) => (
          <View key={charger.id} style={styles.chargerRow}>
            <Text style={styles.chargerText}>{charger.plug_type}</Text>
            <Text style={styles.chargerText}>{charger.power_output} kW</Text>
            <Text style={[styles.chargerText, { color: charger.status === 'Available' ? '#00C853' : '#FF3D00' }]}>
              {charger.status}
            </Text>
          </View>
        ))
      ) : (
        <Text style={styles.noChargers}>Charger details unavailable</Text>
      )}
      <TouchableOpacity style={styles.navButton} onPress={openNavigation}>
        <Text style={styles.navButtonText}>Navigate</Text>
      </TouchableOpacity>
    </View>
  );
}

const styles = StyleSheet.create({
  card: { backgroundColor: '#FFFFFF', borderRadius: 16, padding: 16, elevation: 5 },
  header: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 },
  name: { fontSize: 16, fontWeight: '700', flex: 1, marginRight: 8 },
  badge: { paddingHorizontal: 10, paddingVertical: 3, borderRadius: 12 },
  badgeText: { color: '#FFFFFF', fontSize: 12, fontWeight: '600' },
  provider: { fontSize: 14, color: '#666', marginBottom: 8 },
  chargerRow: { flexDirection: 'row', justifyContent: 'space-between', paddingVertical: 4, borderBottomWidth: 1, borderBottomColor: '#F0F0F0' },
  chargerText: { fontSize: 14, color: '#333' },
  noChargers: { fontSize: 14, color: '#999', fontStyle: 'italic' },
  navButton: { backgroundColor: '#007AFF', borderRadius: 12, paddingVertical: 10, alignItems: 'center', marginTop: 10 },
  navButtonText: { color: '#FFFFFF', fontSize: 15, fontWeight: '600' },
});
