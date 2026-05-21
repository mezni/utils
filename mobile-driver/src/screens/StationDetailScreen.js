import React from 'react';
import { View, Text, StyleSheet, ScrollView } from 'react-native';

export default function StationDetailScreen({ route }) {
  const { station } = route.params;

  return (
    <ScrollView style={styles.container}>
      <Text style={styles.name}>{station.name}</Text>
      {station.address && <Text style={styles.address}>{station.address}</Text>}
      <Text style={styles.status}>
        {station.is_active ? 'Active' : 'Inactive'}
      </Text>

      <View style={styles.section}>
        <Text style={styles.sectionTitle}>Connectors</Text>
        {station.connectors?.map((connector) => (
          <View key={connector.id} style={styles.connector}>
            <Text>{connector.connector_type}</Text>
            <Text>{connector.status}</Text>
          </View>
        ))}
      </View>
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, padding: 16 },
  name: { fontSize: 24, fontWeight: 'bold', marginBottom: 8 },
  address: { fontSize: 16, color: '#666', marginBottom: 8 },
  status: { fontSize: 16, fontWeight: '600', marginBottom: 16 },
  section: { marginTop: 16 },
  sectionTitle: { fontSize: 18, fontWeight: '600', marginBottom: 8 },
  connector: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    padding: 12,
    backgroundColor: '#f5f5f5',
    borderRadius: 8,
    marginBottom: 8,
  },
});
