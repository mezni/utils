import { Marker, Callout } from 'react-native-maps';
import { View, Text, StyleSheet } from 'react-native';
import type { Station } from '../services/api';

interface Props {
  station: Station;
}

export default function StationMarker({ station }: Props) {
  return (
    <Marker
      coordinate={{
        latitude: station.latitude,
        longitude: station.longitude,
      }}
      title={station.name}
      description={`${station.available_chargers}/${station.total_chargers} available`}
    >
      <Callout>
        <View style={styles.callout}>
          <Text style={styles.name}>{station.name}</Text>
          <Text style={styles.address}>{station.address}</Text>
          <Text style={styles.chargers}>
            {station.available_chargers}/{station.total_chargers} available
          </Text>
        </View>
      </Callout>
    </Marker>
  );
}

const styles = StyleSheet.create({
  callout: {
    padding: 8,
    minWidth: 160,
  },
  name: {
    fontWeight: 'bold',
    fontSize: 14,
    marginBottom: 2,
  },
  address: {
    fontSize: 12,
    color: '#666',
    marginBottom: 4,
  },
  chargers: {
    fontSize: 12,
    color: '#166534',
  },
});
