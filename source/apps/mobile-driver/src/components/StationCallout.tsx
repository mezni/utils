import { View, Text, StyleSheet } from 'react-native'
import { Station } from '../types'

interface StationCalloutProps {
  station: Station
}

export function StationCallout({ station }: StationCalloutProps) {
  const distanceText =
    station.distance_meters < 1000
      ? `${Math.round(station.distance_meters)}m`
      : `${(station.distance_meters / 1000).toFixed(1)}km`

  return (
    <View style={styles.container}>
      <Text style={styles.name}>{station.station_name}</Text>
      <Text style={styles.detail}>
        {distanceText} — {station.partner_name}
      </Text>
      {station.is_private && <Text style={styles.private}>Private charger</Text>}
    </View>
  )
}

const styles = StyleSheet.create({
  container: {
    padding: 8,
    minWidth: 150,
  },
  name: {
    fontWeight: '600',
    fontSize: 14,
    marginBottom: 2,
  },
  detail: {
    fontSize: 12,
    color: '#555',
  },
  private: {
    fontSize: 11,
    color: '#9333EA',
    marginTop: 2,
    fontStyle: 'italic',
  },
})
