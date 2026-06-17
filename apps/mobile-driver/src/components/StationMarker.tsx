import React from 'react';
import { Marker, Callout } from 'react-native-maps';
import { View, Text, StyleSheet } from 'react-native';
import type { Station } from '@bornemap/shared-types';

interface StationMarkerProps {
  station: Station;
  onPress?: () => void;
}

export const StationMarker: React.FC<StationMarkerProps> = ({ station, onPress }) => {
  return (
    <Marker
      coordinate={{
        latitude: station.location['lat'],
        longitude: station.location['lon'],
      }}
      identifier={`station-${station.id}`}
      onPress={onPress}
      pinColor={getMarkerColor(station.visibility, station.status)}
    >
      <Callout tooltip>
        <View style={styles.callout}>
          <View style={styles.statusIndicator}>
            <View style={[
              styles.statusDot,
              { backgroundColor: getStatusColor(station.status) }
            ]} />
          </View>
          <Text style={styles.calloutTitle}>{station.name}</Text>
          <Text style={styles.calloutAddress}>{station.address}</Text>
          <Text style={styles.calloutDistance}>
            {Math.round(station.distance_km)} km away
          </Text>
          <Text style={styles.calloutStatus}>
            {formatStatus(station.status)}
          </Text>
        </View>
      </Callout>
    </Marker>
  );
};

function getMarkerColor(visibility: string, status: string): string {
  if (status === 'closed' || status === 'inactive') {
    return '#FF0000';
  }
  
  switch (visibility) {
    case 'private_home':
      return '#FFA500';
    case 'commercial':
      return '#00AA00';
    default:
      return '#0000FF';
  }
}

function getStatusColor(status: string): string {
  switch (status) {
    case 'active':
      return '#00AA00';
    case 'inactive':
      return '#FFA500';
    case 'closed':
      return '#FF0000';
    case 'draft':
      return '#999999';
    default:
      return '#0000FF';
  }
}

function formatStatus(status: string): string {
  switch (status) {
    case 'active':
      return '● Active';
    case 'inactive':
      return '● Inactive';
    case 'closed':
      return '● Closed';
    case 'draft':
      return '● Draft';
    default:
      return status;
  }
}

const styles = StyleSheet.create({
  callout: {
    backgroundColor: 'white',
    borderRadius: 8,
    padding: 12,
    minWidth: 150,
  },
  statusIndicator: {
    flexDirection: 'row',
    alignItems: 'center',
    marginBottom: 8,
  },
  statusDot: {
    width: 8,
    height: 8,
    borderRadius: 4,
    marginRight: 6,
  },
  calloutTitle: {
    fontWeight: 'bold',
    fontSize: 14,
    marginBottom: 4,
  },
  calloutAddress: {
    fontSize: 12,
    color: '#666',
    marginBottom: 4,
  },
  calloutDistance: {
    fontSize: 11,
    color: '#999',
    marginBottom: 4,
  },
  calloutStatus: {
    fontSize: 10,
    fontWeight: '600',
    textTransform: 'uppercase',
  },
});
