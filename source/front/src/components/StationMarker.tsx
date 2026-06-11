import MapView, { Marker } from 'react-native-maps';
import { Station } from '../types';

interface StationMarkerProps {
  station: Station;
  onPress: (stationId: string) => void;
}

export function StationMarker({ station, onPress }: StationMarkerProps) {
  return (
    <Marker
      coordinate={{ latitude: station.latitude, longitude: station.longitude }}
      title={station.name}
      description={
        station.address ?? station.distance_m !== null
          ? `${Math.round(station.distance_m)}m away`
          : undefined
      }
      onPress={() => onPress(station.id)}
    />
  );
}
