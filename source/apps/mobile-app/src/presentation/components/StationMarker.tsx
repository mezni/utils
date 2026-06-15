import React from 'react';
import { Marker } from 'react-native-maps';
import { NearbyStationDto } from '@bornemap/shared-types';

interface StationMarkerProps {
  station: NearbyStationDto;
}

function StationMarkerInner({ station }: StationMarkerProps) {
  return (
    <Marker
      coordinate={{
        latitude: station.latitude,
        longitude: station.longitude,
      }}
      title={station.station_name}
      tracksViewChanges={false}
    />
  );
}

export const StationMarker = React.memo(StationMarkerInner);
