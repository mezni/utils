import React from 'react';
import { Marker, Popup } from 'react-map-gl';
import type { Station } from '@bornemap/shared-types';

interface StationMarkerProps {
  station: Station;
  onClick?: (station: Station) => void;
}

export const StationMarker: React.FC<StationMarkerProps> = ({ station, onClick }) => {
  return (
    <Marker
      longitude={station.location['lon']}
      latitude={station.location['lat']}
      anchor="center"
    >
      <div
        style={{
          backgroundColor: getMarkerColor(station.visibility, station.status),
          width: '24px',
          height: '24px',
          borderRadius: '50%',
          border: '2px solid white',
          boxShadow: '0 2px 8px rgba(0,0,0,0.3)',
          cursor: 'pointer',
        }}
        onClick={(e) => {
          e.stopPropagation();
          onClick?.(station);
        }}
      />
      <Popup
        longitude={station.location['lon']}
        latitude={station.location['lat']}
        anchor="bottom"
        closeButton={true}
        closeOnClick={false}
        offsetTop={-10}
        offsetLeft={-10}
        maxWidth={300}
      >
        <div style={styles.popup}>
          <h3 style={styles.popupTitle}>{station.name}</h3>
          <p style={styles.popupAddress}>{station.address}</p>
          <p style={styles.popupDistance}>
            {Math.round(station.distance_km)} km away
          </p>
        </div>
      </Popup>
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

const styles = {
  popup: {
    padding: '12px',
    minWidth: '200px',
  },
  popupTitle: {
    margin: '0 0 8px 0',
    fontSize: '16px',
    fontWeight: 'bold',
    color: '#333',
  },
  popupAddress: {
    margin: '0 0 8px 0',
    fontSize: '13px',
    color: '#666',
  },
  popupDistance: {
    margin: '0',
    fontSize: '12px',
    color: '#999',
  },
} as const;
