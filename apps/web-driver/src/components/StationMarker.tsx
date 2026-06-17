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
          position: 'relative',
        }}
        onClick={(e) => {
          e.stopPropagation();
          onClick?.(station);
        }}
        title={formatStatus(station.status)}
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
          <div style={styles.statusRow}>
            <div
              style={{
                width: '10px',
                height: '10px',
                borderRadius: '50%',
                backgroundColor: getStatusColor(station.status),
                marginRight: '8px',
              }}
            />
            <h3 style={styles.popupTitle}>{station.name}</h3>
          </div>
          <p style={styles.popupAddress}>{station.address}</p>
          <p style={styles.popupDistance}>
            {Math.round(station.distance_km)} km away
          </p>
          <p style={styles.popupStatus}>
            {formatStatus(station.status)}
          </p>
        </div>
      </Popup>
    </Marker>
  );
}

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
      return 'Active';
    case 'inactive':
      return 'Inactive';
    case 'closed':
      return 'Closed';
    case 'draft':
      return 'Draft';
    default:
      return status;
  }
}

const styles = {
  popup: {
    padding: '12px',
    minWidth: '200px',
  },
  statusRow: {
    display: 'flex',
    alignItems: 'center',
  },
  popupTitle: {
    margin: '0 0 8px 0',
    fontSize: '16px',
    fontWeight: 'bold',
    color: '#333',
    flex: 1,
  },
  popupAddress: {
    margin: '0 0 8px 0',
    fontSize: '13px',
    color: '#666',
  },
  popupDistance: {
    margin: '0 0 8px 0',
    fontSize: '12px',
    color: '#999',
  },
  popupStatus: {
    margin: '0',
    fontSize: '11px',
    fontWeight: '600',
    textTransform: 'uppercase',
    color: '#666',
  },
} as const;
