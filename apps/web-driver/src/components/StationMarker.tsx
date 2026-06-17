import React, { useState } from 'react';
import { Marker, Popup } from 'react-leaflet';
import { divIcon, point } from 'leaflet';
import type { Station } from '@bornemap/shared-types';

interface StationMarkerProps {
  station: Station;
}

function getMarkerColor(visibility: string, status: string): string {
  if (status === 'closed' || status === 'inactive') return '#FF0000';
  switch (visibility) {
    case 'private_home': return '#FFA500';
    case 'commercial': return '#00AA00';
    default: return '#0000FF';
  }
}

function getStatusColor(status: string): string {
  switch (status) {
    case 'active': return '#00AA00';
    case 'inactive': return '#FFA500';
    case 'closed': return '#FF0000';
    case 'draft': return '#999999';
    default: return '#0000FF';
  }
}

function formatStatus(status: string): string {
  switch (status) {
    case 'active': return 'Active';
    case 'inactive': return 'Inactive';
    case 'closed': return 'Closed';
    case 'draft': return 'Draft';
    default: return status;
  }
}

export const StationMarker: React.FC<StationMarkerProps> = ({ station }) => {
  const [showPopup, setShowPopup] = useState(false);
  const color = getMarkerColor(station.visibility, station.status);

  const icon = divIcon({
    className: '',
    iconSize: point(24, 24),
    iconAnchor: point(12, 12),
    html: `<div style="
      width:24px;height:24px;border-radius:50%;background:${color};
      border:2px solid white;box-shadow:0 2px 8px rgba(0,0,0,0.3);
    "></div>`,
  });

  return (
    <Marker
      position={[station.location.lat, station.location.lon]}
      icon={icon}
      eventHandlers={{ click: () => setShowPopup(!showPopup) }}
    >
      {showPopup && (
        <Popup>
          <div style={{ padding: 8, minWidth: 180 }}>
            <h3 style={{ margin: '0 0 4px 0', fontSize: 16, fontWeight: 'bold' }}>{station.name}</h3>
            <p style={{ margin: '0 0 4px 0', fontSize: 13, color: '#666' }}>{station.address}</p>
            <p style={{ margin: '0 0 4px 0', fontSize: 12, color: '#999' }}>
              {station.distance_m >= 1000
                ? `${(station.distance_m / 1000).toFixed(1)} km away`
                : `${Math.round(station.distance_m)} m away`}
            </p>
            <p style={{ margin: 0, fontSize: 11, fontWeight: 600, textTransform: 'uppercase', color: getStatusColor(station.status) }}>
              {formatStatus(station.status)}
            </p>
          </div>
        </Popup>
      )}
    </Marker>
  );
};
