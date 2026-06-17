import React from 'react';
import type { Station } from '@bornemap/shared-types';

interface VisibilityFilterProps {
  selectedVisibility: string;
  onSelectVisibility: (visibility: string) => void;
  stations: Station[];
}

const visibilityOptions = [
  { value: 'all', label: 'All' },
  { value: 'commercial', label: 'Commercial' },
  { value: 'private_home', label: 'Private' },
];

function calculateVisibilityStats(stations: Station[]) {
  const stats: Record<string, number> = { all: stations.length, commercial: 0, private_home: 0 };
  stations.forEach((station) => {
    if (stats[station.visibility] !== undefined) {
      stats[station.visibility] = (stats[station.visibility] || 0) + 1;
    }
  });
  return stats;
}

export const VisibilityFilter: React.FC<VisibilityFilterProps> = ({
  selectedVisibility, onSelectVisibility, stations,
}) => {
  const stats = calculateVisibilityStats(stations);

  return (
    <div style={containerStyle}>
      <div style={titleStyle}>Visibility Filter</div>
      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
        {visibilityOptions.map((option) => {
          const isSelected = selectedVisibility === option.value;
          const isAvailable = (stats[option.value] || 0) > 0;
          return (
            <button
              key={option.value}
              onClick={() => isAvailable && onSelectVisibility(option.value)}
              disabled={!isAvailable}
              style={{
                flex: 1, padding: 12, borderRadius: 6, margin: '0 4px', cursor: 'pointer',
                backgroundColor: isSelected ? '#4CAF50' : '#f5f5f5',
                color: isSelected ? 'white' : '#333',
                border: 'none', fontWeight: 600, fontSize: 12,
                opacity: isAvailable ? 1 : 0.5,
              }}
            >
              <div>{option.label}</div>
              <div style={{ fontSize: 10, marginTop: 4 }}>{stats[option.value] || 0}</div>
            </button>
          );
        })}
      </div>
    </div>
  );
};

const containerStyle: React.CSSProperties = {
  backgroundColor: 'white', padding: 12, borderRadius: 8, marginBottom: 10,
  boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
};
const titleStyle: React.CSSProperties = {
  fontSize: 14, fontWeight: 'bold', color: '#333', marginBottom: 8,
};
