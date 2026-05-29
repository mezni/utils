import React, { useState } from 'react';
import MapView from './MapView';
import ZoomControls from './ZoomControls';
import mockStations from '../data/mockData';

const NAV_LINKS = [
  { label: 'ABOUT', href: '#about' },
  { label: 'APP', href: '#app' },
  { label: 'MAP', href: '#map' },
  { label: 'CONTACT', href: '#contact' },
];

export default function MapPortal() {
  const [stations] = useState(mockStations);
  const [selectedStation, setSelectedStation] = useState(null);
  const [locationDisabled] = useState(false);

  const handleMarkerPress = (station) => setSelectedStation(station);
  const closeDetail = () => setSelectedStation(null);

  return (
    <div style={{ width: '100%', height: '100%', position: 'relative', display: 'flex', flexDirection: 'column' }}>
      <nav style={styles.navbar}>
        <div style={styles.brand}>BorneMap</div>
        <div style={styles.navLinks}>
          {NAV_LINKS.map((link) => (
            <a key={link.label} href={link.href} style={styles.navLink}>{link.label}</a>
          ))}
        </div>
        <button style={styles.registerBtn}>REGISTER NOW</button>
      </nav>

      <div style={{ flex: 1, position: 'relative' }}>
        <MapView
          style={{ width: '100%', height: '100%' }}
          initialRegion={{ latitude: 36.8065, longitude: 10.1815, latitudeDelta: 0.08, longitudeDelta: 0.04 }}
          stations={stations}
          onMarkerPress={handleMarkerPress}
        />

        <div style={styles.searchWrapper}>
          <input
            type="text"
            placeholder="Search stations..."
            style={styles.searchInput}
            aria-label="Search charging stations"
          />
          <div style={styles.filterPills}>
            {['CCS2', 'Type2', 'Available', 'Online'].map((pill) => (
              <button key={pill} style={styles.filterPill}>{pill}</button>
            ))}
          </div>
        </div>

        <ZoomControls
          onZoomIn={() => {}}
          onZoomOut={() => {}}
          onLocateMe={() => {}}
          locationDisabled={locationDisabled}
        />

        {selectedStation && (
          <div style={styles.popoverCard}>
            <div style={styles.popoverHeader}>
              <div>
                <div style={styles.popoverTitle}>{selectedStation.station_name}</div>
                <div style={styles.popoverStatus}>
                  <span style={{ color: selectedStation.status === 'Online' ? '#1E7E34' : '#FF9800' }}>●</span>
                  {' '}{selectedStation.status}
                </div>
              </div>
              <button onClick={closeDetail} style={styles.closeBtn}>✕</button>
            </div>
            <div style={styles.popoverMeta}>{selectedStation.partner_name}</div>
            <div style={styles.chargerRow}>
              {selectedStation.chargers.map((ch) => (
                <span key={ch.id} style={styles.chargerChip}>
                  {ch.plug_type} · {ch.power_output}kW · {ch.status}
                </span>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

const styles = {
  navbar: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    height: 56,
    backgroundColor: '#111111',
    padding: '0 24px',
    zIndex: 100,
  },
  brand: {
    fontSize: 18,
    fontWeight: '800',
    color: '#FFFFFF',
  },
  navLinks: {
    display: 'flex',
    gap: 24,
  },
  navLink: {
    color: '#CCCCCC',
    textDecoration: 'none',
    fontSize: 13,
    fontWeight: '600',
    letterSpacing: 0.5,
  },
  registerBtn: {
    backgroundColor: '#00B653',
    color: '#FFFFFF',
    border: 'none',
    borderRadius: 4,
    padding: '8px 16px',
    fontSize: 13,
    fontWeight: '700',
    cursor: 'pointer',
  },
  searchInput: {
    position: 'absolute',
    top: 8,
    left: '50%',
    transform: 'translateX(-50%)',
    width: 320,
    height: 44,
    padding: '0 16px',
    border: '1px solid #E5E5E5',
    borderRadius: 8,
    fontSize: 14,
    backgroundColor: '#FFFFFF',
    zIndex: 40,
    boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
  },
  searchWrapper: {
    position: 'absolute',
    top: 8,
    left: '50%',
    transform: 'translateX(-50%)',
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    zIndex: 40,
  },
  filterPills: {
    display: 'flex',
    gap: 6,
    marginTop: 8,
  },
  filterPill: {
    background: '#FFFFFF',
    border: '1px solid #DDDDDD',
    borderRadius: 16,
    padding: '4px 12px',
    fontSize: 11,
    fontWeight: '600',
    color: '#666666',
    cursor: 'pointer',
  },
  popoverCard: {
    position: 'absolute',
    bottom: 24,
    left: '50%',
    transform: 'translateX(-50%)',
    width: 400,
    backgroundColor: '#FFFFFF',
    borderRadius: 12,
    padding: 20,
    zIndex: 40,
    boxShadow: '0 4px 20px rgba(0,0,0,0.15)',
  },
  popoverHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    marginBottom: 8,
  },
  popoverTitle: {
    fontSize: 16,
    fontWeight: '700',
    color: '#111111',
  },
  popoverStatus: {
    fontSize: 13,
    fontWeight: '600',
    color: '#666666',
    marginTop: 2,
  },
  popoverMeta: {
    fontSize: 12,
    color: '#888888',
    marginBottom: 12,
  },
  chargerRow: {
    display: 'flex',
    flexWrap: 'wrap',
    gap: 6,
  },
  chargerChip: {
    padding: '4px 10px',
    borderRadius: 12,
    backgroundColor: '#F5F5F5',
    fontSize: 11,
    fontWeight: '600',
    color: '#666666',
  },
  closeBtn: {
    background: 'none',
    border: 'none',
    cursor: 'pointer',
    fontSize: 18,
    color: '#888888',
    padding: 4,
  },
};
