import React from 'react';

export default function DashboardPage() {
  return (
    <div>
      <h1>Dashboard</h1>
      <p>Welcome to the EV Charging Platform Admin Dashboard.</p>
      <div style={{ display: 'flex', gap: '20px', marginTop: '20px' }}>
        <div style={{ padding: '20px', border: '1px solid #ccc', borderRadius: '8px', flex: 1 }}>
          <h3>Total Stations</h3>
          <p style={{ fontSize: '24px', fontWeight: 'bold' }}>--</p>
        </div>
        <div style={{ padding: '20px', border: '1px solid #ccc', borderRadius: '8px', flex: 1 }}>
          <h3>Active Connectors</h3>
          <p style={{ fontSize: '24px', fontWeight: 'bold' }}>--</p>
        </div>
        <div style={{ padding: '20px', border: '1px solid #ccc', borderRadius: '8px', flex: 1 }}>
          <h3>Partners</h3>
          <p style={{ fontSize: '24px', fontWeight: 'bold' }}>--</p>
        </div>
      </div>
    </div>
  );
}
