import React, { useState, useEffect } from 'react';

export default function StationsPage() {
  const [stations, setStations] = useState([]);

  useEffect(() => {
    // TODO: Fetch stations from admin API
    fetch('/api/v1/admin/stations')
      .then((res) => res.json())
      .then(setStations)
      .catch(console.error);
  }, []);

  return (
    <div>
      <h1>Stations</h1>
      <button style={{ marginBottom: '20px', padding: '10px 20px' }}>Add Station</button>
      <table style={{ width: '100%', borderCollapse: 'collapse' }}>
        <thead>
          <tr style={{ borderBottom: '2px solid #ccc' }}>
            <th style={{ textAlign: 'left', padding: '8px' }}>Name</th>
            <th style={{ textAlign: 'left', padding: '8px' }}>Status</th>
            <th style={{ textAlign: 'left', padding: '8px' }}>Connectors</th>
            <th style={{ textAlign: 'left', padding: '8px' }}>Actions</th>
          </tr>
        </thead>
        <tbody>
          {stations.map((station) => (
            <tr key={station.id} style={{ borderBottom: '1px solid #eee' }}>
              <td style={{ padding: '8px' }}>{station.name}</td>
              <td style={{ padding: '8px' }}>{station.is_active ? 'Active' : 'Inactive'}</td>
              <td style={{ padding: '8px' }}>{station.connector_count || 0}</td>
              <td style={{ padding: '8px' }}>
                <button>Edit</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
