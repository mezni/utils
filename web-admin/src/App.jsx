import React from 'react';
import { Outlet, Link, useNavigate } from 'react-router-dom';

export default function App() {
  const navigate = useNavigate();

  const handleLogout = () => {
    // Clear session cookie via backend
    fetch('/api/v1/admin/logout', { method: 'POST' })
      .then(() => navigate('/login'));
  };

  return (
    <div style={{ display: 'flex', minHeight: '100vh' }}>
      <nav style={{ width: '200px', backgroundColor: '#1a1a2e', color: 'white', padding: '20px' }}>
        <h2>EV Admin</h2>
        <ul style={{ listStyle: 'none', padding: 0 }}>
          <li><Link to="/" style={{ color: 'white', textDecoration: 'none' }}>Dashboard</Link></li>
          <li><Link to="/stations" style={{ color: 'white', textDecoration: 'none' }}>Stations</Link></li>
          <li><Link to="/partners" style={{ color: 'white', textDecoration: 'none' }}>Partners</Link></li>
        </ul>
        <button onClick={handleLogout} style={{ marginTop: '20px', width: '100%' }}>
          Logout
        </button>
      </nav>
      <main style={{ flex: 1, padding: '20px' }}>
        <Outlet />
      </main>
    </div>
  );
}
