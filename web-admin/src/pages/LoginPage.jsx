import React from 'react';

export default function LoginPage() {
  return (
    <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100vh' }}>
      <form style={{ padding: '40px', border: '1px solid #ccc', borderRadius: '8px' }}>
        <h2>Admin Login</h2>
        <input type="text" placeholder="Username" style={{ display: 'block', marginBottom: '10px', width: '100%', padding: '8px' }} />
        <input type="password" placeholder="Password" style={{ display: 'block', marginBottom: '10px', width: '100%', padding: '8px' }} />
        <button type="submit" style={{ width: '100%', padding: '10px', backgroundColor: '#007AFF', color: 'white', border: 'none', borderRadius: '4px' }}>
          Login
        </button>
      </form>
    </div>
  );
}
