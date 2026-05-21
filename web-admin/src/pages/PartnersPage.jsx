import React, { useState } from 'react';

export default function PartnersPage() {
  const [partners, setPartners] = useState([]);
  const [inviteEmail, setInviteEmail] = useState('');

  const handleInvite = async (e) => {
    e.preventDefault();
    // TODO: Call admin API to send invitation
    console.log('Inviting:', inviteEmail);
  };

  return (
    <div>
      <h1>Partners</h1>
      <form onSubmit={handleInvite} style={{ marginBottom: '20px' }}>
        <input
          type="email"
          value={inviteEmail}
          onChange={(e) => setInviteEmail(e.target.value)}
          placeholder="Partner email"
          style={{ padding: '8px', marginRight: '10px' }}
        />
        <button type="submit" style={{ padding: '8px 16px' }}>Invite Partner</button>
      </form>
      <table style={{ width: '100%', borderCollapse: 'collapse' }}>
        <thead>
          <tr style={{ borderBottom: '2px solid #ccc' }}>
            <th style={{ textAlign: 'left', padding: '8px' }}>Name</th>
            <th style={{ textAlign: 'left', padding: '8px' }}>Email</th>
            <th style={{ textAlign: 'left', padding: '8px' }}>Stations</th>
          </tr>
        </thead>
        <tbody>
          {partners.map((partner) => (
            <tr key={partner.id} style={{ borderBottom: '1px solid #eee' }}>
              <td style={{ padding: '8px' }}>{partner.name}</td>
              <td style={{ padding: '8px' }}>{partner.email}</td>
              <td style={{ padding: '8px' }}>{partner.station_count || 0}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
