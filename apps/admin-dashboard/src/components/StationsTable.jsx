import React, { useState } from 'react';
import { mockStations } from '../data/mockData';
import theme from '../styles/theme';

export default function StationsTable() {
  const [query, setQuery] = useState('');
  const filtered = mockStations.filter(
    (s) =>
      !query ||
      s.name.toLowerCase().includes(query.toLowerCase()) ||
      s.id.includes(query) ||
      s.location.toLowerCase().includes(query.toLowerCase())
  );

  return (
    <div style={styles.tableSection}>
      <div style={styles.headerRow}>
        <h4 style={styles.sectionTitle}>Deployed System Hub Nodes</h4>
        <input
          type="text"
          placeholder="Search stations..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          style={styles.searchInput}
        />
      </div>
      <table style={styles.dataTable}>
        <thead>
          <tr style={styles.tableHeaderRow}>
            <th>HUB ID</th>
            <th>NAME DESIGNATION</th>
            <th>ZONAL PLACEMENT</th>
            <th>STATUS</th>
          </tr>
        </thead>
        <tbody>
          {filtered.map((s) => (
            <tr key={s.id} style={styles.tableBodyRow}>
              <td style={styles.tdCode}>{s.id}</td>
              <td style={styles.tdBold}>{s.name}</td>
              <td>{s.location}</td>
              <td style={styles.statusGreen}>● {s.status}</td>
            </tr>
          ))}
          {filtered.length === 0 && (
            <tr>
              <td colSpan={4} style={styles.emptyRow}>No stations match your search.</td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

const styles = {
  tableSection: {
    backgroundColor: theme.colors.surface,
    border: `1px solid ${theme.colors.border}`,
    borderRadius: theme.borderRadius.md,
    padding: '24px',
  },
  headerRow: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: '16px',
  },
  sectionTitle: {
    fontSize: theme.fontSize.lg,
    fontWeight: theme.fontWeight.extrabold,
    margin: 0,
  },
  searchInput: {
    padding: '8px 12px',
    border: `1px solid ${theme.colors.border}`,
    borderRadius: theme.borderRadius.md,
    fontSize: theme.fontSize.md,
    outline: 'none',
    width: '240px',
  },
  dataTable: {
    width: '100%',
    borderCollapse: 'collapse',
    textAlign: 'left',
  },
  tableHeaderRow: {
    borderBottom: '2px solid #EEE',
  },
  tableBodyRow: {
    borderBottom: '1px solid #F5F5F5',
  },
  tdCode: {
    padding: '12px',
    fontFamily: 'monospace',
    color: theme.colors.textSecondary,
  },
  tdBold: {
    padding: '12px',
    fontWeight: theme.fontWeight.bold,
  },
  statusGreen: {
    padding: '12px',
    color: theme.colors.success,
    fontWeight: theme.fontWeight.bold,
  },
  emptyRow: {
    padding: '24px',
    textAlign: 'center',
    color: theme.colors.textMuted,
    fontSize: theme.fontSize.md,
  },
};
