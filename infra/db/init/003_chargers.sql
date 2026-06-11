-- Charger seed data
INSERT INTO inventory.charger (id, station_id, connector_type, power_kw, status)
VALUES
  -- STA-001: 4 chargers
  ('CHR-001', 'STA-001', 'CCS2', 150, 'available'),
  ('CHR-002', 'STA-001', 'CCS2', 150, 'available'),
  ('CHR-003', 'STA-001', 'Type2', 22, 'occupied'),
  ('CHR-004', 'STA-001', 'CHAdeMO', 50, 'available'),

  -- STA-002: 3 chargers
  ('CHR-005', 'STA-002', 'CCS2', 150, 'available'),
  ('CHR-006', 'STA-002', 'Type2', 22, 'available'),
  ('CHR-007', 'STA-002', 'Type2', 22, 'occupied'),

  -- STA-003: 3 chargers
  ('CHR-008', 'STA-003', 'CCS2', 100, 'available'),
  ('CHR-009', 'STA-003', 'CCS2', 100, 'offline'),
  ('CHR-010', 'STA-003', 'Type2', 22, 'available'),

  -- STA-004: 3 chargers
  ('CHR-011', 'STA-004', 'CCS2', 150, 'available'),
  ('CHR-012', 'STA-004', 'CHAdeMO', 50, 'available'),
  ('CHR-013', 'STA-004', 'Type2', 22, 'available'),

  -- STA-005: 3 chargers
  ('CHR-014', 'STA-005', 'CCS2', 100, 'occupied'),
  ('CHR-015', 'STA-005', 'CCS2', 100, 'available'),
  ('CHR-016', 'STA-005', 'Type2', 22, 'available'),

  -- STA-006: 3 chargers
  ('CHR-017', 'STA-006', 'CCS2', 150, 'available'),
  ('CHR-018', 'STA-006', 'Type2', 22, 'available'),
  ('CHR-019', 'STA-006', 'Type2', 22, 'maintenance'),

  -- STA-007: 3 chargers
  ('CHR-020', 'STA-007', 'CCS2', 100, 'available'),
  ('CHR-021', 'STA-007', 'CHAdeMO', 50, 'available'),
  ('CHR-022', 'STA-007', 'Type2', 22, 'occupied'),

  -- STA-008: 3 chargers
  ('CHR-023', 'STA-008', 'CCS2', 150, 'available'),
  ('CHR-024', 'STA-008', 'CCS2', 150, 'available'),
  ('CHR-025', 'STA-008', 'Type2', 22, 'available'),

  -- STA-009: 3 chargers
  ('CHR-026', 'STA-009', 'CCS2', 100, 'available'),
  ('CHR-027', 'STA-009', 'Type2', 22, 'occupied'),
  ('CHR-028', 'STA-009', 'CHAdeMO', 50, 'offline'),

  -- STA-010: 2 chargers
  ('CHR-029', 'STA-010', 'CCS2', 150, 'available'),
  ('CHR-030', 'STA-010', 'Type2', 22, 'available');
