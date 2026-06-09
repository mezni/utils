-- Seed 003: Chargers
-- Idempotent: TRUNCATE + INSERT
-- Matches source/mock/db.json chargers (24 rows)
-- Note: CHG023 connector_type changed from 'type1' to 'type2' (type1 not in ev-core enum)

TRUNCATE "ev-platform".charger CASCADE;

INSERT INTO "ev-platform".charger (id, station_id, connector_type, power_kw, status, created_at, created_by, updated_at, updated_by)
VALUES
    ('CHG001', 'STN001', 'type2',   22,   'available',   '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG002', 'STN001', 'ccs',     150,  'available',   '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG003', 'STN001', 'chademo', 50,   'in_use',      '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG004', 'STN002', 'type2',   22,   'available',   '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG005', 'STN002', 'ccs',     100,  'available',   '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG006', 'STN003', 'type2',   7,    'available',   '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG007', 'STN003', 'ccs',     150,  'maintenance', '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG008', 'STN003', 'chademo', 50,   'available',   '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG009', 'STN004', 'type2',   22,   'available',   '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG010', 'STN004', 'type2',   22,   'in_use',      '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG011', 'STN005', 'type2',   22,   'available',   '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG012', 'STN005', 'ccs',     50,   'available',   '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG013', 'STN006', 'chademo', 50,   'available',   '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG014', 'STN006', 'type2',   22,   'offline',     '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG015', 'STN007', 'type2',   7,    'available',   '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG016', 'STN008', 'type2',   22,   'available',   '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG017', 'STN008', 'ccs',     100,  'available',   '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG018', 'STN009', 'type2',   22,   'available',   '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG019', 'STN010', 'type2',   22,   'in_use',      '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG020', 'STN011', 'ccs',     100,  'available',   '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG021', 'STN012', 'chademo', 50,   'available',   '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG022', 'STN013', 'type2',   22,   'available',   '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG023', 'STN014', 'type2',   7,    'available',   '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('CHG024', 'STN015', 'ccs',     350,  'available',   '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001');
