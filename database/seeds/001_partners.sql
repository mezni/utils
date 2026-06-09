-- Seed 001: Partners
-- Idempotent: TRUNCATE + INSERT
-- Matches source/mock/db.json partners (3 rows)

TRUNCATE "ev-platform".partner CASCADE;

INSERT INTO "ev-platform".partner (id, name, type, is_verified, is_live, is_active, created_at, created_by, updated_at, updated_by)
VALUES
    ('PRT001', 'Tunisie Electrique',   'business', true,  true,  true, '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('PRT002', 'EcoCharge Tunisie',    'business', true,  false, true, '2026-02-10T10:30:00Z', 'USR-ADMIN-001', '2026-02-10T10:30:00Z', 'USR-ADMIN-001'),
    ('PRT003', 'Ahmed Ben Salem',      'personal', false, false, true, '2026-03-05T14:00:00Z', 'USR-ADMIN-001', '2026-03-05T14:00:00Z', 'USR-ADMIN-001');
