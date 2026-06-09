-- Seed 004: Station Availability
-- Idempotent: TRUNCATE + INSERT
-- Matches source/mock/db.json station_availability (15 rows)

TRUNCATE "ev-platform".station_availability;

INSERT INTO "ev-platform".station_availability (id, station_id, status, updated_by, updated_at)
VALUES
    ('SA001', 'STN001', 'available',   'USR-PRT001', '2026-06-01T08:00:00Z'),
    ('SA002', 'STN002', 'available',   'USR-PRT001', '2026-06-01T08:00:00Z'),
    ('SA003', 'STN003', 'partial',     'USR-PRT001', '2026-06-01T08:00:00Z'),
    ('SA004', 'STN004', 'available',   'USR-PRT001', '2026-06-01T08:00:00Z'),
    ('SA005', 'STN005', 'available',   'USR-PRT001', '2026-06-01T08:00:00Z'),
    ('SA006', 'STN006', 'partial',     'USR-PRT001', '2026-06-01T08:00:00Z'),
    ('SA007', 'STN007', 'available',   'USR-PRT001', '2026-06-01T08:00:00Z'),
    ('SA008', 'STN008', 'available',   'USR-PRT001', '2026-06-01T08:00:00Z'),
    ('SA009', 'STN009', 'available',   'USR-PRT001', '2026-06-01T08:00:00Z'),
    ('SA010', 'STN010', 'available',   'USR-PRT001', '2026-06-01T08:00:00Z'),
    ('SA011', 'STN011', 'available',   'USR-PRT001', '2026-06-01T08:00:00Z'),
    ('SA012', 'STN012', 'available',   'USR-PRT001', '2026-06-01T08:00:00Z'),
    ('SA013', 'STN013', 'available',   'USR-PRT001', '2026-06-01T08:00:00Z'),
    ('SA014', 'STN014', 'available',   'USR-PRT001', '2026-06-01T08:00:00Z'),
    ('SA015', 'STN015', 'available',   'USR-PRT001', '2026-06-01T08:00:00Z');
