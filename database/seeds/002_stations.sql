-- Seed 002: Stations
-- Idempotent: TRUNCATE + INSERT
-- Matches source/mock/db.json stations (15 rows)

TRUNCATE "ev-platform".station CASCADE;

INSERT INTO "ev-platform".station (id, partner_id, name, address, latitude, longitude, created_at, created_by, updated_at, updated_by)
VALUES
    ('STN001', 'PRT001', 'Tunis Centre Urbain',  'Avenue Habib Bourguiba, Tunis',    36.8008, 10.1815, '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('STN002', 'PRT001', 'Sfax Station',         'Route de Gabès, Sfax',             34.7400, 10.7600, '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('STN003', 'PRT001', 'Sousse Plage',         'Avenue Mohamed V, Sousse',         35.8256, 10.6367, '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('STN004', 'PRT001', 'Ettadhamen City',      'Rue de la Liberté, Ettadhamen',    36.8364, 10.1167, '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('STN005', 'PRT001', 'Kairouan Ouest',       'Avenue de la République, Kairouan', 35.6781, 10.0964, '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('STN006', 'PRT001', 'Bizerte Port',         'Rue de la Gare, Bizerte',          37.2744, 9.8739,  '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('STN007', 'PRT001', 'Gabès Centre',         'Avenue Habib Thameur, Gabès',      33.8833, 10.1000, '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('STN008', 'PRT001', 'Ariana Ville',         'Rue 2 Mars, Ariana',               36.8667, 10.2000, '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('STN009', 'PRT001', 'Gafsa Station',        'Avenue de la Révolution, Gafsa',   34.4167, 8.7833,  '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('STN010', 'PRT001', 'El Mourouj Residence', 'Rue de la Palestine, El Mourouj',  36.6833, 10.1667, '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('STN011', 'PRT001', 'Kasserine Route',      'Avenue de l''Environnement, Kasserine', 35.1667, 8.8333, '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('STN012', 'PRT001', 'Monastir Marina',      'Rue de la Corniche, Monastir',     35.7667, 10.8333, '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('STN013', 'PRT001', 'Hammamet Yasmine',     'Avenue Habib Bourguiba, Hammamet', 36.3833, 10.6167, '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('STN014', 'PRT001', 'Nabeul Centre',        'Rue de la République, Nabeul',     36.4500, 10.7333, '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001'),
    ('STN015', 'PRT001', 'Medenine Sud',         'Route de Zarzis, Medenine',        33.3500, 10.4833, '2026-01-15T08:00:00Z', 'USR-ADMIN-001', '2026-01-15T08:00:00Z', 'USR-ADMIN-001');
