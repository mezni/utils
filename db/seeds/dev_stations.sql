-- Seed Data: Stations
-- Purpose: Development seed data for stations across Tunisia
-- Author: BorneMap Development Team
-- Date: 2026-06-07

-- Clear existing data for idempotent re-runs
TRUNCATE inventory.station CASCADE;

-- Insert stations by region (Tunis, Sfax, Sousse, Nabeul, Bizerte, Gabès, Kairouan, Monastir)
INSERT INTO inventory.station (id, partner_id, name, address, latitude, longitude, created_at, updated_at) VALUES
    -- Tunis (5 stations)
    ('STN-1a2b', 'PRT-A7xL', 'Tunis-Belvedere Station', 'Rue du Lac, Tunis', 36.864702, 10.158423, now(), now()),
    ('STN-2c3d', 'PRT-A7xL', 'Hammamet Station', 'Boulevard du 7 Novembre, Tunis', 36.846200, 10.180000, now(), now()),
    ('STN-3e4f', 'PRT-A7xL', 'Rue de la Liberté Station', 'Avenue Habib Bourguiba, Tunis', 36.863500, 10.162500, now(), now()),
    ('STN-4g5h', 'PRT-A7xL', 'Ettadhamen Station', 'Rue de l'Avenir, Ettadhamen', 36.830000, 10.135000, now(), now()),
    ('STN-5i6j', 'PRT-A7xL', 'Sidi Thabet Station', 'Route de la Soukra, Sidi Thabet', 36.856000, 10.158000, now(), now()),

    -- Sfax (3 stations)
    ('STN-6k7l', 'PRT-B2mN', 'Sfax City Center Station', 'Place du 9 Avril, Sfax', 34.740600, 10.760300, now(), now()),
    ('STN-7m8n', 'PRT-B2mN', 'Sfax Airport Station', 'Route de l'Aéroport, Sfax', 34.690000, 10.660000, now(), now()),
    ('STN-8o9p', 'PRT-B2mN', 'Sfax Port Station', 'Avenue Habib Bourguiba, Sfax', 34.735000, 10.750000, now(), now()),

    -- Sousse (2 stations)
    ('STN-9q0r', 'PRT-C9kP', 'Sousse Medina Station', 'Avenue Habib Bourguiba, Sousse', 35.825600, 10.636300, now(), now()),
    ('STN-10s11', 'PRT-C9kP', 'Port of Sousse Station', 'Place des Prairies, Sousse', 35.823000, 10.620000, now(), now()),

    -- Nabeul (1 station)
    ('STN-12t13', 'PRT-A7xL', 'Nabeul Station', 'Avenue Habib Bourguiba, Nabeul', 36.463500, 10.755000, now(), now()),

    -- Bizerte (1 station)
    ('STN-14u15', 'PRT-B2mN', 'Bizerte Port Station', 'Avenue de la Marine, Bizerte', 37.274700, 9.873900, now(), now()),

    -- Gabès (1 station)
    ('STN-16v17', 'PRT-C9kP', 'Gabès Station', 'Avenue des Martyrs, Gabès', 33.883700, 10.092900, now(), now()),

    -- Kairouan (1 station)
    ('STN-18w19', 'PRT-A7xL', 'Kairouan Station', 'Avenue de la République, Kairouan', 35.678100, 10.096300, now(), now()),

    -- Monastir (1 station)
    ('STN-20x21', 'PRT-C9kP', 'Monastir Station', 'Avenue Habib Bourguiba, Monastir', 35.775400, 10.817200, now(), now());

-- Verify insertion
SELECT 'Stations inserted:' AS message, COUNT(*) AS count FROM inventory.station;
SELECT 'Partners with stations:' AS message, partner_id, COUNT(*) AS station_count
FROM inventory.station
GROUP BY partner_id
ORDER BY station_count DESC;
