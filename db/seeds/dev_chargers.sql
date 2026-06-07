-- Seed Data: Chargers
-- Purpose: Development seed data for chargers across Tunisia
-- Author: BorneMap Development Team
-- Date: 2026-06-07

-- Clear existing data for idempotent re-runs
TRUNCATE inventory.charger CASCADE;

-- Insert chargers (24 chargers across 15 stations, 1-2 per station)
INSERT INTO inventory.charger (id, station_id, connector_type, power_kw, status, updated_at) VALUES
    -- Tunis station chargers
    ('CHG-1a2b', 'STN-1a2b', 'Type2', 22, 'Available', now()),
    ('CHG-2c3d', 'STN-1a2b', 'CCS', 50, 'Available', now()),
    ('CHG-3e4f', 'STN-2c3d', 'Type2', 22, 'Available', now()),
    ('CHG-4g5h', 'STN-2c3d', 'CCS', 50, 'Available', now()),
    ('CHG-5i6j', 'STN-3e4f', 'Type2', 22, 'Available', now()),
    ('CHG-6k7l', 'STN-3e4f', 'Type2Combo', 120, 'Available', now()),
    ('CHG-7m8n', 'STN-4g5h', 'Type2', 11, 'Available', now()),
    ('CHG-8o9p', 'STN-4g5h', 'Type2', 22, 'Available', now()),
    ('CHG-9q0r', 'STN-5i6j', 'Type2', 7, 'Available', now()),
    ('CHG-10s11', 'STN-5i6j', 'Type2', 22, 'Available', now()),

    -- Sfax station chargers
    ('CHG-12t13', 'STN-6k7l', 'Type2', 22, 'Available', now()),
    ('CHG-14u15', 'STN-6k7l', 'Type2', 22, 'Available', now()),
    ('CHG-16v17', 'STN-7m8n', 'Type2', 22, 'Available', now()),
    ('CHG-18w19', 'STN-7m8n', 'CCS', 50, 'Available', now()),
    ('CHG-20x21', 'STN-8o9p', 'Type2', 22, 'Available', now()),
    ('CHG-22y23', 'STN-8o9p', 'Type2', 7, 'Available', now()),

    -- Sousse station chargers
    ('CHG-24z25', 'STN-9q0r', 'Type2', 22, 'Available', now()),
    ('CHG-26a27', 'STN-10s11', 'Type2', 22, 'Available', now()),

    -- Nabeul station charger
    ('CHG-28b29', 'STN-12t13', 'Type2', 22, 'Available', now()),

    -- Bizerte station charger
    ('CHG-30c31', 'STN-14u15', 'Type2', 11, 'Available', now()),

    -- Gabès station charger
    ('CHG-32d33', 'STN-16v17', 'Type2', 22, 'Available', now()),

    -- Kairouan station chargers
    ('CHG-34e35', 'STN-18w19', 'Type2', 22, 'Available', now()),
    ('CHG-36f37', 'STN-18w19', 'Type2', 7, 'Available', now()),

    -- Monastir station chargers
    ('CHG-38g39', 'STN-20x21', 'Type2', 22, 'Available', now()),
    ('CHG-40h41', 'STN-20x21', 'Type2', 50, 'Available', now()),

    -- Additional chargers for partners with more stations
    ('CHG-42i43', 'STN-1a2b', 'Type2', 22, 'Available', now()),
    ('CHG-44j45', 'STN-1a2b', 'CCS', 50, 'Available', now()),
    ('CHG-46k47', 'STN-1a2b', 'Type2', 22, 'Available', now()),
    ('CHG-48l49', 'STN-2c3d', 'CCS', 120, 'Available', now());

-- Verify insertion
SELECT 'Chargers inserted:' AS message, COUNT(*) AS count FROM inventory.charger;
SELECT 'Stations with chargers:' AS message, station_id, connector_type, power_kw, status
FROM inventory.charger
ORDER BY station_id, power_kw DESC;
