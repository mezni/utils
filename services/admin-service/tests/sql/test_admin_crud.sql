-- Test fixtures for admin service integration tests
-- This file should be applied to a test database to provide sample data

-- Clear existing test data
TRUNCATE inventory.partner, inventory.station, inventory.charger, gis.station_locations CASCADE;

-- Insert partners
INSERT INTO inventory.partner (id, name, email, phone, address, created_at, updated_at) VALUES
('PRT-TEST', 'Test Partner', 'test@test.com', '+216 71 111 111', 'Test Address, Tunisia', NOW(), NOW());

-- Insert stations
INSERT INTO inventory.station (id, partner_id, name, latitude, longitude, address, created_at, updated_at) VALUES
('STN-TEST', 'PRT-TEST', 'Test Station', 36.864702, 10.158423, 'Test Address, Tunis', NOW(), NOW()),
('STN-TEST2', 'PRT-TEST', 'Test Station 2', 36.846200, 10.180000, 'Test Address 2, Tunis', NOW(), NOW());

-- Insert chargers
INSERT INTO inventory.charger (id, station_id, connector_type, power_kw, status, created_at, updated_at) VALUES
('CHR-TEST', 'STN-TEST', 'Type 2', 22.0, 'available', NOW(), NOW()),
('CHR-TEST2', 'STN-TEST', 'CCS', 50.0, 'available', NOW(), NOW()),
('CHR-TEST3', 'STN-TEST', 'CHAdeMO', 60.0, 'unavailable', NOW(), NOW()),
('CHR-TEST4', 'STN-TEST2', 'Type 2', 22.0, 'available', NOW(), NOW());

-- Create spatial locations for each station
INSERT INTO gis.station_locations (station_id, geom)
SELECT
    s.id,
    ST_SetSRID(ST_MakePoint(s.longitude, s.latitude), 4326) AS geom
FROM inventory.station s
WHERE s.id NOT IN (SELECT station_id FROM gis.station_locations);

-- Verify data
SELECT 'Partners' AS table_name, COUNT(*) AS count FROM inventory.partner
UNION ALL
SELECT 'Stations', COUNT(*) FROM inventory.station
UNION ALL
SELECT 'Chargers', COUNT(*) FROM inventory.charger
UNION ALL
SELECT 'Spatial Locations', COUNT(*) FROM gis.station_locations;
