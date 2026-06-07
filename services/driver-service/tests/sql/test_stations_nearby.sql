-- Test fixtures for driver service integration tests
-- This file should be applied to a test database to provide sample data

-- Clear existing test data
TRUNCATE inventory.partner, inventory.station, inventory.charger, gis.station_locations CASCADE;

-- Insert partners
INSERT INTO inventory.partner (id, name, email, phone, address, created_at, updated_at) VALUES
('PAR-001', 'Tunis Power', 'contact@tunispower.tn', '+216 71 123 456', 'Tunis, Tunisia', NOW(), NOW()),
('PAR-002', 'Carsharing Tunis', 'support@carsharing.tn', '+216 71 789 012', 'Tunis, Tunisia', NOW(), NOW()),
('PAR-003', 'Green Charge', 'info@greencharge.tn', '+216 71 345 678', 'Hammamet, Tunisia', NOW(), NOW());

-- Insert stations
INSERT INTO inventory.station (id, partner_id, name, latitude, longitude, address, created_at, updated_at) VALUES
('STN-1a2b', 'PAR-001', 'Tunis-Belvedere Station', 36.864702, 10.158423, 'Belvedere Square, Tunis', NOW(), NOW()),
('STN-1c3d', 'PAR-001', 'Avenue Habib Bourguiba Station', 36.8188, 10.1657, 'Avenue Habib Bourguiba, Tunis', NOW(), NOW()),
('STN-2e4f', 'PAR-002', 'Carsharing Hub Station', 36.846200, 10.180000, 'Hammamet Center, Hammamet', NOW(), NOW()),
('STN-3g5h', 'PAR-003', 'Green Charge Station', 36.868000, 10.170000, 'Tunis-Carthage, Tunis', NOW(), NOW()),
('STN-4i6j', 'PAR-001', 'Medina Station', 36.855833, 10.181667, 'Tunis Medina, Tunis', NOW(), NOW()),
('STN-5k7l', 'PAR-002', 'Carsharing Carthage Station', 36.871667, 10.175833, 'Carthage, Tunis', NOW(), NOW()),
('STN-6m8n', 'PAR-003', 'Green Charge Sousse Station', 35.824722, 10.619722, 'Sousse, Tunisia', NOW(), NOW()),
('STN-7o9p', 'PAR-001', 'La Marsa Station', 36.855000, 10.293000, 'La Marsa, Tunis', NOW(), NOW()),
('STN-8q0r', 'PAR-002', 'Carsharing Bizerte Station', 37.273333, 9.873333, 'Bizerte, Tunisia', NOW(), NOW()),
('STN-9s1t', 'PAR-003', 'Green Charge Monastir Station', 35.778333, 10.825833, 'Monastir, Tunisia', NOW(), NOW()),
('STN-10u2v', 'PAR-001', 'Sidi Thabet Station', 36.895833, 10.236667, 'Sidi Thabet, Tunisia', NOW(), NOW()),
('STN-11w3x', 'PAR-002', 'Carsharing Gabes Station', 33.886944, 10.093889, 'Gabes, Tunisia', NOW(), NOW()),
('STN-12y4z', 'PAR-003', 'Green Charge Kairouan Station', 35.678333, 10.101667, 'Kairouan, Tunisia', NOW(), NOW()),
('STN-13a5b', 'PAR-001', 'Sfax Station', 34.740833, 10.760278, 'Sfax, Tunisia', NOW(), NOW()),
('STN-14c6d', 'PAR-002', 'Carsharing Gabès Station', 33.886944, 10.093889, 'Gabès, Tunisia', NOW(), NOW());

-- Insert chargers (one charger per station for this test)
INSERT INTO inventory.charger (id, station_id, connector_type, power_kw, status, created_at, updated_at) VALUES
('CHR-1a2b', 'STN-1a2b', 'Type 2', 22.0, 'available', NOW(), NOW()),
('CHR-1c3d', 'STN-1c3d', 'Type 2', 22.0, 'available', NOW(), NOW()),
('CHR-2e4f', 'STN-2e4f', 'Type 2', 22.0, 'available', NOW(), NOW()),
('CHR-3g5h', 'STN-3g5h', 'Type 2', 22.0, 'available', NOW(), NOW()),
('CHR-4i6j', 'STN-4i6j', 'Type 2', 22.0, 'available', NOW(), NOW()),
('CHR-5k7l', 'STN-5k7l', 'Type 2', 22.0, 'available', NOW(), NOW()),
('CHR-6m8n', 'STN-6m8n', 'Type 2', 22.0, 'available', NOW(), NOW()),
('CHR-7o9p', 'STN-7o9p', 'Type 2', 22.0, 'available', NOW(), NOW()),
('CHR-8q0r', 'STN-8q0r', 'Type 2', 22.0, 'available', NOW(), NOW()),
('CHR-9s1t', 'STN-9s1t', 'Type 2', 22.0, 'available', NOW(), NOW()),
('CHR-10u2v', 'STN-10u2v', 'Type 2', 22.0, 'available', NOW(), NOW()),
('CHR-11w3x', 'STN-11w3x', 'Type 2', 22.0, 'available', NOW(), NOW()),
('CHR-12y4z', 'STN-12y4z', 'Type 2', 22.0, 'available', NOW(), NOW()),
('CHR-13a5b', 'STN-13a5b', 'Type 2', 22.0, 'available', NOW(), NOW()),
('CHR-14c6d', 'STN-14c6d', 'Type 2', 22.0, 'available', NOW(), NOW());

-- Create spatial locations for each station
INSERT INTO gis.station_locations (station_id, geom)
SELECT
    s.id,
    ST_SetSRID(ST_MakePoint(s.longitude, s.latitude), 4326) AS geom
FROM inventory.station s
WHERE s.id NOT IN (SELECT station_id FROM gis.station_locations);

-- Create GiST index for spatial queries
CREATE INDEX IF NOT EXISTS gis_station_locations_geom_gist ON gis.station_locations USING GiST (geom);

-- Verify data
SELECT 'Partners' AS table_name, COUNT(*) AS count FROM inventory.partner
UNION ALL
SELECT 'Stations', COUNT(*) FROM inventory.station
UNION ALL
SELECT 'Chargers', COUNT(*) FROM inventory.charger
UNION ALL
SELECT 'Spatial Locations', COUNT(*) FROM gis.station_locations;
