#!/bin/bash

echo "=== Simple Database Test ==="
echo ""

echo "1. Inserting test company..."
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "
INSERT INTO companies (id, name, description, email, is_active, version, created_at, updated_at) 
VALUES ('CMP-TEST-001', 'Test Company', 'A test company', 'test@example.com', true, 1, NOW(), NOW());
"
echo ""

echo "2. Verifying company was inserted..."
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "
SELECT id, name, email, version FROM companies WHERE id = 'CMP-TEST-001';
"
echo ""

echo "3. Inserting test station..."
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "
INSERT INTO stations (id, company_id, name, address, latitude, longitude, access_type, is_active, version, created_at, updated_at) 
VALUES ('STA-TEST-001', 'CMP-TEST-001', 'Test Station', '123 Test St', 40.7128, -74.0060, 'PUBLIC', true, 1, NOW(), NOW());
"
echo ""

echo "4. Verifying station was inserted..."
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "
SELECT id, name, company_id FROM stations WHERE id = 'STA-TEST-001';
"
echo ""

echo "5. Inserting test charger..."
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "
INSERT INTO chargers (id, station_id, name, charger_type, power_output, voltage, current_type, connector_types, status, is_active, version, created_at, updated_at) 
VALUES ('CHR-TEST-001', 'STA-TEST-001', 'Test Charger', 'FAST_DC', 50.0, 400.0, 'DC', '[\"CCS2\"]'::jsonb, 'AVAILABLE', true, 1, NOW(), NOW());
"
echo ""

echo "6. Verifying charger was inserted..."
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "
SELECT id, name, station_id FROM chargers WHERE id = 'CHR-TEST-001';
"
echo ""

echo "7. Testing relationships..."
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "
SELECT c.name as company, s.name as station, ch.name as charger
FROM companies c
JOIN stations s ON c.id = s.company_id
JOIN chargers ch ON s.id = ch.station_id
WHERE c.id = 'CMP-TEST-001';
"
echo ""

echo "8. Testing soft delete..."
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "
UPDATE companies SET deleted_at = NOW(), version = 2 WHERE id = 'CMP-TEST-001';
"
echo ""

echo "9. Checking soft delete effect..."
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "
SELECT name, deleted_at FROM companies WHERE id = 'CMP-TEST-001';
"
echo ""

echo "10. Testing optimistic concurrency..."
echo "Current version:"
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "
SELECT version FROM companies WHERE id = 'CMP-TEST-001';
"
echo ""

echo "Trying to update with wrong version..."
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "
UPDATE companies 
SET name = 'Updated Company', version = 3 
WHERE id = 'CMP-TEST-001' AND version = 999;
"
echo ""

echo "11. Cleanup..."
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "
DELETE FROM chargers WHERE id = 'CHR-TEST-001';
DELETE FROM stations WHERE id = 'STA-TEST-001';
DELETE FROM companies WHERE id = 'CMP-TEST-001';
"
echo "Test completed!"