#!/bin/bash

# Simple test to verify our database schema and basic connectivity

echo "=== Core Service Database Test ==="
echo ""

echo "1. Testing database connection..."
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "SELECT version();" | head -2
echo ""

echo "2. Testing tables exist..."
echo "Companies table:"
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "\d companies" | head -10
echo ""

echo "Stations table:"
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "\d stations" | head -10
echo ""

echo "Chargers table:"
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "\d stations" | head -10
echo ""

echo "3. Testing basic CRUD operations..."
echo "Inserting test company..."
COMPANY_ID=$(docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -t -c "
INSERT INTO companies (id, name, description, email, is_active, version, created_at, updated_at) 
VALUES ('CMP-TEST-001', 'Test Company', 'A test company', 'test@example.com', true, 1, NOW(), NOW())
RETURNING id;
" | tr -d ' ')

echo "Created company with ID: $COMPANY_ID"
echo ""

echo "Inserting test station..."
STATION_ID=$(docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -t -c "
INSERT INTO stations (id, company_id, name, address, latitude, longitude, access_type, is_active, version, created_at, updated_at) 
VALUES ('STA-TEST-001', '$COMPANY_ID', 'Test Station', '123 Test St', 40.7128, -74.0060, 'PUBLIC', true, 1, NOW(), NOW())
RETURNING id;
" | tr -d ' ')

echo "Created station with ID: $STATION_ID"
echo ""

echo "Inserting test charger..."
CHARGER_ID=$(docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -t -c "
INSERT INTO chargers (id, station_id, name, charger_type, power_output, voltage, current_type, connector_types, status, is_active, version, created_at, updated_at) 
VALUES ('CHR-TEST-001', '$STATION_ID', 'Test Charger', 'FAST_DC', 50.0, 400.0, 'DC', '[\"CCS2\"]'::jsonb, 'AVAILABLE', true, 1, NOW(), NOW())
RETURNING id;
" | tr -d ' ')

echo "Created charger with ID: $CHARGER_ID"
echo ""

echo "4. Testing relationships..."
echo "Company with stations and chargers:"
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "
SELECT c.name as company, s.name as station, ch.name as charger
FROM companies c
LEFT JOIN stations s ON c.id = s.company_id
LEFT JOIN chargers ch ON s.id = ch.station_id
WHERE c.id = '$COMPANY_ID';
"
echo ""

echo "5. Testing soft delete..."
echo "Soft deleting company..."
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "
UPDATE companies SET deleted_at = NOW(), version = 2 WHERE id = '$COMPANY_ID';
"
echo ""

echo "Checking if company is soft deleted (should not appear in normal queries)..."
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "
SELECT name, deleted_at FROM companies WHERE id = '$COMPANY_ID';
"
echo ""

echo "Querying active companies (should not include soft-deleted)..."
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "
SELECT COUNT(*) FROM companies WHERE deleted_at IS NULL;
"
echo ""

echo "6. Testing optimistic concurrency..."
echo "Getting current version..."
VERSION=$(docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -t -c "SELECT version FROM companies WHERE id = '$COMPANY_ID';" | tr -d ' ')
echo "Current version: $VERSION"

echo "Trying to update with wrong version (should affect 0 rows)..."
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -t -c "
UPDATE companies 
SET name = 'Updated Company', version = 3 
WHERE id = '$COMPANY_ID' AND version = 999;
"
echo ""

echo "Updating with correct version..."
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -t -c "
UPDATE companies 
SET name = 'Updated Company', version = 3 
WHERE id = '$COMPANY_ID' AND version = $VERSION;
"
echo ""

echo "7. Testing indexes..."
echo "Checking if indexes exist..."
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "
SELECT indexname FROM pg_indexes WHERE tablename = 'companies' AND indexname LIKE 'idx_%';
"
echo ""

echo "8. Cleanup..."
docker exec -i bornemap-postgres-1 psql -U bornemap -d bornemap -c "
DELETE FROM chargers WHERE id = '$CHARGER_ID';
DELETE FROM stations WHERE id = '$STATION_ID';
DELETE FROM companies WHERE id = '$COMPANY_ID';
"
echo "Test data cleaned up."
echo ""

echo "=== Database Test Complete ==="