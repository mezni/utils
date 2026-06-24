-- Schema validation test
-- Run against PostgreSQL with: psql -U bornemap -d bornemap -f test_schema_validation.sql

BEGIN;

-- 1. Verify gis schema exists
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name = 'gis';

-- 2. Verify staging table exists and has correct columns
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'gis'
  AND table_name = 'osm_charging_stations_temp'
ORDER BY ordinal_position;

-- 3. Verify curated table exists and has correct columns
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'gis'
  AND table_name = 'osm_charging_stations'
ORDER BY ordinal_position;

-- 4. Verify find_nearby_stations function exists
SELECT routine_name, routine_type, data_type
FROM information_schema.routines
WHERE routine_schema = 'gis'
  AND routine_name = 'find_nearby_stations';

-- 5. Verify function returns correct columns
SELECT ordinal_position, column_name, data_type
FROM information_schema.routine_columns
WHERE routine_schema = 'gis'
  AND routine_name = 'find_nearby_stations'
ORDER BY ordinal_position;

-- 6. Test find_nearby_stations with empty table (should return empty)
SELECT COUNT(*) = 0 AS empty_table_check
FROM gis.find_nearby_stations(36.8, 10.18, 5000, 10);

-- 7. Insert test station
INSERT INTO gis.osm_charging_stations (
    station_id, osm_id, name, lat, lon, operator, verified
) VALUES (
    'STA-test000000001', 'test/1', 'Test Station Tunis', 36.8, 10.18, 'Test Operator', TRUE
);

-- 8. Verify find_nearby_stations returns the test station
SELECT station_id, name, ROUND(distance_km::numeric, 4) AS distance_km
FROM gis.find_nearby_stations(36.8, 10.18, 5000, 10);

-- 9. Verify station at same point has distance ~0
SELECT ROUND(distance_km::numeric, 4) = 0 AS zero_distance_check
FROM gis.find_nearby_stations(36.8, 10.18, 1000, 1);

-- 10. Verify station outside 5km radius not returned (coords ~555km away)
SELECT COUNT(*) = 0 AS far_station_excluded
FROM gis.find_nearby_stations(30.0, 10.0, 5000, 10);

-- 11. Cleanup test data
DELETE FROM gis.osm_charging_stations WHERE station_id = 'STA-test000000001';

COMMIT;
