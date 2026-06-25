-- Test 015: Verify geography column type (PostGIS)
SELECT 'T-015: location is GEOGRAPHY type' AS test_name,
       CASE WHEN data_type = 'USER-DEFINED' AND udt_name = 'geography'
            THEN 'PASS' ELSE format('FAIL (type=%s, udt=%s)', data_type, udt_name) END AS status
FROM information_schema.columns
WHERE table_schema = 'ev' AND table_name = 'stations' AND column_name = 'location';

-- Test 016: Verify GIST spatial index on location
SELECT 'T-016: GIST spatial index exists on ev.stations.location' AS test_name,
       CASE WHEN EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'ev' AND tablename = 'stations'
            AND LOWER(indexdef) LIKE '%gist%location%'
       ) THEN 'PASS' ELSE 'FAIL' END AS status;

-- Test 017: Verify soft-delete columns exist
SELECT 'T-017: ev.partners has deleted_at' AS test_name,
       CASE WHEN EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'ev' AND table_name = 'partners' AND column_name = 'deleted_at'
       ) THEN 'PASS' ELSE 'FAIL' END AS status;

SELECT 'T-018: ev.stations has deleted_at' AS test_name,
       CASE WHEN EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'ev' AND table_name = 'stations' AND column_name = 'deleted_at'
       ) THEN 'PASS' ELSE 'FAIL' END AS status;

SELECT 'T-019: ev.chargers has deleted_at' AS test_name,
       CASE WHEN EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'ev' AND table_name = 'chargers' AND column_name = 'deleted_at'
       ) THEN 'PASS' ELSE 'FAIL' END AS status;

-- Test 020: Verify unique constraint on connectors
SELECT 'T-020: unique_connector constraint exists' AS test_name,
       CASE WHEN EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_namespace n ON n.oid = c.connamespace
            WHERE n.nspname = 'ev' AND c.conname = 'unique_connector'
       ) THEN 'PASS' ELSE 'FAIL' END AS status;

-- Test 021: Verify FK: chargers → stations
SELECT 'T-021: FK chargers.station_id → ev.stations' AS test_name,
       CASE WHEN EXISTS (
            SELECT 1 FROM information_schema.table_constraints tc
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = 'ev' AND tc.table_name = 'chargers'
            AND tc.constraint_name LIKE '%station_id%fkey%' ESCAPE ''
       ) OR EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_namespace n ON n.oid = c.connamespace
            WHERE n.nspname = 'ev' AND c.conrelid = 'ev.chargers'::regclass
            AND c.confrelid = 'ev.stations'::regclass AND c.contype = 'f'
       ) THEN 'PASS' ELSE 'FAIL' END AS status;

-- Test 022: Verify FK: stations → partners
SELECT 'T-022: FK ev.stations.partner_id → ev.partners' AS test_name,
       CASE WHEN EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_namespace n ON n.oid = c.connamespace
            WHERE n.nspname = 'ev' AND c.conrelid = 'ev.stations'::regclass
            AND c.confrelid = 'ev.partners'::regclass AND c.contype = 'f'
       ) THEN 'PASS' ELSE 'FAIL' END AS status;

-- Test 023: Verify charger count constraints
SELECT 'T-023: count_available CHECK >= 0' AS test_name,
       CASE WHEN EXISTS (
            SELECT 1 FROM information_schema.check_constraints cc
            JOIN pg_constraint c ON c.conname = cc.constraint_name
            WHERE cc.constraint_schema = 'ev'
            AND c.conrelid = 'ev.chargers'::regclass
            AND cc.check_clause LIKE '%count_available%0%'
       ) THEN 'PASS' ELSE 'FAIL (check clause not found)' END AS status;

-- Test 024: Verify lookup tables have seed data
SELECT 'T-024: ev.access_types has seed data' AS test_name,
       CASE WHEN (SELECT COUNT(*) FROM ev.access_types) >= 3 THEN 'PASS' ELSE 'FAIL' END AS status;

SELECT 'T-025: ev.data_sources has seed data' AS test_name,
       CASE WHEN (SELECT COUNT(*) FROM ev.data_sources) >= 3 THEN 'PASS' ELSE 'FAIL' END AS status;

SELECT 'T-026: ev.connector_types has seed data' AS test_name,
       CASE WHEN (SELECT COUNT(*) FROM ev.connector_types) >= 6 THEN 'PASS' ELSE 'FAIL' END AS status;
