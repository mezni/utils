-- Test 001: Verify ev schema exists
SELECT 'T-001: ev schema exists' AS test_name,
       CASE WHEN EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'ev')
            THEN 'PASS' ELSE 'FAIL' END AS status;

-- Test 002: Verify extensions installed
SELECT 'T-002: postgis extension installed' AS test_name,
       CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis')
            THEN 'PASS' ELSE 'FAIL' END AS status;

SELECT 'T-003: hstore extension installed' AS test_name,
       CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'hstore')
            THEN 'PASS' ELSE 'FAIL' END AS status;

-- Test 004-008: Verify all 5 lookup tables exist
SELECT 'T-004: ev.access_types exists' AS test_name,
       CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'ev' AND table_name = 'access_types')
            THEN 'PASS' ELSE 'FAIL' END AS status;

SELECT 'T-005: ev.data_sources exists' AS test_name,
       CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'ev' AND table_name = 'data_sources')
            THEN 'PASS' ELSE 'FAIL' END AS status;

SELECT 'T-006: ev.connector_types exists' AS test_name,
       CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'ev' AND table_name = 'connector_types')
            THEN 'PASS' ELSE 'FAIL' END AS status;

SELECT 'T-007: ev.current_types exists' AS test_name,
       CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'ev' AND table_name = 'current_types')
            THEN 'PASS' ELSE 'FAIL' END AS status;

SELECT 'T-008: ev.connector_statuses exists' AS test_name,
       CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'ev' AND table_name = 'connector_statuses')
            THEN 'PASS' ELSE 'FAIL' END AS status;

-- Test 009: Verify entity tables exist
SELECT 'T-009: ev.partners exists' AS test_name,
       CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'ev' AND table_name = 'partners')
            THEN 'PASS' ELSE 'FAIL' END AS status;

SELECT 'T-010: ev.stations exists' AS test_name,
       CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'ev' AND table_name = 'stations')
            THEN 'PASS' ELSE 'FAIL' END AS status;

SELECT 'T-011: ev.chargers exists' AS test_name,
       CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'ev' AND table_name = 'chargers')
            THEN 'PASS' ELSE 'FAIL' END AS status;
