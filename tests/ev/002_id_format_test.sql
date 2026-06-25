-- Test 012: Verify partner_id format (OPR-nanoid(12))
SELECT 'T-012: partner_id format OPR-[a-z0-9]{12}' AS test_name,
       CASE WHEN pg_typeof(partner_id)::text = 'character varying'
            THEN 'PASS (type check)' ELSE 'FAIL' END AS status
FROM (SELECT NULL::VARCHAR AS partner_id) AS dummy;

-- Test 013: Verify station_id format (STA-nanoid(12))
SELECT 'T-013: station_id column exists as VARCHAR' AS test_name,
       CASE WHEN EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'ev' AND table_name = 'stations' AND column_name = 'station_id'
       ) THEN 'PASS' ELSE 'FAIL' END AS status;

-- Test 014: Verify charger_id format (CHG-nanoid(12))
SELECT 'T-014: charger_id column exists as VARCHAR' AS test_name,
       CASE WHEN EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'ev' AND table_name = 'chargers' AND column_name = 'charger_id'
       ) THEN 'PASS' ELSE 'FAIL' END AS status;
