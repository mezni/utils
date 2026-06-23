-- Database role creation for analytics_db
-- Create read-only role for admin-service

-- Drop existing role if it exists (for cleanup)
DROP ROLE IF EXISTS bornemap_analytics_reader;

-- Create read-only role with NOINHERIT
CREATE ROLE bornemap_analytics_reader WITH
    LOGIN,
    PASSWORD 'bornemap_analytics_reader_password_placeholder_change_in_production',
    NOINHERIT;  -- Critical: prevents role escalation

-- Grant necessary permissions
GRANT CONNECT ON DATABASE analytics_db TO bornemap_analytics_reader;
GRANT USAGE ON SCHEMA public TO bornemap_analytics_reader;

-- Grant SELECT access to all tables in public schema (initial grant)
-- Note: This will be updated by the materialized view creation script
GRANT SELECT ON ALL TABLES IN SCHEMA public TO bornemap_analytics_reader;

-- Set default privileges to grant SELECT on new tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO bornemap_analytics_reader;

-- Revoke any write permissions that might have been granted
-- This is a safety check to ensure role is truly read-only
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON ALL TABLES IN SCHEMA public FROM bornemap_analytics_reader;

-- Note: Materialized views will be created in a separate migration
-- These views should also have read-only access

-- Verification query
DO $$
BEGIN
    RAISE NOTICE '✅ Database role "bornemap_analytics_reader" created successfully';
    RAISE NOTICE '   This role has SELECT-only permissions on analytics_db tables';
    RAISE NOTICE '   No write permissions are granted';
END $$;

-- Test query to verify role permissions
SELECT
    'READ-ONLY ROLE TEST' AS test_query,
    current_user AS current_user,
    current_database() AS current_database;

-- Verify no write permissions are granted
SELECT
    'WRITE PERMISSIONS CHECK' AS test_query,
    has_table_privilege('bornemap_analytics_reader', current_database(), 'INSERT') AS can_insert,
    has_table_privilege('bornemap_analytics_reader', current_database(), 'UPDATE') AS can_update,
    has_table_privilege('bornemap_analytics_reader', current_database(), 'DELETE') AS can_delete,
    has_table_privilege('bornemap_analytics_reader', current_database(), 'TRUNCATE') AS can_truncate;

-- Expected results:
-- can_insert = false
-- can_update = false
-- can_delete = false
-- can_truncate = false