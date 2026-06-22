-- Setup analytics reader role for admin-service
-- This script should be run in the analytics_db database

-- Create analytics reader role with SELECT-only privileges
-- This role is used by admin-service for querying telemetry events
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bornemap_analytics_reader') THEN
        CREATE ROLE bornemap_analytics_reader WITH LOGIN PASSWORD 'changeme';
        RAISE NOTICE 'Created role bornemap_analytics_reader';
    ELSE
        RAISE NOTICE 'Role bornemap_analytics_reader already exists';
    END IF;
END $$;

-- Grant SELECT-only privileges on analytics_events table
GRANT SELECT ON analytics_events TO bornemap_analytics_reader;
GRANT SELECT ON analytics_events_dead_letter TO bornemap_analytics_reader;

-- Revoke all privileges (ensuring read-only access)
-- This is a safety check - GRANT SELECT already ensures no write access
-- but this line makes it explicit that no other privileges exist
-- ReVOKE ALL PRIVILEGES ON analytics_events FROM bornemap_analytics_reader;
-- ReVOKE ALL PRIVILEGES ON analytics_events_dead_letter FROM bornemap_analytics_reader;

-- Create policy for row-level security (optional, for multi-tenant scenarios)
-- Uncomment if needed:
-- CREATE POLICY is_admin ON analytics_events FOR SELECT USING (true);
-- CREATE POLICY is_admin ON analytics_events_dead_letter FOR SELECT USING (true);

-- Grant usage on schemas
GRANT USAGE ON SCHEMA analytics_db TO bornemap_analytics_reader;

-- Add comment for documentation
COMMENT ON ROLE bornemap_analytics_reader IS 'Read-only role for admin-service to query telemetry events';

-- Verification query
SELECT 'Role setup complete' as status;
