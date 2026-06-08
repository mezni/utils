-- Admin Service Migrations
-- Sprint 1.4 - Admin Service

-- Migration 001: Create admin service migration directory
-- No schema changes needed for this sprint
-- The service reads from existing inventory and gis schemas

-- Verify that the inventory and gis schemas exist
-- This is enforced by the ev-db shared crate and Sprint 1.2 migrations
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'inventory') THEN
        RAISE EXCEPTION 'inventory schema does not exist';
    END IF;

    IF NOT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'gis') THEN
        RAISE EXCEPTION 'gis schema does not exist';
    END IF;

    IF NOT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'users') THEN
        RAISE EXCEPTION 'users schema does not exist';
    END IF;

    IF NOT EXISTS (SELECT FROM pg_namespace WHERE nspname = 'analytics') THEN
        RAISE EXCEPTION 'analytics schema does not exist';
    END IF;
END $$;

-- Verify required tables exist in inventory schema
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'inventory' AND table_name = 'partner') THEN
        RAISE EXCEPTION 'inventory.partner table does not exist';
    END IF;

    IF NOT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'inventory' AND table_name = 'station') THEN
        RAISE EXCEPTION 'inventory.station table does not exist';
    END IF;

    IF NOT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'inventory' AND table_name = 'charger') THEN
        RAISE EXCEPTION 'inventory.charger table does not exist';
    END IF;
END $$;

-- Verify required tables exist in gis schema
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'gis' AND table_name = 'station_locations') THEN
        RAISE EXCEPTION 'gis.station_locations table does not exist';
    END IF;
END $$;

-- Verify GiST index exists on spatial data
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'gis' AND indexname = 'gis_station_locations_geom_gist'
    ) THEN
        RAISE EXCEPTION 'GiST index on gis.station_locations.geom does not exist';
    END IF;
END $$;

-- Migration 001 complete
SELECT 'Migration 001 complete: Admin Service prerequisites verified' AS message;
