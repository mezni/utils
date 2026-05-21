-- 01_init.sql
-- PostgreSQL initialization script for EV Charging Discovery Platform
-- Executed automatically on first container startup via /docker-entrypoint-initdb.d/

-- Enable PostGIS extension for spatial queries
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create application database
SELECT 'CREATE DATABASE everest_platform'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'everest_platform')\gexec

-- Connect to the application database
\c everest_platform;

-- Enable PostGIS in the application database too
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create read-only user for spatial discovery queries
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ro_user') THEN
        CREATE ROLE ro_user WITH LOGIN PASSWORD 'CHANGE_ME_IN_ENV';
    END IF;
END
$$;

-- Create read-write user for admin and operational writes
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rw_user') THEN
        CREATE ROLE rw_user WITH LOGIN PASSWORD 'CHANGE_ME_IN_ENV';
    END IF;
END
$$;

-- Grant minimal necessary permissions
GRANT CONNECT ON DATABASE everest_platform TO ro_user, rw_user;
GRANT USAGE ON SCHEMA public TO ro_user, rw_user;

-- Read-only: SELECT on all current and future tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ro_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ro_user;

-- Read-write: SELECT, INSERT, UPDATE, DELETE on all current and future tables
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO rw_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO rw_user;

-- Allow rw_user to use sequences
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO rw_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO rw_user;

-- Create core operational tables

-- Partners (station owners / tenants)
CREATE TABLE IF NOT EXISTS partners (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Charging stations
CREATE TABLE IF NOT EXISTS stations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    partner_id UUID NOT NULL REFERENCES partners(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    address TEXT,
    is_active BOOLEAN NOT NULL DEFAULT true,
    geom GEOGRAPHY(Point, 4326) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create spatial index on stations
CREATE INDEX IF NOT EXISTS idx_stations_geom ON stations USING GIST (geom);

-- Connectors (individual charging ports at a station)
CREATE TABLE IF NOT EXISTS connectors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    station_id UUID NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
    connector_type VARCHAR(50) NOT NULL,
    max_power_kw DECIMAL(5,2),
    status VARCHAR(20) NOT NULL DEFAULT 'available',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Registered drivers
CREATE TABLE IF NOT EXISTS drivers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    display_name VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Saved favorites
CREATE TABLE IF NOT EXISTS favorites (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    driver_id UUID NOT NULL REFERENCES drivers(id) ON DELETE CASCADE,
    station_id UUID NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(driver_id, station_id)
);

-- Station reviews
CREATE TABLE IF NOT EXISTS reviews (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    driver_id UUID NOT NULL REFERENCES drivers(id) ON DELETE CASCADE,
    station_id UUID NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
    rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
    comment TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(driver_id, station_id)
);

-- Invitations (operator onboarding)
CREATE TABLE IF NOT EXISTS invitations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token UUID UNIQUE NOT NULL DEFAULT gen_random_uuid(),
    email VARCHAR(255) NOT NULL,
    partner_id UUID REFERENCES partners(id) ON DELETE CASCADE,
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- App configurations (dynamic client customization)
CREATE TABLE IF NOT EXISTS app_configurations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key VARCHAR(255) UNIQUE NOT NULL,
    value JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Insert default configuration
INSERT INTO app_configurations (key, value) VALUES
    ('search_radius_default', '5000'),
    ('theme_colors', '{"primary": "#007AFF", "secondary": "#5856D6"}')
ON CONFLICT (key) DO NOTHING;
