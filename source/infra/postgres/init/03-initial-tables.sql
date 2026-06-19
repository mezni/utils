\c platform_db

-- Enable PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- Partners table
CREATE TABLE inventory.partners (
    id TEXT PRIMARY KEY CHECK (id ~ '^OPR-.+'),
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    phone TEXT,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Stations table
CREATE TABLE inventory.stations (
    id TEXT PRIMARY KEY CHECK (id ~ '^STA-.+'),
    partner_id TEXT NOT NULL REFERENCES inventory.partners(id),
    name TEXT NOT NULL,
    location GEOGRAPHY(POINT, 4326) NOT NULL,
    address TEXT,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Chargers table
CREATE TABLE inventory.chargers (
    id TEXT PRIMARY KEY CHECK (id ~ '^CHG-.+'),
    station_id TEXT NOT NULL REFERENCES inventory.stations(id),
    connector_type TEXT NOT NULL,
    power_kw NUMERIC(5,1) NOT NULL,
    status TEXT NOT NULL DEFAULT 'offline',
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- User Profiles table (for Auth Service)
-- Note: This will be updated with triggers and permissions in 02-schemas-and-roles.sql
CREATE TABLE users.user_profiles (
    id TEXT PRIMARY KEY CHECK (id ~ '^USR-.+'),
    keycloak_sub TEXT UNIQUE NOT NULL,
    email VARCHAR(255) NOT NULL,
    display_name VARCHAR(255),
    roles TEXT[] NOT NULL DEFAULT '{}',
    last_login_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create trigger function for updated_at
CREATE OR REPLACE FUNCTION users.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for updated_at
CREATE TRIGGER set_updated_at
    BEFORE UPDATE ON users.user_profiles
    FOR EACH ROW EXECUTE FUNCTION users.set_updated_at();

-- Partner Roles
-- Note: Roles will be created in Keycloak and referenced here

-- Audit log table (analytics_db)
\c analytics_db

CREATE TABLE audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    before_snapshot JSONB,
    after_snapshot JSONB,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_audit_actor ON audit_log (actor_id);
CREATE INDEX idx_audit_target ON audit_log (target_type, target_id);
CREATE INDEX idx_audit_created ON audit_log (created_at DESC);
