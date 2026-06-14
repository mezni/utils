-- ============================================================================
-- 🛠️ STAGE 01: SYSTEM EXTENSIONS & NAMESPACE ISOLATION
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA IF NOT EXISTS configuration;
CREATE SCHEMA IF NOT EXISTS inventory;
CREATE SCHEMA IF NOT EXISTS gis;

-- ============================================================================
-- 🔐 STAGE 02: THE USERS SCHEMA (Identity Management)
-- ============================================================================

CREATE TABLE users.user (
    id VARCHAR(64) PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    full_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users.user_role (
    user_id VARCHAR(64) NOT NULL REFERENCES users.user(id) ON DELETE CASCADE,
    role VARCHAR(50) NOT NULL,
    PRIMARY KEY (user_id, role)
);

CREATE TABLE users.user_permission (
    user_id VARCHAR(64) NOT NULL REFERENCES users.user(id) ON DELETE CASCADE,
    permission VARCHAR(100) NOT NULL,
    PRIMARY KEY (user_id, permission)
);

CREATE INDEX idx_user_email ON users.user(email);
CREATE INDEX idx_user_role_user_id ON users.user_role(user_id);
CREATE INDEX idx_user_permission_user_id ON users.user_permission(user_id);

-- ============================================================================
-- ⚙️ STAGE 03: THE CONFIGURATION SCHEMA (Global Metadata Lookups)
-- ============================================================================

CREATE TABLE configuration.plug_types (
    -- e.g., 'ccs2', 'type2', 'nacs' (lowercase code keys are ideal for URL/API slugs)
    code_key VARCHAR(32) PRIMARY KEY,
    display_name VARCHAR(100) NOT NULL,
    description TEXT,
    
    -- Standardized Audit Matrix Layer
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Seed the baseline global charging standard configurations immediately
INSERT INTO configuration.plug_types (code_key, display_name, description) VALUES
('ccs2', 'Combined Charging System 2', 'DC High-Power fast charging standard dominant across Europe and Tunisia.'),
('type2', 'Mennekes Type 2', 'AC standard for slower overnight or destination charging infrastructure.'),
('chademo', 'CHAdeMO', 'Legacy Japanese DC fast-charging standard format.');

-- ============================================================================
-- 🏢 STAGE 04: THE INVENTORY SCHEMA (Relational Core Infrastructure)
-- ============================================================================

-- 1. PARTNERS TABLE
CREATE TABLE inventory.partners (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(20) NOT NULL CONSTRAINT chk_partner_type CHECK (type IN ('BUSINESS', 'PRIVATE')),
    email VARCHAR(255) NOT NULL,
    phone VARCHAR(50) NOT NULL,
    verified BOOLEAN NOT NULL DEFAULT FALSE,
    
    created_by VARCHAR(64) NOT NULL DEFAULT 'usr-mvp1-fallback',
    updated_by VARCHAR(64) NOT NULL DEFAULT 'usr-mvp1-fallback',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 2. STATIONS TABLE
CREATE TABLE inventory.stations (
    id VARCHAR(64) PRIMARY KEY,
    partner_id VARCHAR(64) NOT NULL REFERENCES inventory.partners(id) ON DELETE RESTRICT,
    name VARCHAR(255) NOT NULL,
    address TEXT NOT NULL,
    email VARCHAR(255) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    
    availability VARCHAR(32) NOT NULL DEFAULT 'AVAILABLE' 
        CONSTRAINT chk_station_availability CHECK (availability IN ('AVAILABLE', 'OCCUPIED', 'OUT_OF_SERVICE')),
    verified BOOLEAN NOT NULL DEFAULT FALSE,
    is_live BOOLEAN NOT NULL DEFAULT FALSE,
    
    created_by VARCHAR(64) NOT NULL DEFAULT 'usr-mvp1-fallback',
    updated_by VARCHAR(64) NOT NULL DEFAULT 'usr-mvp1-fallback',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 3. CHARGERS TABLE (Migrated from CHECK constraint to FOREIGN KEY validation lookup)
CREATE TABLE inventory.chargers (
    id VARCHAR(64) PRIMARY KEY,
    station_id VARCHAR(64) NOT NULL REFERENCES inventory.stations(id) ON DELETE CASCADE,
    identifier_code VARCHAR(50) NOT NULL,
    
    -- 🔥 NORMALIZED REFERENCE: Validates incoming hardware codes directly against configuration metadata
    plug_type_code VARCHAR(32) NOT NULL REFERENCES configuration.plug_types(code_key) ON DELETE RESTRICT,
    
    max_power_kw INT NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'ONLINE' 
        CONSTRAINT chk_charger_status CHECK (status IN ('ONLINE', 'CHARGING', 'FAULTED', 'OFFLINE')),
    
    created_by VARCHAR(64) NOT NULL DEFAULT 'usr-mvp1-fallback',
    updated_by VARCHAR(64) NOT NULL DEFAULT 'usr-mvp1-fallback',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_station_charger_code UNIQUE (station_id, identifier_code)
);

-- ============================================================================
-- 🚦 STAGE 05: AUTOMATED TIMESTAMP MANAGEMENT PROCEDURES
-- ============================================================================

CREATE OR REPLACE FUNCTION inventory.update_modified_timestamp_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_auto_timestamp_plug_types BEFORE UPDATE ON configuration.plug_types FOR EACH ROW EXECUTE FUNCTION inventory.update_modified_timestamp_column();
CREATE TRIGGER trg_auto_timestamp_partners BEFORE UPDATE ON inventory.partners FOR EACH ROW EXECUTE FUNCTION inventory.update_modified_timestamp_column();
CREATE TRIGGER trg_auto_timestamp_stations BEFORE UPDATE ON inventory.stations FOR EACH ROW EXECUTE FUNCTION inventory.update_modified_timestamp_column();
CREATE TRIGGER trg_auto_timestamp_chargers BEFORE UPDATE ON inventory.chargers FOR EACH ROW EXECUTE FUNCTION inventory.update_modified_timestamp_column();

-- ============================================================================
-- 🗺️ STAGE 06: THE GIS SCHEMA (High-Speed Viewport Cache Layer)
-- ============================================================================

CREATE TABLE gis.osm_stations (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    address TEXT,
    coordinates GEOMETRY(Point, 4326) NOT NULL,
    source VARCHAR(32) NOT NULL,
    is_available BOOLEAN NOT NULL DEFAULT TRUE,
    last_modified_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_osm_stations_spatial ON gis.osm_stations USING GIST (coordinates);

-- ============================================================================
-- ⚡ STAGE 07: REPLICATION ROUTER WITH INTEGRATED AGGREGATION LOOKUP
-- ============================================================================

CREATE OR REPLACE FUNCTION gis.sync_inventory_station_to_gis_cache()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.is_live = FALSE THEN
        DELETE FROM gis.osm_stations WHERE id = NEW.id;
        RETURN NEW;
    END IF;

    INSERT INTO gis.osm_stations (
        id, name, address, coordinates, source, is_available, last_modified_at
    )
    VALUES (
        NEW.id, NEW.name, NEW.address,
        ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326),
        'PLATFORM_SYNC', (NEW.availability = 'AVAILABLE'), NEW.updated_at
    )
    ON CONFLICT (id) DO UPDATE 
    SET name = EXCLUDED.name,
        address = EXCLUDED.address,
        coordinates = EXCLUDED.coordinates,
        is_available = EXCLUDED.is_available,
        last_modified_at = EXCLUDED.last_modified_at;
        
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE TRIGGER trg_replicate_station_to_gis_cache
AFTER INSERT OR UPDATE ON inventory.stations
FOR EACH ROW
EXECUTE FUNCTION gis.sync_inventory_station_to_gis_cache();
