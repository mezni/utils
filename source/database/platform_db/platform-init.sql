CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA IF NOT EXISTS configuration;
CREATE SCHEMA IF NOT EXISTS inventory;
CREATE SCHEMA IF NOT EXISTS gis;

CREATE TABLE configuration.plug_types (
    code_key VARCHAR(32) PRIMARY KEY,
    display_name VARCHAR(100) NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO configuration.plug_types (code_key, display_name, description) VALUES
    ('ccs2', 'Combined Charging System 2', 'DC fast charging standard dominant across Europe and Tunisia.'),
    ('type2', 'Mennekes Type 2', 'AC standard for slower overnight or destination charging.'),
    ('chademo', 'CHAdeMO', 'Legacy Japanese DC fast-charging standard.');

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

CREATE TABLE inventory.chargers (
    id VARCHAR(64) PRIMARY KEY,
    station_id VARCHAR(64) NOT NULL REFERENCES inventory.stations(id) ON DELETE CASCADE,
    identifier_code VARCHAR(50) NOT NULL,
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
