CREATE TABLE IF NOT EXISTS ev.access_types (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS ev.data_sources (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS ev.connector_types (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS ev.current_types (
    id SERIAL PRIMARY KEY,
    name VARCHAR(20) UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS ev.connector_statuses (
    id SERIAL PRIMARY KEY,
    name VARCHAR(20) UNIQUE NOT NULL,
    description TEXT
);

INSERT INTO ev.access_types (name, description) VALUES
    ('public', 'Open to the general public'),
    ('restricted', 'Limited access, e.g., private parking'),
    ('private', 'Private use only')
ON CONFLICT (name) DO NOTHING;

INSERT INTO ev.data_sources (name, description) VALUES
    ('osm', 'OpenStreetMap import'),
    ('partner', 'Partner/operator provided data'),
    ('manual', 'Manually entered by operator')
ON CONFLICT (name) DO NOTHING;

INSERT INTO ev.connector_types (name, description) VALUES
    ('CCS', 'Combined Charging System (CCS Combo 2)'),
    ('CHAdeMO', 'CHAdeMO DC fast charging'),
    ('Type 2', 'IEC 62196 Type 2 (Mennekes)'),
    ('Type 1', 'SAE J1772 Type 1'),
    ('GB/T', 'Chinese standard GB/T'),
    ('Tesla', 'Tesla proprietary connector')
ON CONFLICT (name) DO NOTHING;

INSERT INTO ev.current_types (name, description) VALUES
    ('AC', 'Alternating Current'),
    ('DC', 'Direct Current')
ON CONFLICT (name) DO NOTHING;

INSERT INTO ev.connector_statuses (name, description) VALUES
    ('available', 'Connector is available for use'),
    ('in_use', 'Connector is currently in use'),
    ('offline', 'Connector is offline/unavailable'),
    ('faulted', 'Connector has a fault condition')
ON CONFLICT (name) DO NOTHING;
