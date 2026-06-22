-- Migrations for admin-service: inventory schema
-- Purpose: Store business entity data (stations, chargers, connectors, partners)
-- NOTE: inventory is a DATA DOMAIN within admin-service, NOT a standalone service

-- Create inventory schema
CREATE SCHEMA IF NOT EXISTS inventory;

-- Stations table (STA-nanoid(12) identifiers)
CREATE TABLE IF NOT EXISTS inventory.stations (
    station_id VARCHAR(15) PRIMARY KEY CHECK (station_id ~ '^STA[a-zA-Z0-9]{11}$'),
    name VARCHAR(255) NOT NULL,
    address TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Partners table (PRT-nanoid(12) identifiers)
CREATE TABLE IF NOT EXISTS inventory.partners (
    partner_id VARCHAR(15) PRIMARY KEY CHECK (partner_id ~ '^PRT[a-zA-Z0-9]{11}$'),
    name VARCHAR(255) NOT NULL,
    partner_type VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Chargers table (CHG-nanoid(12) identifiers)
CREATE TABLE IF NOT EXISTS inventory.chargers (
    charger_id VARCHAR(15) PRIMARY KEY CHECK (charger_id ~ '^CHG[a-zA-Z0-9]{11}$'),
    station_id VARCHAR(15) NOT NULL REFERENCES inventory.stations(station_id),
    connector_type VARCHAR(50),
    status VARCHAR(50) NOT NULL,
    max_power_kw DOUBLE PRECISION,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Connectors table (CON-nanoid(12) identifiers)
CREATE TABLE IF NOT EXISTS inventory.connectors (
    connector_id VARCHAR(15) PRIMARY KEY CHECK (connector_id ~ '^CON[a-zA-Z0-9]{11}$'),
    charger_id VARCHAR(15) NOT NULL REFERENCES inventory.chargers(charger_id),
    connector_number INT NOT NULL,
    connector_type VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_inventory_stations_location ON inventory.stations USING GIST (location);

CREATE INDEX IF NOT EXISTS idx_inventory_stations_status ON inventory.stations(status);
CREATE INDEX IF NOT EXISTS idx_inventory_stations_name ON inventory.stations(name);

CREATE INDEX IF NOT EXISTS idx_inventory_partners_type ON inventory.partners(partner_type);
CREATE INDEX IF NOT EXISTS idx_inventory_partners_name ON inventory.partners(name);

CREATE INDEX IF NOT EXISTS idx_inventory_chargers_station ON inventory.chargers(station_id);
CREATE INDEX IF NOT EXISTS idx_inventory_chargers_status ON inventory.chargers(status);
CREATE INDEX IF NOT EXISTS idx_inventory_chargers_connector_type ON inventory.chargers(connector_type);

CREATE INDEX IF NOT EXISTS idx_inventory_connectors_charger ON inventory.connectors(charger_id);
CREATE INDEX IF NOT EXISTS idx_inventory_connectors_status ON inventory.connectors(status);
CREATE INDEX IF NOT EXISTS idx_inventory_connectors_connector_type ON inventory.connectors(connector_type);

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA inventory TO bornemap_admin;
GRANT ALL PRIVILEGES ON TABLE inventory.stations TO bornemap_admin;
GRANT ALL PRIVILEGES ON TABLE inventory.partners TO bornemap_admin;
GRANT ALL PRIVILEGES ON TABLE inventory.chargers TO bornemap_admin;
GRANT ALL PRIVILEGES ON TABLE inventory.connectors TO bornemap_admin;
GRANT USAGE ON SCHEMA inventory TO bornemap_driver;
GRANT SELECT ON TABLE inventory.stations TO bornemap_driver;
GRANT SELECT ON TABLE inventory.chargers TO bornemap_driver;
