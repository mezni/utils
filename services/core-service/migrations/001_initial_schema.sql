-- Create companies table
CREATE TABLE IF NOT EXISTS companies (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    email VARCHAR(255),
    phone VARCHAR(50),
    website VARCHAR(255),
    address TEXT,
    logo_url TEXT,
    is_active BOOLEAN NOT NULL DEFAULT true,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE
);

-- Create stations table
CREATE TABLE IF NOT EXISTS stations (
    id VARCHAR(36) PRIMARY KEY,
    company_id VARCHAR(36) NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    address TEXT NOT NULL,
    latitude DECIMAL(10, 8) NOT NULL,
    longitude DECIMAL(11, 8) NOT NULL,
    phone VARCHAR(50),
    email VARCHAR(255),
    website VARCHAR(255),
    access_type VARCHAR(50) NOT NULL DEFAULT 'PUBLIC',
    operating_hours JSONB,
    amenities JSONB,
    is_active BOOLEAN NOT NULL DEFAULT true,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
);

-- Create chargers table
CREATE TABLE IF NOT EXISTS chargers (
    id VARCHAR(36) PRIMARY KEY,
    station_id VARCHAR(36) NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    charger_type VARCHAR(50) NOT NULL,
    power_output DECIMAL(10, 2) NOT NULL,
    voltage DECIMAL(10, 2) NOT NULL,
    current_type VARCHAR(50) NOT NULL,
    connector_types JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'AVAILABLE',
    last_status_update TIMESTAMP WITH TIME ZONE,
    is_public BOOLEAN NOT NULL DEFAULT true,
    pricing_info JSONB,
    is_active BOOLEAN NOT NULL DEFAULT true,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (station_id) REFERENCES stations(id) ON DELETE CASCADE
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(name) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_companies_email ON companies(email) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_companies_active ON companies(is_active) WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_stations_company_id ON stations(company_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_stations_name ON stations(name) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_stations_location ON stations(latitude, longitude) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_stations_access_type ON stations(access_type) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_stations_active ON stations(is_active) WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_chargers_station_id ON chargers(station_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_chargers_name ON chargers(name) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_chargers_type ON chargers(charger_type) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_chargers_status ON chargers(status) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_chargers_active ON chargers(is_active) WHERE deleted_at IS NULL;

-- Create unique constraints
CREATE UNIQUE INDEX IF NOT EXISTS idx_companies_email_unique ON companies(email) WHERE email IS NOT NULL AND deleted_at IS NULL;