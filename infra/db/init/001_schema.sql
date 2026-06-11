-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;
SELECT PostGIS_Version();

-- Inventory schema
CREATE SCHEMA IF NOT EXISTS inventory;

-- Partner table
CREATE TABLE inventory.partner (
  id TEXT PRIMARY KEY,

  name TEXT NOT NULL,
  type TEXT NOT NULL CHECK (type IN ('business', 'personal')),

  is_verified BOOLEAN DEFAULT FALSE,
  is_active   BOOLEAN DEFAULT TRUE,
  is_live     BOOLEAN DEFAULT FALSE,

  created_at TIMESTAMPTZ DEFAULT NOW(),
  created_by TEXT,
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  updated_by TEXT,

  CONSTRAINT partner_live_requires_verified
    CHECK (is_live = FALSE OR is_verified = TRUE)
);

-- Station table
CREATE TABLE inventory.station (
  id TEXT PRIMARY KEY,

  partner_id TEXT NOT NULL
    REFERENCES inventory.partner(id)
    ON DELETE CASCADE,

  name TEXT NOT NULL,
  address TEXT,

  latitude  NUMERIC(10,7) NOT NULL CHECK (latitude BETWEEN -90 AND 90),
  longitude NUMERIC(10,7) NOT NULL CHECK (longitude BETWEEN -180 AND 180),

  created_at TIMESTAMPTZ DEFAULT NOW(),
  created_by TEXT,
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  updated_by TEXT
);

-- Charger table
CREATE TABLE inventory.charger (
  id TEXT PRIMARY KEY,

  station_id TEXT NOT NULL
    REFERENCES inventory.station(id)
    ON DELETE CASCADE,

  connector_type TEXT NOT NULL,
  power_kw NUMERIC(6,2) NOT NULL CHECK (power_kw > 0),

  status TEXT NOT NULL DEFAULT 'available',

  created_at TIMESTAMPTZ DEFAULT NOW(),
  created_by TEXT,
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  updated_by TEXT
);

-- Reference tables
CREATE TABLE inventory.connector_type (
  code TEXT PRIMARY KEY,
  label TEXT NOT NULL,
  description TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE inventory.charger_status (
  code TEXT PRIMARY KEY,
  label TEXT NOT NULL,
  description TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_station_partner ON inventory.station(partner_id);
CREATE INDEX idx_station_location ON inventory.station(latitude, longitude);
CREATE INDEX idx_charger_station ON inventory.charger(station_id);

-- Spatial index on station coordinates
CREATE INDEX idx_station_geog ON inventory.station
  USING GIST (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326));
