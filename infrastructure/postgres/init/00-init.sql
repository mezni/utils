CREATE SCHEMA IF NOT EXISTS ev;

-- Admins table (assumed to exist for FK references)
CREATE TABLE IF NOT EXISTS admins (
    id TEXT PRIMARY KEY
);

INSERT INTO admins (id) VALUES ('admin-user-id') ON CONFLICT DO NOTHING;

-- Partners table
CREATE TABLE IF NOT EXISTS ev.partners (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    is_valid BOOLEAN NOT NULL DEFAULT TRUE,
    created_by TEXT NOT NULL REFERENCES admins(id),
    updated_by TEXT NOT NULL REFERENCES admins(id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_partners_name ON ev.partners(name);
CREATE INDEX IF NOT EXISTS idx_partners_deleted_at ON ev.partners(deleted_at);

ALTER TABLE ev.partners DROP CONSTRAINT IF EXISTS chk_partners_id;
ALTER TABLE ev.partners
ADD CONSTRAINT chk_partners_id CHECK (id ~ '^PRT-[A-Za-z0-9]{12}$');

-- Stations table (CASCADE for hard delete)
CREATE TABLE IF NOT EXISTS ev.stations (
    id TEXT PRIMARY KEY,
    partner_id TEXT NOT NULL REFERENCES ev.partners(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    location TEXT,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    created_by TEXT NOT NULL REFERENCES admins(id),
    updated_by TEXT NOT NULL REFERENCES admins(id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP NULL
);

CREATE INDEX IF NOT EXISTS idx_stations_partner_id ON ev.stations(partner_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_stations_name ON ev.stations(name);
CREATE INDEX IF NOT EXISTS idx_stations_deleted_at ON ev.stations(deleted_at);

ALTER TABLE ev.stations DROP CONSTRAINT IF EXISTS chk_stations_id;
ALTER TABLE ev.stations
ADD CONSTRAINT chk_stations_id CHECK (id ~ '^STA-[A-Za-z0-9]{12}$');

-- Chargers table (CASCADE for hard delete)
CREATE TABLE IF NOT EXISTS ev.chargers (
    id TEXT PRIMARY KEY,
    station_id TEXT NOT NULL REFERENCES ev.stations(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    power_rating INTEGER NOT NULL,
    created_by TEXT NOT NULL REFERENCES admins(id),
    updated_by TEXT NOT NULL REFERENCES admins(id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP NULL
);

CREATE INDEX IF NOT EXISTS idx_chargers_station_id ON ev.chargers(station_id);
CREATE INDEX IF NOT EXISTS idx_chargers_status ON ev.chargers(status);
CREATE INDEX IF NOT EXISTS idx_chargers_deleted_at ON ev.chargers(deleted_at);

ALTER TABLE ev.chargers DROP CONSTRAINT IF EXISTS chk_chargers_id;
ALTER TABLE ev.chargers
ADD CONSTRAINT chk_chargers_id CHECK (id ~ '^CHR-[A-Za-z0-9]{12}$');

ALTER TABLE ev.chargers DROP CONSTRAINT IF EXISTS chk_power_rating;
ALTER TABLE ev.chargers
ADD CONSTRAINT chk_power_rating CHECK (power_rating > 0 AND power_rating <= 1000);

-- Views for active records
CREATE OR REPLACE VIEW ev.active_partners AS
SELECT * FROM ev.partners WHERE deleted_at IS NULL;

CREATE OR REPLACE VIEW ev.active_stations AS
SELECT * FROM ev.stations WHERE deleted_at IS NULL;

CREATE OR REPLACE VIEW ev.active_chargers AS
SELECT * FROM ev.chargers WHERE deleted_at IS NULL;
