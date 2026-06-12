-- 002-inventory-schema.sql
-- Target: platform_db
-- Purpose: partner, station, charger tables + indexes
-- Idempotent: YES (IF NOT EXISTS guards)

CREATE TABLE IF NOT EXISTS inventory.partner (
    id            VARCHAR(50)  PRIMARY KEY,
    name          VARCHAR(255) NOT NULL UNIQUE,
    contact_email VARCHAR(255) NOT NULL,
    created_at    TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_partner_email
    ON inventory.partner (contact_email);

CREATE TABLE IF NOT EXISTS inventory.station (
    id            VARCHAR(50)        PRIMARY KEY,
    name          VARCHAR(255)       NOT NULL,
    address       VARCHAR(255)       NOT NULL,
    lat           DOUBLE PRECISION   NOT NULL,
    lng           DOUBLE PRECISION   NOT NULL,
    location      GEOMETRY(Point, 4326) GENERATED ALWAYS AS (ST_Point(lng, lat)) STORED,
    status        VARCHAR(20)        NOT NULL DEFAULT 'offline',
    opening_hours VARCHAR(255),
    partner_id    VARCHAR(50)        NOT NULL REFERENCES inventory.partner(id),
    created_at    TIMESTAMP          NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP          NOT NULL DEFAULT NOW(),
    deleted_at    TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_station_location_gist
    ON inventory.station USING GIST (location)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_station_partner_id
    ON inventory.station (partner_id)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_station_status
    ON inventory.station (status)
    WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS inventory.charger (
    id            VARCHAR(50)  PRIMARY KEY,
    station_id    VARCHAR(50)  NOT NULL REFERENCES inventory.station(id),
    type          VARCHAR(20)  NOT NULL,
    power_kw      FLOAT        NOT NULL,
    status        VARCHAR(20)  NOT NULL DEFAULT 'offline',
    price_per_kwh FLOAT        NOT NULL DEFAULT 0,
    created_at    TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP    NOT NULL DEFAULT NOW(),
    deleted_at    TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_charger_station_id
    ON inventory.charger (station_id)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_charger_type
    ON inventory.charger (type)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_charger_status
    ON inventory.charger (status)
    WHERE deleted_at IS NULL;
