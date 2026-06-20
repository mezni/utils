-- Migration 004: Stations table
-- Physical charging location with spatial coordinates

CREATE TABLE inventory.stations (
    id             VARCHAR(32)      PRIMARY KEY CHECK (id ~ '^STA-[A-Za-z0-9_]{12}$'),
    partner_id     VARCHAR(32)      REFERENCES inventory.partners(id),
    name           VARCHAR(255)     NOT NULL,
    address        TEXT,
    location       GEOGRAPHY(Point, 4326) NOT NULL,
    deleted_at     TIMESTAMPTZ,
    created_at     TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_stations_location ON inventory.stations USING GIST (location);
