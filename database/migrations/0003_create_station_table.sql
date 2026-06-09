-- Migration 0003: Create station table
-- Stores charging station locations with spatial data.

CREATE TABLE IF NOT EXISTS "ev-platform".station (
    id          TEXT             PRIMARY KEY,
    partner_id  TEXT             NOT NULL,
    name        TEXT             NOT NULL,
    address     TEXT,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    location    GEOMETRY(Point, 4326) NOT NULL,
    created_at  TIMESTAMPTZ      NOT NULL,
    created_by  TEXT             NOT NULL,
    updated_at  TIMESTAMPTZ      NOT NULL,
    updated_by  TEXT             NOT NULL,
    CONSTRAINT ck_station_latitude  CHECK (latitude  BETWEEN -90  AND 90),
    CONSTRAINT ck_station_longitude CHECK (longitude BETWEEN -180 AND 180),
    CONSTRAINT fk_station_partner   FOREIGN KEY (partner_id) REFERENCES "ev-platform".partner(id)
);

-- Spatial index for ST_DWithin and other PostGIS queries
CREATE INDEX IF NOT EXISTS idx_station_location
    ON "ev-platform".station
    USING GIST (location);

-- Trigger function: compute location geometry from lat/lng
CREATE OR REPLACE FUNCTION "ev-platform".trg_station_location_fn()
RETURNS TRIGGER AS $$
BEGIN
    NEW.location := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger: set location before insert or update
DROP TRIGGER IF EXISTS trg_station_location ON "ev-platform".station;
CREATE TRIGGER trg_station_location
    BEFORE INSERT OR UPDATE ON "ev-platform".station
    FOR EACH ROW
    EXECUTE FUNCTION "ev-platform".trg_station_location_fn();
