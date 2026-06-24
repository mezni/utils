CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS gis.osm_charging_stations (
    station_id TEXT PRIMARY KEY,
    osm_id TEXT UNIQUE NOT NULL,
    name TEXT,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    operator TEXT,
    verified BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_osm_charging_stations_lat_lon
    ON gis.osm_charging_stations (lat, lon);

CREATE INDEX IF NOT EXISTS idx_osm_charging_stations_osm_id
    ON gis.osm_charging_stations (osm_id);
