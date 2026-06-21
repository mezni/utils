CREATE SCHEMA IF NOT EXISTS gis;

CREATE TABLE gis.osm_charging_stations_temp (
    id SERIAL PRIMARY KEY,
    osm_id BIGINT NOT NULL UNIQUE,
    lat DOUBLE PRECISION NOT NULL,
    lng DOUBLE PRECISION NOT NULL,
    raw_tags HSTORE,
    fetched_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_osm_staging_fetched ON gis.osm_charging_stations_temp (fetched_at);
