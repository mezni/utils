CREATE TABLE IF NOT EXISTS gis.osm_charging_stations_temp (
    id SERIAL PRIMARY KEY,
    osm_id TEXT NOT NULL,
    name TEXT,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    tags JSONB DEFAULT '{}',
    imported_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_osm_charging_stations_temp_osm_id
    ON gis.osm_charging_stations_temp (osm_id);
