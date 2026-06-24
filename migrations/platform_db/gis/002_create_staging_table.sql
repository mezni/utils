CREATE TABLE IF NOT EXISTS gis.osm_charging_stations_temp (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    osm_id TEXT NOT NULL,
    name TEXT,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    tags JSONB DEFAULT '{}',
    imported_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_osm_cst_osm_id
    ON gis.osm_charging_stations_temp (osm_id);
