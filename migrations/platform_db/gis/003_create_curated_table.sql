CREATE TABLE IF NOT EXISTS gis.osm_charging_stations (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    station_id TEXT NOT NULL UNIQUE,
    osm_id TEXT UNIQUE,
    name TEXT,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    operator TEXT,
    verified BOOLEAN NOT NULL DEFAULT false,
    is_test BOOLEAN NOT NULL DEFAULT false,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_osm_cs_location
    ON gis.osm_charging_stations (lat, lon);

CREATE INDEX IF NOT EXISTS idx_osm_cs_active
    ON gis.osm_charging_stations (deleted_at)
    WHERE deleted_at IS NULL AND is_test = FALSE;
