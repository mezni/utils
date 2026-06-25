CREATE TABLE IF NOT EXISTS ev.stations (
    station_id VARCHAR(16) PRIMARY KEY,
    osm_id BIGINT UNIQUE,
    partner_id VARCHAR(16) REFERENCES ev.partners(partner_id),
    name VARCHAR(255) NOT NULL,
    address TEXT,
    location GEOGRAPHY(Point, 4326) NOT NULL,
    tags HSTORE,
    created_by_uuid UUID,
    updated_by_uuid UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_stations_location
    ON ev.stations
    USING GIST(location);
