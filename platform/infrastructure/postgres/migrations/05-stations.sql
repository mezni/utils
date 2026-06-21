CREATE TABLE inventory.stations (
    station_id VARCHAR(32) PRIMARY KEY
        CHECK (station_id ~ '^STA-[A-Za-z0-9_-]{12}$'),

    partner_id VARCHAR(32)
        REFERENCES inventory.partners(partner_id),

    osm_id BIGINT UNIQUE,

    name VARCHAR(255) NOT NULL,

    address TEXT,

    location GEOGRAPHY(Point, 4326) NOT NULL,

    tags HSTORE,

    status_id INT REFERENCES inventory.station_statuses(id),

    is_test BOOLEAN NOT NULL DEFAULT FALSE,

    source_id INT REFERENCES inventory.data_sources(id),
    source_external_id VARCHAR(255),

    metadata JSONB NOT NULL DEFAULT '{}',

    version BIGINT NOT NULL DEFAULT 1,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ,

    created_by VARCHAR(32),
    updated_by VARCHAR(32),

    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMPTZ,
    deleted_by VARCHAR(32)
);

CREATE INDEX idx_stations_location ON inventory.stations USING GIST (location);
CREATE INDEX idx_stations_is_test ON inventory.stations (is_test) WHERE is_test = FALSE;
CREATE INDEX idx_stations_partner ON inventory.stations (partner_id) WHERE partner_id IS NOT NULL;
CREATE INDEX idx_stations_status ON inventory.stations (status_id);
