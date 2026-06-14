CREATE SCHEMA IF NOT EXISTS inventory;
CREATE SCHEMA IF NOT EXISTS gis;

CREATE TABLE IF NOT EXISTS inventory.station (
    id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(255),
    status VARCHAR(20) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'maintenance', 'inactive')),
    latitude NUMERIC(10,8),
    longitude NUMERIC(11,8),
    location GEOGRAPHY(POINT, 4326) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_station_location ON inventory.station USING GIST(location);
