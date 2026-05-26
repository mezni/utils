CREATE TABLE stations (
    id TEXT PRIMARY KEY CHECK (id ~ '^STN-[a-z0-9]{12}$'),
    owner_id TEXT NOT NULL REFERENCES users(id),
    name TEXT NOT NULL,
    address TEXT NOT NULL,
    city TEXT NOT NULL,
    coordinates GEOGRAPHY(Point, 4326) NOT NULL,
    is_operational BOOLEAN NOT NULL DEFAULT true,
    is_test BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    CONSTRAINT valid_longitude CHECK (ST_X(coordinates::geometry) >= -180 AND ST_X(coordinates::geometry) <= 180),
    CONSTRAINT valid_latitude CHECK (ST_Y(coordinates::geometry) >= -90 AND ST_Y(coordinates::geometry) <= 90)
);

CREATE INDEX idx_stations_coordinates ON stations USING GIST (coordinates);
CREATE INDEX idx_stations_created_at_id ON stations (created_at ASC, id ASC) WHERE deleted_at IS NULL;
