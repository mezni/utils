CREATE TABLE chargers (
    id TEXT PRIMARY KEY CHECK (id ~ '^CHG-[a-z0-9]{12}$'),
    station_id TEXT NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
    connector_type_id TEXT NOT NULL REFERENCES connector_types(id),
    power_kw FLOAT8 NOT NULL CHECK (power_kw > 0),
    current_type current_type NOT NULL,
    status charger_status NOT NULL DEFAULT 'available',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_chargers_station_created_at_id ON chargers (station_id, created_at ASC, id ASC);
