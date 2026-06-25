CREATE TABLE IF NOT EXISTS ev.chargers (
    charger_id VARCHAR(16) PRIMARY KEY,
    station_id VARCHAR(16) NOT NULL REFERENCES ev.stations(station_id) ON DELETE CASCADE,
    connector_type_id INTEGER NOT NULL REFERENCES ev.connector_types(id),
    status_id INTEGER NOT NULL REFERENCES ev.connector_statuses(id),
    current_type_id INTEGER NOT NULL REFERENCES ev.current_types(id),
    power_kw DECIMAL(5, 2),
    voltage INTEGER,
    amperage INTEGER,
    count_available INTEGER DEFAULT 1 CHECK (count_available >= 0),
    count_total INTEGER DEFAULT 1 CHECK (count_total >= 1 AND count_total >= count_available),
    created_by_uuid UUID,
    updated_by_uuid UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    CONSTRAINT unique_connector UNIQUE (station_id, connector_type_id, current_type_id)
);
