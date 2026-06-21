CREATE TABLE inventory.chargers (
    charger_id VARCHAR(32) PRIMARY KEY
        CHECK (charger_id ~ '^CHR-[A-Za-z0-9_-]{12}$'),

    station_id VARCHAR(32) NOT NULL
        REFERENCES inventory.stations(station_id)
        ON DELETE CASCADE,

    serial_number VARCHAR(255),

    vendor VARCHAR(255),
    model VARCHAR(255),
    firmware_version VARCHAR(255),

    status_id INT REFERENCES inventory.charger_statuses(id),

    source_id INT REFERENCES inventory.data_sources(id),
    source_external_id VARCHAR(255),

    metadata JSONB NOT NULL DEFAULT '{}',

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ,

    created_by VARCHAR(32),
    updated_by VARCHAR(32),

    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMPTZ,
    deleted_by VARCHAR(32)
);

CREATE INDEX idx_chargers_station ON inventory.chargers (station_id);
