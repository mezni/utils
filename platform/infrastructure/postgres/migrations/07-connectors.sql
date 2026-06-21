CREATE TABLE inventory.connectors (
    connector_id VARCHAR(32) PRIMARY KEY
        CHECK (connector_id ~ '^CON-[A-Za-z0-9_-]{12}$'),

    charger_id VARCHAR(32) NOT NULL
        REFERENCES inventory.chargers(charger_id)
        ON DELETE CASCADE,

    connector_type_id INT NOT NULL REFERENCES inventory.connector_types(id),
    current_type_id INT NOT NULL REFERENCES inventory.current_types(id),
    status_id INT NOT NULL REFERENCES inventory.connector_statuses(id),

    max_power_kw NUMERIC(6,2) CHECK (max_power_kw > 0),

    min_voltage INT,
    max_voltage INT,

    min_amperage INT,
    max_amperage INT,

    count_available INT DEFAULT 1 CHECK (count_available >= 0),
    count_total INT DEFAULT 1 CHECK (count_total >= 1 AND count_total >= count_available),

    metadata JSONB NOT NULL DEFAULT '{}',

    source_id INT REFERENCES inventory.data_sources(id),
    source_external_id VARCHAR(255),

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ,

    created_by VARCHAR(32),
    updated_by VARCHAR(32),

    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMPTZ,
    deleted_by VARCHAR(32),

    UNIQUE (charger_id, connector_type_id, current_type_id)
);

CREATE INDEX idx_connectors_charger ON inventory.connectors (charger_id);
