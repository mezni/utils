-- Migration 005: Chargers table
-- Individual charging unit at a station

CREATE TABLE inventory.chargers (
    id                VARCHAR(32)    PRIMARY KEY CHECK (id ~ '^CHG-[A-Za-z0-9_]{12}$'),
    station_id        VARCHAR(32)    NOT NULL REFERENCES inventory.stations(id),
    connector_type_id BIGINT         NOT NULL REFERENCES inventory.connector_types(id),
    current_type_id   BIGINT         NOT NULL REFERENCES inventory.current_types(id),
    status_id         BIGINT         NOT NULL REFERENCES inventory.connector_statuses(id),
    power_kw          DECIMAL(5,2),
    voltage           INT,
    amperage          INT,
    count_available   INT            NOT NULL DEFAULT 1 CHECK (count_available >= 0),
    count_total       INT            NOT NULL DEFAULT 1 CHECK (count_total >= 1),
    deleted_at        TIMESTAMPTZ,
    created_at        TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_active_charger_type UNIQUE (station_id, connector_type_id)
);

CREATE INDEX idx_chargers_station ON inventory.chargers (station_id);
