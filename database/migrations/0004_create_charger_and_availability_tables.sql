-- Migration 0004: Create charger and station_availability tables
-- Stores individual charger units and station availability history.

CREATE TABLE IF NOT EXISTS "ev-platform".charger (
    id              TEXT             PRIMARY KEY,
    station_id      TEXT             NOT NULL,
    connector_type  TEXT             NOT NULL,
    power_kw        DOUBLE PRECISION NOT NULL,
    status          TEXT             NOT NULL,
    created_at      TIMESTAMPTZ      NOT NULL,
    created_by      TEXT             NOT NULL,
    updated_at      TIMESTAMPTZ      NOT NULL,
    updated_by      TEXT             NOT NULL,
    CONSTRAINT ck_charger_connector_type CHECK (connector_type IN ('type2', 'type3', 'ccs', 'chademo')),
    CONSTRAINT ck_charger_power_kw        CHECK (power_kw > 0),
    CONSTRAINT ck_charger_status          CHECK (status IN ('available', 'in_use', 'maintenance', 'offline')),
    CONSTRAINT fk_charger_station         FOREIGN KEY (station_id) REFERENCES "ev-platform".station(id)
);

CREATE TABLE IF NOT EXISTS "ev-platform".station_availability (
    id          TEXT        PRIMARY KEY,
    station_id  TEXT        NOT NULL,
    status      TEXT        NOT NULL,
    updated_by  TEXT        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    CONSTRAINT ck_availability_status   CHECK (status IN ('available', 'partial', 'unavailable')),
    CONSTRAINT fk_availability_station  FOREIGN KEY (station_id) REFERENCES "ev-platform".station(id)
);
