-- Migration 003: Partners table
-- Core entity representing an operator/company managing charging stations

CREATE TABLE inventory.partners (
    id             VARCHAR(32)    PRIMARY KEY CHECK (id ~ '^OPR-[A-Za-z0-9_]{12}$'),
    name           VARCHAR(255)   NOT NULL,
    network_type   VARCHAR(20)    NOT NULL CHECK (network_type IN ('INDIVIDUAL', 'COMPANY')),
    support_phone  VARCHAR(50),
    support_email  VARCHAR(255),
    is_verified    BOOLEAN        NOT NULL DEFAULT FALSE,
    deleted_at     TIMESTAMPTZ,
    created_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);
