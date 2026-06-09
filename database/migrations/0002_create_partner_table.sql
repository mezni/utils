-- Migration 0002: Create partner table
-- Stores partner organizations and individuals who own charging stations.

CREATE TABLE IF NOT EXISTS "ev-platform".partner (
    id          TEXT        PRIMARY KEY,
    name        TEXT        NOT NULL,
    type        TEXT        NOT NULL,
    is_verified BOOLEAN     NOT NULL DEFAULT false,
    is_live     BOOLEAN     NOT NULL DEFAULT false,
    is_active   BOOLEAN     NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL,
    created_by  TEXT        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    updated_by  TEXT        NOT NULL,
    CONSTRAINT ck_partner_type CHECK (type IN ('business', 'personal'))
);
