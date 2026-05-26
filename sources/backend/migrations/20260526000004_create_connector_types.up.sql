CREATE TABLE connector_types (
    id TEXT PRIMARY KEY CHECK (id ~ '^CNT-[a-z0-9]{12}$'),
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL,
    is_test BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX idx_connector_types_created_at_id ON connector_types (created_at ASC, id ASC) WHERE deleted_at IS NULL;
