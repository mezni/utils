CREATE TABLE IF NOT EXISTS ev.partners (
    id TEXT PRIMARY KEY,

    name TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_partners_name UNIQUE (name)
);
