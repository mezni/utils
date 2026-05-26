CREATE TABLE users (
    id TEXT PRIMARY KEY CHECK (id ~ '^USR-[a-z0-9]{12}$'),
    email TEXT NOT NULL UNIQUE,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role user_role NOT NULL,
    is_test BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX idx_users_created_at_id ON users (created_at ASC, id ASC) WHERE deleted_at IS NULL;
