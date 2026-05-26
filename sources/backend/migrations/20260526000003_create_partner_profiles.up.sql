CREATE TABLE partner_profiles (
    id TEXT PRIMARY KEY CHECK (id ~ '^PRT-[a-z0-9]{12}$'),
    user_id TEXT NOT NULL UNIQUE REFERENCES users(id),
    classification partner_classification NOT NULL,
    display_name TEXT NOT NULL,
    tax_id TEXT,
    contact_phone TEXT,
    is_test BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX idx_partner_profiles_created_at_id ON partner_profiles (created_at ASC, id ASC) WHERE deleted_at IS NULL;
