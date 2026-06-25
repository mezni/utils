-- Auth-service owns user_profiles table.
-- Constitution §10.3: user_uuid = Keycloak UUID (sub claim).
CREATE TABLE IF NOT EXISTS users.user_profiles (
    user_uuid UUID PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone VARCHAR(50),
    locale VARCHAR(20),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ NULL
);
