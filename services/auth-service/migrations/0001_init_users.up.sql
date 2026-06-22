-- Migrations for auth-service: users schema
-- Purpose: Store user profiles and authentication data

-- Create users schema
CREATE SCHEMA IF NOT EXISTS users;

-- Users table with UUID identity
CREATE TABLE IF NOT EXISTS users.user_profiles (
    user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_users_email ON users.user_profiles(email);

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA users TO bornemap_admin;
GRANT ALL PRIVILEGES ON TABLE users.user_profiles TO bornemap_admin;
GRANT USAGE ON SCHEMA users TO bornemap_driver, bornemap_analytics_reader;
GRANT SELECT ON TABLE users.user_profiles TO bornemap_driver, bornemap_analytics_reader;
