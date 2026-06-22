-- Rollback: users schema

-- Revoke permissions
REVOKE ALL PRIVILEGES ON SCHEMA users FROM bornemap_admin;
REVOKE ALL PRIVILEGES ON TABLE users.user_profiles FROM bornemap_admin;
REVOKE USAGE ON SCHEMA users FROM bornemap_driver, bornemap_analytics_reader;
REVOKE SELECT ON TABLE users.user_profiles FROM bornemap_driver, bornemap_analytics_reader;

-- Drop tables
DROP TABLE IF EXISTS users.user_profiles;

-- Drop schema
DROP SCHEMA IF EXISTS users;
