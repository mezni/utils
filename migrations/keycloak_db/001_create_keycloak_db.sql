-- Create dedicated database for Keycloak.
-- Runs only on first PostgreSQL initialization via docker-entrypoint-initdb.d.
-- Keycloak manages this database exclusively — no application tables.

SELECT 'CREATE DATABASE keycloak_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'keycloak_db')\gexec
