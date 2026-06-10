-- Create a dedicated schema for Keycloak-managed tables.
-- This isolates Keycloak's internal tables from the application
-- schemas (public, ev-platform, inventory).
CREATE SCHEMA IF NOT EXISTS keycloak;
