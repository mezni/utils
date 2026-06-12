-- 001-platform-db-init.sql
-- Target: platform_db
-- Purpose: Create database, enable PostGIS, create schemas
-- Idempotent: YES (IF NOT EXISTS guards)

CREATE SCHEMA IF NOT EXISTS inventory;
CREATE SCHEMA IF NOT EXISTS gis;

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
