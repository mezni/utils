-- Create PostGIS extension if not already available.
-- Required by migration 0003 (GEOMETRY column, GIST index).
-- The Docker image postgis/postgis pre-installs PostGIS,
-- but standalone PostgreSQL instances need this explicitly.
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- Record this migration in sqlx's tracking table.
-- Note: sqlx uses its own _sqlx_migrations table,
-- so this comment is informational only.
