# Migration Contract

## Contract

- File: `db/migrations/0001_extensions.sql` through `0006_gis_indexes.sql`
- Format: Raw SQL
- Naming: Numeric prefix (0001, 0002, ...) + descriptive name
- Order: Applied in ascending numeric order
- Runner: `db/migrations/migrate.sh`
- Connection: `DATABASE_URL` environment variable
- Idempotency: Each migration must be safe to re-run (IF NOT EXISTS / IF EXISTS)
- Error handling: Runner stops on first error; error output to stderr
- Exit codes: 0 = success, non-zero = failure

## Apply Sequence

1. `0001_extensions.sql` — CREATE EXTENSION IF NOT EXISTS
2. `0002_schemas.sql` — CREATE SCHEMA IF NOT EXISTS
3. `0003_inventory_tables.sql` — CREATE TABLE inventory.*
4. `0004_inventory_indexes.sql` — CREATE INDEX ON inventory.*
5. `0005_gis_tables.sql` — CREATE TABLE gis.*
6. `0006_gis_indexes.sql` — CREATE INDEX ON gis.* (GiST)
