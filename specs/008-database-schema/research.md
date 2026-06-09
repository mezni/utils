# Research: Database Schema

**Phase**: Phase 0 — Technology & pattern research for Sprint 2.2

**Date**: 2026-06-09

## Technology Decisions

### Migration Framework

- **Decision**: Raw SQL files in `database/migrations/` executed by `sqlx::migrate!()`.
- **Rationale**: sqlx embeds migrations in the compiled binary, ensuring schema version is always in sync with the application code. This is the standard pattern for sqlx-based Rust projects and matches the ev-db crate from Sprint 2.1.
- **Alternatives considered**: `diesel` CLI (rejected — adds ORM dependency not needed for Sprint 2.2), manual `psql` execution (rejected — error-prone, no version tracking), `flyway`/`liquibase` (rejected — Java dependency).

### Spatial Column Strategy

- **Decision**: Non-nullable `GEOMETRY(Point, 4326)` column populated by a `BEFORE INSERT OR UPDATE` trigger that computes `ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326)`.
- **Rationale**: A trigger guarantees the geometry column is always in sync with lat/lng — no application code can forget to set it. Generated columns (PostgreSQL 12+) would be simpler but are not supported for PostGIS geometry types in all versions.
- **Alternatives considered**: Application-layer computation (rejected — every service must independently ensure correctness), generated column (rejected — not universally supported with PostGIS geometry types).

### Constraint Naming Convention

- **Decision**: Use descriptive names: `ck_{table}_{column}` for CHECK, `fk_{child}_{parent}` for foreign keys, `idx_{table}_{column}` for indexes.
- **Rationale**: PostgreSQL's default constraint names are auto-generated and unhelpful in error messages. Descriptive names allow developers to immediately identify which constraint was violated from the error message alone.
- **Alternatives considered**: Default names (rejected — cryptic), numeric prefixes (rejected — harder to read than descriptive names).

### Seed Idempotency

- **Decision**: Each seed file starts with `TRUNCATE {table} CASCADE` followed by `INSERT` statements.
- **Rationale**: Simplest approach that guarantees idempotency. Running seeds twice produces identical state because the first run's data is truncated before re-inserting. CASCADE handles FK dependencies.
- **Alternatives considered`: `INSERT ... ON CONFLICT DO NOTHING` (rejected — requires unique constraint matching), `DELETE` + `INSERT` in a transaction (rejected — equivalent to TRUNCATE but slower), upsert pattern (rejected — more complex, no benefit over TRUNCATE).

## Migration Details

| Migration | Creates | Key Constraints |
|-----------|---------|-----------------|
| 0001 | ev-platform schema, schema_version table | — |
| 0002 | partner table | ck_partner_type (IN 'business', 'personal') |
| 0003 | station table | ck_station_latitude (BETWEEN -90 AND 90), ck_station_longitude (BETWEEN -180 AND 180), fk_station_partner, idx_station_location (GIST) |
| 0004 | charger table, station_availability table | ck_charger_connector_type, ck_charger_power_kw (> 0), ck_charger_status, ck_availability_status, fk_charger_station, fk_availability_station |

## Best Practices

- **Descriptive constraint names** on all CHECK and FK constraints.
- **IF NOT EXISTS** on schema and table creation for idempotent re-runs.
- **TRUNCATE CASCADE** in seed files for idempotency.
- **Transaction-wrapped migrations** — each migration is a single transaction (sqlx default).
- **Lowercase enum values** matching MVP-1 JSON convention and ev-core enum serialization.
- **Audit fields** on all data tables (created_at, created_by, updated_at, updated_by) matching MVP-1 schema.
