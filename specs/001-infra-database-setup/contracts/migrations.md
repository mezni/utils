# Migration Contract

## Naming Convention

Sequential numeric prefix + descriptive name:
```
NNN-description.sql
```

## Migration Files

| File | Target DB | Purpose |
|------|-----------|---------|
| 001-platform-db-init.sql | platform_db | Create database, enable PostGIS, create schemas |
| 002-inventory-schema.sql | platform_db | partner, station, charger tables + indexes |
| 003-gis-schema.sql | platform_db | GIS tables (osm_region, osm_road) |
| 004-analytics-db-init.sql | analytics_db | raw_events table + append-only rules |
| 005-seed-data.sql | platform_db | Tunisia test stations and chargers |

## Idempotency Rules

Every migration MUST be safe to run multiple times:

- `CREATE TABLE IF NOT EXISTS`
- `CREATE INDEX IF NOT EXISTS`
- `CREATE SCHEMA IF NOT EXISTS`
- `CREATE EXTENSION IF NOT EXISTS`

## Execution Order

1. platform_db migrations (001, 002, 003) in sequence
2. analytics_db migration (004)
3. Seed data migration (005) — runs only on platform_db

## Rollback Policy

No rollback scripts for MVP-1. If a migration fails:
1. Drop and recreate the target database
2. Re-run all migrations in order
