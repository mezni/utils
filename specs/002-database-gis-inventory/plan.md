# Implementation Plan: Database — GIS and Inventory Schemas

**Branch**: `002-database-gis-inventory` | **Date**: 2026-06-07 | **Spec**: specs/002-database-gis-inventory/spec.md

**Input**: Feature specification from `specs/002-database-gis-inventory/spec.md`

## Summary

Create 6 SQL migrations for the BorneMap database: extensions, schemas (inventory + gis), inventory tables (partner, station, charger, station_availability), inventory indexes, GIS tables (osm_nodes, osm_ways, roads, boundaries, amenity_points, station_locations), and GIS GiST indexes. Provide a migration runner script and development seed data (3 partners, 15 stations, 24 chargers).

## Technical Context

**Language/Version**: SQL (PostgreSQL 16 + PostGIS 3.4), Shell script (bash)

**Primary Dependencies**: psql (PostgreSQL client), postgis/postgis:16-3.4 Docker image

**Storage**: PostgreSQL 16 + PostGIS 3.4 (defined in Docker Compose from Sprint 1.1)

**Testing**: Manual migration apply + verify using psql; automated via shell script exit codes

**Target Platform**: Linux (Docker container or developer host), CI: GitHub Actions ubuntu-latest

**Project Type**: Database schema migrations + seed data (supports web-service monorepo)

**Performance Goals**: All 6 migrations complete in under 30 seconds; spatial query under 100ms with 15 stations; seeds under 5 seconds

**Constraints**: Migrations never edited after commit (constitution rule); all idempotent; only 3 extensions installed (PostGIS, uuid-ossp, pgcrypto)

**Scale/Scope**: 6 migration files, 3 seed files, 1 runner script, ~10 database tables across 2 schemas. Single developer operating everything.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- **Principle IV (Domain Separation by Schema)**: Compliant. Creates `inventory` and `gis` schemas as separate domains. station_locations in gis references inventory.station via FK (read-only derived data, consistent with "gis is never the master of any business entity").
- **Principle II (Single Source of Truth)**: Compliant. inventory.station remains the authoritative source; gis.station_locations is derived.
- **Principle V (Build for Current Scale)**: Compliant. 6 migrations, 2 schemas, 10 tables — proportional to project needs. GiST indexes are standard spatial practice, not premature optimization.
- **Migrations never edited after commit**: Acknowledged. All migrations must be correct before commit.

**Gate status**: ✅ PASS — no violations. Complexity tracking not required.

## Project Structure

### Documentation (this feature)

```text
specs/002-database-gis-inventory/
├── plan.md              # This file
├── spec.md              # Feature specification
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code (repository root)

```text
db/
├── migrations/
│   ├── 0001_extensions.sql       # PostGIS, uuid-ossp, pgcrypto
│   ├── 0002_schemas.sql          # inventory, gis
│   ├── 0003_inventory_tables.sql # partner, station, charger, station_availability
│   ├── 0004_inventory_indexes.sql
│   ├── 0005_gis_tables.sql       # osm_nodes, osm_ways, roads, boundaries, amenity_points, station_locations
│   ├── 0006_gis_indexes.sql      # GiST indexes
│   └── migrate.sh                # Migration runner
└── seeds/
    ├── dev_partners.sql          # 3 partners
    ├── dev_stations.sql          # 15 stations
    └── dev_chargers.sql          # 24 chargers
```

**Structure Decision**: Flat migration directory with numeric prefix ordering. Seeds separated from migrations to distinguish schema changes from test data. A single runner script applies migrations in order and seeds optionally.

## Complexity Tracking

> Not needed — Constitution Check passed without violations.
