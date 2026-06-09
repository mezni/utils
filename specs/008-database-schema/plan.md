# Implementation Plan: Database Schema

**Branch**: `008-database-schema` | **Date**: 2026-06-09 | **Spec**: [spec.md](./spec.md)

**Input**: Sprint 2.2 — PostgreSQL + PostGIS schema migrations for partner, station, and charger tables. CHECK constraints on lat/lng ranges, connector types, and power values. Spatial index on station coordinates. Dev seeds replacing json-server db.json.

## Summary

Create 4 sequential SQL migrations (`database/migrations/0001–0004`) that build the `ev-platform` schema with partner, station, charger, and station_availability tables. All tables have CHECK constraints matching the MVP-1 data model, foreign key relationships, audit fields, and a spatial GIST index on station coordinates. Dev seed scripts populate all tables with data equivalent to `source/mock/db.json` (3 partners, 15 stations, 24 chargers, 15 availability records).

## Technical Context

**Language/Version**: SQL (PostgreSQL 17 + PostGIS 3)

**Primary Dependencies**: PostgreSQL 17 with PostGIS 3 extension

**Storage**: PostgreSQL via sqlx migrations (matching ev-db from Sprint 2.1)

**Testing**: Manual EXPLAIN ANALYZE for spatial index verification; manual INSERT tests for CHECK constraint verification

**Target Platform**: Linux server (x86_64) running PostgreSQL 17 + PostGIS 3

**Project Type**: database schema (SQL migrations + seed scripts)

**Performance Goals**: Spatial ST_DWithin query < 50ms on seeded data (15 stations); migrations apply in under 30s

**Constraints**: All CHECK constraints must have descriptive names for clear error messages; migrations must be idempotent (IF NOT EXISTS); seeds must be idempotent (TRUNCATE + INSERT)

**Scale/Scope**: 4 SQL migrations, 4 seed SQL files, 4 tables, 5 CHECK constraints, 3 FK relationships, 1 GIST spatial index

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

The constitution file (`.specify/memory/constitution.md`) is a template with `[PLACEHOLDER]` tokens — not ratified. No gates apply.

## Phase 0: Research

All technical decisions are documented in the spec and implementation plan. No `[NEEDS CLARIFICATION]` markers remain. Research is minimal — confirming established PostgreSQL patterns.

### Key Decisions

| Decision | Chosen Approach | Rationale |
|----------|----------------|-----------|
| Migration framework | Raw SQL via sqlx migrations | Matches ev-db crate; sqlx embeds migrations at compile time; no external tooling needed |
| Spatial column | `GEOMETRY(Point, 4326)` computed from lat/lng | ST_DWithin requires geometry column; generated column via trigger or application layer |
| Spatial index | GIST index on `station.location` | Standard PostGIS pattern for ST_DWithin queries |
| Location computation | Database trigger on INSERT/UPDATE | Ensures location is always in sync with lat/lng; application layer cannot forget to set it |
| Migration directory | `database/migrations/` at repo root | Standard sqlx convention; placed outside `source/` since no Rust crate owns it |
| Seed directory | `database/seeds/` at repo root | Parallel to migrations; SQL files with TRUNCATE + INSERT |
| Constraint naming | Descriptive names: `ck_partner_type`, `ck_station_latitude`, etc. | Clear error messages identify the violated constraint by name |

## Phase 1: Design

### Project Structure

```
database/
├── migrations/
│   ├── 0001_create_ev_platform_schema.sql
│   ├── 0002_create_partner_table.sql
│   ├── 0003_create_station_table.sql
│   └── 0004_create_charger_and_availability_tables.sql
└── seeds/
    ├── 001_partners.sql
    ├── 002_stations.sql
    ├── 003_chargers.sql
    └── 004_station_availability.sql

source/                        # Existing JS/TS + Rust (unchanged)
```

**Structure Decision**: Database artifacts live under `database/` at the repo root, parallel to `source/` and `specs/`. This keeps SQL separate from application code and matches the standard sqlx convention where `sqlx::migrate!()` looks for a `migrations/` directory.

### Data Model

See `data-model.md` for full table definitions, column types, CHECK constraints, FK relationships, and index definitions.

### Contracts

Not applicable — this sprint produces SQL migration files and seed scripts, not an external API or library interface. The schema itself IS the contract consumed by downstream services (Sprint 2.3, 2.4).

### Quickstart

See `quickstart.md` for applying migrations and seeding the database.

### Agent Context

AGENTS.md will be updated to reference this plan file.
