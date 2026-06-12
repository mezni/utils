# Implementation Plan: Infrastructure & Database Setup

**Branch**: `001-infra-database-setup` | **Date**: 2026-06-11 | **Spec**: specs/001-infra-database-setup/spec.md

**Input**: Feature specification from specs/001-infra-database-setup/spec.md

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/plan-template.md` for the execution workflow.

## Summary

Provision a reproducible local development environment for BorneMap MVP-1. Deliver
Docker Compose orchestration with two PostgreSQL 16 instances (platform_db +
PostGIS, analytics_db), idempotent SQL migrations for inventory/gis/analytics
schemas, Tunisia seed data, environment variable documentation, and startup
connectivity verification.

## Technical Context

**Language/Version**: Docker Compose v3.9, SQL (PostgreSQL 16 + PostGIS 3.3)

**Primary Dependencies**: Docker Compose, postgis/postgis:16-3.3 image,
postgres:16 image

**Storage**: Two PostgreSQL 16 instances — platform_db (with PostGIS extension for
spatial queries), analytics_db (plain PostgreSQL, append-only)

**Testing**: psql schema inspection (table DDL, index existence, constraint
validation), explain-plan verification for spatial index usage, restart/persistence
tests

**Target Platform**: Linux containers (Docker Engine), macOS/Windows via Docker
Desktop

**Project Type**: Infrastructure setup — Docker Compose orchestration + SQL
migrations + seed data scripts

**Performance Goals**: DB containers healthy within 2 minutes of `docker compose
up`, nearby spatial query under 100ms p95 with up to 1000 stations

**Constraints**: Ports 5432 (platform_db) and 5433 (analytics_db) reserved; all
migrations idempotent; soft-delete on infrastructure entities; append-only
constraints on analytics_db; no runtime code in infra/ directory

**Scale/Scope**: 2 database instances, 3 schemas (inventory, gis, users), 4 core
tables (partner, station, charger, raw_events), seed data with 2+ partners, 3+
stations, 5+ chargers

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Requirement | Status |
|-----------|-------------|--------|
| **IV. Source-Rooted Codebase** | All runtime code under `source/`; infra configs outside | ✅ PASS — Docker Compose, migrations, .env go under `infra/` |
| **V. Immutable Data & Append-Only Analytics** | analytics_db is APPEND-ONLY — no UPDATE/DELETE | ✅ PASS — raw_events table with append-only constraints |
| **Stack & Tooling Mandate** | PostgreSQL 16 + PostGIS; pnpm only; Traefik gateway | ✅ PASS — PostGIS on platform_db, plain PostgreSQL on analytics_db |
| **I. UX-First** | UX quality supersedes system complexity | ✅ PASS — infra setup enables <100ms map search target |

No violations found. Complexity Tracking not required.

## Project Structure

### Documentation (this feature)

```text
specs/001-infra-database-setup/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
infra/
├── docker-compose.yml            ← Service orchestration (postgres + postgis)
├── .env.example                  ← Environment variable registry
└── migrations/
    ├── 001-platform-db-init.sql   ← platform_db database + extensions
    ├── 002-inventory-schema.sql   ← partner, station, charger tables + indexes
    ├── 003-gis-schema.sql         ← GIS schema + spatial indexes
    ├── 004-analytics-db-init.sql  ← analytics_db raw_events + append-only rules
    └── 005-seed-data.sql          ← Tunisia test stations and chargers

scripts/
└── dev.sh                         ← Single-command startup wrapper
```

**Structure Decision**: Standard monorepo infra layout per constitution. All
infrastructure configs under `infra/`, dev tooling under `scripts/`. No runtime
code in either directory.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

No violations. Complexity Tracking not required.
