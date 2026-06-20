# Implementation Plan: EV Charging Platform Foundation

**Branch**: `001-ev-charging-foundation` | **Date**: 2026-06-20 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-ev-charging-foundation/spec.md`

## Summary

Deliver a geospatial EV charging backbone: PostGIS database with inventory schema (Partner→Station→Charger→Connector), OSM ingestion pipeline, idempotent sync engine, materialized view for nearby queries, driver REST API, and a minimal web map application — all running via Docker Compose.

## Technical Context

**Language/Version**: Rust 1.85+ (backend services), Node.js 22+ (web app)

**Primary Dependencies**: Axum or Actix-web (Rust HTTP), sqlx (Rust DB driver), Leaflet (web map), Redis crate (Rust caching)

**Storage**: PostgreSQL 16 + PostGIS 3.4+, Redis 7+ (optional cache)

**Testing**: cargo test (Rust unit/integration), Playwright or Cypress (web E2E)

**Target Platform**: Linux x86_64 (Docker containers)

**Project Type**: web-service (backend) + web-application (frontend)

**Performance Goals**: Nearby query <50ms server-side, API response <150ms p95, map render <2s user-perceived

**Constraints**: GiST index mandatory on spatial columns; all geo queries via materialized view only; no cross-schema writes; typed nanoid IDs (PAR-, STA-, CHR-, CON-, JOB-)

**Scale/Scope**: Single region (Tunisia), estimated <500 stations initially, <100 concurrent drivers

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Gate | Status |
|-----------|------|--------|
| I. Service Ownership & Data Isolation | driver-service queries MV via read-only role; admin-service owns inventory schema | ✅ Pass |
| II. Spatial-First Design | `mv_stations_geo` as sole query target; GiST on stations.location | ✅ Pass |
| III. Idempotent Data Operations | Upsert-merge for OSM imports; sync_jobs audit trail | ✅ Pass |
| IV. Strict Entity Hierarchy | FK cascade enforced; typed prefix+nanoid(12) on all IDs | ✅ Pass |
| V. Observability & Audit | sync_jobs logging; latency tracking in driver-service; structured logging | ✅ Pass |

**No violations — all gates pass.**

## Project Structure

### Documentation (this feature)

```text
specs/001-ev-charging-foundation/
├── plan.md              # This file
├── research.md          # Technology research & decisions
├── data-model.md        # Entity schema & relationships
├── quickstart.md        # Development setup guide
├── contracts/           # API contracts & interface definitions
├── checklists/
│   └── requirements.md  # Spec quality checklist
└── tasks.md             # (created by /speckit.tasks)
```

### Source Code (repository root)

```text
services/
├── driver-api/          # Rust — nearby search, station detail (read-only), health
│   ├── src/
│   │   ├── main.rs
│   │   ├── routes/
│   │   │   ├── health.rs
│   │   │   └── nearby.rs
│   │   ├── db/
│   │   │   └── queries.rs
│   │   └── models/
│   └── Cargo.toml
├── admin-api/           # Rust — partner/station/charger/connector CRUD
│   ├── src/
│   │   ├── main.rs
│   │   ├── routes/
│   │   │   ├── stations.rs
│   │   │   ├── chargers.rs
│   │   │   └── connectors.rs
│   │   └── services/
│   │       ├── stations.rs
│   │       ├── chargers.rs
│   │       └── connectors.rs
│   └── Cargo.toml
├── sync-engine/         # Rust — OSM ingestion, geospatial sync
│   ├── src/
│   │   ├── main.rs
│   │   ├── ingest/
│   │   │   └── osm.rs
│   │   ├── sync/
│   │   │   └── pipeline.rs
│   │   └── db/
│   └── Cargo.toml
└── ingestion/           # Rust — OSM Overpass API fetcher
    ├── src/
    │   └── main.rs
    └── Cargo.toml

apps/
└── web/                 # Node.js — driver map application
    ├── src/
    │   ├── components/
    │   │   ├── MapView.tsx
    │   │   ├── StationList.tsx
    │   │   └── StationDetail.tsx
    │   ├── pages/
    │   ├── services/
    │   │   └── api.ts
    │   └── App.tsx
    ├── package.json
    └── index.html

db/
├── migrations/          # SQL migration files
│   ├── 001_extensions.sql
│   ├── 002_inventory_schema.sql
│   ├── 003_gis_layer.sql
│   ├── 004_materialized_views.sql
│   └── 005_seed_data.sql
├── functions/
│   └── find_nearby_stations.sql
└── init.sh

docker/
├── docker-compose.yml
├── Dockerfile.driver-api
├── Dockerfile.admin-api
├── Dockerfile.sync-engine
├── Dockerfile.ingestion
├── Dockerfile.web
└── postgres/
    └── init.sql

scripts/
├── import-osm.sh        # OSM Overpass API → staging
├── refresh-mv.sh        # Refresh materialized views
└── seed-dev.sh          # Seed development data
```

**Structure Decision**: Multi-service monorepo with Rust backend services under `services/` (driver-api read-only, admin-api CRUD, sync-engine, ingestion), Node.js web app under `apps/`, shared SQL migrations under `db/`, and Docker Compose under `docker/`. This follows the project's existing monorepo scaffold and constitution's three-service topology with strict Service Ownership.

## Complexity Tracking

All gates pass — no complexity justification needed.
