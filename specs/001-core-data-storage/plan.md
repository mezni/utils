# Implementation Plan: Core Data & Storage Foundations

**Branch**: `001-core-data-storage` | **Date**: 2026-06-17 | **Spec**: `specs/001-core-data-storage/spec.md`

**Input**: Feature specification from `specs/001-core-data-storage/spec.md`

## Summary

Establish the spatially-enabled database foundation for the BorneMap EV charging
station platform. Deliver a containerized PostgreSQL 16 + PostGIS 3.4 instance
with dual-schema layout (`gis` for OSM reference data, `inventory` for
infrastructure entities), a Tunisia OpenStreetMap data ingestion pipeline using
osm2pgsql, and a native geodesic proximity function (`get_nearby_stations`)
for finding charging stations by distance. All components run inside a unified
Docker Compose mesh with zero mock data layers, per the "validation before
optimization" mandate.

## Technical Context

**Language/Version**: SQL / PL/pgSQL (PostgreSQL 16), Shell (bash)

**Primary Dependencies**: PostgreSQL 16 + PostGIS 3.4 (`postgis/postgis:16-3.4`
Docker image), osm2pgsql, wget/curl for Tunisia OSM PBF download

**Storage**: PostgreSQL 16 with PostGIS 3.4, persistent Docker named volume
(`pgdata`) for data survival across restarts

**Testing**: Manual verification via `psql` queries: schema inspection,
record counts, function invocation with known coordinates

**Target Platform**: Linux x86_64 (Docker container runtime)

**Project Type**: Infrastructure / database provisioning

**Performance Goals**:
- Cold database startup and schema initialization within 60 seconds
- Tunisia OSM import completes within 30 minutes
- Spatial distance function accurate to within 1 meter over 100km range

**Constraints**:
- Must use the standard `postgis/postgis:16-3.4` Docker image
- All spatial queries must use geography type for geodesic accuracy
- Schema init scripts must be idempotent (`CREATE IF NOT EXISTS`)
- Spatial indexes (GIST) required on all geometry/geography columns

**Scale/Scope**: Single PostGIS instance, Tunisia country-level OSM data
(~200-500MB PBF), 10 seed charging stations across 3 cities

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Principle I — Validation Before Optimization
✅ **PASS**. This feature is pure foundational database setup. No excluded
features (OCPP, billing, streaming, real-time telemetry, native mobile builds,
autoscaling, or distributed tracing) are introduced.

### Principle II — Strict Service Topology
✅ **PASS**. No microservices are created. The deliverables are Docker
infrastructure and database objects — well within the three-service topology.

### Principle III — Compile-Time Safety & Type Strictness
✅ **PASS**. This sprint produces no Rust code (no sqlx) and no TypeScript
code. SQL DDL and PL/pgSQL functions are the sole outputs.

### Principle IV — Read/Write Separation & Transactional Integrity
✅ **PASS**. Database layer only; no caching, no outbox patterns, no
application-level transactions defined at this layer.

### Principle V — Security & Identity Isolation
✅ **PASS**. No credentials stored in code. The `.env` file pattern and
Keycloak identity layer are introduced in later sprints. Soft delete rules
are schema-defined but not enforced at the database layer.

**Gate verdict**: All gates pass. Proceed to Phase 0 research.

## Project Structure

### Documentation (this feature)

```text
specs/001-core-data-storage/
├── plan.md              # This file
├── spec.md              # Feature specification
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Created by /speckit.tasks
```

### Source Code (repository root)

```text
source/
├── infra/
│   ├── docker-compose.yml          # PostGIS + Traefik services
│   └── db/
│       └── init.sql                # Schema DDL + seed data + spatial function
├── crates/
│   ├── db-models/
│   │   └── src/lib.rs              # SQLx structs (scaffolded, not active)
│   └── validation/
│       └── src/lib.rs              # Geo bounds validation (scaffolded)
└── docs/
    ├── sprint_backlog.md
    ├── roadmap_status.md
    └── system_state.md
```

**Structure Decision**: Infrastructure code lives under `source/infra/` per the
monorepo convention (§3 of constitution). The `db-models` and `validation`
crates are scaffolded as empty workspaces but not populated until Sprint 1.2.
Documentation tracking files are updated under `source/docs/`.

## Complexity Tracking

*No constitution violations to justify — all gates pass cleanly.*
