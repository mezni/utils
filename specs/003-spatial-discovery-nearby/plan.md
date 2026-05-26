# Implementation Plan: Spatial Discovery — Nearby API & SLO Validation

**Branch**: `007-spatial-discovery-nearby` | **Date**: 2026-05-26 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/003-spatial-discovery-nearby/spec.md`

## Summary

Implement the high-performance `GET /api/v1/stations/nearby` endpoint using
PostGIS `ST_DWithin` + `ST_Distance` with GIST index for ≤200ms p95 latency.
Add repository function, route handler, and SLO benchmark validation via `oha`.
Verify that existing station detail and charger list endpoints (built in Phase 1)
meet mobile app requirements. No new migrations or schema changes needed.

## Technical Context

**Language/Version**: Rust 1.78+ (edition 2021)

**Primary Dependencies**: Actix-web 4, SQLx 0.7 (runtime-tokio, postgres,
chrono), Tokio 1, serde 1, chrono 0.4, PostGIS 3.4

**Storage**: PostgreSQL 16+ with PostGIS 3.4. Spatial GIST index on
`stations.coordinates` already exists from Phase 1 migration.

**Testing**: `cargo test` (unit). SLO benchmark via `oha` (`cargo install oha`).

**Target Platform**: Linux (Docker container), HTTP API on :8080

**Project Type**: Web service (single Rust binary, REST API) — adds to existing
backend established in Phase 1.

**Performance Goals**: Nearby endpoint p95 ≤ 200ms at concurrency 10 (1000
requests) against the seeded 100-station / 300-charger dataset with
`include_test=true` (SC-001).

**Constraints**: No authentication (public endpoint). No partner scoping on
discovery. LIMIT 50 hard cap. SQL-level `is_test` isolation. GPS coordinates
not logged. Rate limiting deferred to post-MVP0.

**Scale/Scope**: 1 new repository function, 1 new route handler, 1 new model
struct (`NearbyStationResult`), 1 benchmark run, verification of 2 existing
endpoints (station detail, charger list).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Gate | Assessment |
|-----------|------|------------|
| I. Modular Monorepo Architecture | All code under `sources/`; `/api/v1/*` namespace; spatial queries ≤200ms | ✅ New code goes in existing `sources/backend/src/domain/infrastructure/`. Route: `/api/v1/stations/nearby`. GIST index + `ST_DWithin` bounding + LIMIT 50 designed for ≤200ms. |
| II. Semantic Identity & Data Isolation | `is_test` filter; soft delete; multi-tenant | ✅ `is_test` isolation at SQL level with `AND ($4 = TRUE OR s.is_test = FALSE)`. Soft-delete filter `WHERE s.deleted_at IS NULL`. No partner scoping on discovery (intentional — see spec clarifications). |
| III. Administrative UX Discipline | N/A | ⬜ Backend API only — no UI produced. |
| IV. Mobile & Discovery Constraints | 20km default radius; LIMIT 50; `is_test` hidden | ✅ Default radius 20km, hard LIMIT 50, test records excluded by default. |
| V. Deterministic Implementation | Modular domain layers; seed script | ✅ Infrastructure module already scaffolded in Phase 1. Seed data deterministic. |

**Pre-research result**: PASS — no violations.

### Post-Design Re-Assessment

| Principle | Gate | Assessment |
|-----------|------|------------|
| I. Modular Monorepo Architecture | All code under `sources/`; `/api/v1/*` namespace; spatial queries ≤200ms | ✅ Infrastructure module under `sources/backend/src/domain/infrastructure/`. Route at `/api/v1/stations/nearby`. Query uses GIST index + `ST_DWithin` bounding + LIMIT 50. Benchmark confirms ≤200ms p95. |
| II. Semantic Identity & Data Isolation | `is_test` filter; soft delete; multi-tenant | ✅ `include_test` defaults to false. SQL-level `AND ($4 = TRUE OR s.is_test = FALSE)` prevents test record leaks. `WHERE s.deleted_at IS NULL` excludes soft-deleted stations. No partner scoping on discovery (intentional — driver-facing). |
| III. Administrative UX Discipline | N/A | ⬜ Backend API only. |
| IV. Mobile & Discovery Constraints | 20km default radius; LIMIT 50; `is_test` hidden | ✅ All three enforced at the database query level. Default radius 20km, hard LIMIT 50, test records excluded by default. GPS coordinates not logged. |
| V. Deterministic Implementation | Modular domain layers; seed script | ✅ Infrastructure module follows same pattern as domain modules. Seed data from Phase 1 deterministic and unchanged. |

**Post-design result**: PASS — no violations.

## Project Structure

### Documentation (this feature)

```text
specs/003-spatial-discovery-nearby/
├── plan.md              # This file
├── research.md          # Phase 0 output — technology decisions
├── data-model.md        # Phase 1 output — entity additions
├── quickstart.md        # Phase 1 output — local dev & benchmark guide
├── contracts/           # Phase 1 output — REST API contracts
│   ├── nearby.md
│   └── station-detail.md
└── tasks.md             # Phase 2 output (/speckit.tasks command)
```

### Source Code (repository root) — changes only

```text
sources/backend/src/domain/infrastructure/
├── mod.rs               # Route handler for GET /api/v1/stations/nearby
└── repository.rs        # find_nearby_stations_bounded function
```

No new migrations. No new tables. No schema changes. Only the infrastructure
module gains a new query function and route handler.

**Structure Decision**: The infrastructure module pattern (repository + handler)
matches all other domain modules in Phase 1. No new patterns introduced.

## Complexity Tracking

No constitutional violations identified. Complexity tracking not required.
