# Implementation Plan: Spatial Discovery — Nearby API & SLO Validation

**Branch**: `007-spatial-discovery-nearby` | **Date**: 2026-05-26 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/003-spatial-discovery-nearby/spec.md`

## Summary

Implement the high-performance `/api/v1/stations/nearby` endpoint using PostGIS
`ST_DWithin` + `ST_Distance` with GIST index for ≤200ms p95 latency. Add
repository function, route handler, and SLO benchmark validation. Verify that
existing station detail and charger list endpoints meet mobile app requirements.

## Technical Context

**Language/Version**: Rust 1.78+ (edition 2021)

**Primary Dependencies**: Actix-web 4, SQLx 0.7 (runtime-tokio, postgres,
chrono), Tokio 1, serde 1, chrono 0.4, PostGIS 3.4

**Storage**: PostgreSQL 16+ with PostGIS 3.4. Spatial GIST index on
`stations.coordinates` already created in Phase 1 migration.

**Testing**: `cargo test` (unit). SLO benchmark via `oha` (external tool:
`cargo install oha`).

**Target Platform**: Linux (Docker container), HTTP API on :8080

**Project Type**: Web service (single Rust binary, REST API) — adds to existing
backend established in Phase 1.

**Performance Goals**: p95 ≤ 200ms at concurrency 10 (1000 requests) for the
nearby endpoint against the 100-station seed dataset (SC-001).

**Constraints**: No authentication required on nearby endpoint (public read);
no partner scoping on discovery; `LIMIT 50` hard cap; `is_test` SQL-level
isolation; existing infrastructure module structure maintained

**Scale/Scope**: 1 new repository function, 1 new route handler, 1 new model
(struct), 1 benchmark script, verification of 2 existing endpoints

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Pre-Research Assessment

| Principle | Gate | Assessment |
|-----------|------|------------|
| I. Modular Monorepo Architecture | All code under `sources/backend/`; `/api/v1/*` namespace; spatial queries ≤200ms | ✅ New code goes in existing `sources/backend/src/domain/infrastructure/`. Route registered under `/api/v1/stations/nearby`. GIST index + LIMIT 50 + `ST_DWithin` bounding designed for ≤200ms. |
| II. Semantic Identity & Data Isolation | `is_test` filter; soft delete; multi-tenant | ✅ `is_test` isolation at SQL level with `AND ($4 = TRUE OR s.is_test = FALSE)`. Soft-delete filter `WHERE s.deleted_at IS NULL`. No partner scoping needed on discovery endpoint. |
| III. Administrative UX Discipline | N/A | ⬜ N/A — backend API only. |
| IV. Mobile & Discovery Constraints | 20km default radius; LIMIT 50; `is_test` hidden | ✅ Default radius 20km, hard LIMIT 50, test records excluded by default. Mobile app never sends `include_test`. |
| V. Deterministic Implementation | Modular domain layers; seed script | ✅ Infrastructure module already scaffolded in Phase 1. Seed data from Phase 1 fully deterministic. |

**Pre-research result**: PASS — no violations.

### Post-Design Re-Assessment

| Principle | Gate | Assessment |
|-----------|------|------------|
| I. Modular Monorepo Architecture | All code under `sources/backend/`; `/api/v1/*` namespace; spatial queries ≤200ms | ✅ Infrastructure module under `sources/backend/src/domain/infrastructure/`. Nearby route at `/api/v1/stations/nearby`. Query uses GIST index + `ST_DWithin` bounding + LIMIT 50. Verified via SLO benchmark. |
| II. Semantic Identity & Data Isolation | `is_test` filter | ✅ `include_test` defaults to false. SQL-level `AND ($4 = TRUE OR s.is_test = FALSE)` prevents test record leaks. No partner scoping on discovery (intentional). |
| III. Administrative UX Discipline | N/A | ⬜ N/A |
| IV. Mobile & Discovery Constraints | 20km radius; LIMIT 50; test exclusion | ✅ All three enforced at the database query level. |
| V. Deterministic Implementation | Modular layers; seed | ✅ Infrastructure module follows same patterns as domain modules. Seed data unchanged. |

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
├── mod_impl.rs          # (unchanged from Phase 1)
└── repository.rs        # find_nearby_stations_bounded function
```

No new migrations. No new tables. No schema changes.

### Existing Files (referenced, not modified)

- `sources/backend/src/main.rs` — wire new nearby route into `/api/v1/stations/` scope
- `sources/backend/src/domain/stations/handlers.rs` — verify station detail endpoint
- `sources/backend/src/domain/chargers/handlers.rs` — verify charger list endpoint

## Complexity Tracking

No constitutional violations identified. Complexity tracking is not required for
this phase. The infrastructure module pattern (repository + handler) is already
established in Phase 1 and reused here with zero new patterns.
