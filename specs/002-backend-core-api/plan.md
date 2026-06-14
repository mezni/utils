# Implementation Plan: MVP-1 Sprint 1 — Backend Core API (driver-service)

**Branch**: `002-backend-core-api` | **Date**: 2026-06-13 | **Spec**: specs/002-backend-core-api/spec.md

**Input**: Feature specification from `specs/002-backend-core-api/spec.md`

## Summary

Implement three read-only REST API endpoints (`GET /api/v1/stations`, `GET /api/v1/stations/{id}`, `GET /api/v1/stations/nearby`) plus `GET /api/v1/health` in a Rust async driver-service using Clean Architecture (Handler → Service → Repository) against platform_db's PostGIS `inventory.station` table.

## Technical Context

**Language/Version**: Rust 1.75+ (edition 2021) — per Constitution V

**Primary Dependencies**:
- tokio (async runtime)
- axum (HTTP framework)
- SeaORM (database queries, per Constitution V)
- serde / serde_json (serialization)
- tracing / tracing-subscriber (structured logging)
- thiserror (typed errors)

**Storage**: PostgreSQL 16 + PostGIS 3 — `platform_db.inventory.station` (read-only, GIS schema)

**Testing**: `cargo test` with:
- Unit tests for service and repository layers
- Integration tests for API endpoint contracts
- Test station fixtures in a dedicated test DB or Docker-based integration test setup

**Target Platform**: Linux (Docker container, `x86_64-unknown-linux-gnu`)

**Project Type**: Web service (REST API)

**Performance Goals**: `<200ms p95` for all endpoints (FR-010)

**Constraints**:
- Read-only API (no writes)
- All endpoints under `/api/v1/` prefix (FR-007)
- PostGIS `ST_DWithin` for nearby search (FR-004)
- `status = 'active'` filter for nearby search (FR-005)
- Connection pool default 10-20 (FR-008)
- 5-50 concurrent users
- No authentication (deferred to MVP-3)
- Async Rust only (tokio/axum)

**Scale/Scope**: 4 seeded stations (Sprint 0), 5-50 concurrent users, single Docker container

### Unknowns to Research

1. **Crate versions**: Exact SeaORM, axum, tokio versions to use (latest stable as of June 2026)
2. **Logging format**: tracing-subscriber configuration (JSON vs text, log level filtering)
3. **Configuration**: Environment variable loading pattern (dotenvy vs figment vs std::env)
4. **Integration test DB setup**: Approach for spinning up PostGIS in tests (testcontainers vs Docker compose dependency)
5. **API documentation**: OpenAPI generation approach (utoipa vs aide vs manual)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|---|---|---|
| I. Documentation-First | ✅ PASS | Spec exists, clarified, checklist validated |
| II. LLM-Driven Deterministic | ✅ PASS | Following plan workflow |
| III. MVP Isolation | ✅ PASS | MVP-1 only, no future features referenced |
| IV. Complete Testing | ✅ PASS | Unit, integration, E2E tests included in scope |
| V. Architecture Discipline (Backend) | ✅ PASS | Clean Architecture, PostGIS isolation, async Rust |
| VI. Architecture Discipline (Frontend) | ✅ N/A | Sprint 1 is backend-only |
| VII. Data Ownership | ✅ PASS | platform_db read-only, no cross-service access |
| VIII. Skill System Enforcement | ✅ PASS | Skills loaded and enforced |

**GATE RESULT: PASS** — No violations. No complexity justification needed.

## Project Structure

### Documentation (this feature)

```text
specs/002-backend-core-api/
├── spec.md              # Feature specification
├── plan.md              # This file — implementation plan
├── research.md          # Phase 0 — technology decisions
├── data-model.md        # Phase 1 — entities and validation
├── quickstart.md        # Phase 1 — setup and run guide
├── contracts/           # Phase 1 — API contracts
│   └── api.md
└── tasks.md             # Phase 2 — task breakdown (future)
```

### Source Code (repository root)

```text
source/services/driver-service/
├── Cargo.toml
├── Dockerfile
├── src/
│   ├── main.rs              # Entry point (axum server setup)
│   ├── config.rs            # Configuration (env vars)
│   ├── lib/
│   │   ├── mod.rs
│   │   ├── handlers/        # HTTP request/response handling
│   │   │   ├── mod.rs
│   │   │   ├── health.rs
│   │   │   ├── stations.rs
│   │   │   └── nearby.rs
│   │   ├── models/          # Domain models
│   │   │   ├── mod.rs
│   │   │   └── station.rs
│   │   ├── services/        # Business logic
│   │   │   ├── mod.rs
│   │   │   └── station_service.rs
│   │   └── repositories/    # Data access (PostGIS)
│   │       ├── mod.rs
│   │       └── station_repository.rs
│   └── error.rs             # Typed error types
└── tests/
    ├── integration/
    │   ├── mod.rs
    │   ├── health_test.rs
    │   ├── stations_test.rs
    │   └── nearby_test.rs
    └── unit/
        ├── mod.rs
        ├── service_test.rs
        └── repository_test.rs
```

**Structure Decision**: Standard Rust Clean Architecture under `source/services/driver-service/` with separate `lib/` module for handler/service/repository layers, integration tests under `tests/integration/`, and unit tests co-located or under `tests/unit/`.

## Complexity Tracking

> No Constitution Check violations — complexity tracking not required.
