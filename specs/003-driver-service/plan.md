# Implementation Plan: Driver Service

**Branch**: `003-driver-service` | **Date**: 2026-06-11 | **Spec**: [`spec.md`](./spec.md)

**Input**: Feature specification from `specs/003-driver-service/spec.md`

## Summary

Build the Driver Service — an Actix-web REST API exposing station discovery endpoints (list, nearby, detail) backed by `borne-data` (Sprint 1.1) and PostGIS. Consistent JSON response envelopes, health check, max 100 result limit, field-level validation errors.

## Technical Context

**Language/Version**: Rust 1.96 (edition 2021)

**Primary Dependencies**: Actix-web 4, serde, tokio, tracing, `borne-data` (workspace crate)

**Storage**: PostgreSQL 16 + PostGIS 3.4 (via `borne-data` connection pool)

**Testing**: `cargo test` (integration tests via testcontainers + unit tests via mock pool)

**Target Platform**: Linux Docker container (x86_64), port 8080

**Project Type**: Web service (REST API, read-only for MVP-1)

**Performance Goals**: Nearby query <200ms server-side (SC-002), service ready in <10s (SC-003), 100 concurrent requests without degradation (SC-005)

**Constraints**: No auth (MVP-3), no admin endpoints, /api/v1/ prefix mandatory, port 8080, JSON only, consistent envelope format

**Scale/Scope**: Up to 1000 stations, 100 req/s burst, single-instance deployment

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Gate | Status | Notes |
|------|--------|-------|
| All runtime code under `/source` | ✅ Pass | Service lives at `source/services/driver-service/` |
| /api/v1/* mandatory | ✅ Pass | All endpoints under `/api/v1/` |
| No API gateway | ✅ Pass | Direct service access |
| Database via sqlx | ✅ Pass | Uses `borne-data` (sqlx-based) |
| Tracing for logging | ✅ Pass | FR-011: structured logging |
| No service overlap | ✅ Pass | Driver service = discovery only; no admin/auth logic |
| No cross-MVP features | ✅ Pass | Within MVP-1 scope per `docs/mvp/mvp-1-discovery.md` |

No violations found. Complexity tracking not required.

## Project Structure

### Documentation (this feature)

```text
specs/003-driver-service/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   └── rest-api.md      # HTTP API contract
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code (repository root)

```text
source/services/
├── Cargo.toml              # Workspace root (libs/borne-data + driver-service)
├── rust-toolchain.toml
├── libs/
│   └── borne-data/         # Sprint 1.1 — consumed as workspace dependency
└── driver-service/
    ├── Cargo.toml
    └── src/
        ├── main.rs
        ├── api/
        │   └── v1/
        │       ├── mod.rs
        │       ├── stations.rs
        │       ├── nearby.rs
        │       ├── station_detail.rs
        │       └── health.rs
        ├── handlers/
        │   ├── station_handler.rs
        │   ├── nearby_handler.rs
        │   └── health_handler.rs
        ├── dto/
        │   ├── station_response.rs
        │   ├── station_detail_response.rs
        │   ├── nearby_query.rs
        │   ├── error_response.rs
        │   └── health_response.rs
        ├── config/
        │   └── settings.rs
        ├── errors/
        │   └── app_error.rs
        └── telemetry/
            └── middleware.rs
```

**Structure Decision**: Single Actix-web binary service consuming `borne-data` as a workspace dependency. Follows controller → handler → service pattern per `docs/backend/services.md`. No separate repository layer — `borne-data` already provides query functions (find_nearby, find_by_id, list_all).

## Complexity Tracking

> No constitution violations — complexity tracking not required.
