# Implementation Plan: Driver Service

**Branch**: `009-driver-service` | **Date**: 2026-06-09 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/009-driver-service/spec.md`

## Summary

Build the Driver Service — an Actix-web REST API (port 8080) that exposes station discovery endpoints (nearby, markers, search, detail) with partner visibility enforcement. Uses the existing ev-db (PgPool, Paginated) and ev-core (enums, NanoID) crates. Lives under `source/apps/driver-service/` as a workspace member.

## Technical Context

**Language/Version**: Rust 2024 edition (1.95.0)

**Primary Dependencies**: Actix-web (HTTP framework), sqlx 0.8 (PostgreSQL), serde/serde_json (serialization), ev-core (shared enums, NanoID), ev-db (PgPool init), tokio (async runtime), thiserror (error types)

**Storage**: PostgreSQL 17 via sqlx, `"ev-platform"` schema, spatial queries via PostGIS ST_DWithin

**Testing**: cargo test with `sqlx::test` for integration tests against a test database

**Target Platform**: Linux x86_64, Docker container (Debian-based)

**Project Type**: Web service (REST API)

**Performance Goals**: <200ms p95 for detail endpoint, <500ms p95 for nearby with 100km radius, <100 concurrent connections

**Constraints**: Must run on port 8080 internally, all endpoints under `/api` prefix, no authentication in this sprint

**Scale/Scope**: Single binary, 5 read-only endpoints + health check, ~50 stations in dev, scalable to 10k stations

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Constitution (`docs/constitution.md`) references:
- **API Prefix Rule (Section 7)**: All endpoints under `/api` — ✓ satisfied
- **Service Responsibilities**: Driver Service reads from `inventory` and `gis` — ✓ satisfied (reads from `"ev-platform"` schema)
- **No auth in MVP-2**: Auth deferred to MVP-3 (Keycloak + JWT) — ✓ satisfied
- **Workspace member**: Must be added to `source/Cargo.toml` workspace members — ✓ tracked in tasks
- **No additional services without ADR**: Service already defined in constitution — ✓ satisfied

**No gate violations.**

## Project Structure

### Documentation (this feature)

```text
specs/009-driver-service/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output (API endpoint contracts)
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code

```text
source/
├── apps/
│   └── driver-service/
│       ├── Cargo.toml
│       └── src/
│           ├── main.rs            # Entry point, server bootstrap
│           ├── config.rs          # Env-based configuration
│           ├── error.rs           # AppError type
│           ├── routes/
│           │   ├── mod.rs         # Route registration
│           │   ├── health.rs      # GET /api/health
│           │   ├── nearby.rs      # GET /api/stations/nearby
│           │   ├── markers.rs     # GET /api/stations/markers
│           │   ├── search.rs      # GET /api/stations/search
│           │   ├── detail.rs      # GET /api/stations/:id
│           │   └── reviews.rs     # GET /api/stations/:id/reviews (stub)
│           ├── models/
│           │   ├── mod.rs
│           │   ├── station.rs     # Station, Charger response types
│           │   └── error.rs       # API error response types
│           └── db/
│               ├── mod.rs
│               ├── nearby.rs      # ST_DWithin query
│               ├── markers.rs     # Bbox query
│               ├── search.rs      # Text + connector filter query
│               └── detail.rs      # Station + chargers query
├── crates/
│   ├── ev-core/      # Existing shared crate
│   └── ev-db/        # Existing shared crate
└── Cargo.toml        # Workspace root (add driver-service to members)
```

**Structure Decision**: Single binary under `source/apps/driver-service/`, consistent with existing app layout. Direct workspace member, no intermediary packages.

## Complexity Tracking

No constitution violations to justify.
