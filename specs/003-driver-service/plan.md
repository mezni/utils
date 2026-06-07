# Implementation Plan: Driver Service

**Branch**: `003-driver-service` | **Date**: 2026-06-07 | **Spec**: specs/003-driver-service/spec.md

**Input**: Feature specification from `specs/003-driver-service/spec.md`

## Summary

Create a Rust service that exposes REST API endpoints for the Driver application: health check and stations nearby. The service connects to the PostgreSQL + PostGIS database, queries the inventory schema using spatial queries, and returns station data including distance from the requested location.

## Technical Context

**Language/Version**: Rust 1.70+, Actix-web 4.0
**Primary Dependencies**: actix-web, tokio, ev-core, ev-db, serde, serde_json
**Storage**: PostgreSQL 16 + PostGIS 3.4 (from Sprint 1.1 Docker Compose)
**Testing**: Integration tests with postgres test container
**Target Platform**: Docker container, CI: GitHub Actions ubuntu-latest
**Project Type**: Web service API (driver-facing)
**Performance Goals**: Health check < 50ms, nearby query < 200ms with 15 stations
**Constraints**: Service must be containerized, connect to database via environment variable
**Scale/Scope**: 2 endpoints (health + nearby), integration tests, single service binary

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- **Principle IV (Domain Separation by Schema)**: Compliant. Driver service queries `inventory.station` via SQL query using `ST_DWithin`. No direct domain crossing.
- **Principle II (Single Source of Truth)**: Compliant. inventory.station remains authoritative. Service only reads data.
- **Principle V (Build for Current Scale)**: Compliant. Single service, 2 endpoints, ~200ms response time for nearby queries. No caching layer needed yet.
- **Migrations never edited after commit**: Acknowledged. SQL migrations (from Sprint 1.2) will be applied at startup.

**Gate status**: ✅ PASS — no violations. Complexity tracking not required.

## Project Structure

### Documentation (this feature)

```text
specs/003-driver-service/
├── plan.md              # This file
├── spec.md              # Feature specification
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output (API request/response schemas)
├── quickstart.md        # Phase 1 output
└── contracts/           # Phase 1 output
```

### Source Code (services/driver-service)

```text
services/driver-service/
├── Cargo.toml           # Rust project configuration
├── src/
│   ├── main.rs          # Entry point, migration application
│   ├── lib.rs           # Library entry point, public API
│   ├── config.rs        # Configuration struct (Postgres URL)
│   ├── db.rs            # Database connection pool
│   ├── routes.rs        # API routes (health, nearby)
│   ├── handlers.rs      # Request handlers
│   ├── error.rs         # Error types and responses
│   └── migrations.rs    # Migration application logic
├── tests/
│   ├── integration_test.rs
│   └── sql/
│       └── test_stations_nearby.sql
├── Dockerfile           # Multi-stage Rust build
└── Dockerfile.dev       # Dev environment (with hot reload)
```

**Structure Decision**: Single service binary with separation of concerns (config, database, routes, handlers, errors). Integration tests in tests/ directory. Dockerfile for production deployment.

## Complexity Tracking

> Not needed — Constitution Check passed without violations.
