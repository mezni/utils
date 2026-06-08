# Implementation Plan: Admin Service

**Branch**: `004-admin-service` | **Date**: 2026-06-07 | **Spec**: specs/004-admin-service/spec.md

**Input**: Feature specification from `specs/004-admin-service/spec.md`

## Summary

Create a Rust service that exposes REST API endpoints for administrative operations: health check and CRUD operations for partners, stations, and chargers. The service connects to the PostgreSQL + PostGIS database, reads/writes to the inventory schema using existing data models, and provides a complete admin interface.

## Technical Context

**Language/Version**: Rust 1.70+, Actix-web 4.0
**Primary Dependencies**: actix-web, tokio, ev-core, ev-db, serde, serde_json, sqlx
**Storage**: PostgreSQL 16 + PostGIS 3.4 (from Sprint 1.1 Docker Compose)
**Testing**: Integration tests with postgres test container
**Target Platform**: Docker container, CI: GitHub Actions ubuntu-latest
**Project Type**: Web service API (admin-facing)
**Performance Goals**: Health check < 50ms, CRUD operations < 200ms, 15 CRUD endpoints
**Constraints**: Service must be containerized, connect to database via environment variable, secure CRUD operations
**Scale/Scope**: 2 endpoints (health + CRUD), 15 CRUD endpoints (5 per entity), integration tests, single service binary

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- **Principle IV (Domain Separation by Schema)**: Compliant. Admin service writes to `inventory` schema (partners, stations, chargers) and reads from `gis` schema (station_locations). No cross-schema writes except where explicitly permitted by constitution.
- **Principle II (Single Source of Truth)**: Compliant. `inventory.partner` remains authoritative source of truth for partners. Service reads from and writes to inventory schema.
- **Principle V (Build for Current Scale)**: Compliant. Single service, 15 CRUD endpoints, ~200ms response time for CRUD operations. No caching layer needed yet.
- **Migrations never edited after commit**: Acknowledged. SQL migrations (from Sprint 1.2) will be applied at startup.

**Gate status**: ✅ PASS — no violations. Complexity tracking not required.

## Project Structure

### Documentation (this feature)

```text
specs/004-admin-service/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output (API request/response schemas)
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (services/admin-service)

```text
services/admin-service/
├── Cargo.toml           # Rust project configuration
├── src/
│   ├── main.rs          # Entry point, migration application
│   ├── lib.rs           # Library entry point, public API
│   ├── config.rs        # Configuration struct (Postgres URL)
│   ├── db.rs            # Database connection pool
│   ├── routes.rs        # API routes (health, partner CRUD, station CRUD, charger CRUD)
│   ├── handlers.rs      # Request handlers
│   ├── error.rs         # Error types and responses
│   └── migrations.rs    # Migration application logic
├── tests/
│   ├── integration_test.rs
│   └── sql/
│       └── test_admin_crud.sql
├── Dockerfile           # Multi-stage Rust build
└── Dockerfile.dev       # Dev environment (with hot reload)
```

**Structure Decision**: Single service binary with separation of concerns (config, database, routes, handlers, errors). Integration tests in tests/ directory. Dockerfile for production deployment. Follows same structure as Driver Service for consistency.

## Complexity Tracking

> Not needed — Constitution Check passed without violations.
