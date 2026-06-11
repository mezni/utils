# Implementation Plan: Data Layer

**Branch**: `002-data-layer` | **Date**: 2026-06-10 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/002-data-layer/spec.md`

## Summary

Build a shared Rust library crate providing database connection management, PostGIS spatial queries (ST_DWithin), data models for the inventory schema, a file-based SQL migration runner, and an integration test suite against a containerized platform_db. This library will be consumed by the Driver Service (Sprint 1.2).

## Technical Context

**Language/Version**: Rust 1.80+

**Primary Dependencies**: SQLx (async PostgreSQL driver with PostGIS type support), tokio (async runtime), testcontainers (integration test database orchestration)

**Storage**: PostgreSQL 16 + PostGIS 3.4 (platform_db from Sprint 0)

**Testing**: cargo test (unit + integration), testcontainers for database test lifecycle management

**Target Platform**: Linux (Docker Compose for local dev, CI containers for automated testing)

**Project Type**: Library crate (shared data layer consumed by Driver Service and Admin Service binaries)

**Performance Goals**: Spatial query returns results within 200ms on seed dataset (SC-002), integration suite completes under 60s (SC-003)

**Constraints**: Typed errors only — no panics on connection failure (SC-004, FR-007); max 3 startup retries with exponential backoff (FR-011); connection pool min 2 / max 10 (assumptions)

**Scale/Scope**: Develop-time library — actual service scaling determined by Driver Service (Sprint 1.2)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

No constitution file with active gates exists. Default constraints apply:
- Follow Rust ecosystem conventions
- All tests must be runnable with a single `cargo test` command
- No hardcoded credentials or secrets in source code
- Library must not spawn HTTP servers or daemons — it is a pure data access library

## Project Structure

### Documentation (this feature)

```text
specs/002-data-layer/
├── plan.md              # This file
├── research.md          # Technology decisions and rationale
├── data-model.md        # Entity definitions and relationships
├── quickstart.md        # Setup and first-run guide
├── contracts/           # Interface contracts (library API surface)
└── tasks.md             # Task breakdown (created by /speckit.tasks)
```

### Source Code (repository root)

```text
source/services/
├── Cargo.workspace.toml          # Workspace root
├── libs/
│   └── borne-data/               # Shared data layer library crate
│       ├── Cargo.toml
│       ├── src/
│       │   ├── lib.rs            # Re-exports
│       │   ├── pool.rs           # Connection pool
│       │   ├── models/
│       │   │   ├── mod.rs
│       │   │   ├── partner.rs    # inventory.partner model
│       │   │   ├── station.rs    # inventory.station model
│       │   │   └── charger.rs    # inventory.charger model
│       │   ├── queries/
│       │   │   ├── mod.rs
│       │   │   ├── stations.rs   # Spatial + detail queries
│       │   │   └── partners.rs   # Partner queries
│       │   ├── migration/
│       │   │   ├── mod.rs
│       │   │   └── runner.rs     # SQL migration runner
│       │   └── error.rs          # Typed error types
│       ├── migrations/           # SQL migration files
│       │   ├── 001_initial.sql
│       │   └── README.md
│       └── tests/                # Integration tests
│           ├── common/
│           │   └── mod.rs        # Test helpers (container setup)
│           ├── queries_test.rs
│           └── migration_test.rs
├── driver-service/               # Sprint 1.2 (placeholder)
└── clickstream-service/          # Sprint 1.3 (placeholder)
```

**Structure Decision**: Workspace monorepo with a single shared library crate (`borne-data`) plus placeholder service directories. The library is versioned independently and consumed via workspace dependency. Integration tests use `testcontainers` to spin up a PostGIS instance, avoiding dependency on external Docker Compose during test runs.

## Complexity Tracking

No constitution violations — standard Rust library crate pattern.

