# Implementation Plan: Admin Service

**Branch**: `010-admin-service` | **Date**: 2026-06-09 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/010-admin-service/spec.md`

## Summary

Admin REST service (port 8081) for partners, stations, and chargers CRUD, plus availability updates. Uses Actix-web 4, sqlx with PostgreSQL/PostGIS, and shared ev-core/ev-db crates. Dev `X-Partner-Id` header simulates scope testing until MVP-3 authentication.

## Technical Context

**Language/Version**: Rust 1.85 (edition 2024, workspace)

**Primary Dependencies**: actix-web 4, sqlx 0.8 (postgres + runtime-tokio + macros), serde, thiserror, tokio, log, env_logger, ev-core (workspace), ev-db (workspace)

**Storage**: PostgreSQL 17 + PostGIS 3.5 via sqlx

**Testing**: `cargo test` (unit + integration), sqlx test fixtures against live PostgreSQL

**Target Platform**: Linux (Docker multi-stage: rust:1.85-slim-bookworm → debian:bookworm-slim)

**Project Type**: Web service (Rust binary in workspace)

**Performance Goals**: Admin CRUD — no specific performance targets (low-traffic internal tooling)

**Constraints**: Must match existing Driver Service patterns (error handling, config, module layout)

**Scale/Scope**: Internal admin tooling — single-user or small team in MVP-2

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

**Constitution Status**: Template only — not ratified. No binding governance gates.

| Check | Status | Notes |
|-------|--------|-------|
| Documented governance framework? | ⚠️ Template | Constitution still contains placeholder text |
| Architecture constraints apply? | ❌ No | No ratified constraints |
| Design freedom? | ✅ Yes | Follow existing Driver Service patterns |

**Decision**: Proceed with no constitution gates. Follow established project patterns.

## Project Structure

### Documentation (this feature)

```text
specs/010-admin-service/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   └── api.md           # API contracts
└── tasks.md             # Phase 2 output (created by /speckit.tasks)
```

### Source Code (repository root)

```text
source/
├── Cargo.toml           # workspace root
├── crates/
│   ├── ev-core/         # enums + id generation
│   └── ev-db/           # PgPool + pagination
└── services/
    ├── driver-service/  # existing (port 8080)
    └── admin-service/   # NEW (port 8081)
        ├── Cargo.toml
        ├── Dockerfile
        └── src/
            ├── main.rs
            ├── config.rs
            ├── error.rs
            ├── models/
            │   └── mod.rs
            ├── db/
            │   ├── mod.rs
            │   ├── partners.rs
            │   ├── stations.rs
            │   ├── chargers.rs
            │   └── availability.rs
            └── routes/
                ├── mod.rs
                ├── health.rs
                ├── partners.rs
                ├── stations.rs
                ├── chargers.rs
                └── availability.rs
```

**Structure Decision**: Mirror Driver Service exactly — one file per endpoint group for routes and one per query concern for db.

## Complexity Tracking

> No Constitution violations to justify. Skip.
