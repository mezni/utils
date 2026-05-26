# Implementation Plan: Backend Core — Schema, Identity & CRUD

**Branch**: `006-backend-schema-crud` | **Date**: 2026-05-26 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/002-backend-schema-crud/spec.md`

## Summary

Implement the full backend data layer for BorneMap: PostgreSQL/PostGIS schema
migrations, CRUD repositories for five domain entities (users, partner
profiles, stations, chargers, connector types), authentication (registration +
login with JWT), seed data, and Actix-web route handlers — all mounted under
`/api/v1/*`. Builds on the scaffolded domain modules from Phase 0.

## Technical Context

**Language/Version**: Rust 1.78+ (edition 2021)

**Primary Dependencies**: Actix-web 4, SQLx 0.7 (runtime-tokio, postgres,
chrono), Tokio 1, serde 1, chrono 0.4, fastrand 2, jsonwebtoken 10.x, argon2 0.5

**Storage**: PostgreSQL 16+ with PostGIS 3.4 (containerized via
docker-compose.dev.yml). Spatial columns use `GEOGRAPHY(Point, 4326)`.

**Testing**: `cargo test` (unit + integration). SQLx compile-time query
verification against live PostGIS container.

**Target Platform**: Linux (Docker container), HTTP API on :8080

**Project Type**: Web service (single Rust binary, REST API)

**Performance Goals**: Single entity CRUD under 1s (SC-001); spatial queries
≤200ms under concurrent load (Constitution Principle I); DB setup + seed under
10s (SC-008)

**Constraints**: All endpoints behind `/api/v1/*` (Constitution I); semantic
IDs with `[PREFIX]-[12-char]` format (Constitution II); soft-delete on users,
partner_profiles, stations, connector_types (Constitution II); `is_test`
filtering on all queries (Constitution II); cursor-based pagination on all list
endpoints; optimistic locking on update; JWT expiry 24h

**Scale/Scope**: 5 domain entities, ~15 API endpoints, 1 seed migration,
1 auth module, PostGIS spatial indexing

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Pre-Research Assessment

| Principle | Gate | Assessment |
|-----------|------|------------|
| I. Modular Monorepo Architecture | All code under `sources/backend/`; `/api/v1/*` namespace; spatial queries ≤200ms | ✅ Backend binary already under `sources/backend/`. Health endpoint at `/api/v1/health`. All new routes will mount under `/api/v1/`. Spatial query performance verified via PostGIS indexing. |
| II. Semantic Identity & Data Isolation | `USR-`/`PRT-`/`STN-`/`CHG-`/`CNT-` prefixes; `is_test` filter; soft delete; multi-tenant owner_id injection | ✅ `id_generator.rs` already produces prefixed IDs. Migrations will add `deleted_at` columns for soft-delete entities, `is_test` columns for all entities, and `owner_id` on stations. Repository queries will include `WHERE deleted_at IS NULL` and `($4 = TRUE OR is_test = FALSE)`. Partner-scoped endpoints inject `owner_id`. |
| III. Administrative UX Discipline | Design tokens; ScrollableTable; destructive confirmation | ⬜ N/A — this phase builds the backend API only. No UI is produced. |
| IV. Mobile & Discovery Constraints | Expo Go; 20km radius; LIMIT 50 | ⬜ N/A — discovery/nearby search is deferred to Phase 2 (infrastructure domain). Pagination hard-cap of 50 will be enforced on all list endpoints as preparation. |
| V. Deterministic Implementation | Modular domain layers; seed script; sandbox indicator | ✅ Domain modules already scaffolded per entity. Seed migration `20260525000001_seed_sandbox.up.sql` will produce deterministic data. Seed records carry `is_test = true`. |

**Pre-research result**: PASS — no violations.

### Post-Design Re-Assessment

| Principle | Gate | Assessment |
|-----------|------|------------|
| I. Modular Monorepo Architecture | All code under `sources/backend/`; `/api/v1/*` namespace; spatial queries ≤200ms | ✅ All new code resides under `sources/backend/src/domain/` and `sources/backend/src/auth/`. Every endpoint is prefixed `/api/v1/`. Station coordinates use `GEOGRAPHY(Point, 4326)` with GIST index for ≤200ms spatial queries. |
| II. Semantic Identity & Data Isolation | Prefixes; `is_test` filter; soft delete; multi-tenant | ✅ All five entities use their designated prefixes. Soft-delete (`deleted_at`) on users, partner_profiles, stations, connector_types. `is_test` column on all entities with `WHERE ($include_test = TRUE OR is_test = FALSE)` pattern. Partner-scoped station listing injects `owner_id` from JWT `sub` claim. Chargers are permanently deleted per FR-008. |
| III. Administrative UX Discipline | Design tokens; ScrollableTable; destructive confirmation | ⬜ N/A — backend API only. |
| IV. Mobile & Discovery Constraints | LIMIT 50; test record exclusion | ✅ All list endpoints enforce `limit` capped at 50 (default) / 100 (max). Test records excluded by default. |
| V. Deterministic Implementation | Modular domain layers; seed script | ✅ Domain layers follow per-entity module pattern (`models.rs`, `repository.rs`, `handlers.rs`). Seed migration uses hardcoded IDs and data — deterministic across runs. |

**Post-design result**: PASS — no violations. All applicable gates confirmed satisfied after design phase.

**Result**: PASS — no violations. All applicable gates are satisfied by the
planned design. Principles III and IV are deferred to phases that produce UI
and discovery features.

## Project Structure

### Documentation (this feature)

```text
specs/002-backend-schema-crud/
├── plan.md              # This file
├── research.md          # Phase 0 output — technology decisions
├── data-model.md        # Phase 1 output — entity schema & relationships
├── quickstart.md        # Phase 1 output — local dev & API usage guide
├── contracts/           # Phase 1 output — REST API contracts
│   ├── users.md
│   ├── partners.md
│   ├── stations.md
│   ├── chargers.md
│   ├── connector-types.md
│   └── auth.md
└── tasks.md             # Phase 2 output (/speckit.tasks command)
```

### Source Code (repository root)

```text
sources/backend/
├── Cargo.toml
├── Dockerfile.dev
├── migrations/
│   ├── 20260526000001_create_enums.up.sql
│   ├── 20260526000001_create_enums.down.sql
│   ├── 20260526000002_create_users.up.sql
│   ├── 20260526000002_create_users.down.sql
│   ├── 20260526000003_create_partner_profiles.up.sql
│   ├── 20260526000003_create_partner_profiles.down.sql
│   ├── 20260526000004_create_connector_types.up.sql
│   ├── 20260526000004_create_connector_types.down.sql
│   ├── 20260526000005_create_stations.up.sql
│   ├── 20260526000005_create_stations.down.sql
│   ├── 20260526000006_create_chargers.up.sql
│   ├── 20260526000006_create_chargers.down.sql
│   ├── 20260527000001_seed_sandbox.up.sql
│   └── 20260527000001_seed_sandbox.down.sql
├── src/
│   ├── main.rs
│   ├── domain/
│   │   ├── mod.rs
│   │   ├── users/
│   │   │   ├── mod.rs
│   │   │   ├── models.rs
│   │   │   ├── repository.rs
│   │   │   └── handlers.rs
│   │   ├── partners/
│   │   │   ├── mod.rs
│   │   │   ├── models.rs
│   │   │   ├── repository.rs
│   │   │   └── handlers.rs
│   │   ├── stations/
│   │   │   ├── mod.rs
│   │   │   ├── models.rs
│   │   │   ├── repository.rs
│   │   │   └── handlers.rs
│   │   ├── chargers/
│   │   │   ├── mod.rs
│   │   │   ├── models.rs
│   │   │   ├── repository.rs
│   │   │   └── handlers.rs
│   │   ├── connector_types/
│   │   │   ├── mod.rs
│   │   │   ├── models.rs
│   │   │   ├── repository.rs
│   │   │   └── handlers.rs
│   │   └── infrastructure/
│   │       ├── mod.rs
│   │       ├── mod_impl.rs
│   │       └── repository.rs
│   ├── auth/
│   │   ├── mod.rs
│   │   ├── jwt.rs
│   │   ├── middleware.rs
│   │   └── handlers.rs
│   └── utils/
│       ├── mod.rs
│       └── id_generator.rs
└── sqlx-data.json
```

**Structure Decision**: Existing monorepo layout under `sources/backend/`. Each
domain module gains a `handlers.rs` file for Actix-web route handlers. A new
`auth/` module handles JWT generation, middleware, and login/registration
endpoints. Migrations directory receives six schema migrations and one seed
migration.

## Complexity Tracking

No constitutional violations identified. Complexity tracking is not required for
this phase.
