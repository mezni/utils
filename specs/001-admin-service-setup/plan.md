# Implementation Plan: Admin Service Setup (Sprint 1.1)

**Branch**: `001-admin-service-setup` | **Date**: 2026-06-19 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/001-admin-service-setup/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/plan-template.md` for the execution workflow.

## Summary

Build the admin-service backend (:3002) with Rust/Actix/SQLx and a dashboard frontend shell with React/shadcn/ui. The sprint covers the inventory schema (partners, stations, chargers + 5 lookup tables), an OpenAPI-first contract for 6 endpoints (/health, /partners, /stations, /chargers with CRUD), and a speckit-lint CI validator for architecture enforcement. All CRUD uses soft deletion with PATCH semantics for updates.

## Technical Context

**Language/Version**: Rust 1.70+ (stable), TypeScript 5.x (strict mode)

**Primary Dependencies**:
- Backend: Actix-web 4, SQLx 0.7+ (compile-time), tokio, serde, nanoid, chrono, postgis
- Frontend: React 18+, shadcn/ui, Tailwind CSS, generated OpenAPI client
- CI: speckit-lint (Rust CLI, custom build), clap for CLI args

**Storage**: PostgreSQL 16 + PostGIS extension (platform_db, inventory schema only)

**Testing**: cargo test (unit + integration), Playwright (E2E), speckit-lint (CI validation)

**Target Platform**: Linux Docker container (x86_64), multi-stage Rust build

**Project Type**: web-service (backend) + web-application (frontend dashboard shell)

**Performance Goals**:
- API response < 200ms p95 for CRUD operations (no spatial queries in this sprint)
- Health check < 50ms
- Migration execution < 30s

**Constraints**:
- Single inventory schema only — no gis, users, or analytics access
- No external service calls from admin-service
- SQLx compile-time queries only — zero raw SQL strings
- No auth/authorization (deferred to Auth Service sprint)
- Entity IDs must be OPR/STA/CHG-nanoid(12) with DB CHECK constraints
- Soft deletion with deleted_at timestamp (no restore endpoint)
- PATCH semantics for all update operations

**Scale/Scope**: Single admin-service instance, single PostgreSQL instance, single dashboard deployment. Validation-phase scale (< 1000 partners).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Principle I — OpenAPI-First Architecture ✅
- api/openapi/admin.yaml is being defined before any backend/frontend code
- OpenAPI spec is the single source of truth
- No routes implemented before contract finalized

### Principle II — Deterministic Sprint Lifecycle ✅
- All 6 phases followed: Ingestion → Contract → Design → Implementation → Review → Testing
- No speculative features or cross-sprint logic
- Single vertical slice (admin-service only)

### Principle III — Contract-Driven Development ✅
- OpenAPI contract design is Phase 2 (complete before code generation)
- Schema generation follows contract definitions
- nanoid utility used for ID generation (no manual assignment)

### Principle IV — Identity System Enforcement ✅
- OPR/STA/CHG prefixes with nanoid(12) format
- Database CHECK constraints enforced
- No hardcoded IDs

### Principle V — Architecture Isolation ✅
- Single service: admin-service (:3002)
- inventory schema only (no cross-schema access)
- No external integrations
- Dashboard is UI shell only

### Gate Check Result: ✅ ALL PRINCIPLES PASS — No violations to justify.

## Project Structure

### Documentation (this feature)

```text
specs/001-admin-service-setup/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
# Option 2: Web application (backend + frontend)
api/
├── openapi/
│   └── admin.yaml                        # OpenAPI contract (source of truth)

services/
├── admin-service/                         # Backend Rust service
│   ├── src/
│   │   ├── main.rs
│   │   ├── config.rs                     # Environment configuration
│   │   ├── routes/
│   │   │   ├── mod.rs
│   │   │   ├── health.rs                 # GET /health
│   │   │   ├── partners.rs               # CRUD /partners
│   │   │   ├── stations.rs               # CRUD /stations
│   │   │   └── chargers.rs               # CRUD /chargers
│   │   ├── models/
│   │   │   ├── mod.rs
│   │   │   ├── partner.rs
│   │   │   ├── station.rs
│   │   │   └── charger.rs
│   │   ├── db/
│   │   │   ├── mod.rs
│   │   │   ├── partners.rs               # SQLx queries
│   │   │   ├── stations.rs
│   │   │   └── chargers.rs
│   │   └── error.rs                      # Error types
│   ├── migrations/
│   │   ├── 001_inventory_schema.sql      # inventory schema init
│   │   ├── 002_lookup_tables.sql         # ENUM lookup tables
│   │   ├── 003_partners.sql
│   │   ├── 004_stations.sql
│   │   ├── 005_chargers.sql
│   │   └── 006_seed_data.sql             # Lookup table seed values
│   ├── Cargo.toml
│   └── Dockerfile

apps/
├── dashboard/                             # Frontend React app
│   ├── src/
│   │   ├── App.tsx
│   │   ├── main.tsx
│   │   ├── components/
│   │   │   ├── ui/                       # shadcn/ui components
│   │   │   ├── partners/
│   │   │   ├── stations/
│   │   │   └── chargers/
│   │   ├── pages/
│   │   │   ├── Partners.tsx
│   │   │   ├── Stations.tsx
│   │   │   └── Chargers.tsx
│   │   └── lib/
│   │       └── api-client.ts             # Generated OpenAPI client
│   ├── package.json
│   ├── tailwind.config.ts
│   └── Dockerfile

speckit/
├── speckit-lint/                          # CI validator (Rust CLI)
│   ├── src/
│   │   ├── main.rs
│   │   ├── rules/
│   │   │   ├── service_topology.rs
│   │   │   ├── schema_isolation.rs
│   │   │   ├── naming.rs
│   │   │   ├── openapi_first.rs
│   │   │   ├── sqlx_safety.rs
│   │   │   ├── frontend_boundary.rs
│   │   │   └── migration_validation.rs
│   └── Cargo.toml

infrastructure/
├── docker/
│   ├── docker-compose.dev.yml
│   └── env/
│       └── .env.dev
├── postgres/
│   └── init/
│       └── 01-platform.sql               # CREATE EXTENSION postgis, CREATE SCHEMA inventory
└── traefik/
    ├── traefik.yml
    └── dynamic/
        ├── routers.yml
        └── middlewares.yml
```

**Structure Decision**: Web application with separated backend (services/admin-service), frontend (apps/dashboard), and shared infrastructure (docker, postgres, traefik). The speckit-lint validator lives in its own directory under speckit/. The OpenAPI contract is the source of truth at api/openapi/.

## Complexity Tracking

> No constitution violations detected. All principles are satisfied by the selected architecture. Complexity tracking is not required.

## Architecture Map

### Service Topology
```
Traefik Gateway (:80/:443)
    │
    ├── /api/v1/health  → admin-service (:3002)
    ├── /api/v1/partners → admin-service (:3002)
    ├── /api/v1/stations → admin-service (:3002)
    └── /api/v1/chargers → admin-service (:3002)
```

### Data Flow
```
Dashboard (React/shadcn/ui)
    │
    ├── OpenAPI-generated client
    │   └── HTTP PATCH/GET/POST/DELETE via Traefik
    │
    ↓
admin-service (:3002)
    │
    ├── Validate input (serde)
    ├── Generate nanoid(12) IDs
    ├── SQLx compile-time query
    │
    ↓
platform_db.inventory
    ├── partners (OPR-*)
    ├── stations (STA-*, GEOGRAPHY)
    ├── chargers (CHG-*)
    ├── access_types (seed)
    ├── connector_types (seed)
    ├── current_types (seed)
    ├── connector_statuses (seed)
    └── data_sources (seed)
```

### API ↔ DB Mapping
```
Partner CRUD:
  POST   /partners       → INSERT inventory.partners
  GET    /partners       → SELECT * FROM inventory.partners WHERE deleted_at IS NULL
  GET    /partners/{id}  → SELECT * FROM inventory.partners WHERE id=$1 AND deleted_at IS NULL
  PATCH  /partners/{id}  → UPDATE inventory.partners SET ... WHERE id=$1
  DELETE /partners/{id}  → UPDATE inventory.partners SET deleted_at=NOW() WHERE id=$1

Station CRUD:
  POST   /stations       → INSERT inventory.stations
  GET    /stations       → SELECT * FROM inventory.stations WHERE deleted_at IS NULL
  GET    /stations/{id}  → SELECT * FROM inventory.stations WHERE id=$1 AND deleted_at IS NULL
  PATCH  /stations/{id}  → UPDATE inventory.stations SET ... WHERE id=$1
  DELETE /stations/{id}  → UPDATE inventory.stations SET deleted_at=NOW() WHERE id=$1

Charger CRUD:
  POST   /chargers       → INSERT inventory.chargers
  GET    /chargers       → SELECT * FROM inventory.chargers WHERE deleted_at IS NULL
  GET    /chargers/{id}  → SELECT * FROM inventory.chargers WHERE id=$1 AND deleted_at IS NULL
  PATCH  /chargers/{id}  → UPDATE inventory.chargers SET ... WHERE id=$1
  DELETE /chargers/{id}  → UPDATE inventory.chargers SET deleted_at=NOW() WHERE id=$1

Health:
  GET    /health         → Return JSON {"status":"healthy","service":"admin-service","version":"1.0.0"}
```
