# Implementation Plan: EV Dashboard Platform Kernel

**Branch**: `001-ev-dashboard` | **Date**: 2026-06-23 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/001-ev-dashboard/spec.md`

## Summary

Build a full-stack EV infrastructure dashboard with CRUD operations for Partners (operators), Stations (charging locations), and Chargers (charging units). The system enforces Clean Architecture with strict layering, external ID model (PRT/STA/CHR), and standardized API contracts under `/api/v1`. Dashboard provides KPIs showing total counts. All data stored in PostgreSQL 'ev' schema with cascading deletes enforced.

## Technical Context

**Language/Version**: Rust 1.75+ | TypeScript 5+

**Primary Dependencies**:
- Backend: Actix-Web, SQLx, Tokio, serde, chrono
- Frontend: React 18+, TypeScript, React Query, React Router, TailwindCSS, Vite
- Shared: platform-core (error/result/config/ID utilities), platform-db (SQLx pool/repositories)

**Storage**: PostgreSQL 16+ with 'ev' schema namespace

**Testing**: Rust (cargo test), PostgreSQL (SQLx tests), React (vitest/cypress)

**Target Platform**: Linux server (backend), Web browser (frontend desktop/tablet)

**Project Type**: Full-stack web application with clean architecture separation

**Performance Goals**: 95% of requests <500ms for <1000 records, dashboard loads <2s, supports 1000+ partners and 10,000+ stations

**Constraints**: <200ms p95 for dashboard KPIs, external IDs only (PRT/STA/CHR), cascading deletes, domain purity enforced

**Scale/Scope**: Dashboard + Partners/Stations/CRUD + pagination + KPIs, no auth/RBAC/billing/IoT/event streaming

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### I. Clean Architecture ✅
- [x] Strict layering: presentation → application → domain → infrastructure
- [x] Domain is pure (zero frameworks, no IO, no HTTP)
- [x] Application layer orchestrates use-cases, no DB access
- [x] Infrastructure contains ALL SQLx operations, no business logic
- [x] Presentation contains ONLY HTTP handlers and response mapping

### II. External Identity Model ✅
- [x] Only external IDs used (PRT-<12-char> for partners, STA-<12-char> for stations, CHR-<12-char> for chargers)
- [x] NO UUIDs anywhere
- [x] `id` is ONLY public identifier in APIs
- [x] IDs are immutable and globally unique
- [x] All relationships use `id` as foreign keys
- [x] Cascading deletes: Partners → Stations → Chargers

### III. API Contract Compliance ✅
- [x] Standardized success/error format
- [x] All endpoints use `/api/v1` versioning
- [x] No raw framework responses (Actix responses, HTTP status codes not as API contract)
- [x] No untyped errors
- [x] Only `id` exposed externally

### IV. Domain Purity ✅
- [x] Domain contains ONLY business rules and invariants
- [x] No database operations in domain
- [x] No HTTP handling in domain
- [x] No external IO in domain
- [x] No framework usage in domain
- [x] Infrastructure handles SQLx and repository implementations, no business logic

### V. Test-Driven Development ✅
- [x] Unit tests for domain logic (mandatory)
- [x] Integration tests for API endpoints (mandatory)
- [x] Repository tests for database operations (mandatory)
- [x] Backend testing requirements defined
- [x] Frontend testing requirements defined (component, API mock, React Query)

**Status**: ✅ ALL CONSTITUTION GATES PASSED - No violations requiring justification

## Project Structure

### Documentation (this feature)

```text
specs/001-ev-dashboard/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   ├── api.yaml         # OpenAPI specification
│   └── dashboard.yaml   # KPIs endpoint contract
├── tasks.md             # Phase 2 output (/speckit.tasks)
└── checklists/
    └── requirements.md  # Validation checklist
```

### Source Code (repository root)

```text
# Web application structure

services/admin-service/
├── src/
│   ├── presentation/    # HTTP handlers, routing, validation, response mapping
│   ├── application/     # use-cases, orchestrations, command/query dispatch
│   ├── domain/          # business logic, entities, invariants
│   ├── infrastructure/  # SQLx repositories, database operations
│   ├── config/          # configuration management
│   ├── db/              # database pool, connection management
│   ├── middleware/      # request/response middleware
│   └── common/          # shared utilities, error types, result types
├── migrations/          # SQLx database migrations
├── Cargo.toml
└── .env

apps/admin-dashboard/
├── src/
│   ├── pages/           # routing layer only (Dashboard, Partners, Stations, Chargers)
│   ├── features/        # business UI logic (partner management, station management)
│   ├── components/      # pure UI primitives (forms, tables, cards)
│   ├── api/             # transport layer (apiClient)
│   ├── hooks/           # custom React hooks (usePartners, useStations)
│   ├── types/           # TypeScript types (Partner, Station, Charger)
│   └── utils/           # utilities (formatters, validators)
├── package.json
├── tailwind.config.js
└── vite.config.ts

crates/
├── platform-core/       # error system, result types, config utilities, ID utilities (nanoid)
└── platform-db/         # SQLx pool, migrations, repository implementations

infrastructure/
├── docker/
│   ├── postgres/
│   │   ├── init.sql
│   │   └── Dockerfile
│   ├── admin-service/
│   │   └── Dockerfile
│   └── admin-dashboard/
│       └── Dockerfile
├── postgres/
│   ├── data/            # volume mount
│   └── logs/
├── observability/       # optional future: prometheus, grafana
└── network/
    └── nginx.conf       # optional reverse proxy

docs/
├── core/
│   ├── constitution.md
│   ├── architecture.md
│   └── api-standards.md
└── epics/
    └── E001-dashboard-core/
```

**Structure Decision**: Selected Option 2 (Web application structure). This provides clear separation between backend (Rust/Actix-Web) and frontend (React/TypeScript) while maintaining Clean Architecture principles. Each service runs independently in Docker, enabling parallel development and testing.

## Complexity Tracking

> **No constitution violations requiring justification**

## Phase 0: Research

### Research Tasks

**Backend Technologies**:
- **nanoid**: For generating unique external IDs (PRT/STA/CHR formats). Decision: Use rust-nanoid crate for deterministic nanoid generation.

**Database**: **SQLx migration strategy**: Forward-only migrations with timestamp ordering. Decision: Use SQLx migrations with migrations/ directory, forward-only execution, no rollback dependencies.

**API Design**: **Pagination implementation**: Offset-based pagination for list endpoints. Decision: Use query parameters (page, limit, offset) with default limit of 50 items per page.

**Frontend Architecture**: **State management**: React Query for server state, React Router for routing. Decision: Use React Query for data fetching and caching, React Router v6 for routing.

**External ID Generation**: **nanoid deterministic generation**: Need consistent ID generation across service instances. Decision: Use nanoid with seed to ensure consistent IDs from generated strings.

### Research Findings Summary

Will be documented in `research.md`

## Phase 1: Design & Contracts

### Design Artifacts to Generate

1. **data-model.md**: Entity definitions with fields, relationships, validation rules
2. **contracts/api.yaml**: OpenAPI specification for all API endpoints
3. **contracts/dashboard.yaml**: KPIs endpoint contract
4. **quickstart.md**: Setup and run guide for developers
5. **updated AGENTS.md**: Agent context with plan reference

## Next Steps

1. Generate `research.md` resolving all NEEDS CLARIFICATION items
2. Generate `data-model.md` with entity definitions
3. Generate API contracts in `/contracts/` directory
4. Generate `quickstart.md` setup guide
5. Update AGENTS.md with plan reference
6. Run `/speckit.tasks` to generate implementation tasks
