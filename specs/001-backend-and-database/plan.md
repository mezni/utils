# Implementation Plan: API Versioning

**Branch**: `001-backend-and-database` | **Date**: 2026-06-08 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-backend-and-database/spec.md`

**Note**: This plan outlines the design and implementation approach for adding versioning to all BorneMap API endpoints.

## Summary

Add URL-based versioning to all API endpoints (starting with v1 in Sprint 1.1) to enable long-term API evolution without breaking client integrations. All endpoints will be served under `/api/v<number>/<resource>` paths. The health endpoint will be versioned. Unversioned paths will return 404. Versions will be immutable once released; new versions introduced in MVP-2 will follow the same pattern.

## Technical Context

**Language/Version**: Python 3.11+ (MVP-1); Rust 1.75+ (MVP-2+)

**Primary Dependencies**: FastAPI (MVP-1), Actix-web (MVP-2+), SQLAlchemy (MVP-1), sqlx (MVP-2+)

**Storage**: PostgreSQL 15+ (single database across all MVPs)

**Testing**: pytest (MVP-1), cargo test (MVP-2+)

**Target Platform**: Linux server (x86_64), Docker-compatible

**Project Type**: Web service (FastAPI/Rust microservices)

**Performance Goals**: <200ms p95 latency per endpoint; support 1000 req/s per service

**Constraints**: API prefix always `/api`; versioning always in URL path; zero breaking changes to v1 during MVP-1 and MVP-2 transition

**Scale/Scope**: 16 endpoints in Sprint 1.1; 3+ services by MVP-2; 12-month API version support window

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

**Principle 1 — MVP-first Delivery**: ✅ PASS. API versioning is minimal and proven. v1 launched with Sprint 1.1. No infrastructure added beyond routing logic.

**Principle 2 — Layered Complexity**: ✅ PASS. v1 endpoints remain unchanged. New versions introduced in MVP-2+ without breaking v1. Python service can be replaced by Rust without v1 clients knowing.

**Principle 9 — API Prefix Consistency**: ✅ PASS. All versioned endpoints use `/api/v<number>/` prefix. No endpoint without `/api`.

**Principle 10 — Tooling Separation**: ✅ PASS. Implementation handled by SpecKit. Design by this planning process. Documentation maintained here.

**Additional Gates**:
- **API Naming Convention**: ✅ PASS. URL-based versioning (not header-based) aligns with constitution section 6 (API Prefix Rule).
- **Backward Compatibility**: ✅ PASS. 12-month support window ensures no surprise client breakage. Aligns with Principle 2.
- **Single Source of Truth**: ✅ PASS. Version immutability (v1 schema locked) ensures endpoint contracts are definitive.

## Project Structure

### Documentation (this feature)

```text
specs/[###-feature]/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
source/
├── services/
│   └── bornemap-service/      # Python FastAPI (MVP-1)
│       ├── app/
│       │   ├── main.py        # Entry point with versioned routing
│       │   ├── routers/        # Versioned route modules (v1, v2, ...)
│       │   │   ├── v1/
│       │   │   │   ├── partners.py
│       │   │   │   ├── stations.py
│       │   │   │   ├── chargers.py
│       │   │   │   └── health.py
│       │   │   └── v2/        # Future: MVP-2+ versions
│       │   └── models/        # SQLAlchemy models
│       ├── migrations/        # Alembic migration files
│       ├── tests/
│       │   ├── test_v1_api.py
│       │   ├── test_v2_api.py # When v2 added
│       │   └── smoke/
│       └── requirements.txt

apps/
├── driver-web/                # React + Vite (calls /api/v1/...)
├── driver-mobile/             # React Native + Expo (calls /api/v1/...)
└── dashboard/                 # React + Vite (calls /api/v1/...)

docs/
├── api/
│   └── bornemap-service.md   # Updated to document v1 URLs and 12-month support
└── adr/
    └── ADR-018-api-versioning.md  # New ADR
```

**Structure Decision**: FastAPI router-based versioning. Each API version isolated in its own router module under `app/routers/v<number>/`. Version selection happens at `main.py` route registration. This approach enables clean v1→v2 migration in MVP-2 by simply adding a new router without modifying v1 code.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| [e.g., 4th project] | [current need] | [why 3 projects insufficient] |
| [e.g., Repository pattern] | [specific problem] | [why direct DB access insufficient] |
