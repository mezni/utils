# Implementation Plan: Sprint 1 Backend and Database

**Branch**: `[001-sprint-01]` | **Date**: `2026-06-08` | **Spec**: `specs/001-sprint-01/spec.md`

**Input**: Feature specification from `/specs/001-sprint-01/spec.md`

## Summary

Deliver a local FastAPI backend and PostgreSQL schema for MVP-1 with `/api`
health, partner, station, charger, and nearby endpoints, backed by Alembic
migrations, seeded Tunisia catalog data, and smoke-test coverage.

## Technical Context

**Language/Version**: Python 3.11

**Primary Dependencies**: FastAPI, Uvicorn, SQLAlchemy, psycopg2, Pydantic, Alembic

**Storage**: PostgreSQL with `inventory` and `gis` schemas

**Testing**: pytest smoke tests plus manual curl/Postman verification

**Target Platform**: Linux local development via Docker Compose

**Project Type**: backend web-service / API

**Performance Goals**: no formal throughput SLO; CRUD and nearby lookup should remain responsive for small seed data

**Constraints**: `/api` prefix; `GET /api/health` performs a database check; MVP-1 remains unauthenticated; `inventory.station` is the source of truth; SQL uses bind parameters only; no PostGIS; no auth/accounts; secrets never committed; zero Class A bugs; API documentation complete

**Scale/Scope**: 15 endpoints, 3 partners, 15 stations across Tunisia, 2-4 chargers per station, one local seed dataset

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- The feature preserves the current MVP slice and does not introduce unneeded
  infrastructure.
- All API work stays under `/api`, and `GET /api/health` includes a database
  check.
- Data ownership stays aligned with `inventory.station`, `gis`, and
  `analytics` rules.
- MVP-1 remains unauthenticated, and runtime work keeps the real-DB,
  graceful-failure, API-doc, and zero-Class-A-bug gates intact.

## Project Structure

### Documentation (this feature)

```text
specs/001-sprint-01/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
└── contracts/
    └── api.md
```

### Source Code (repository root)

```text
source/
└── services/
    └── bornemap-service/
        ├── app/
        └── tests/

database/
├── migrations/
└── seeds/
```

**Structure Decision**: Keep all MVP-1 runtime code under `source/services/bornemap-service/`, database migrations under `database/migrations/`, and seed assets under `database/seeds/` so the backend slice stays isolated and the later frontend MVPs can land independently.

## Complexity Tracking

No constitution violations require justification for this sprint.
