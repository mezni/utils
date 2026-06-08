# Research: Sprint 1 Backend and Database

## Language and Runtime

- Decision: Python 3.11
- Rationale: the sprint already names Python + FastAPI, and 3.11 is a stable current default with broad ecosystem support.
- Alternatives considered: Python 3.10, Python 3.12

## Backend Stack

- Decision: FastAPI, Uvicorn, SQLAlchemy, psycopg2, Pydantic, Alembic
- Rationale: these dependencies are explicitly called out by the sprint scope and match the intended backend slice.
- Alternatives considered: Flask, Django, async-only drivers, handwritten SQL access

## Storage

- Decision: PostgreSQL with `inventory` and `gis` schemas
- Rationale: the spec requires a real PostgreSQL database and the constitution assigns `inventory.station` as the source of truth.
- Alternatives considered: SQLite, Postgres without schemas, PostGIS

## Testing Approach

- Decision: pytest smoke tests plus manual curl/Postman verification
- Rationale: the sprint requires smoke coverage for every endpoint and explicit endpoint verification.
- Alternatives considered: unit-only tests, no manual verification, end-to-end UI tests

## Runtime Model

- Decision: local Linux development with Docker Compose
- Rationale: repo guidance and MVP-1 notes emphasize reproducible local startup and onboarding.
- Alternatives considered: bare-metal local startup, production-only deployment, hosted dev environment

## Scope and Scale

- Decision: a small MVP slice with 15 endpoints, 3 partners, 15 stations, and 2-4 chargers per station
- Rationale: this matches the sprint definition and keeps seed data and smoke testing tractable.
- Alternatives considered: larger catalog seed, broader MVP-1 frontend scope, production-scale load assumptions

## Remaining Assumptions

- Public API identifiers should use non-sequential opaque IDs.
- Nearby lookup should order matches by ascending distance.
- No formal performance SLO is required for this sprint.
