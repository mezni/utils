# Implementation Plan: Infrastructure & Multi-Schema Engine

**Branch**: `001-infra-multi-schema-engine` | **Date**: 2026-09-05 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/001-infra-multi-schema-engine/spec.md`

## Summary

Sprint 1 establishes the containerized monolith-first multi-schema environment: a
Docker Compose topology with a FastAPI `app` service and a PostgreSQL 16 `db`
service; configuration-driven asynchronous database connectivity (SQLModel +
asyncpg); and an Alembic migration framework that auto-creates and versions all
six domain schemas (`catalog`, `inventory`, `crm`, `usage`, `billing`, `dunning`).
Migrations run automatically at container startup behind a bounded retry, an HTTP
health endpoint and a readiness log line confirm readiness, and database data
persists across app restarts via a named volume.

## Technical Context

**Language/Version**: Python 3.12+

**Primary Dependencies**: FastAPI, SQLModel, SQLAlchemy 2.0 (async), asyncpg,
Alembic, Pydantic v2 (`pydantic-settings`), uvicorn

**Storage**: PostgreSQL 16 — a single instance with six application schemas

**Testing**: pytest + httpx (async) for the health endpoint and migration startup;
compose-based smoke checks in `quickstart.md`

**Target Platform**: Linux containers (Docker); any host with Docker Compose

**Project Type**: monolith-first web-service (multi-schema API), single project

**Performance Goals**: startup readiness in under 5 minutes (cold); a migration
no-op re-run completes in a few seconds; health endpoint responds immediately
(<1s). No throughput targets in Phase 1.

**Constraints**: schema isolation — no cross-schema foreign keys; canonical
module path `app.*` (FR-010); automatic migrations at startup with bounded retry
then hard stop (FR-011/FR-014); health endpoint must report database connectivity
(FR-012); data persisted in a named volume (FR-003).

**Scale/Scope**: single-node development environment; low-to-medium throughput;
six schemas; Phase 1 infrastructure only (no domain endpoints yet).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

The project constitution (`.specify/memory/constitution.md`) is an unfilled
template with no ratified principles. No gates are violated; the plan follows
documented project conventions (`docs/ARCHITECTURE.md`, `docs/PLAN.md`).
Re-checked after design: still compliant — no constitution constraints defined.

## Project Structure

### Documentation (this feature)

```text
specs/001-infra-multi-schema-engine/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
│   ├── health-api.md
│   ├── environment-config.md
│   ├── startup-migrations.md
│   └── container-topology.md
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
Dockerfile                    # Python 3.12 multi-stage build
docker-compose.yml            # services: app (FastAPI), db (PostgreSQL 16, named volume)
alembic.ini                   # multi-schema autogenerate configuration
pyproject.toml                # project metadata, runtime + dev dependencies

app/                          # canonical application module (FR-010)
├── __init__.py
├── main.py                   # FastAPI app, /health endpoint, startup migration runner
├── config.py                 # pydantic-settings: DATABASE_URL, API_HOST, API_PORT
└── database.py               # SQLModel async engine (asyncpg), session factory, conn check

migrations/
├── env.py                    # include_schemas=True; auto-create missing schemas; six-schema list
└── versions/                 # initial_multi_schema revision (creates empty domain schemas)

tests/
├── __init__.py
├── contract/
│   ├── __init__.py
│   └── test_health.py        # GET /health contract (status + database fields)
└── integration/
    ├── __init__.py
    ├── test_config.py        # Settings env-var override of DATABASE_URL (US3)
    └── test_startup.py       # schemas exist after startup; migration no-op idempotent
```

**Structure Decision**: Single project at the repository root (template Option 1),
matching the layout prescribed in `docs/ARCHITECTURE.md`. The canonical module is
`app` per the FR-010 resolution; Alembic lives in `migrations/`; integration and
contract tests live under `tests/`.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

No constitution violations exist (unfilled template). The six-schema topology and
startup migration runner are mandated by the spec and the roadmap docs, so no
complexity justification is required here.