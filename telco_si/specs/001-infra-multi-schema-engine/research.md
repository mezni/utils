# Research — Infrastructure & Multi-Schema Engine

Phase 0 output of `/speckit.plan`. The technology stack was already fixed by
`docs/ARCHITECTURE.md` and the roadmap in `docs/PLAN.md`, so this document
consolidates those decisions with rationale and the alternatives considered.
There were no open `[NEEDS CLARIFICATION]` markers in the spec after
`/speckit.specify` and `/speckit.clarify`.

## Decision 1: Containerized Development Environment

**Decision**: Docker Compose with two services — `app` (FastAPI) and `db`
(PostgreSQL 16) — plus a named volume for database persistence.

**Rationale**: Reproducible across host machines with a single startup/teardown
command (FR-001/FR-002). Matches the workflow documented in `docs/OPERATIONS.md`.
A named volume satisfies FR-003 (data survives app restarts and whole-stack
recreation).

**Alternatives considered**: Local virtualenv + system PostgreSQL (not
reproducible or isolated); Kubernetes (operational overhead is out of Phase 1
scope per `docs/BRIEF.md`).

## Decision 2: Async Python API Stack

**Decision**: Python 3.12 + FastAPI + SQLModel (SQLAlchemy 2.0 async) + asyncpg.

**Rationale**: The documented stack in `docs/ARCHITECTURE.md`. SQLModel provides
single-class entities shared between API and DB. An async engine backed by
asyncpg gives connection pooling and non-blocking I/O, satisfying FR-006, and is
required for the later asynchronous CDR ingestion path.

**Alternatives considered**: Sync SQLAlchemy + psycopg (simpler, but would force a
blocking API path and a later migration); Tortoise ORM (smaller ecosystem, away
from the documented stack).

## Decision 3: Configuration-Driven Connectivity

**Decision**: Pydantic v2 `pydantic-settings` (`BaseSettings`) reading
`DATABASE_URL`, `API_HOST`, `API_PORT` from environment variables with
documented local defaults.

**Rationale**: Satisfies FR-004/FR-005 — validated, typed configuration with
env-var overrides and no code changes when switching database targets.

**Alternatives considered**: Plain `os.environ` (no validation/defaults);
dynaconf (heavier than needed for Phase 1).

## Decision 4: Multi-Schema Alembic Migrations

**Decision**: A single Alembic environment with `include_schemas=True` that
auto-creates missing schemas before upgrade and targets exactly six schemas:
`catalog`, `inventory`, `crm`, `usage`, `billing`, `dunning`. A baseline revision
`initial_multi_schema` creates the empty domain schemas; later sprints add the
domain tables.

**Rationale**: Satisfies FR-007/FR-008/FR-009. `migrations/env.py` must
`CREATE SCHEMA IF NOT EXISTS` for each target schema so autogenerate can
introspect them on a fresh instance, and so a fresh `upgrade head` succeeds with
zero manual steps. Schema isolation (no cross-schema FKs) is preserved by
long-running conventions — cross-schema references use global identifiers
(`UUID`, `MSISDN`, `ICCID`) per `docs/DATA_MODEL.md`.

**Alternatives considered**: Per-schema migration directories (more moving parts
for a monolith-first system); raw SQL scripts (not versioned, not reviewable —
fails FR-008).

## Decision 5: Startup Migration Execution with Bounded Retry

**Decision**: The FastAPI lifespan handler runs `alembic upgrade head` at
container startup, first polling database connectivity with a bounded retry
window (e.g., up to 30s), then exiting with a clear error and non-zero status if
migrations ultimately fail.

**Rationale**: Satisfies FR-011/FR-014 and the Q3 (automatic-on-startup)
clarification. Polling absorbs the db-not-ready race without a fatal crash
(US3/AC4); failing hard on real migration errors avoids a silently degraded boot
and gives the acceptance tests an unambiguous outcome.

**Alternatives considered**: Manual `alembic upgrade head` only (rejected per Q3);
continue in degraded mode (violates FR-011); unbounded retry (hangs on permanent
errors).

## Decision 6: Health Endpoint + Readiness Log

**Decision**: Expose `GET /health` returning JSON with application and database
connectivity status (`{"status":"ok","database":"up"}` or a 503 error body), and
emit a single startup readiness log line after migrations succeed and the app is
listening.

**Rationale**: Satisfies FR-012/FR-013 and the Q1 (D) clarification. The endpoint
gives an automated, scriptable check for SC-001/SC-005; the log line gives a
human-visible signal during `docker compose logs`.

**Alternatives considered**: Container status only (option B — verifies a
process, not readiness); log line only (option C — not machine-verifiable).