# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added — Feature `001-infra-multi-schema-engine`

- Containerized development environment: multi-stage `Dockerfile` (Python 3.12) and `docker-compose.yml` (`app` FastAPI + `db` PostgreSQL 16).
- Typed, environment-driven configuration (`app/config.py`, pydantic-settings): `DATABASE_URL`, `API_HOST`, `API_PORT`, `DB_RETRY_WINDOW` — all with documented local defaults.
- Async SQLModel/SQLAlchemy engine and session factory backed by asyncpg (`app/database.py`).
- Alembic multi-schema migrations (`include_schemas=True`) with a baseline revision `0001_initial_multi_schema` creating exactly six isolated domain schemas: `catalog`, `inventory`, `crm`, `usage`, `billing`, `dunning`.
- Automatic migrations at startup with applied-revision integrity checks: each applied revision's file checksum is recorded in `public.alembic_revision_checksum`; an applied revision edited in place fails startup with a clear error.
- Startup lifecycle: database retry loop (`DB_RETRY_WINDOW`, default 30s) → migration + integrity verification → pinned readiness line `READY: app listening on {API_HOST}:{API_PORT}`.
- `GET /health` contract: `200 {"status":"ok","database":"up"}` when connected, `503 {"status":"error","database":"down"}` when unreachable.
- Test suite (pytest + pytest-asyncio + httpx): contract tests for `/health`, integration tests for startup/schema topology/versioning and environment configuration — 8 tests.
- Lint/format via Ruff (`ruff check`, `ruff format`), configured in `pyproject.toml`.
- Planning/spec artifacts under `specs/001-infra-multi-schema-engine/` (spec, contracts, plan, tasks, quick-start scenarios, docs).

### Planned (not yet implemented)

- Domain SQLModel entities and REST endpoints (`/api/v1/*`) for catalog, inventory, crm, usage, billing, and dunning.
- Dunning & collections lifecycle state machine (`FIRST_NOTICE → WARNING → SUSPENDED → TERMINATED | RESOLVED`) with SIM barring.
- Standalone CLI synthetic-data generator (Faker & Typer) with topological seeding order.