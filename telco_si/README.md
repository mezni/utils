# Telco SI — BSS/OSS Reference API

A reference-grade **Telecommunications Business Support System & Operations Support System (BSS/OSS) API** demonstrating high domain modularity via schema-isolated database design — built one feature at a time through a specification-driven workflow (`specs/`), with no operational overhead from a distributed microservices network.

## Status

| State | Description |
| --- | --- |
| **Implemented** | Feature `001-infra-multi-schema-engine`: containerized FastAPI app + PostgreSQL, six isolated domain schemas, automatic Alembic migrations with applied-revision integrity checks, configurable via environment variables, `/health` endpoint. |
| **Planned** | Domain SQLModel entities, domain REST endpoints (`/api/v1/*`), the dunning & collections lifecycle, and the synthetic-data CLI seeder. See [Plan](docs/PLAN.md) and `specs/`. |

## Highlights

- **Domain Segregation** — six bounded contexts (`catalog`, `inventory`, `crm`, `usage`, `billing`, `dunning`) enforced via isolated PostgreSQL schemas on a single instance.
- **Database Migrations** — Alembic multi-schema introspection; migrations run automatically at startup. Already-applied revisions are checksummed (`public.alembic_revision_checksum`); an applied revision edited in place fails startup with a clear error.
- **Environment-Driven Configuration** — connection string, bind host/port, and retry window settable via environment variables with documented local defaults.
- **Containerized Development Environment** — fully repeatable setup using Docker Compose (`app`, `db`).
- **Health & Readiness** — `GET /health` reports database connectivity; startup emits the pinned `READY:` line after migrations succeed.

> Dunning & collections lifecycle, domain REST APIs, and the synthetic-data CLI seeder are **planned**, not yet implemented (details below).

## Documentation

| Document | Description |
| --- | --- |
| [Vision, Scope & Assumptions](docs/BRIEF.md) | Foundational vision and in/out-of-scope boundaries. |
| [Architecture](docs/ARCHITECTURE.md) | System topology, schema isolation, and startup/readiness. |
| [Plan](docs/PLAN.md) | Sprint roadmap and definition of done. |
| [Data Model](docs/DATA_MODEL.md) | Schemas (implemented) and domain entities (planned). |
| [API](docs/API.md) | Endpoint reference — current and planned. |
| [Seeding](docs/SEEDING.md) | Planned synthetic-data generator design. |
| [Operations](docs/OPERATIONS.md) | Running, migration, configuration, and development workflows. |
| [Feature Spec](specs/001-infra-multi-schema-engine/) | Requirements, contracts, plan, tasks, and quick-start scenarios. |

## Quick Start

```bash
# Build and start the app + database (runs migrations automatically)
docker compose up -d --build

# Confirm readiness (expect {"status":"ok","database":"up"})
curl http://localhost:8000/health
```

On startup the app waits for the database (bounded by `DB_RETRY_WINDOW`), verifies
applied revisions, runs `alembic upgrade head` (creating the six domain schemas on
a fresh instance), and logs the pinned readiness line
`READY: app listening on 0.0.0.0:8000`.

See [Operations](docs/OPERATIONS.md) for full details.