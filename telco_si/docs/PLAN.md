# Telco Information System — Implementation Plan & Roadmap

## 1. Plan Overview

The Telco SI system is built feature-by-feature from specifications under `specs/`
(each feature directory holds a spec, contracts, plan, task list, and quick-start
scenarios). This document is the upstream roadmap that those features progress
through. **Feature `001-infra-multi-schema-engine` is complete** and covers
Sprint 1 below.

Legend: `[X]` done · `[ ]` planned.

---

## 2. User Stories Summary

### Epic 1: Core Infrastructure & Multi-Schema Foundations
- **US1.1:** As a developer, I want a multi-container Docker Compose setup (`app`, `db`) so that I can run the system consistently in local environments. — `[X]`
- **US1.2:** As a developer, I want Alembic configured for multi-schema introspection so that database migrations automatically manage `catalog`, `inventory`, `crm`, `usage`, `billing`, and `dunning` schemas. — `[X]`
- **US1.3:** As an operator, I want environment-driven configuration so the system can point at any PostgreSQL target and tune retry windows without code changes. — `[X]`
- **US1.4:** As an operator, I want startup to fail clearly (non-zero exit) if the database is unreachable or an applied migration is tampered with. — `[X]`

### Epic 2: Domain SQLModels & Schema Declarations (planned)
- **US2.1 (Catalog):** … models for rate plans and price specs in the `catalog` schema. — `[ ]`
- **US2.2 (Inventory):** … models for ICCIDs, IMSIs, and MSISDN pools in the `inventory` schema. — `[ ]`
- **US2.3 (CRM):** … customer account and subscriber models in the `crm` schema. — `[ ]`
- **US2.4 (Usage & Billing):** … models for CDR event logging in `usage` and invoices/line items in `billing`. — `[ ]`

### Epic 3: Dunning & Lifecycle Management (planned)
- **US3.1 (Delinquency Tracking):** … overdue invoices initiate a `DunningCase` (`FIRST_NOTICE` → `WARNING` → `SUSPENDED` → `TERMINATED`). — `[ ]`
- **US3.2 (Resource Barring):** … account status `SUSPENDED` triggers `BARRED` state on associated SIMs in `inventory`. — `[ ]`
- **US3.3 (Resolution):** … `DunningCase` set to `RESOLVED` and service restored upon settling an overdue balance. — `[ ]`

### Epic 4: REST API Endpoints & State Machine (planned)
- **US4.1 (Domain Routers):** … RESTful CRUD endpoints for each domain context under `/api/v1` with automated OpenAPI documentation. — `[ ]`
- **US4.2 (Dunning Transitions):** … dedicated endpoints to trigger state evaluations and actions. — `[ ]`

### Epic 5: Standalone CLI Data Generator (planned)
- **US5.1:** … a CLI command that generates schema-aligned synthetic data — healthy accounts, active dunning cases, and barred SIMs — in strict topological order. — `[ ]`

---

## 3. Sprint Backlog

### Sprint 1: Infrastructure & Multi-Schema Engine — `[X]` complete

Delivered as feature `specs/001-infra-multi-schema-engine/`:

- `[X]` Multi-stage `Dockerfile` (Python 3.12) and `docker-compose.yml` (PostgreSQL 16 + FastAPI).
- `[X]` `app/config.py` — pydantic-settings `Settings` (`DATABASE_URL`, `API_HOST`, `API_PORT`, `DB_RETRY_WINDOW`).
- `[X]` `app/database.py` — SQLModel async engine (`asyncpg`), session factory, connectivity check.
- `[X]` Alembic multi-schema introspection (`include_schemas=True`); baseline revision `0001` creates exactly six schemas (`catalog`, `inventory`, `crm`, `usage`, `billing`, `dunning`).
- `[X]` Migrations run automatically at startup with an applied-revision checksum ledger (`public.alembic_revision_checksum`); modified applied revisions fail startup.
- `[X]` `GET /health` (200 ok / 503 error) and pinned `READY:` readiness line.
- `[X]` Test suite (8 tests: contract + integration) and Ruff lint/format, all green in-container.

Detailed task breakdown: `specs/001-infra-multi-schema-engine/tasks.md` (T001–T028, all `[X]`).

### Sprint 2: Domain Data Modeling (SQLModel) — `[ ]` planned

Serve as entity definitions for all domain contexts with explicit schema assignments.

- `[ ]` `app/models/catalog.py` — `ProductOffering`, `PricePlan` (`schema="catalog"`).
- `[ ]` `app/models/inventory.py` — `PhysicalResource` (SIM/ICCID), `LogicalResource` (MSISDN).
- `[ ]` `app/models/crm.py` — `CustomerAccount`, `SubscriberProfile`.
- `[ ]` `app/models/usage.py` — `UsageRecord` (CDRs).
- `[ ]` `app/models/billing.py` — `Invoice`, `LineItem`, `DunningCase`, `DunningActionLog`.
- `[ ]` Generate the initial multi-schema migration (`alembic revision --autogenerate`) and upgrade.

### Sprint 3: REST API Routers & Dunning State Machine — `[ ]` planned

- `[ ]` Domain routers under `/api/v1` (`catalog`, `inventory`, `crm`, `usage`, `billing`, `dunning`).
- `[ ]` Dunning state-transition evaluations (`FIRST_NOTICE → WARNING → SUSPENDED → TERMINATED | RESOLVED`).
- `[ ]` Wire routers in `app/main.py` and verify OpenAPI docs at `/docs`.

### Sprint 4: Standalone Seeder CLI — `[ ]` planned

- `[ ]` Typer CLI seed runner; topological order per docs/[SEEDING.md](SEEDING.md).
- `[ ]` Enforce distributions: 80% healthy (`CURRENT`), 15% overdue (`FIRST_NOTICE`/`WARNING`), 5% suspended (`SUSPENDED` + barred SIMs).

### Sprint 5: System Verification & Documentation — `[ ]` planned

- `[ ]` Cross-schema constraint validation (no FK leaks across schemas).
- `[ ]` End-to-end dunning flow verification.
- `[ ]` Finalize docs tree to match the codebase after each feature lands.

---

## 4. Definition of Done (DoD)

| # | Criterion | Status |
| --- | --- | --- |
| 1 | All DB tables reside exclusively in their assigned PostgreSQL domain schemas (`catalog`, `inventory`, `crm`, `usage`, `billing`, `dunning`). | Six schemas exist; no domain tables yet (Sprint 2). |
| 2 | `alembic upgrade head` executes cleanly on a fresh PostgreSQL instance. | `[X]` verified (spec `001`). |
| 3 | Overdue invoices properly initiate `DunningCase` records and trigger `BARRED` resource updates at `SUSPENDED`. | `[ ]` planned (Sprint 3). |
| 4 | Running the CLI seeder populates all schemas in topological order without FK violations. | `[ ]` planned (Sprint 4). |
| 5 | All endpoints return valid JSON with appropriate statuses and OpenAPI documentation. | Partial: `/health` `[X]`; domain endpoints `[ ]` planned. |