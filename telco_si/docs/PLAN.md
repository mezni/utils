# Telco Information System — Implementation Plan & Roadmap

## 1. Plan Overview
This execution plan breaks down the development of the multi-schema Telco API and synthetic data generator into 5 sequential Sprints. Each sprint focuses on delivering testable domain capabilities with strict schema isolation.

---

## 2. User Stories Summary

### Epic 1: Core Infrastructure & Multi-Schema Foundations
- **US1.1:** As a developer, I want a multi-container Docker Compose setup (`app`, `db`) so that I can run the system consistently in local environments.
- **US1.2:** As a developer, I want Alembic configured for multi-schema introspection so that database migrations automatically manage `catalog`, `inventory`, `crm`, `usage`, and `billing` schemas.

### Epic 2: Domain SQLModels & Schema Declarations
- **US2.1 (Catalog):** As a product manager, I want models for rate plans and price specs so that available offerings can be defined in the `catalog` schema.
- **US2.2 (Inventory):** As a network operator, I want models for ICCIDs, IMSIs, and MSISDN pools so that physical/logical assets are tracked in the `inventory` schema.
- **US2.3 (CRM):** As a support agent, I want customer account and subscriber models in the `crm` schema to bind identity to assigned assets and plans.
- **US2.4 (Usage & Billing):** As a system, I want models for CDR event logging in `usage` and invoices/line items in `billing`.

### Epic 3: Dunning & Lifecycle Management
- **US3.1 (Delinquency Tracking):** As a billing administrator, I want overdue invoices to initiate a `DunningCase` in the `billing` schema to track delinquency stages (`FIRST_NOTICE` -> `WARNING` -> `SUSPENDED` -> `TERMINATED`).
- **US3.2 (Resource Barring):** As a network controller, I want account status changes to `SUSPENDED` to trigger resource state updates (`BARRED`) on associated SIMs in `inventory`.
- **US3.3 (Resolution):** As a customer, I want my `DunningCase` set to `RESOLVED` and my service restored upon settling my overdue balance.

### Epic 4: REST API Endpoints & State Machine
- **US4.1 (Domain Routers):** As an API consumer, I want RESTful CRUD endpoints for each domain context under `/api/v1` with automated OpenAPI documentation.
- **US4.2 (Dunning Transitions):** As an administrative API consumer, I want dedicated endpoints (`/api/v1/dunning/evaluate`, `/api/v1/dunning/{id}/transition`) to trigger state evaluations and actions.

### Epic 5: Standalone CLI Data Generator
- **US5.1:** As a test engineer, I want a CLI command (`python -m app.cli.seed`) that generates schema-aligned synthetic data—including healthy accounts, active dunning cases, and barred SIMs—in strict topological order.

---

## 3. Sprint Backlog & Task Breakdown

### Sprint 1: Infrastructure & Multi-Schema Engine
**Goal:** Establish containerization, multi-schema database engine, and Alembic migration framework.

- [ ] **Task 1.1:** Create `Dockerfile` (Python 3.12 multi-stage) and `docker-compose.yml` (PostgreSQL 16 + FastAPI service).
- [ ] **Task 1.2:** Configure `app/config.py` using Pydantic `BaseSettings` for database connection string handling.
- [ ] **Task 1.3:** Setup `app/database.py` with SQLModel async engine (`asyncpg`) and session factories.
- [ ] **Task 1.4:** Initialize Alembic and update `migrations/env.py` to auto-create missing schemas (`crm`, `billing`, `inventory`, `catalog`, `usage`) and set `include_schemas=True`.
- [ ] **Task 1.5:** Validate baseline migrations run against PostgreSQL on container startup.

---

### Sprint 2: Domain Data Modeling (SQLModel)
**Goal:** Define single-class SQLModel entities for all domain contexts with explicit schema assignments.

- [ ] **Task 2.1:** Implement `app/models/catalog.py`
  - Models: `ProductOffering`, `PricePlan` (`schema="catalog"`).
- [ ] **Task 2.2:** Implement `app/models/inventory.py`
  - Models: `PhysicalResource` (SIM/ICCID), `LogicalResource` (MSISDN) (`schema="inventory"`).
- [ ] **Task 2.3:** Implement `app/models/crm.py`
  - Models: `CustomerAccount`, `SubscriberProfile` (`schema="crm"`).
- [ ] **Task 2.4:** Implement `app/models/usage.py`
  - Models: `UsageRecord` (CDRs) (`schema="usage"`).
- [ ] **Task 2.5:** Implement `app/models/billing.py`
  - Models: `Invoice`, `LineItem`, `DunningCase`, `DunningActionLog` (`schema="billing"`).
- [ ] **Task 2.6:** Generate initial multi-schema migration via Alembic:
  `alembic revision --autogenerate -m "initial_multi_schema_with_dunning"` and execute upgrade.

---

### Sprint 3: REST API Routers & Dunning State Machine
**Goal:** Build domain API endpoints and state transition logic using FastAPI APIRouter.

- [ ] **Task 3.1:** Build `app/api/v1/catalog.py` (`POST /plans`, `GET /plans`).
- [ ] **Task 3.2:** Build `app/api/v1/inventory.py` (`POST /sims`, `GET /sims/available`).
- [ ] **Task 3.3:** Build `app/api/v1/crm.py` (`POST /accounts`, `GET /accounts/{id}`).
- [ ] **Task 3.4:** Build `app/api/v1/usage.py` (`POST /cdrs`, `GET /usage/{msisdn}`).
- [ ] **Task 3.5:** Build `app/api/v1/billing.py` (`POST /invoices/generate`, `GET /invoices/{account_id}`).
- [ ] **Task 3.6:** Build `app/api/v1/dunning.py` handling state transition evaluations:
  $$\text{FIRST\_NOTICE} \longrightarrow \text{WARNING} \longrightarrow \text{SUSPENDED} \longrightarrow \text{TERMINATED} \mid \text{RESOLVED}$$
- [ ] **Task 3.7:** Wire routers into `app/main.py` under `/api/v1` prefix and verify OpenAPI documentation at `/docs`.

---

### Sprint 4: Standalone Seeder CLI Development
**Goal:** Build a Typer-powered CLI seed runner for topological synthetic data generation.

- [ ] **Task 4.1:** Setup `app/cli/seed.py` using Typer framework.
- [ ] **Task 4.2:** Implement Catalog Seeder (generates base 5G/4G rate plans).
- [ ] **Task 4.3:** Implement Inventory Seeder (generates pools of valid ICCIDs, IMSIs, and MSISDNs using `Faker`).
- [ ] **Task 4.4:** Implement CRM Seeder (creates customer accounts and binds available SIMs/MSISDNs and plans).
- [ ] **Task 4.5:** Implement Usage Seeder (generates Call Detail Records linked to active MSISDNs).
- [ ] **Task 4.6:** Implement Billing & Dunning Seeder following strict topological dependency order:
  $$\text{Catalog} \longrightarrow \text{Inventory} \longrightarrow \text{CRM} \longrightarrow \text{Usage} \longrightarrow \text{Billing} \longrightarrow \text{Dunning}$$
  - Enforces distributions: 80% healthy (`CURRENT`), 15% overdue (`FIRST_NOTICE`/`WARNING`), 5% suspended (`SUSPENDED` + barred SIMs in `inventory`).
- [ ] **Task 4.7:** Test CLI execution via Docker environment:
  `docker compose exec app python -m app.cli.seed --accounts 50 --cdrs-per-account 20`

---

### Sprint 5: System Verification & Documentation
**Goal:** Validate schema isolation, API contracts, and update technical documentation.

- [ ] **Task 5.1:** Perform cross-schema constraint validation to verify no foreign keys leak outside designated schemas without explicit schema qualification.
- [ ] **Task 5.2:** Test end-to-end dunning flow: generate overdue invoice -> evaluate dunning -> verify SIM status set to `BARRED` in `inventory.physical_resource`.
- [ ] **Task 5.3:** Finalize `BRIEF.md`, `ARCHITECTURE.md`, and `PLAN.md` to match final codebase layout.

---

## 4. Definition of Done (DoD)

1. **Schema Integrity:** All DB tables reside exclusively in their assigned PostgreSQL domain schemas (`catalog`, `inventory`, `crm`, `usage`, `billing`).
2. **Migration Success:** `alembic upgrade head` executes cleanly on a fresh PostgreSQL instance.
3. **State Machine Execution:** Overdue invoices properly initiate `DunningCase` records and trigger resource status updates (`BARRED`) upon reaching `SUSPENDED` state.
4. **Data Generation:** Running the CLI seeder populates all schemas in topological order without foreign-key constraint violations.
5. **API Functionality:** All endpoints return valid JSON responses with appropriate statuses and OpenAPI documentation.
