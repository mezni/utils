# Telco Information System — Architecture & Technology Design

## 1. Executive Summary & System Vision

This system is a modular, domain-driven Business Support System / Operations Support System (BSS/OSS) API modeled after industry-standard TM Forum Open Digital Architecture (ODA) guidelines.

To balance strict domain boundaries with minimal operational overhead during early-stage development, the system uses a **Monolith-First, Multi-Schema Architecture**. A single PostgreSQL instance maintains logical isolation between domain bounded contexts via explicit schemas (`catalog`, `inventory`, `crm`, `usage`, `billing`, `dunning`), enabling clean future microservice extraction if scale demands it.

**Feature `001-infra-multi-schema-engine` is implemented** (schemas, migrations, health, containerization, configuration). Domain entities, API routers, the dunning lifecycle, and the seeder are planned follow-on features (see `docs/PLAN.md`).

---

## 2. Selected Technology Stack

| Layer | Selected Tech | Rationale & Trade-Offs | Status |
| :--- | :--- | :--- | :--- |
| **Language** | **Python 3.12** | Native typing support, high developer velocity. | Implemented |
| **API Framework** | **FastAPI** | High-performance asynchronous OpenAPI generation, native Pydantic integration. | Implemented |
| **ORM / Data Layer** | **SQLModel** | Blends Pydantic validation with SQLAlchemy 2.0 Async ORM; single class handles API schemas and DB models. | Implemented (engine/sessions); entities planned |
| **Database** | **PostgreSQL 16** | Native multi-schema support, robust transaction guarantees. | Implemented |
| **Database Driver** | **Asyncpg** | High-throughput asynchronous PostgreSQL driver for Python. | Implemented |
| **Migrations** | **Alembic** | Industry-standard DB migration tool configured for multi-schema introspection (`include_schemas=True`). | Implemented |
| **Configuration** | **pydantic-settings** | Environment-driven, validated runtime settings with local defaults. | Implemented |
| **CLI Framework** | **Typer + Faker** | Standalone data-generation CLI. | Planned (Sprint 4) |
| **Containerization** | **Docker & Docker Compose** | Reproducible multi-container environment (`app`, `db`). | Implemented |

---

## 3. High-Level System Architecture

The following diagram illustrates the logical architecture as implemented today
(section A) and the planned application layers (section B).

```text
A) Implemented (feature 001)

   +---------------------------------------+
   |            Client / Consumer           |
   +-------------------+-------------------+
                       |
                HTTP: GET /health, /docs
                       v
   +---------------------------------------------+
   | FastAPI Application Container (Async)       |
   |                                             |
   |  app/main.py   startup: retry DB → verify  |
   |                migration checksums →       |
   |                ``alembic upgrade head`` →  |
   |                log READY line              |
   |                                             |
   |  app/database.py: SQLModel async engine    |
   |                (asyncpg, pooled)           |
   +---------------------+---------------------+
                         |
              Multi-Schema SQL Queries
                         v
   +--------------------------------------------------+
   | PostgreSQL Container (database: telco)           |
   |                                                  |
   |  public:  alembic_version, alembic_revision_    |
   |           checksum  (startup integrity ledger)   |
   |  +--------+--------+-------+------+--------+----+--+
   |  | catalog|inventory| crm  | usage |billing |dunning|
   |  | (empty)| (empty) |(empty)| (empty)(empty) |(empty) |
   |  +--------+--------+-------+------+--------+----+--+
   +--------------------------------------------------+

B) Planned (Sprints 2–4)

   - app/models/  SQLModel entities per schema
   - app/api/v1/  domain REST routers  (catalog, inventory, crm,
                 usage, billing, dunning)
   - app/cli/     Typer + Faker topological seeder
```

---

## 4. Multi-Schema Database Topology

Domain entities map to dedicated PostgreSQL schemas using SQLModel metadata
arguments (`__table_args__ = {"schema": "<domain>"}`). **Currently** the baseline
migration creates exactly six empty domain schemas; domain tables are added by
later features.

```text
PostgreSQL Instance: telco
 ├── public
 │    ├── alembic_version            (Tracks global migration state)
 │    └── alembic_revision_checksum  (Startup integrity: checksums of applied revisions)
 │
 ├── catalog       (Offers & pricing — empty until Sprint 2)
 ├── inventory     (SIM / resource assets — empty until Sprint 2)
 ├── crm           (Subscriber & account identity — empty until Sprint 2)
 ├── usage         (Call Detail Records — empty until Sprint 2)
 ├── billing       (Invoicing & receivables — empty until Sprint 2)
 └── dunning       (Collections enforcement — empty until Sprint 2)
```

Planned entities (deferred): `catalog.product_offering`, `inventory.physical_resource`,
`crm.customer_account`, `usage.usage_record`, `billing.invoice`, and
`dunning.dunning_case`, per `docs/DATA_MODEL.md`.

### Startup & Readiness

On container start the app (`app.main:app` via uvicorn) runs the startup sequence
defined in `contracts/startup-migrations.md`:

1. Polls the database for up to `DB_RETRY_WINDOW` seconds (default 30).
2. Verifies the checksums of already-applied migrations against
   `public.alembic_revision_checksum` (a modified applied revision fails startup).
3. Runs `alembic upgrade head` — creating the six domain schemas on a fresh
   instance and applying only pending revisions otherwise.
4. Logs the pinned readiness line `READY: app listening on {API_HOST}:{API_PORT}`
   and starts serving.

`GET /health` reports `200 {"status":"ok","database":"up"}` when connected and
`503 {"status":"error","database":"down"}` when the target is unreachable.

## 5. Dunning & Collections State Machine *(planned)*

Dunning will evaluate delinquent invoices and transition subscriber accounts
through lifecycle escalation steps. Not implemented yet (Sprint 3).

```text
       [ Invoice Due Date Passed (Unpaid) ]
                         |
                         v
               +-------------------+
               |   FIRST_NOTICE    | -> Send SMS/Email reminder (Days 1–7)
               +---------+---------+
                         | (Unpaid after grace period)
                         v
               +-------------------+
               |      WARNING      | -> Final notice prior to suspension (Days 8–14)
               +---------+---------+
                         | (Unpaid after grace period)
                         v
               +-------------------+ -> Event: SubscriberSuspended
               |     SUSPENDED     | -> Inventory Action: Set SIM status = "BARRED"
               +---------+---------+
                         |
               +---------+---------+
               |                   |
      (Invoice Settled)     (Unpaid after 30+ days)
               |                   |
               v                   v
     +-------------------+ +-------------------+ -> Event: SubscriberTerminated
     |     RESOLVED      | |    TERMINATED     | -> Inventory Action: Recycle MSISDN/ICCID
     +-------------------+ +-------------------+    to unassigned pool
```

## 6. Topological Data Ingestion Pipeline (CLI Seeder) *(planned)*

Synthetic data generation will follow a 6-stage topological sequence to maintain
referential integrity without foreign-key constraint violations. Not implemented
yet (Sprint 4); see `docs/SEEDING.md`.

$$\text{1. Catalog} \longrightarrow \text{2. Inventory} \longrightarrow \text{3. CRM} \longrightarrow \text{4. Usage} \longrightarrow \text{5. Billing} \longrightarrow \text{6. Dunning}$$

### Target Dataset Distributions

- **80% Healthy Accounts**: Invoices paid on time (`CURRENT` status).
- **15% Delinquent Accounts**: Active `DunningCase` records in `FIRST_NOTICE` or `WARNING` states.
- **5% Suspended Accounts**: Accounts in `SUSPENDED` state with associated SIM resources set to `BARRED` in `inventory.physical_resource`.

## 7. Directory & Codebase Structure

```text
/
├── Dockerfile                  # Python 3.12 multi-stage build
├── docker-compose.yml          # Services: app (FastAPI), db (PostgreSQL 16)
├── alembic.ini                 # Configured for multi-schema autogenerate
├── pyproject.toml              # Dependencies + pytest + Ruff configuration
├── .dockerignore
│
├── app/
│   ├── __init__.py
│   ├── main.py                 # FastAPI entrypoint: startup retry + migrations + READY
│   ├── config.py               # Pydantic Settings (DATABASE_URL, API_HOST, API_PORT, DB_RETRY_WINDOW)
│   └── database.py             # Async engine & session factory, check_connection()
│
├── migrations/
│   ├── env.py                  # Multi-schema aware migration runner
│   ├── script.py.mako          # Revision template
│   └── versions/
│       └── 0001_initial_multi_schema.py  # Baseline: six domain schemas
│
├── tests/
│   ├── contract/test_health.py     # GET /health contract tests
│   ├── integration/test_startup.py # Schema topology + migration head + idempotency
│   └── integration/test_config.py  # Environment-driven configuration
│
├── docs/                          # BRIEF, ARCHITECTURE, PLAN, DATA_MODEL, API, SEEDING, OPERATIONS
└── specs/                         # Feature specs (001-infra-multi-schema-engine), contracts, tasks
```

Later features add `app/models/`, `app/api/v1/`, and `app/cli/` for the domain
APIs, SQLModel entities, and the data-generation CLI (Sprints 2–4).