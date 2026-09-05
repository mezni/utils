# Telco Information System — Architecture & Technology Design

## 1. Executive Summary & System Vision

This system is a modular, domain-driven Business Support System / Operations Support System (BSS/OSS) API modeled after industry-standard TM Forum Open Digital Architecture (ODA) guidelines.

To balance strict domain boundaries with minimal operational overhead during early-stage development, the system uses a **Monolith-First, Multi-Schema Architecture**. A single PostgreSQL instance maintains logical isolation between domain bounded contexts via explicit schemas (`catalog`, `inventory`, `crm`, `usage`, `billing`), enabling clean future microservice extraction if scale demands it.

---

## 2. Selected Technology Stack

| Layer | Selected Tech | Rationale & Trade-Offs |
| :--- | :--- | :--- |
| **Language** | **Python 3.12+** | Native typing support, high developer velocity, rich data generation libraries (`Faker`). |
| **API Framework** | **FastAPI** | High-performance asynchronous OpenAPI generation, native Pydantic integration. |
| **ORM / Data Layer** | **SQLModel** | Blends Pydantic validation with SQLAlchemy 2.0 Async ORM; single class handles API schemas and DB models. |
| **Database** | **PostgreSQL 16** | Native multi-schema support, robust transaction guarantees, JSONB support for dynamic telemetry metadata. |
| **Database Driver** | **Asyncpg** | High-throughput asynchronous PostgreSQL driver for Python. |
| **Migrations** | **Alembic** | Industry-standard DB migration tool configured for multi-schema introspection (`include_schemas=True`). |
| **CLI Framework** | **Typer** | Builds typed, self-documenting CLI entrypoints for standalone data generation. |
| **Containerization** | **Docker & Docker Compose** | Reproducible multi-container environment (`app`, `db`). |

---

## 3. High-Level System Architecture

The following diagram illustrates the logical architecture, request pipeline, multi-schema isolation, Dunning state evaluation loop, and CLI runner topology.

```text
                               +---------------------------------------+
                               |            Client / Consumer           |
                               +-------------------+-------------------+
                                                   |
                                            HTTP / REST Calls
                                                   v
+---------------------------------------------------------------------------------------------------+
| FastAPI Application Container (Async)                                                            |
|                                                                                                   |
|  +--------------------+  +--------------------+  +--------------------+  +--------------------+  |
|  | Catalog Domain API |  | Inventory Domain   |  | CRM Domain API     |  | Billing & Dunning  |  |
|  | /api/v1/catalog    |  | /api/v1/inventory  |  | /api/v1/crm        |  | /api/v1/billing    |  |
|  +---------+----------+  +---------+----------+  +---------+----------+  +---------+----------+  |
|            |                       |                       |                       |              |
|            +-----------------------+-----------+-----------+-----------------------+              |
|                                                |                                                  |
|                                     SQLModel Async Engine                                         |
|                                     (Connection Pool: asyncpg)                                    |
+------------------------------------------------+--------------------------------------------------+
                                                 |
                                     Multi-Schema SQL Queries
                                                 v
+---------------------------------------------------------------------------------------------------+
| PostgreSQL Container (mono_db)                                                                    |
|                                                                                                   |
|  +------------------+  +------------------+  +------------------+  +------------------+           |
|  | Schema: catalog  |  | Schema: inventory|  | Schema: crm      |  | Schema: usage    |           |
|  | - product_plans  |  | - physical_sims  |  | - accounts       |  | - cdrs           |           |
|  +------------------+  +------------------+  +------------------+  +------------------+           |
|                                                                                                   |
|  +-----------------------------------------------------------------+  +------------------+        |
|  | Schema: billing                                                 |  | Schema: public   |        |
|  | - invoices  - dunning_cases  - dunning_action_logs              |  | - alembic_version|        |
|  +-----------------------------------------------------------------+  +------------------+        |
+---------------------------------------------------------------------------------------------------+
                                                 ^
                                                 | Writes Synthetic Relational Data
+------------------------------------------------+--------------------------------------------------+
| Standalone CLI Seed Runner                                                                        |
| Exec Command: docker compose exec app python -m app.cli.seed --accounts 100                       |
| Engine: Typer + Faker + SQLModel Topological Ingestion                                            |
+---------------------------------------------------------------------------------------------------+
```

## 4. Multi-Schema Database Topology

To eliminate domain coupling, entities are mapped directly to dedicated database schemas using SQLModel metadata arguments (`__table_args__ = {"schema": "<domain>"}`).

```text
PostgreSQL Instance: mono_db
 ├── public
 │    └── alembic_version (Tracks global migration state)
 │
 ├── catalog
 │    ├── product_offering (Base rate plans, speeds, data caps)
 │    └── price_plan (Monthly costs, excess usage rates)
 │
 ├── inventory
 │    ├── physical_resource (ICCID, SIM state: IN_STOCK, ASSIGNED, BARRED)
 │    └── logical_resource (MSISDN pool, IP allocations)
 │
 ├── crm
 │    ├── customer_account (Identity, billing profile, credit tier)
 │    └── subscriber_profile (Links Account + MSISDN + ICCID + Rate Plan)
 │
 ├── usage
 │    └── usage_record (CDRs: timestamps, bytes transferred, duration, cell_tower_id)
 │
 └── billing
      ├── invoice (Monthly statement, total due, billing period)
      ├── line_item (Breakdown of plan charge vs. usage excess)
      ├── dunning_case (Delinquency tracker: amount_overdue, state, next_action_at)
      └── dunning_action_log (Audit trail: SMS_NOTICE, BAR_OUTGOING, SUSPEND_SERVICE)
```

## 5. Dunning & Collections State Machine

Dunning evaluates delinquent invoices and transitions subscriber accounts through lifecycle escalation steps.

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

## 6. Topological Data Ingestion Pipeline (CLI Seeder)

Synthetic data generation strictly follows a 6-stage topological sequence to maintain referential integrity without foreign-key constraint violations.

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
├── BRIEF.md                    # Core vision, scope, and assumptions
├── ARCHITECTURE.md             # Stack selection, database schemas, and state machine design
├── PLAN.md                     # Roadmap, epics, user stories, and task backlog
│
├── migrations/
│   ├── env.py                  # Multi-schema aware migration runner
│   └── versions/               # Generated migration scripts
│
└── app/
    ├── main.py                 # FastAPI application entrypoint
    ├── config.py               # Pydantic Settings
    ├── database.py             # Async engine & session factories (asyncpg)
    │
    ├── models/                 # Domain SQLModel definitions
    │   ├── __init__.py
    │   ├── catalog.py          # schema="catalog"
    │   ├── inventory.py        # schema="inventory"
    │   ├── crm.py              # schema="crm"
    │   ├── usage.py            # schema="usage"
    │   └── billing.py          # schema="billing" (includes Invoices & Dunning)
    │
    ├── api/                    # REST Endpoint Routers
    │   ├── v1/
    │   │   ├── catalog.py
    │   │   ├── inventory.py
    │   │   ├── crm.py
    │   │   ├── usage.py
    │   │   ├── billing.py
    │   │   └── dunning.py      # Dunning evaluation & state transitions
    │   └── router.py
    │
    └── cli/                    # Standalone CLI tools
        ├── __main__.py
        └── seed.py             # Typer/Faker topological data generator
```
