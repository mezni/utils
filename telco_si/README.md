# Telco SI — BSS/OSS Reference API

A reference-grade **Telecommunications Business Support System & Operations Support System (BSS/OSS) API** demonstrating high domain modularity, schema-isolated database design, lifecycle state management — including **delinquency, dunning escalations, and automated service barring** — and relational test-data generation, all without the operational overhead of a distributed microservices network.

## Highlights

- **Domain Segregation** — 5 primary bounded contexts (`catalog`, `inventory`, `crm`, `usage`, `billing`) enforced via isolated PostgreSQL database schemas.
- **Unified Data & API Layer** — Declarative, typed RESTful endpoints and database entities defined via single-class `SQLModel` structures.
- **Dunning & Collections Lifecycle** — End-to-end delinquency state machine tracking overdue invoices:
  `FIRST_NOTICE → WARNING → SUSPENDED → TERMINATED | RESOLVED`
  - Automated notices
  - Service suspensions (barring SIM resources in Inventory)
  - Balance settlement
- **Database Migrations** — Automated multi-schema introspection and version tracking using Alembic.
- **Synthetic Data Generation Engine** — A standalone, CLI-based seeder powered by Python **Faker** & **Typer** that ingests schema-aligned relational test data in strict topological order.
- **Containerized Development Environment** — Fully repeatable setup using Docker Compose (`app`, `db`).

## Documentation

| Document | Description |
| --- | --- |
| [Vision, Scope & Assumptions](docs/BRIEF.md) | Foundational vision, in/out of scope, and technical assumptions. |
| [Architecture](docs/ARCHITECTURE.md) | System topology, schema isolation, and cross-domain identifiers. |
| [Plan](docs/PLAN.md) | Phase 1 work breakdown and milestones. |
| [Data Model](docs/DATA_MODEL.md) | Domain schemas, entities, and state machines. |
| [API](docs/API.md) | Endpoint reference and usage patterns. |
| [Seeding](docs/SEEDING.md) | CLI seeder, topological order, and data distribution. |
| [Operations](docs/OPERATIONS.md) | Running, migration, and development workflows. |

## Quick Start

```bash
# Start the app and database
docker compose up -d

# Run migrations
docker compose exec app alembic upgrade head

# Seed synthetic data
docker compose exec app python -m telco_si.seed --size demo

# Run the API
docker compose exec app uvicorn telco_si.main:app --host 0.0.0.0 --port 8000
```

See [Operations](docs/OPERATIONS.md) for full details.
