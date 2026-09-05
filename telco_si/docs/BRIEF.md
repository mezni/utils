# Vision, Scope & Assumptions

This document records the foundational vision, scope, and technical assumptions for the Telco SI reference BSS/OSS API.

## System Vision

To build a **reference-grade Telecommunications Business Support System & Operations Support System (BSS/OSS) API** that demonstrates:

- High **domain modularity**
- **Schema-isolated** database design
- **Lifecycle state management** — including delinquency, dunning escalations, and automated service barring
- **Relational test-data generation**

…all without the operational overhead of a distributed microservices network.

## Implementation Status

- **Implemented (Feature `001-infra-multi-schema-engine`):** containerized FastAPI app + PostgreSQL, six isolated domain schemas, automatic multi-schema migrations with applied-revision integrity checks, environment-driven configuration, and the `/health` endpoint. See `docs/ARCHITECTURE.md`, `docs/OPERATIONS.md`, and `specs/001-infra-multi-schema-engine/`.
- **Planned:** domain entities and REST APIs, the dunning & collections lifecycle, and the synthetic-data CLI seeder. The remainder of this document describes the **target** system; items marked *(planned)* are not yet built.

## System Scope

### In Scope

**Domain Segregation** *(implemented)*
Six bounded contexts — `catalog`, `inventory`, `crm`, `usage`, `billing`, `dunning` — enforced via isolated PostgreSQL database schemas. The baseline migration creates the six schemas; domain tables are added by later features.

**Database Migrations** *(implemented)*
Automated multi-schema introspection and version tracking using Alembic, run automatically at startup. Each applied revision's file checksum is recorded (`public.alembic_revision_checksum`); an applied revision edited in place fails startup with a clear error.

**Containerized Development Environment** *(implemented)*
Fully repeatable setup using Docker Compose (`app`, `db`).

**Unified Data & API Layer** *(planned)*
Declarative, typed RESTful endpoints and database entities defined via single-class `SQLModel` structures.

**Dunning & Collections Lifecycle** *(planned)*
End-to-end delinquency state machine tracking overdue invoices:

$$\text{FIRST\_NOTICE} \rightarrow \text{WARNING} \rightarrow \text{SUSPENDED} \rightarrow \text{TERMINATED} \mid \text{RESOLVED}$$

Including:
- Automated notices
- Service suspensions (barring SIM resources in Inventory)
- Balance settlement

**Synthetic Data Generation Engine** *(planned)*
A standalone, CLI-based seeder powered by Python Faker & Typer that ingests schema-aligned relational test data — including overdue invoices, dunning cases, and barred SIM states — in strict topological order.

### Out of Scope

- **Real-time network interfaces** — e.g., direct GTP/SGi-LAN packet inspection, Diameter/RADIUS protocol drivers.
- **Microservice orchestration engines** — e.g., Kubernetes, service meshes like Istio.
- **Complex multi-tenant org structures** or enterprise hierarchy trees.
- **External payment gateway processing** — e.g., Stripe/Adyen webhooks or direct PCI-DSS handling.

## Key Technical Assumptions

**Architecture Trade-off**
A single PostgreSQL database instance with logical schema separation provides sufficient performance for low-to-medium throughput while maintaining clean boundaries for future microservice extraction if required.

**Cross-Domain Identifiers**
Global identifiers — specifically `UUID`, `MSISDN` (E.164 phone number), and `ICCID` (SIM serial) — serve as primary cross-domain join keys without breaking schema boundaries.

**Usage Ingestion Pattern** *(planned)*
Call Detail Records (CDRs) are ingested asynchronously and resolve against pre-existing subscriber MSISDN records created in the CRM context.

**Topological Seeding Order** *(planned)*
Synthetic data ingestion strictly follows the relational dependency chain:

$$\text{Catalog} \longrightarrow \text{Inventory} \longrightarrow \text{CRM} \longrightarrow \text{Usage} \longrightarrow \text{Billing} \longrightarrow \text{Dunning}$$

**Data Generation Capacity & Distribution** *(planned)*
The CLI seeder generates dataset proportions of:

- ~**80%** healthy accounts (`CURRENT`)
- ~**15%** delinquent accounts (`FIRST_NOTICE` / `WARNING`)
- ~**5%** suspended accounts (`SUSPENDED` + barred SIMs in Inventory)