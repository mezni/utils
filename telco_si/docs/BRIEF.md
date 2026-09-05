# Vision, Scope & Assumptions

This document records the foundational vision, scope, and technical assumptions for the Telco SI reference BSS/OSS API.

## System Vision

To build a **reference-grade Telecommunications Business Support System & Operations Support System (BSS/OSS) API** that demonstrates:

- High **domain modularity**
- **Schema-isolated** database design
- **Lifecycle state management** — including delinquency, dunning escalations, and automated service barring
- **Relational test-data generation**

…all without the operational overhead of a distributed microservices network.

## System Scope

### In Scope (Phase 1)

**Domain Segregation**
5 primary bounded contexts — `catalog`, `inventory`, `crm`, `usage`, `billing` — enforced via isolated PostgreSQL database schemas.

**Unified Data & API Layer**
Declarative, typed RESTful endpoints and database entities defined via single-class `SQLModel` structures.

**Dunning & Collections Lifecycle**
End-to-end delinquency state machine tracking overdue invoices:

$$\text{FIRST\_NOTICE} \rightarrow \text{WARNING} \rightarrow \text{SUSPENDED} \rightarrow \text{TERMINATED} \mid \text{RESOLVED}$$

Including:
- Automated notices
- Service suspensions (barring SIM resources in Inventory)
- Balance settlement

**Database Migrations**
Automated multi-schema introspection and version tracking using Alembic.

**Synthetic Data Generation Engine**
A standalone, CLI-based seeder powered by Python Faker & Typer that ingests schema-aligned relational test data — including overdue invoices, dunning cases, and barred SIM states — in strict topological order.

**Containerized Development Environment**
Fully repeatable setup using Docker Compose (`app`, `db`).

### Out of Scope (Phase 1)

- **Real-time network interfaces** — e.g., direct GTP/SGi-LAN packet inspection, Diameter/RADIUS protocol drivers.
- **Microservice orchestration engines** — e.g., Kubernetes, service meshes like Istio.
- **Complex multi-tenant org structures** or enterprise hierarchy trees.
- **External payment gateway processing** — e.g., Stripe/Adyen webhooks or direct PCI-DSS handling.

## Key Technical Assumptions

**Architecture Trade-off**
A single PostgreSQL database instance with logical schema separation provides sufficient performance for low-to-medium throughput while maintaining clean boundaries for future microservice extraction if required.

**Cross-Domain Identifiers**
Global identifiers — specifically `UUID`, `MSISDN` (E.164 phone number), and `ICCID` (SIM serial) — serve as primary cross-domain join keys without breaking schema boundaries.

**Usage Ingestion Pattern**
Call Detail Records (CDRs) are ingested asynchronously and resolve against pre-existing subscriber MSISDN records created in the CRM context.

**Topological Seeding Order**
Synthetic data ingestion strictly follows the relational dependency chain:

$$\text{Catalog} \longrightarrow \text{Inventory} \longrightarrow \text{CRM} \longrightarrow \text{Usage} \longrightarrow \text{Billing} \longrightarrow \text{Dunning}$$

**Data Generation Capacity & Distribution**
The CLI seeder generates dataset proportions of:

- ~**80%** healthy accounts (`CURRENT`)
- ~**15%** delinquent accounts (`FIRST_NOTICE` / `WARNING`)
- ~**5%** suspended accounts (`SUSPENDED` + barred SIMs in Inventory)
