# Data Model

This document describes the data model as it exists today and the entity design
planned for later features.

> **Status:** the six domain schemas exist (created by baseline migration `0001`),
> but **no domain tables are defined yet**. The entity layout below is the
> **planned** target for feature `002`+ (see `docs/PLAN.md`, Sprint 2).

## Implemented Today

Database instance: `telco` (single PostgreSQL 16 instance, logical schema separation).

```text
PostgreSQL Instance: telco
 └── public
      ├── alembic_version            (alembic migration state — single row, "0001")
      └── alembic_revision_checksum  (startup integrity: sha256 of each applied revision file)
 └── catalog     (empty schema — planned: offers & pricing)
 └── inventory   (empty schema — planned: SIM / resource assets)
 └── crm         (empty schema — planned: subscriber & account identity)
 └── usage       (empty schema — planned: Call Detail Records)
 └── billing     (empty schema — planned: invoicing & receivables)
 └── dunning     (empty schema — planned: collections enforcement)
```

The two `public` tables are maintained by the application:
- `alembic_version` — current migration revision number (op maintained by Alembic).
- `alembic_revision_checksum` — `version_num` (PK) + `checksum` (sha256) per applied
  revision; used by the startup runner to refuse starting when an applied migration
  file was edited in place.

## Planned Domain Entities

All entities are designed as **single-class `SQLModel` structures** with explicit
`schema="<domain>"` assignments. Cross-schema references use global identifiers
(`UUID`, `MSISDN`, `ICCID`) rather than physical foreign keys, preserving schema
isolation.

### `catalog` — Catalog

Products, offers, and pricing plans offered to subscribers.

| Entity | Notes |
| --- | --- |
| `Product` | Core product definition. |
| `Offer` | Sellable offer wrapping products. |
| `PricePlan` | Pricing rules / tariff associated with an offer. |

### `inventory` — Inventory

SIM / resource inventory including barring states used by the Dunning lifecycle.

| Entity | Notes |
| --- | --- |
| `SimCard` | Physical SIM resource keyed by **ICCID** and **MSISDN**. |
| `SimState` | Lifecycle / barring state (e.g., `ACTIVE`, `BARRED`). |

### `crm` — CRM

Subscribers and their account relationships.

| Entity | Notes |
| --- | --- |
| `Subscriber` | Customer entity keyed by **MSISDN** / account UUID. |
| `Account` | Billing account; carries the delinquency state. |
| `SubscriberAccount` | Relation between subscriber and account. |

### `usage` — Usage

Call Detail Records (CDRs) and aggregated usage.

| Entity | Notes |
| --- | --- |
| `Cdr` | Raw call detail record, resolved against subscriber **MSISDN**. |
| `UsageSummary` | Aggregated usage per subscriber / period. |

### `billing` — Billing

Invoicing, accounts receivable, and balance.

| Entity | Notes |
| --- | --- |
| `Invoice` | Billable document; **overdue** state triggers dunning. |
| `InvoiceLine` | Line items on an invoice. |
| `AccountReceivable` | Outstanding balance / receivable per account. |
| `Balance` | Account balance ledger. |
| `Payment` | Recorded payment applied against balance (internal settlement only; no external gateways in scope). |

### `dunning` — Dunning & Collections

The collections enforcement context.

| Entity | Notes |
| --- | --- |
| `DunningCase` | A delinquency case tracking an overdue account. |
| `DunningNotice` | Automated notice (`FIRST_NOTICE`, `WARNING`, ...). |
| `DunningEvent` | State transition audit record. |
| `DunningSettlement` | Record of balance settlement that resolves a case. |

## Dunning State Machine *(planned)*

```
FIRST_NOTICE → WARNING → SUSPENDED → TERMINATED
                        ↘              ↘
                         RESOLVED        RESOLVED
```

| State | Meaning | Side Effects |
| --- | --- | --- |
| `FIRST_NOTICE` | Initial delinquency; first automated notice issued. | Emit `DunningNotice`. |
| `WARNING` | Continued delinquency; escalated notice. | Emit escalated `DunningNotice`. |
| `SUSPENDED` | Service barred. | **Bar SIM in Inventory** (`SimState = BARRED`). |
| `TERMINATED` | Unresolved delinquency; account terminated. | Account termination recorded. |
| `RESOLVED` | Balance settled. | Unbar SIM, restore account to `CURRENT`. |

Transitions are recorded as `DunningEvent` rows to preserve an audit trail.

## Cross-Domain Identifier Usage

| Identifier | Roles |
| --- | --- |
| `UUID` | Primary key across all domains; application-level joins. |
| `MSISDN` | Subscriber / account identity (CRM → usage → billing). |
| `ICCID` | SIM identity (inventory barring ↔ dunning suspension). |