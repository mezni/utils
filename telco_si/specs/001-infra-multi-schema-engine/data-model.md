# Data Model — Infrastructure & Multi-Schema Engine

Phase 1 (Sprint 1) establishes the **schema topology and migration metadata**
only; domain entities and tables are added by later sprints. This document
records what the baseline migration must create and how schema isolation is
enforced.

## Schemas (created & versioned by the baseline migration)

FR-007 fixes exactly six domain schemas in a single PostgreSQL instance. The
Alembic baseline revision `initial_multi_schema` creates each schema empty;
domain tables are deferred to later sprints.

| Schema | Bounded context | Planned entities (deferred) |
|--------|-----------------|------------------------------|
| `catalog` | Offers & pricing | `Product`, `Offer`, `PricePlan` |
| `inventory` | SIM / resource assets | `PhysicalResource` (ICCID), `LogicalResource` (MSISDN), `SimState` |
| `crm` | Subscriber & account identity | `CustomerAccount`, `SubscriberProfile` |
| `usage` | Call Detail Records | `UsageRecord` (CDR) |
| `billing` | Invoicing & receivables | `Invoice`, `LineItem`, `AccountReceivable`, `Payment` |
| `dunning` | Collections enforcement | `DunningCase`, `DunningNotice`, `DunningEvent`, `DunningSettlement` |

## Migration Metadata

- The `alembic_version` table lives in the `public` schema and tracks the
  globally applied revision for all six schemas (matches `docs/ARCHITECTURE.md`).

## Identity & Uniqueness Rules (Phase 1 scope)

- Schema names are unique per PostgreSQL instance (enforced by the database).
- Migration revision identifiers are unique within the installed Alembic history
  (enforced by `versions/`).

## Relationships

- Phase 1 defines no application-level relationships. Schema isolation is
  preserved via the convention from `docs/DATA_MODEL.md`: cross-domain references
  MUST use global identifiers (`UUID`, `MSISDN`, `ICCID`) rather than physical
  foreign keys across schemas.

## State Transitions

- Not applicable in Phase 1. The `dunning` state machine
  (`FIRST_NOTICE -> WARNING -> SUSPENDED -> TERMINATED | RESOLVED`) is a later
  sprint; its canonical description lives in `docs/DATA_MODEL.md`.

## Validation Rules

- After `upgrade head` on a fresh instance: exactly the six schemas above exist,
  and `alembic_version` reports the head revision (FR-007, SC-002).
- Re-running migrations at the head is an idempotent no-op (FR-009, SC-006).
- No cross-schema foreign keys exist (schema-isolation constraint).