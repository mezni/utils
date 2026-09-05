# Data Model

All entities are declared as **single-class `SQLModel` structures**. Cross-schema references use global identifiers (`UUID`, `MSISDN`, `ICCID`) rather than physical foreign keys, preserving schema isolation.

## Schemas & Entities

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

Barring a SIM is the enforcement mechanism for service suspension in the Dunning state machine.

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

CDRs are ingested **asynchronously** and resolve against pre-existing MSISDN records.

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

## Dunning State Machine

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
