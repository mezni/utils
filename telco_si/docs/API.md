# API Reference

The API is a **declarative, typed RESTful** layer defined via single-class `SQLModel` structures. All endpoints are exposed under a unified service with domain-prefixed routes.

## Base Paths

| Prefix | Context |
| --- | --- |
| `/catalog` | Products, offers, price plans. |
| `/inventory` | SIM resources and barring states. |
| `/crm` | Subscribers and accounts, account delinquency state. |
| `/usage` | CDRs and usage summaries. |
| `/billing` | Invoices, receivables, balances, payments. |
| `/dunning` | Dunning cases, notices, transitions, settlements. |

## Core Patterns

- **Declarative entities** — every request/response payload is a `SQLModel` class, giving typed validation and OpenAPI generation.
- **Global identifiers** — resources are addressed by `UUID`; cross-domain joins use `MSISDN` and `ICCID`.
- **Lifecycle operations** — transitions (e.g., case escalation, suspension, resolution) are explicit, stateful operations on the Dunning context.

## Dunning & Collections Endpoints

### Cases

```
GET    /dunning/cases                     List dunning cases (filterable by state)
POST   /dunning/cases                     Create a dunning case from an overdue account
GET    /dunning/cases/{case_id}           Retrieve a dunning case
PATCH  /dunning/cases/{case_id}           Update case details
```

### State Transitions

```
POST   /dunning/cases/{case_id}/notices   Issue an automated notice (FIRST_NOTICE / WARNING)
POST   /dunning/cases/{case_id}/suspend   Suspend service → bars SIM in Inventory
POST   /dunning/cases/{case_id}/terminate Terminate account
POST   /dunning/cases/{case_id}/resolve   Settle balance → resolves case, unbars SIM
```

### Notices, Events & Settlements

```
GET    /dunning/cases/{case_id}/notices   List notices issued for a case
GET    /dunning/cases/{case_id}/events    List state-transition audit events
POST   /dunning/cases/{case_id}/settlements  Record a balance settlement
```

## Inventory Barring (used by Dunning)

```
GET    /inventory/sims                    List SIM resources (filterable by state)
GET    /inventory/sims/{iccid}            Retrieve a SIM by ICCID
POST   /inventory/sims/{iccid}/bar        Bar a SIM (service suspension enforcement)
POST   /inventory/sims/{iccid}/unbar      Unbar a SIM (settlement restoration)
```

## Billing

```
GET    /billing/invoices                  List invoices (filterable by overdue / status)
POST   /billing/invoices                  Create an invoice
GET    /billing/invoices/{invoice_id}     Retrieve an invoice
GET    /billing/accounts/{account_id}/balance  Account balance
POST   /billing/accounts/{account_id}/payments Record an internal payment / settlement
```

## CRM

```
GET    /crm/subscribers                   List subscribers
POST   /crm/subscribers                   Create a subscriber (with MSISDN)
GET    /crm/subscribers/{uuid}            Retrieve a subscriber
GET    /crm/accounts/{account_id}         Retrieve a billing account + delinquency state
```

## Usage

```
POST   /usage/cdrs                        Ingest CDRs (asynchronous)
GET    /usage/cdrs                        List CDRs
GET    /usage/summaries                   Aggregated usage summaries
```

## Catalog

```
GET    /catalog/products                  List products
GET    /catalog/offers                    List offers
GET    /catalog/price-plans               List price plans
```

> Detailed OpenAPI documentation is generated from the `SQLModel` schema definitions at runtime.
