# API Reference

> **Status:** today the service exposes a single health endpoint (`/health`) plus
> auto-generated OpenAPI docs (`/docs`, `/openapi.json`). The domain REST API
> described in the second half of this document is **planned** (Sprint 3,
> `docs/PLAN.md`).

## Implemented Today

### `GET /health`

Reports application readiness and live database connectivity.

| State | Status | Body |
| --- | --- | --- |
| Database reachable | `200` | `{"status":"ok","database":"up"}` |
| Database unreachable | `503` | `{"status":"error","database":"down"}` |

No body content is returned on error beyond the JSON above. Contract tests live in
`tests/contract/test_health.py`; the contract specification is in
`specs/001-infra-multi-schema-engine/contracts/health-api.md`.

### Interactive Docs

FastAPI serves auto-generated OpenAPI documentation at:

- `GET /docs` — Swagger UI
- `GET /redoc` — ReDoc
- `GET /openapi.json` — machine-readable schema

## Planned Domain Endpoints

Once Sprint 3 lands, domain routers will be exposed under an `/api/v1` prefix with
declarative SQLModel payloads. Target surface (subject to the feature spec):

### Dunning & Collections

```
GET    /dunning/cases                     List dunning cases (filterable by state)
POST   /dunning/cases                     Create a dunning case from an overdue account
GET    /dunning/cases/{case_id}           Retrieve a dunning case
PATCH  /dunning/cases/{case_id}           Update case details
POST   /dunning/cases/{case_id}/notices   Issue an automated notice (FIRST_NOTICE / WARNING)
POST   /dunning/cases/{case_id}/suspend   Suspend service → bars SIM in Inventory
POST   /dunning/cases/{case_id}/terminate Terminate account
POST   /dunning/cases/{case_id}/resolve   Settle balance → resolves case, unbars SIM
GET    /dunning/cases/{case_id}/notices   List notices issued for a case
GET    /dunning/cases/{case_id}/events    List state-transition audit events
POST   /dunning/cases/{case_id}/settlements  Record a balance settlement
```

### Inventory (used by Dunning)

```
GET    /inventory/sims                    List SIM resources (filterable by state)
GET    /inventory/sims/{iccid}            Retrieve a SIM by ICCID
POST   /inventory/sims/{iccid}/bar        Bar a SIM (service suspension enforcement)
POST   /inventory/sims/{iccid}/unbar      Unbar a SIM (settlement restoration)
```

### Billing

```
GET    /billing/invoices                  List invoices (filterable by overdue / status)
POST   /billing/invoices                  Create an invoice
GET    /billing/invoices/{invoice_id}     Retrieve an invoice
GET    /billing/accounts/{account_id}/balance  Account balance
POST   /billing/accounts/{account_id}/payments Record an internal payment / settlement
```

### CRM

```
GET    /crm/subscribers                   List subscribers
POST   /crm/subscribers                   Create a subscriber (with MSISDN)
GET    /crm/subscribers/{uuid}            Retrieve a subscriber
GET    /crm/accounts/{account_id}         Retrieve a billing account + delinquency state
```

### Usage

```
POST   /usage/cdrs                        Ingest CDRs (asynchronous)
GET    /usage/cdrs                        List CDRs
GET    /usage/summaries                   Aggregated usage summaries
```

### Catalog

```
GET    /catalog/products                  List products
GET    /catalog/offers                    List offers
GET    /catalog/price-plans               List price plans
```