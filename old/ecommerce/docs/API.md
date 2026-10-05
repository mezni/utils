# API Specification

## Fake E-Commerce Server

> **Status:** Draft — normative contract, not yet implemented.
> **Base path:** `/api/v1` · **Unversioned:** `/health`, `/ready`, `/docs`, `/redoc`, `/openapi.json`
> **Authority:** `.specify/memory/constitution.md` v1.1.1
> **Stack:** Python 3.12+ · FastAPI · Pydantic v2 · SQLAlchemy 2.x · SQLite

---

## Divergences from other documents

Reconciled against the constitution, `architecture.md`, and `database.md`. Every conflict is recorded
and resolved explicitly — none silently (the constitution's *Explicit Business Rules*, III).

| # | Divergence in this draft | Where | Resolution |
|---|---|---|---|
| E1 | "Resource identifiers are opaque integers"; `"id": 1`, `product_id: int`. | §2.8, §5, throughout | **Resolved** — opaque UUID **strings** stored as `TEXT` (`database.md` D3). See §5.1. |
| E2 | "Monetary values are represented as decimal numbers"; `"price": 89.99`. | §2.9, throughout | **Resolved** — integer minor units. This would have regressed decision A5. See §12.2. |
| E3 | Default currency `CAD`. | throughout | **Resolved** — `USD` (`database.md` D4). |
| E4 | Enum values uppercase (`ACTIVE`, `CONFIRMED`, `PENDING`). | throughout | **Resolved** — lowercase snake_case (`database.md` D6). Uppercase survives only in state-transition diagrams. |
| E5 | `products.status` field, `POST /products/{id}/status`, `?status=ACTIVE` filter. | §16 | **Resolved** — `is_active` boolean, updated via `PATCH` (`database.md` D8). A `DISCONTINUOUS` state remains deferred. |
| E6 | Payment methods `CARD` / `BANK_TRANSFER` / `CASH_ON_DELIVERY`. | §23 | **Resolved** — `fake_card` (`database.md` D5). |
| E7 | Category payload drops `slug` and `parent_id`. | §15 | **Resolved** — both kept; they are already published. |
| E8 | Customer splits `first_name`/`last_name`, adds `phone`, flattens the address. | §17 | **Resolved** — `full_name` plus a nested `address` object (`database.md` D9). |
| E9 | "Invalid states such as `reserved_quantity > quantity`". | §18.1 | **Resolved** — the rejection is correct, but the draft's stated reason was not. `quantity_reserved > quantity_on_hand` is invalid because it breaks `quantity_reserved <= quantity_on_hand` (D17), not because the two columns measure different things. |
| E10 | Checkout is `POST /api/v1/carts/{cart_id}/checkout`. | §21, §32, §43 | **Deferred** — this is divergence A6, already deferred to `specs/005-orders`. Interim contract is `POST /api/v1/checkout`. |
| E11 | "A retry creates a new payment attempt". | §22.6 | **Resolved** — one payment per order (`UNIQUE (order_id)`, `database.md` D13). Retry re-attempts the same row; multiple attempts need a separate table. |
| E12 | Failure config example sets `FAKE_FAILURE_RATE=0.10` alone. | §41 | **Resolved** — `FAKE_FAILURE_ENABLED=true` is the master switch; without it the rate is ignored. |
| E13 | `AUTHORIZED`, `DECLINED`, `TIMEOUT`, `PROVIDER_ERROR` returned as one list. | §22.5 | **Resolved** — those are *transport* outcomes, not payment states. They map onto payment states plus `502`/`504`. See §22.5. |
| E14 | Flat pagination envelope (`page`, `total`, `pages` at top level). | §9 | **Resolved** — nested `pagination` object kept, as published. |
| E15 | `sort_by` + `sort_order` query parameters. | §11 | **Resolved** — single `sort` parameter with a `-` prefix for descending. |
| E16 | `search` query parameter. | §10, §16 | **Resolved** — `q` kept, as published. |
| E17 | `error.details` shown as an object. | §12 | **Resolved** — an **array** of field issues, as published. |
| E18 | Drops soft-delete `DELETE` routes, `/customers/me`, cart clear-all, `/orders/{id}/payment`, inventory restock, `capture`/`refund`, the `X-Fake-Failure` header, and the `X-Request-ID` response echo. | throughout | **Resolved** — all restored. This draft was not additive; it narrowed an existing contract. |
| E19 | New endpoints: `GET /api/v1/health`, `POST /api/v1/orders`, `POST /api/v1/payments`, `POST /api/v1/payments/{id}/authorize`. | §20.3, §22.4, §25.2 | **Retained as proposals** — additive, but each needs sign-off in its feature spec. See §48. |
| E20 | `402` appears in the draft's status table but is missing from the previously published list. | §13 | **Resolved** — `402` added. This corrects a genuine inconsistency in the prior draft. |

---

# 1. Purpose

This document defines the HTTP API contract for the Fake E-Commerce Server.

The API is implemented with:

* Python 3.12+
* FastAPI
* Pydantic v2
* SQLAlchemy 2.x
* SQLite
* DDD-inspired modular monolith architecture

The API exposes realistic e-commerce operations while remaining deterministic, testable, and suitable
for:

* Local development
* Integration testing
* Frontend development
* API-client development
* AI-agent and tool-calling experiments
* Failure and resilience testing

All request and response bodies are JSON. All schemas are **explicit Pydantic schemas**. Database
models are never exposed directly as API schemas.

---

# 2. API Design Principles

1. All public endpoints are versioned under `/api/v1`. Operational endpoints (`/health`, `/ready`)
   are deliberately unversioned, because a load balancer must be able to probe them without
   knowing the API version.
2. Request and response models use Pydantic v2.
3. HTTP concerns remain inside the API layer.
4. Business rules belong to the application and domain layers.
5. Route handlers do not manipulate SQLAlchemy models or sessions.
6. Errors use a single consistent envelope (§12).
7. Collection endpoints support pagination (§9).
8. Resource identifiers are **opaque strings**. Clients MUST NOT parse, generate, or rely on their
   internal form (see §5.1).
9. Monetary values are **integer minor units** plus a currency code (§12.2).
10. Timestamps are ISO 8601 / RFC 3339 with an explicit UTC offset.
11. Enumerated values are lowercase `snake_case`.
12. Behavior is deterministic when a seed is configured.
13. Contracts remain backward-compatible within a major version (§45).
14. The API never exposes raw SQL, database errors, or stack traces.

---

# 3. Base URL

Local development:

```text
http://localhost:8000
```

API base path:

```text
/api/v1
```

Example:

```text
http://localhost:8000/api/v1/products
```

`127.0.0.1` and `localhost` are used interchangeably in examples; the server binds to `127.0.0.1`
by default (`docs/configuration.md`).

---

# 4. Content Type

Requests containing JSON bodies must use:

```http
Content-Type: application/json
```

Successful JSON responses use:

```http
Content-Type: application/json
```

A request with a body and a missing or wrong `Content-Type` yields `415 Unsupported Media Type`.

---

# 5. API Versioning

The initial version is `v1`. All product endpoints live under `/api/v1/...`:

```text
GET /api/v1/products
GET /api/v1/products/{product_id}
```

Breaking changes require a new version (`/api/v2/...`). Non-breaking changes — new endpoints, new
optional fields, new filters — may be introduced within `v1`. See §45 for the precise boundary,
because "additive" is the kind of word that quietly loses arguments.

## 5.1 Identifier format — divergence E1 (RESOLVED)

**This draft specifies integer identifiers. The project publishes UUIDs.** Every payload in the
previously published contract uses `"id": "uuid"`, and `architecture.md` §68 defines an
`IdGenerator` port so that generated fixtures are byte-stable across runs.

**Decision: identifiers are opaque UUID strings, accepted and returned as strings.** The
path-parameter handler is typed `str`, and a malformed value yields `422`, not `500`. Storage is
`TEXT` — the single place the decision is recorded is `database.md` D3, so the two documents cannot
disagree.

| | **Opaque UUID string** (adopted) | Integer (rejected) |
|---|---|---|
| Path parameter type | `str` | `int` |
| Already published | Yes | No — breaking |
| Deterministic fixtures | Yes, via `IdGenerator` | Only if sequences are reset |
| Client coupling | None | Leaks volume and ordering |
| Introspection | Not sortable by meaning | Sortable but meaningless |

Integer identifiers are workable and are a legitimate simplification. They are **not** adopted here
because doing so silently rewrites a published contract across this document, `database.md` §7, and
`generator.md`.

---

# 6. Resource Overview

| Resource   | Purpose                | Spec        |
| ---------- | ---------------------- | ----------- |
| Categories | Product categorization | `002`       |
| Products   | Product catalog        | `002`       |
| Customers  | Customer management    | `003`       |
| Carts      | Shopping carts         | `004`       |
| Cart Items | Products inside carts  | `004`       |
| Orders     | Customer purchases     | `005`       |
| Inventory  | Stock management       | `006`       |
| Payments   | Payment attempts       | `007`       |
| Health     | Service health         | `001`       |

## 6.1 Canonical resource schemas

These shapes are referenced by every endpoint below. Field-level details are finalized in each
feature spec, but these are the published contract.

```jsonc
// Money — see §12.2
{ "amount": 2499, "currency": "USD" }

// Category
{
  "id": "uuid",
  "name": "Audio",
  "slug": "audio",
  "parent_id": null
}

// Product
{
  "id": "uuid",
  "sku": "AUDIO-0001",
  "name": "Wireless Earbuds",
  "description": "Synthetic product description.",
  "category_id": "uuid",
  "price": { "amount": 7999, "currency": "USD" },
  "is_active": true,
  "created_at": "2026-09-25T14:03:00Z",
  "updated_at": "2026-09-25T14:03:00Z"
}

// Customer
{
  "id": "uuid",
  "email": "customer1@example.invalid",
  "full_name": "Synthetic Customer One",
  "status": "active",            // active | inactive | suspended
  "address": {
    "line1": "1 Example Street",
    "city": "Springfield",
    "postal_code": "00000",
    "country_code": "US"
  }
}

// Inventory
{
  "product_id": "uuid",
  "sku": "AUDIO-0001",
  "quantity_on_hand": 40,
  "quantity_reserved": 2
}

// CartItem
{ "id": "uuid", "product_id": "uuid", "quantity": 2, "unit_price": { "amount": 7999, "currency": "USD" } }

// Cart
{
  "id": "uuid",
  "customer_id": "uuid",
  "status": "active",            // active | checked_out | abandoned
  "items": [ /* CartItem */ ],
  "subtotal": { "amount": 15998, "currency": "USD" },
  "created_at": "2026-09-25T14:03:00Z"
}

// OrderItem — price is snapshotted at order time
{ "id": "uuid", "product_id": "uuid", "quantity": 2, "unit_price": { "amount": 7999, "currency": "USD" }, "line_total": { "amount": 15998, "currency": "USD" } }

// Order
{
  "id": "uuid",
  "customer_id": "uuid",
  "status": "confirmed",         // pending|confirmed|processing|shipped|delivered|cancelled|failed
  "items": [ /* OrderItem */ ],
  "total": { "amount": 15998, "currency": "USD" },
  "created_at": "2026-09-25T14:03:00Z"
}

// Payment
{
  "id": "uuid",
  "order_id": "uuid",
  "status": "authorized",        // pending|authorized|captured|declined|failed|refunded
  "amount": { "amount": 15998, "currency": "USD" },
  "method": "fake_card",         // synthetic only
  "transaction_id": "faketxn_0001",
  "created_at": "2026-09-25T14:03:00Z"
}
```

> Order item prices are **snapshotted** at order time so a later catalog price change never rewrites
> order history. Order totals are always recomputed server-side from those snapshots, never trusted
> from the request body.

---

# 7. HTTP Methods

| Method | Meaning                                     |
| ------ | ------------------------------------------- |
| `GET`  | Retrieve resources                          |
| `POST` | Create a resource or execute a command      |
| `PUT`  | Replace a resource                          |
| `PATCH`| Partially update a resource                 |
| `DELETE`| Remove or deactivate a resource            |

Business operations that are not CRUD use explicit action endpoints rather than overloading
`PATCH`:

```text
POST /api/v1/checkout
POST /api/v1/orders/{order_id}/cancel
POST /api/v1/payments/{payment_id}/capture
POST /api/v1/payments/{payment_id}/refund
```

The verb in the path states the transition, so the endpoint name reads as the operation it performs.

---

# 8. Standard Headers

Clients should send:

```http
Accept: application/json
Content-Type: application/json
```

### 8.1 Request correlation

Clients may supply:

```http
X-Request-ID: <request-id>
```

If absent, the server generates one. The server **always** echoes it back:

```http
X-Request-ID: 7d3f4a2e-...
```

and includes the same value in the `request_id` field of every error body (§12). A supplied value is
used as-is after validation, so a client can correlate its own logs with the server's.

### 8.2 Failure injection

```http
X-Fake-Failure: <scenario>
```

Forces a named failure scenario for one request. Ignored entirely when failure simulation is
disabled. See `docs/failure-simulation.md` — this is the single most useful affordance for testing
compensation paths, and it is why the header is part of the contract rather than an internal detail.

### 8.3 Idempotency

```http
Idempotency-Key: <unique-key>
```

Accepted on state-creating `POST`s where retries are plausible, primarily checkout (§32).

---

# 9. Pagination

Collection endpoints use offset pagination.

| Query param | Default | Rules |
|---|---|---|
| `page` | `1` | `ge=1` |
| `page_size` | `20` | `1..100`; out of range → `422` |

Response envelope — **nested** (divergence E14):

```json
{
  "items": [],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_items": 137,
    "total_pages": 7
  }
}
```

```text
PaginatedResponse[T]
  items:      list[T]
  pagination:
    page:         int
    page_size:    int
    total_items:  int
    total_pages:  int
```

Nesting the counters keeps room to add `has_next` or cursor links later without adding siblings to
every collection response, and it makes the shape obvious to a client: `items` is the data,
`pagination` is the metadata about the data.

`total_items` requires a `COUNT(*)` per request. That is acceptable at current data volumes
(`generator.md` §4); if it ever is not, a cursor-based scheme is the change, and it is a breaking
change to the envelope.

---

# 10. Filtering

Filters are explicit query parameters per resource — never a free-form query language. Each endpoint
declares its own supported filters.

```text
GET /api/v1/products?category_id=...&is_active=true&min_price=1000&max_price=50000
GET /api/v1/products?q=wireless%20keyboard
```

* `q` performs a substring match on name, and on description where useful (§36).
* `min_price` / `max_price` are **integer minor units**, matching §12.2. `min_price=1000` means
  $10.00.
* Enumerated filter values MUST be constrained; an invalid enum yields `422`, not an empty result.
* Unknown query parameters are **ignored** rather than rejected, so a client written against a
  newer version still works against this one. This satisfies the requirement that an unrecognized
  parameter never silently change results — it changes nothing at all.

---

# 11. Sorting

A single `sort` parameter (divergence E15):

```text
sort=name
sort=-created_at
```

A `-` prefix means descending. Supported fields MUST be documented per endpoint. An unsupported
`sort` field yields `422` — silently ignoring it would return results in an order the client did not
ask for, which is worse than an error.

---

# 12. Common Error Response

All expected errors use one envelope. Error codes are stable identifiers.

```json
{
  "error": {
    "code": "INSUFFICIENT_INVENTORY",
    "message": "Insufficient inventory for product",
    "details": [
      { "field": "quantity", "issue": "requested 5, available 2" }
    ],
    "request_id": "7d3f4a2e-..."
  }
}
```

| Field        | Type            | Required | Description                             |
| ------------ | --------------- | :------: | --------------------------------------- |
| `code`       | string          |   yes    | Stable machine-readable code            |
| `message`    | string          |   yes    | Human-readable, safe for a developer    |
| `details`    | array of issues |    no    | Field-level specifics                   |
| `request_id` | string          |   yes    | Matches the `X-Request-ID` header       |

`details` is an **array** (divergence E17) because a request can fail in several ways at once, and a
single object forces an arbitrary choice about which failure to report.

`code` is screaming snake case, never localized, never reworded. Clients branch on `code`, not on
`message`.

## 12.1 Error codes

| Code                        | HTTP  | Meaning                                        |
| --------------------------- | ----: | ---------------------------------------------- |
| `VALIDATION_ERROR`          |   422 | Request failed schema or semantic validation   |
| `NOT_FOUND`                 |   404 | Resource does not exist                        |
| `CONFLICT`                  |   409 | Generic uniqueness or state conflict           |
| `DUPLICATE_SKU`             |   409 | SKU already taken                              |
| `DUPLICATE_EMAIL`           |   409 | Email already registered                       |
| `EMPTY_CART`                |   409 | Checkout attempted with no items               |
| `INSUFFICIENT_INVENTORY`    |   409 | Not enough stock to reserve                    |
| `PRODUCT_INACTIVE`          |   409 | Product exists but is not purchasable          |
| `INVALID_ORDER_TRANSITION`  |   409 | Order state change is not allowed              |
| `INVALID_PAYMENT_TRANSITION`|   409 | Payment state change is not allowed            |
| `PAYMENT_DECLINED`          |   402 | Provider declined the charge                   |
| `PAYMENT_PROVIDER_ERROR`    |   502 | Fake provider returned an unexpected error     |
| `PAYMENT_PROVIDER_TIMEOUT`  |   504 | Fake provider did not respond in time          |
| `UNAUTHENTICATED`           |   401 | Missing or invalid credentials                 |
| `FORBIDDEN`                 |   403 | Authenticated but not permitted                |
| `SIMULATED_FAILURE`         | varies | Injected via `X-Fake-Failure`                 |
| `INTERNAL_ERROR`            |   500 | Unexpected server fault                        |

Internal exception types and stack traces MUST NOT be exposed. Domain, application, infrastructure,
and HTTP errors are distinguished internally and mapped to codes at the API boundary only.

## 12.2 Money representation — divergence E2

**This draft specifies decimal money (`"price": 89.99`). That would regress decision A5.** Money on
the wire is an **integer count of minor units** plus a currency code:

```json
{ "amount": 2499, "currency": "USD" }
```

`amount: 2499` means $24.99.

`float` and `Decimal` are both prohibited as the wire representation, including as JSON numbers and
as numeric strings. A JSON number has no type distinction between `89.99` and a float; sending money
as a bare JSON number makes it impossible for a client to know whether the server rounded. The
explicit `amount`/`currency` pair removes the ambiguity, and the integer guarantees the total
computed by a JavaScript client equals the total computed by the server.

Rationale and the full trade-off table: `architecture.md` §20.1. Storage: `database.md` §13.

---

# 13. HTTP Status Codes

| Status | Meaning                                    |
| -----: | ------------------------------------------ |
|    200 | Successful request with a body             |
|    201 | Resource created (with `Location` header)  |
|    204 | Successful request, no body                |
|    400 | Malformed request                          |
|    401 | Authentication required                    |
|    402 | Payment required / declined                |
|    403 | Permission denied                         |
|    404 | Resource not found                         |
|    409 | Business or state conflict                |
|    415 | Unsupported media type                     |
|    422 | Validation error                           |
|    429 | Rate limit exceeded, if ever enabled       |
|    500 | Unexpected server error                    |
|    502 | External (fake) provider failure           |
|    503 | Service temporarily unavailable            |
|    504 | External (fake) provider timeout           |

`402` was missing from the previously published status list while the error-code table already used
it for `PAYMENT_DECLINED`; adding it resolves that inconsistency (divergence E20). `502`/`504` exist
because the fake payment provider is a real network boundary with a real timeout, and collapsing
provider failure into `500` would make provider faults indistinguishable from server bugs.

---

# 14. Validation Errors

Pydantic validation failures are normalized into the §12 envelope:

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Request validation failed",
    "details": [
      { "field": "quantity", "issue": "Input should be greater than 0" }
    ],
    "request_id": "7d3f4a2e-..."
  }
}
```

Validation happens at the API boundary. Business invariants stay in the domain and application
layers — a Pydantic validator MUST NOT query the database to check whether a product exists; that is
a `404` from the application layer, and a validator that does I/O turns a cheap rejection into an
expensive one.

---

# 15. Categories

### 15.1 List Categories

```http
GET /api/v1/categories
```

Query: `page`, `page_size`, `sort`.

```json
{
  "items": [
    {
      "id": "uuid",
      "name": "Electronics",
      "slug": "electronics",
      "parent_id": null,
      "description": "Electronic devices and accessories",
      "created_at": "2026-09-25T14:03:00Z",
      "updated_at": "2026-09-25T14:03:00Z"
    }
  ],
  "pagination": { "page": 1, "page_size": 20, "total_items": 1, "total_pages": 1 }
}
```

### 15.2 Get Category

```http
GET /api/v1/categories/{category_id}
```

### 15.3 Create Category

```http
POST /api/v1/categories
```

```json
{ "name": "Electronics", "slug": "electronics", "description": "Electronic devices and accessories" }
```

`201` with a `Location` header.

### 15.4 Update Category

```http
PATCH /api/v1/categories/{category_id}
```

```json
{ "name": "Consumer Electronics" }
```

### 15.5 Delete Category

```http
DELETE /api/v1/categories/{category_id}
```

`204` on success. If any product still references the category, the delete is **rejected** with
`409 CONFLICT`. There is no soft-delete for categories: `categories` has no `is_active` column
(`database.md` §10), so a category must be emptied of products before it can be removed. Contrast
products, which deactivate via `is_active = 0` (`API.md` §16.5).

---

# 16. Products

### 16.1 List Products

```http
GET /api/v1/products
```

Query: `page`, `page_size`, `category_id`, `is_active`, `min_price`, `max_price`, `q`, `sort`.

```http
GET /api/v1/products?category_id=uuid&is_active=true&page=1&page_size=20
```

```json
{
  "items": [
    {
      "id": "uuid",
      "category_id": "uuid",
      "sku": "ELEC-0001",
      "name": "Wireless Keyboard",
      "description": "Wireless mechanical keyboard",
      "price": { "amount": 8999, "currency": "USD" },
      "is_active": true,
      "created_at": "2026-09-25T14:03:00Z",
      "updated_at": "2026-09-25T14:03:00Z"
    }
  ],
  "pagination": { "page": 1, "page_size": 20, "total_items": 1, "total_pages": 1 }
}
```

### 16.2 Get Product

```http
GET /api/v1/products/{product_id}
```

### 16.3 Create Product

```http
POST /api/v1/products
```

```json
{
  "category_id": "uuid",
  "sku": "ELEC-0001",
  "name": "Wireless Keyboard",
  "description": "Wireless mechanical keyboard",
  "price": { "amount": 8999, "currency": "USD" }
}
```

`201`. `409 DUPLICATE_SKU` if the SKU is taken.

### 16.4 Update Product

```http
PATCH /api/v1/products/{product_id}
```

```json
{ "price": { "amount": 7999, "currency": "USD" } }
```

Price changes affect **future** orders only. Existing order snapshots are never rewritten
(§6.1).

### 16.5 Deactivate Product — divergence E5

This draft proposed a `status` field with a dedicated transition endpoint:

```http
POST /api/v1/products/{product_id}/status
{ "status": "INACTIVE" }
```

**Not adopted.** `is_active` is the published field (§6.1), and it is a boolean, so a two-state
transition endpoint adds a route and a request body to express something `PATCH` already expresses.
A three-state lifecycle (`discontinued`, meaning still visible in history but no longer sellable)
remains deferred to `specs/002-product-catalog` (`database.md` D8, §12).

```http
PATCH /api/v1/products/{product_id}
{ "is_active": false }
```

### 16.6 Delete Product

```http
DELETE /api/v1/products/{product_id}
```

`204`. Deactivates; never deletes a product referenced by an order (`database.md` §35).

---

# 17. Customers

### 17.1 List Customers

```http
GET /api/v1/customers
```

Admin only. Query: `page`, `page_size`, `status`, `q`, `sort`.

### 17.2 Get Current Customer

```http
GET /api/v1/customers/me
```

Returns the authenticated customer. Preferred over `GET /customers/{id}` for self-access, because it
cannot be pointed at another customer's id by a confused or malicious client.

### 17.3 Get Customer

```http
GET /api/v1/customers/{customer_id}
```

`403` for another customer's id.

### 17.4 Create Customer

```http
POST /api/v1/customers
```

```json
{
  "email": "alex.morgan@example.invalid",
  "full_name": "Alex Morgan",
  "address": {
    "line1": "100 Main Street",
    "city": "Kingston",
    "postal_code": "K7K 1A1",
    "country_code": "CA"
  }
}
```

`201`; `409 DUPLICATE_EMAIL` if taken.

The `example.invalid` TLD is reserved by RFC 2606 and can never resolve, so example data cannot
accidentally reach a real mail server. All example and generated contact data MUST be synthetic.

This draft split the name into `first_name`/`last_name` and added a `phone` field. **Not adopted** —
`full_name` is published, and neither `generator.md` nor `PRD.md` currently requires a phone number
(`database.md` D9).

### 17.5 Update Customer

```http
PATCH /api/v1/customers/{customer_id}
```

Updating the profile address does **not** alter the shipping address stored on existing orders —
each order carries its own snapshot (`database.md` D12).

### 17.6 Delete Customer

```http
DELETE /api/v1/customers/{customer_id}
```

`204`. Deactivates (`status = inactive`); a customer with order history is never physically deleted.

---

# 18. Inventory

Inventory is owned by the Inventory context; the Product Catalog owns only `is_active`. A product's
purchasability is `is_active AND available_to_sell > 0` — never a single catalog field
(`architecture.md` §7.1).

### 18.1 Get Product Inventory

```http
GET /api/v1/inventory/{product_id}
```

```json
{
  "product_id": "uuid",
  "sku": "ELEC-0001",
  "quantity_on_hand": 100,
  "quantity_reserved": 15
}
```

Available-to-sell is **derived**, never stored:

```text
available_to_sell = quantity_on_hand - quantity_reserved
```

The invariants are `quantity_on_hand >= 0`, `quantity_reserved >= 0`, and
`quantity_reserved <= quantity_on_hand` (`database.md` §23). The last one is what makes the derived
value non-negative; a stored "available" column would be a third copy of the same fact, free to drift
from the other two.

The draft rejected `reserved_quantity > quantity` as an invalid state (divergence E9). That rejection
is **correct** — `quantity_reserved > quantity_on_hand` is exactly the out-of-stock-but-reserved state,
and it is invalid for precisely the reason the D17 naming makes visible: a reservation must be
backed by physical stock. It is invalid because it breaks `quantity_reserved <= quantity_on_hand`,
not because reserved stock is conceptually a different thing from on-hand stock. The draft reached
the right verdict from a wrong premise, and this section previously preserved the wrong premise.

### 18.2 List Inventory

```http
GET /api/v1/inventory
```

Admin only. Paginated.

### 18.3 Restock

```http
POST /api/v1/inventory/{product_id}/stock
```

```json
{ "quantity": 50 }
```

Admin only. Adds stock. This is the only route that may increase `quantity_on_hand`; the response
reflects the new totals.

### 18.4 Reserve Inventory

```http
POST /api/v1/inventory/{product_id}/reserve
```

```json
{ "quantity": 2 }
```

Service-authenticated. Called by checkout; not normally called by a client directly.

```json
{ "product_id": "uuid", "quantity_on_hand": 100, "quantity_reserved": 17 }
```

Insufficient stock:

```http
409 Conflict
```

```json
{
  "error": {
    "code": "INSUFFICIENT_INVENTORY",
    "message": "Insufficient inventory for product",
    "details": [ { "field": "quantity", "issue": "requested 20, available 5" } ],
    "request_id": "7d3f4a2e-..."
  }
}
```

### 18.5 Release Inventory

```http
POST /api/v1/inventory/{product_id}/release
```

```json
{ "quantity": 2 }
```

Service-authenticated. Used by the checkout compensation path.

---

# 19. Carts

### 19.1 Create Cart

```http
POST /api/v1/carts
```

Customer-authenticated. The customer is taken from the token, **not** from a request body, so a
client cannot create a cart for someone else. Returns `201` with an empty `active` cart.

```json
{
  "id": "uuid",
  "customer_id": "uuid",
  "status": "active",
  "items": [],
  "subtotal": { "amount": 0, "currency": "USD" },
  "created_at": "2026-09-25T14:03:00Z",
  "updated_at": "2026-09-25T14:03:00Z"
}
```

This draft's request body, `{ "customer_id": 10 }`, is **not** adopted — see §31.

### 19.2 Get Cart

```http
GET /api/v1/carts/{cart_id}
```

Owner or admin. `403` otherwise.

### 19.3 Add Cart Item

```http
POST /api/v1/carts/{cart_id}/items
```

```json
{ "product_id": "uuid", "quantity": 2 }
```

Rules:

* `quantity` must be `>= 1`.
* The product must exist, else `404`.
* The product must be active, else `409 PRODUCT_INACTIVE`.
* The cart must be `active`, else `409`.
* An existing line for the same product has its `quantity` incremented — never a second row
  (`database.md` §20).

Inventory is **not** checked or reserved here. A cart is a wish list; reserving at add-time would
hold stock for a cart that may never be checked out.

### 19.4 Update Cart Item

```http
PATCH /api/v1/carts/{cart_id}/items/{item_id}
```

```json
{ "quantity": 3 }
```

### 19.5 Remove Cart Item

```http
DELETE /api/v1/carts/{cart_id}/items/{item_id}
```

`204`.

### 19.6 Clear Cart

```http
DELETE /api/v1/carts/{cart_id}/items
```

`204`. Removes all items. Distinct from deleting the cart, which does not exist as an operation.

---

# 20. Orders

### 20.1 List Orders

```http
GET /api/v1/orders
```

Query: `page`, `page_size`, `customer_id`, `status`, `created_from`, `created_to`, `sort`.

A customer sees only their own orders; `customer_id` is not a way to see someone else's.

### 20.2 Get Order

```http
GET /api/v1/orders/{order_id}
```

Owner or admin.

```json
{
  "id": "uuid",
  "customer_id": "uuid",
  "cart_id": "uuid",
  "status": "confirmed",
  "total": { "amount": 15998, "currency": "USD" },
  "shipping_address": {
    "line1": "100 Market St",
    "city": "Springfield",
    "postal_code": "62704",
    "country_code": "US"
  },
  "items": [
    {
      "id": "uuid",
      "product_id": "uuid",
      "quantity": 2,
      "unit_price": { "amount": 7999, "currency": "USD" },
      "line_total": { "amount": 15998, "currency": "USD" }
    }
  ],
  "created_at": "2026-09-25T14:03:00Z"
}
```

`cart_id` is `null` for an order created directly rather than through checkout (D11). It is otherwise
always present, and is `UNIQUE` in storage — a cart yields at most one order.

`shipping_address` is the **snapshot taken at checkout**, not a live join against the customer profile
(D12). A later `PATCH /api/v1/customers/{customer_id}` that changes the profile address does not
change the value returned here.

### 20.3 Create Order Directly — divergence E19

```http
POST /api/v1/orders
```

```json
{ "customer_id": "uuid", "items": [ { "product_id": "uuid", "quantity": 2 } ] }
```

**This is a proposed addition, not a published route.** The preferred and primary path is checkout
(§21). A direct-create route is genuinely useful for fixtures and administrative use, and this draft
argues for it correctly — but it creates a second way to reach every order invariant, so it needs
explicit sign-off in `specs/005-orders` before it is treated as contract.

If adopted, it MUST run the same validation, inventory reservation, and payment path as checkout. A
route that bypasses them would let a caller create an order for stock that does not exist.

### 20.4 Cancel Order

```http
POST /api/v1/orders/{order_id}/cancel
```

```json
{ "reason": "Customer requested cancellation" }
```

Only from a cancellable state. Anything else → `409 INVALID_ORDER_TRANSITION`. Cancelling a
`shipped` or `delivered` order is a returns process, not a cancellation.

---

# 21. Checkout — divergence E10

Checkout is an application-level command, not CRUD. It spans four tables and two bounded contexts
(`architecture.md` §6).

### 21.1 Route

**Interim contract: `POST /api/v1/checkout`.**

```http
POST /api/v1/checkout
```

```json
{ "cart_id": "uuid", "payment_method": "fake_card" }
```

This draft specifies `POST /api/v1/carts/{cart_id}/checkout`. That is divergence **A6**, which was
**deferred to `specs/005-orders`** and has not been decided. Until that spec resolves it,
`POST /api/v1/checkout` is the published route in this document, `README.md`,
`docs/failure-simulation.md`, and `docs/database.md` §49. The two forms are recorded with their
trade-offs in `architecture.md` §60.1.

### 21.2 Workflow

```text
Validate cart is active and non-empty
    ↓
Validate customer may purchase
    ↓
Validate every product still exists and is active
    ↓
Resolve pricing                              (see below)
    ↓
Reserve inventory
    ↓
Create order + snapshotted order items
    ↓
Create payment (pending)
    ↓
Authorize payment
    ├── declined/failed ──> release reservation, mark order failed
    └── authorized ──> confirm order, mark cart checked_out
COMMIT
```

The whole sequence runs in one transaction, with an explicit compensation path for failures that
occur after the transaction commits (`docs/failure-simulation.md`).

**Pricing is not yet decided.** Whether checkout honours the cart's snapshotted price or the current
catalog price is deferred to `specs/005-orders` alongside A6 (`database.md` D15). Until then the cart
snapshot is display-only.

### 21.3 Success

```http
201 Created
```

```json
{
  "order": {
    "id": "uuid",
    "customer_id": "uuid",
    "status": "confirmed",
    "total": { "amount": 15998, "currency": "USD" }
  },
  "payment": {
    "id": "uuid",
    "order_id": "uuid",
    "status": "authorized",
    "amount": { "amount": 15998, "currency": "USD" },
    "method": "fake_card",
    "transaction_id": "faketxn_0001"
  }
}
```

Returning both resources is deliberate: a client that only wants the order id should not have to
parse it out of a nested structure it does not use.

### 21.4 Failure

```json
{
  "error": {
    "code": "PAYMENT_DECLINED",
    "message": "Payment authorization was declined",
    "details": [ { "field": "payment", "issue": "card declined by issuer" } ],
    "request_id": "7d3f4a2e-..."
  }
}
```

A failed checkout MUST NOT leave inconsistent state: no order in `pending`, no reserved inventory, no
payment row stranded outside a terminal state. `docs/failure-simulation.md` asserts this for every
scenario.

---

# 22. Payments

All payment data is synthetic. The server MUST NOT accept, store, or process real payment
credentials.

### 22.1 Get Payment

```http
GET /api/v1/payments/{payment_id}
```

### 22.2 List Payments

```http
GET /api/v1/payments?order_id=uuid
```

Query: `order_id`, `status`, `page`, `page_size`, `sort`. Returns the standard nested `meta`
pagination envelope (§9).

Since a payment is 1:1 with an order (`database.md` D13), this collection returns at most one payment
per order. Most clients should prefer §22.4, which addresses the payment by order id directly and
needs no filter.

### 22.3 Get Payment for Order

```http
GET /api/v1/orders/{order_id}/payment
```

The convenient inverse of "list payments by order" (§22.2), and it returns at most one payment per
order (`database.md` D13).

### 22.4 Create Payment — divergence E19

```http
POST /api/v1/payments
```

```json
{ "order_id": "uuid", "amount": { "amount": 15998, "currency": "USD" }, "method": "fake_card" }
```

**Proposed addition, not a published route.** Payments are normally created by checkout. A
standalone create route is useful for controlled testing, but it can create a payment for an order
that was never reserved or charged, so it needs sign-off in `specs/007-payments` and a stated
relationship to order state.

### 22.5 Authorize Payment — divergence E13, E19

```http
POST /api/v1/payments/{payment_id}/authorize
```

**Proposed addition.** Authorization normally happens inside checkout.

The fake provider can return four outcomes. They are **transport results, not payment states**, and
this draft's flattening of them into one list is the mistake worth avoiding:

| Provider outcome | Payment `status` | HTTP |
| ---------------- | ---------------- | ---- |
| approved | `authorized` | `200` |
| declined | `declined` | `402` |
| provider error | `failed` | `502` `PAYMENT_PROVIDER_ERROR` |
| timeout | `failed` | `504` `PAYMENT_PROVIDER_TIMEOUT` |

A `payment.status` of `authorized` and an HTTP status of `200` are not the same claim. Conflating a
provider timeout with a decline would make a retry strategy impossible to write: a decline should
not be retried, a timeout should be.

Failure injection is driven by configuration and the `X-Fake-Failure` header
(`docs/failure-simulation.md`).

### 22.6 Retry Payment — divergence E11

```http
POST /api/v1/payments/{payment_id}/retry
```

Re-attempts authorization on the **same** payment row.

This draft states that a retry creates a new payment attempt. That would require dropping
`UNIQUE (order_id)` (`database.md` D13) and adding a payment-attempt history table. A single row that
retries is simpler and sufficient while the project has one payment per order; if
`specs/007-payments` later needs multiple attempts — distinct provider transactions, partial
capture — that is a schema change with its own migration, tracked in `database.md` §68.

A retry is only valid from `pending` or `failed`. Retrying a `declined` payment requires a new
payment method, and a retry of a `captured` payment is `409 INVALID_PAYMENT_TRANSITION`.

### 22.7 Capture Payment

```http
POST /api/v1/payments/{payment_id}/capture
```

`authorized` → `captured`. Admin only.

### 22.8 Refund Payment

```http
POST /api/v1/payments/{payment_id}/refund
```

`captured` → `refunded`. Admin only. A refund from `authorized` is a void, not a refund, and is a
different operation.

---

# 23. Payment Methods — divergence E6

Initial supported method:

```text
fake_card
```

This draft proposes `CARD`, `BANK_TRANSFER`, `CASH_ON_DELIVERY`. **Not adopted** — `fake_card` is the
published value, and the `fake_` prefix is deliberate: it makes it impossible to confuse a
simulated payment with a real one in a log, a test fixture, or a screenshot.

`fake_bank_transfer` and `fake_wallet` are the natural extensions if `specs/007-payments` adds them.

No real card numbers, bank credentials, or payment tokens are ever accepted or stored.

---

# 24. Payment Statuses

```text
pending
authorized
captured
declined
failed
refunded
```

```text
PENDING
   ├──► AUTHORIZED ──► CAPTURED ──► REFUNDED
   ├──► DECLINED
   └──► FAILED
```

Any transition not drawn above MUST be rejected with `409 INVALID_PAYMENT_TRANSITION`.

`CAPTURED` exists because capture and refund are separate endpoints (§22.7, §22.8). Without it, a
capture followed by a refund is indistinguishable from an authorization that was never captured, and
the system cannot report authorized-but-uncaptured exposure.

---

# 25. Health Endpoints

### 25.1 Basic Health

```http
GET /health
```

```json
{ "status": "ok" }
```

Unversioned, unauthenticated, no database access — it reports that the process is alive, which is
what a liveness probe needs to know.

### 25.2 Readiness — divergence E19

```http
GET /ready
```

```json
{ "status": "ok", "version": "1.0.0", "database": "ok" }
```

Planned, not required for the initial implementation. Checks database connectivity.

This draft proposes `GET /api/v1/health` returning a version. **Not adopted**: a versioned health
endpoint cannot be probed by a load balancer that does not know the version, and duplicating
liveness under `/api/v1` invites the two to disagree. The version belongs on `/ready`.

## Description switches (FR-004)

The table below records the served URLs and the settings that control their availability:

| URL | Controlling setting | Default | Behaviour when `false` |
|---|---|---|---|
| `/docs` | `API_DOCS_ENABLED` | `true` | Returns 404; schema not retrievable at alternate path |
| `/redoc` | `API_REDOC_ENABLED` | `true` | Returns 404 |
| `/openapi.json` | `API_DOCS_ENABLED` | `true` | Returns 404 |

Both switches are independently controllable convenience controls, not a security boundary
(research D-05). Turning one off does not make the other's schema retrievable at an alternate path.

---

# 26. OpenAPI

FastAPI generates the OpenAPI document, which is the machine-readable contract:

```text
/docs          Swagger UI
/redoc         ReDoc
/openapi.json  OpenAPI schema
```

Route metadata MUST be filled in — `summary`, `description`, `response_model`, `status_code`, and
documented error responses — so the generated document is usable without reading this file.

---

# 27. API Schema Organization

```text
src/ecommerce/api/v1/
├── router.py
├── dependencies.py
├── health.py
├── products.py
├── categories.py
├── customers.py
├── carts.py
├── orders.py
├── inventory.py
└── payments.py
```

```text
src/ecommerce/api/v1/schemas/
├── common.py        # Money, PaginatedResponse, ErrorResponse
├── category.py
├── product.py
├── customer.py
├── cart.py
├── order.py
├── inventory.py
└── payment.py
```

Request and response schemas are **separate types**. A single schema reused for both directions lets
a response field leak into the accepted request body — which is how a client ends up able to set its
own `status` or `total`.

---

# 28. API Layer Dependency Flow

```text
HTTP Request
    ↓
FastAPI Router
    ↓
Pydantic Request Schema
    ↓
Application Command / Query
    ↓
Domain Logic
    ↓
Repository / Unit of Work
    ↓
Infrastructure
    ↓
SQLite
```

The API layer MUST NOT do this:

```text
HTTP Request → Router → SQLAlchemy Session → Database
```

Business logic does not live in route handlers. A handler's job is to translate HTTP into a command
or query, call one application method, and translate the result back.

---

# 29. Dependency Injection

FastAPI dependencies provide database sessions, the Unit of Work, application services, the
authentication context, the request id, and configuration.

```python
@router.get("/products/{product_id}", response_model=ProductResponse)
async def get_product(
    product_id: str,
    service: ProductService = Depends(get_product_service),
) -> ProductResponse:
    return await service.get(product_id)
```

`product_id` is typed `str` per §5.1, and the response model is explicit — an untyped handler
returns whatever the ORM happens to have loaded, which is how internal fields reach the wire.

The router stays responsible for HTTP concerns only.

---

# 30. Authentication

Authentication is fake and introduced as a separate capability (`specs/010-authentication`). Never
real credentials.

```http
Authorization: Bearer <fake-token>
```

Roles:

```text
customer
admin
service
```

| Route group | Auth |
|---|---|
| Catalog reads, `/health`, `/ready`, `/docs` | public |
| Own cart, orders, payments, profile | `customer` |
| Product, category, inventory management; all-orders read | `admin` |
| Programmatic checkout, inventory reserve/release | `service` |

All example tokens in this document are synthetic.

---

# 31. Authorization

Authorization is evaluated at the application boundary, never in the route body.

```text
customer → own cart, own orders, own payments, own profile
admin    → products, categories, inventory, customers, all orders
service  → checkout, inventory reserve/release, bulk reads
```

**The authenticated customer is always taken from the token, never from the request body.** This
draft's `POST /api/v1/carts` with `{ "customer_id": 10 }` is not adopted: a body-supplied customer
id is a horizontal privilege escalation waiting to happen, and it is trivially avoided by never
accepting the field.

A customer MUST NOT read or mutate another customer's cart, orders, or profile; such an attempt is
`403`, not `404`, so the client learns its token lacks the permission.

The exact authorization matrix is finalized in `specs/010-authentication`.

---

# 32. Idempotency

State-creating `POST`s that a client may retry SHOULD accept:

```http
Idempotency-Key: <unique-key>
```

The primary candidate is checkout:

```http
POST /api/v1/checkout
Idempotency-Key: checkout-cart-001
```

Repeating a request with the same valid key MUST NOT create a second order; the original response is
replayed instead. Without this, a client that times out mid-checkout and retries gets two orders and
two reservations for one cart.

Which routes require the header versus merely accept it, the key scope, and the retention period are
deferred to `specs/005-orders`. It depends on the deferred A6 route shape, and the idempotency
storage is listed in `database.md` §68.

---

# 33. Error Taxonomy

Errors are classified internally and mapped to HTTP codes at the API boundary only.

### Domain errors

```text
INVALID_QUANTITY
INVALID_ORDER_STATE
INVALID_PAYMENT_STATE
INSUFFICIENT_INVENTORY
PRODUCT_INACTIVE
EMPTY_CART
```

### Application errors

```text
DUPLICATE_SKU
DUPLICATE_EMAIL
CART_NOT_CHECKOUTABLE
NOT_FOUND
```

### Infrastructure errors

```text
DATABASE_ERROR
PAYMENT_PROVIDER_ERROR
PAYMENT_PROVIDER_TIMEOUT
```

### API errors

```text
VALIDATION_ERROR
UNAUTHENTICATED
FORBIDDEN
```

Each maps to a code in §12.1. A new error code is a **non-breaking** addition (§45); changing the
meaning or status of an existing one is breaking.

---

# 34. Resource Not Found

```http
GET /api/v1/products/00000000-0000-0000-0000-000000000000
```

```http
404 Not Found
```

```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "Product not found",
    "details": [],
    "request_id": "7d3f4a2e-..."
  }
}
```

A **malformed** identifier yields `422 VALIDATION_ERROR`, not `404` and never `500` — the request
was syntactically wrong, and conflating that with absence hides client bugs behind retry logic.

---

# 35. Conflict Handling

`409 Conflict` means the request was well-formed and authorized, but the current state forbids it.

```text
Adding an item to a checked-out cart
Cancelling an already delivered order
Reserving unavailable inventory
Authorizing an already captured payment
Creating a duplicate SKU or email
Checking out an empty cart
```

`409` is the right code for all of these and each carries a specific `code` from §12.1, so a client
can distinguish "retry later" from "never retry".

---

# 36. Search

Product search starts as database-backed substring matching:

```http
GET /api/v1/products?q=wireless%20keyboard
```

`q` matches name, and description where useful. Implemented with `LIKE` against SQLite.

A dedicated search engine can be introduced later without changing the external contract, because
the parameter name and response shape stay fixed. Full-text search is deferred
(`database.md` §68); at 5,000 products `LIKE` is not the bottleneck.

---

# 37. API Filtering Examples

```text
GET /api/v1/products?is_active=true
GET /api/v1/products?category_id=uuid
GET /api/v1/products?min_price=2000&max_price=10000
GET /api/v1/products?q=keyboard&sort=-price
GET /api/v1/orders?customer_id=uuid
GET /api/v1/orders?status=shipped&created_from=2026-09-01T00:00:00Z
GET /api/v1/payments?order_id=uuid
GET /api/v1/inventory?low_stock=true
```

`min_price` and `max_price` are integer minor units. Filter values are validated against their
enumerations.

---

# 38. API Consistency Rules

Every endpoint must:

* Use an explicit request schema and an explicit response schema.
* Validate input and return appropriate status codes.
* Produce structured errors in the §12 envelope.
* Include `X-Request-ID` in the response and `request_id` in every error body.
* Expose no database implementation detail, internal identifiers, or stack traces.
* Expose no secrets, including in error paths.
* Respect domain invariants rather than reimplementing them.
* Document itself in OpenAPI metadata (§46).

---

# 39. API and Domain Separation

A request body of

```json
{ "product_id": "uuid", "quantity": 2 }
```

MUST NOT require the caller to understand internal domain objects.

The following MUST NOT appear anywhere in the public contract:

```text
SQLAlchemy Session
SQLAlchemy declarative model
Database row
Internal exception class names
Internal enum member names that differ from the wire values
```

Response schemas are built from domain/application read models, never serialized ORM instances. A
serialized ORM instance silently becomes part of the contract, and the next schema change becomes a
breaking API change.

---

# 40. API and AI-Agent Readiness

The API is deliberately suitable for AI-agent and tool-calling experiments. Candidate tools:

```text
list_products        get_product        search_products
get_inventory        create_cart        add_cart_item
get_cart             checkout_cart      get_order
list_orders          get_payment
```

Each tool maps to a stable application-level operation, and each returns the same structured errors
as the HTTP API, so an agent can distinguish "out of stock" from "bad request" without parsing prose.

The API MUST NOT expose unrestricted database operations:

```text
execute_sql
drop_database
delete_all_products
bulk_truncate
```

An agent that can run arbitrary SQL defeats every invariant in the domain layer, and the point of
this project is that the invariants hold.

---

# 41. Failure Simulation

Failure simulation affects external provider operations and is driven by
`docs/configuration.md`:

| Variable                 | Default | Purpose                                        |
| ------------------------ | ------- | ---------------------------------------------- |
| `FAKE_FAILURE_ENABLED`   | `false` | **Master switch.** `false` ignores everything. |
| `FAKE_FAILURE_RATE`      | `0.0`   | Probability of a randomized failure, `0.0`–`1.0` |
| `FAKE_LATENCY_MS`        | `0`     | Artificial latency added per request            |
| `RANDOM_SEED`            | `42`    | Makes randomized failures reproducible         |

```env
FAKE_FAILURE_ENABLED=true
FAKE_FAILURE_RATE=0.10
FAKE_LATENCY_MS=200
RANDOM_SEED=42
```

This draft's example sets `FAKE_FAILURE_RATE` alone (divergence E12), which enables nothing: with
the master switch `false` the rate is never read. That is a confusing way to discover the feature
does not work, so the master switch is named explicitly here.

A named scenario is forced per request with `X-Fake-Failure` (§8.2) and is **named, not sampled** —
`payment_declined` always declines, so a test never fails intermittently.

Failure simulation is off by default and MUST NOT be enabled during normal development unless a
developer opts in explicitly.

---

# 42. API Testing Requirements

### Unit tests

Application services, domain rules, state transitions, validation logic.

### Integration tests

Repository implementations, SQLAlchemy mappings, SQLite persistence, transactions.

### API tests

HTTP status codes, request validation, response schemas, error envelopes, and endpoint behavior.

```text
tests/
├── unit/
├── integration/
└── api/
```

Every error path requires a test asserting **both** the response and the resulting database state.
A `409` test that never checks the inventory row is how a compensation bug survives
(`docs/testing.md`, `docs/failure-simulation.md`).

---

# 43. Example End-to-End Workflow

```text
1. GET  /api/v1/products
2. GET  /api/v1/products/{product_id}
3. POST /api/v1/carts
4. POST /api/v1/carts/{cart_id}/items
5. GET  /api/v1/carts/{cart_id}
6. POST /api/v1/checkout
7. GET  /api/v1/orders/{order_id}
8. GET  /api/v1/orders/{order_id}/payment
```

```bash
BASE=http://127.0.0.1:8000

curl -s $BASE/health

curl -s "$BASE/api/v1/products?is_active=true&page=1&page_size=5"

curl -s -X POST $BASE/api/v1/customers \
  -H 'Content-Type: application/json' \
  -d '{"email":"dev1@example.invalid","full_name":"Dev One"}'

CART=$(curl -s -X POST $BASE/api/v1/carts \
  -H "Authorization: Bearer $TOKEN" | jq -r .id)

curl -s -X POST $BASE/api/v1/carts/$CART/items \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"product_id":"'"$PRODUCT_ID"'","quantity":2}'

# Checkout
curl -s -X POST $BASE/api/v1/checkout \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: checkout-001" \
  -d '{"cart_id":"'"$CART"'","payment_method":"fake_card"}'

# Deterministic failure injection
curl -s -X POST $BASE/api/v1/checkout \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H 'X-Fake-Failure: payment_declined' \
  -d '{"cart_id":"'"$CART"'","payment_method":"fake_card"}'
```

```text
Browse → Product → Cart → Cart Items → Checkout
       → Inventory Reservation → Order → Payment → Confirmed Order
```

---

# 44. Administrative Workflow

```text
1. POST   /api/v1/categories
2. POST   /api/v1/products
3. POST   /api/v1/inventory/{product_id}/stock
4. PATCH  /api/v1/products/{product_id}
5. GET    /api/v1/inventory
6. GET    /api/v1/orders
7. GET    /api/v1/orders/{order_id}/payment
```

These routes require the **`admin`** role (`§31`). They are not exempt: `AUTH_ENABLED` defaults to
`true` in every environment including development and test (`configuration.md` G5), so the fake auth
boundary applies to them from the first run. The exact matrix is finalized in
`specs/010-authentication`; until then, the table above is the rule.

The tokens are static fixtures and the boundary is a local stand-in, not a security control. This
MUST be called out in the README, and `AUTH_ENABLED=false` exists for tests that need to exercise
the unauthenticated path deliberately.

---

# 45. API Compatibility Rules

Within `v1`, non-breaking:

* Adding an endpoint
* Adding an optional request or response field
* Adding a filter, sort field, or query parameter
* Adding a new error code
* Adding an optional request header

Breaking, and requiring `/api/v2`:

* Removing or renaming a field, endpoint, or query parameter
* Changing a field's type or meaning
* Changing an existing error code's meaning or HTTP status
* Making an optional request field required
* Changing documented business semantics — including a state machine, a money representation, or a
  default

The last item is why §24's transition diagram and §12.2's money format are normative: changing
`authorized → captured → refunded` to allow a refund from `authorized` breaks every client that
displays payment state, even though no field name changed.

---

# 46. API Documentation Requirements

Every endpoint documents:

* HTTP method and path
* Purpose
* Authentication requirement
* Path, query, and header parameters
* Request body
* Response body
* Success status
* Every possible error status and its `code`
* Business rules and state preconditions
* One example request and one example response

FastAPI route metadata MUST carry this, so the generated OpenAPI document is the documentation rather
than a supplement to it.

---

# 47. Definition of Done

An API feature is complete when:

* The endpoint is implemented with an explicit request and response schema.
* An application service exists where required, and domain rules are enforced there.
* Repository access is abstracted; no session or ORM type appears in the handler.
* Status codes are correct, including every documented error path.
* Errors use the §12 envelope with a specific `code`.
* `X-Request-ID` is echoed and appears in error bodies.
* OpenAPI metadata is complete.
* Unit, integration, and API tests exist, including error and state-transition paths.
* The endpoint works against generated fake data.
* No internal database detail, stack trace, or secret leaks through the API.

---

# 48. Initial Endpoint Summary

`★` marks a **proposed** route that needs sign-off in its feature spec (divergence E19).

| Resource          | Method | Endpoint                                 | Auth    |
| ----------------- | ------ | ---------------------------------------- | ------- |
| Health            | GET    | `/health`                                | public  |
| Readiness         | GET    | `/ready`                                 | public  |
| Categories        | GET    | `/api/v1/categories`                     | public  |
| Categories        | POST   | `/api/v1/categories`                     | admin   |
| Category          | GET    | `/api/v1/categories/{id}`                | public  |
| Category          | PATCH  | `/api/v1/categories/{id}`                | admin   |
| Category          | DELETE | `/api/v1/categories/{id}`                | admin   |
| Products          | GET    | `/api/v1/products`                       | public  |
| Products          | POST   | `/api/v1/products`                       | admin   |
| Product           | GET    | `/api/v1/products/{id}`                  | public  |
| Product           | PATCH  | `/api/v1/products/{id}`                  | admin   |
| Product           | DELETE | `/api/v1/products/{id}`                  | admin   |
| Customers         | POST   | `/api/v1/customers`                      | public  |
| Customers         | GET    | `/api/v1/customers`                      | admin   |
| Customer (self)   | GET    | `/api/v1/customers/me`                   | customer|
| Customer          | GET    | `/api/v1/customers/{id}`                 | self    |
| Customer          | PATCH  | `/api/v1/customers/{id}`                 | self    |
| Customer          | DELETE | `/api/v1/customers/{id}`                 | self    |
| Cart              | POST   | `/api/v1/carts`                          | customer|
| Cart              | GET    | `/api/v1/carts/{id}`                     | owner   |
| Cart Item         | POST   | `/api/v1/carts/{id}/items`               | owner   |
| Cart Item         | PATCH  | `/api/v1/carts/{id}/items/{item_id}`     | owner   |
| Cart Item         | DELETE | `/api/v1/carts/{id}/items/{item_id}`     | owner   |
| Cart (clear)      | DELETE | `/api/v1/carts/{id}/items`               | owner   |
| Checkout          | POST   | `/api/v1/checkout`                       | customer|
| Orders            | GET    | `/api/v1/orders`                         | customer|
| Order             | GET    | `/api/v1/orders/{id}`                    | owner   |
| Order ★           | POST   | `/api/v1/orders`                         | admin   |
| Cancel Order      | POST   | `/api/v1/orders/{id}/cancel`             | owner   |
| Inventory         | GET    | `/api/v1/inventory`                      | admin   |
| Inventory         | GET    | `/api/v1/inventory/{product_id}`         | public  |
| Restock           | POST   | `/api/v1/inventory/{product_id}/stock`   | admin   |
| Reserve Inventory | POST   | `/api/v1/inventory/{product_id}/reserve` | service |
| Release Inventory | POST   | `/api/v1/inventory/{product_id}/release` | service |
| Payment           | GET    | `/api/v1/payments/{id}`                  | owner   |
| Payments          | GET    | `/api/v1/payments`                       | owner   |
| Payment (by order)| GET    | `/api/v1/orders/{id}/payment`            | owner   |
| Payment ★         | POST   | `/api/v1/payments`                       | admin   |
| Authorize ★       | POST   | `/api/v1/payments/{id}/authorize`        | service |
| Retry Payment     | POST   | `/api/v1/payments/{id}/retry`            | owner   |
| Capture Payment   | POST   | `/api/v1/payments/{id}/capture`          | admin   |
| Refund Payment    | POST   | `/api/v1/payments/{id}/refund`           | admin   |

---

# 49. Final API Architecture

```text
                    ┌───────────────────────┐
                    │       API Client      │
                    │  Web / CLI / Agent    │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │      FastAPI API      │
                    │       /api/v1         │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │   Pydantic Schemas    │
                    │ Validation / Mapping   │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │ Application Services  │
                    │ Commands / Queries    │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │    Domain Model       │
                    │ Rules / Invariants    │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │ Repository / UoW      │
                    └───────────┬───────────┘
                                │
                       ┌────────┴────────┐
                       ▼                 ▼
                ┌─────────────┐  ┌───────────────┐
                │   SQLite    │  │ Fake Providers│
                │   Database  │  │ Payment/etc.  │
                └─────────────┘  └───────────────┘
```

The API is a thin transport boundary over the application layer. It is where HTTP is understood and
nowhere else — every rule that decides whether a request succeeds lives below it, which is what
makes the same rules enforceable from the CLI, the generator, and the tests.
