# Architecture

## Fake E-Commerce Server

**Project:** `fake-ecommerce-server`
**Architecture Style:** Modular Monolith / DDD-inspired Layered Architecture
**Language:** Python 3.12+
**Framework:** FastAPI
**Database:** SQLite
**ORM:** SQLAlchemy 2.x
**Migrations:** Alembic
**Validation:** Pydantic v2
**Dependency Management:** uv

---

> **Divergences from previously published docs — identified per constitution's *Explicit Business Rules* (III), not silently
> resolved.**
>
> | # | Divergence | Where | Handling |
> |---|---|---|---|
> | A1 | Documentation listed at repository **root**; every other doc places them under **`docs/`** and `README.md`/`CHANGELOG.md` link them there. | §14 | **Resolved** — `docs/` retained (matches your earlier explicit instruction and the existing links). Same class as constitution conflict C2. |
> | A2 | `seed/` omits `__main__.py`, which `python -m ecommerce.seed` requires. | §14 | **Resolved** — `__main__.py` included (constitution conflict C3). |
> | A3 | Order lifecycle omits **`FAILED`**. | §11 | **Resolved** — added. Constitution §5 and §22 require it ("Order marked failed"); the entire failure-simulation story depends on it. |
> | A4 | Payment lifecycle omits **`CAPTURED`** and shows `AUTHORIZED → REFUNDED`. | §12 | **Resolved** — `CAPTURED` added; refund is `CAPTURED → REFUNDED`, matching the `capture`/`refund` endpoints in `API.md` §22.7–22.8. |
> | A5 | **Money: `Decimal` vs integer minor units.** This document suggests `Decimal`; `API.md`, `database.md`, and `generator.md` all specify **integer minor units** (e.g. `2499` = $24.99). | §20 | **Resolved** — integer minor units authoritative. See §20.1. |
> | A6 | **Checkout route: `/api/v1/carts/{id}/checkout` vs `/api/v1/checkout` with `cart_id` in the body.** | §60 | **Deferred** — becomes an acceptance criterion in `specs/005-orders`. See §60.1. |
> | A7 | Product Catalog claims "product **availability**", but Inventory owns availability. | §7 | **Resolved** — clarified; see §7.1. |
> | A8 | Determinism **ports** (Clock, ID generation) absent, breaking cross-references in `testing.md` §5 and `failure-simulation.md` §7. | new §68 | **Resolved** — §68 added. |

---

# 1. Architecture Goals

The architecture is designed to provide:

* Clear domain boundaries
* Low coupling between components
* Testability
* Replaceable infrastructure
* Explicit business rules
* Transactional consistency
* Simple local development
* Deterministic fake data
* Controlled failure simulation
* AI-agent-friendly APIs
* A path toward future infrastructure changes

The architecture intentionally avoids premature complexity.

The system is a **modular monolith**, not a distributed microservice system.

---

# 2. Architectural Style

The application combines:

* Domain-Driven Design concepts
* Layered architecture
* Dependency inversion
* Repository pattern
* Application service pattern
* Adapter pattern
* Dependency injection

The primary dependency direction is:

```text
┌───────────────────────────────┐
│             API               │
│       FastAPI / Pydantic      │
└───────────────┬───────────────┘
                │
                ▼
┌───────────────────────────────┐
│         Application           │
│     Use Cases / Services      │
└───────────────┬───────────────┘
                │
                ▼
┌───────────────────────────────┐
│            Domain             │
│ Entities / Rules / Contracts  │
└───────────────┬───────────────┘
                │
                │ abstractions
                ▼
┌───────────────────────────────┐
│       Infrastructure          │
│ SQLAlchemy / SQLite / Fakes   │
└───────────────────────────────┘
```

The key principle is:

> Business logic must not depend on infrastructure technology.

---

# 3. High-Level System

```text
                         HTTP Client
                              │
                              ▼
                    ┌──────────────────┐
                    │     FastAPI      │
                    │   API Layer      │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │  Application     │
                    │     Layer        │
                    │                  │
                    │ Use Cases        │
                    │ Commands         │
                    │ Queries          │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │     Domain       │
                    │      Layer       │
                    │                  │
                    │ Entities         │
                    │ Value Objects    │
                    │ Domain Rules     │
                    │ Repository Ports │
                    └────────┬─────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
              ▼                             ▼
     ┌──────────────────┐         ┌──────────────────┐
     │   Persistence    │         │ External/Fake    │
     │   Infrastructure │         │   Providers      │
     │                  │         │                  │
     │ SQLAlchemy       │         │ Payment          │
     │ SQLite           │         │ Failure          │
     │ Repositories     │         │ Simulation       │
     └────────┬─────────┘         └──────────────────┘
              │
              ▼
        ┌─────────────┐
        │   SQLite    │
        │ ecommerce.db│
        └─────────────┘
```

---

# 4. Architectural Boundaries

The application contains four primary layers.

## 4.1 API Layer

Responsible for:

* HTTP routing
* Request parsing
* Response serialization
* Authentication boundaries
* HTTP error translation
* Dependency injection
* API versioning

The API layer must not contain core business logic.

---

## 4.2 Application Layer

Responsible for:

* Use cases
* Application workflows
* Transaction orchestration
* Repository coordination
* Domain service invocation
* External provider coordination
* Application-level error handling

Examples:

```text
CreateProduct
CreateCustomer
AddCartItem
ReserveInventory
CreateOrder
AuthorizePayment
CheckoutCart
```

---

## 4.3 Domain Layer

Responsible for:

* Business entities
* Value objects
* Domain rules
* Invariants
* Domain exceptions
* Repository interfaces
* Provider interfaces

The domain layer is the most important architectural boundary.

It must remain independent of:

* FastAPI
* SQLAlchemy
* SQLite
* HTTPX
* Pydantic API schemas
* Uvicorn

---

## 4.4 Infrastructure Layer

Responsible for implementing technical details:

* Database connections
* SQLAlchemy mappings
* Repository implementations
* SQLite configuration
* Fake payment provider
* Failure simulation
* Logging adapters
* External integrations

Infrastructure implements interfaces defined by inner layers.

---

# 5. Dependency Rule

Dependencies should point inward.

```text
API
 │
 ▼
Application
 │
 ▼
Domain
 ▲
 │
Infrastructure
```

Infrastructure depends on domain abstractions.

Domain does not depend on infrastructure.

For example:

```text
Correct:

Application
    ↓
PaymentProvider interface
    ↑
FakePaymentProvider
```

Incorrect:

```text
Domain
    ↓
SQLAlchemy
```

or:

```text
Domain
    ↓
FastAPI
```

---

# 6. Bounded Contexts

The system is divided into six initial bounded contexts.

```text
Product Catalog
Customer
Cart
Order
Inventory
Payment
```

Each context owns its business rules.

---

# 7. Product Catalog Context

## Responsibility

The Product Catalog context manages:

* Categories
* Products
* Product identity
* Product pricing
* Product lifecycle status (active/inactive)

> **See §7.1** — stock availability belongs to Inventory, not Product Catalog.

## Entities

```text
Category
Product
```

## Responsibilities

```text
Category
 ├── identity
 ├── name
 ├── slug
 ├── parent
 └── description

Product
 ├── identity
 ├── SKU
 ├── name
 ├── description
 ├── price
 ├── category
 └── active          (is_active — a boolean, not a status enum)
```

## Invariants

* SKU must be unique.
* Category slug must be unique.
* Price cannot be negative.
* Product must reference a valid category.
* Deactivating a product is a flag change, not a state machine.

---

## 7.1 Availability boundary (divergence A7)

An earlier draft listed "product **availability**" under Product Catalog. That responsibility
belongs to the **Inventory context** (§10), which owns stock quantities and reservations.

The correct split:

| Concern | Owner |
|---|---|
| Product exists and is purchasable (`is_active`) | Product Catalog |
| How many units are in stock / reserved | Inventory |
| Whether a quantity can be reserved | Inventory |

"Availability" in Product Catalog means the **catalog** flag only — whether the product may be
ordered at all. The API expresses this as `GET /api/v1/inventory/{product_id}` returning
quantities, and `409 PRODUCT_INACTIVE` when a cart references a deactivated product.

---

# 8. Customer Context

## Responsibility

The Customer context manages customer profiles.

## Entity

```text
Customer
```

## Responsibilities

```text
Customer
 ├── identity
 ├── name
 ├── email
 ├── address
 └── status
```

`phone` is deliberately absent: it was proposed and rejected (`database.md` D9, `API.md` E8). The
address is stored as discrete `address_*` columns on `customers`, not as a nested object.

The system uses synthetic customer information.

No real customer data is required.

---

# 9. Cart Context

## Responsibility

The Cart context manages shopping sessions before checkout.

## Entities

```text
Cart
CartItem
```

## Relationships

```text
Cart
 └── CartItem
       └── Product reference
```

The Cart context should not own Product.

It stores a reference to the product identity and obtains product information through appropriate
application/domain interfaces.

## Invariants

* Quantity must be greater than zero.
* Product must exist.
* Cart cannot contain invalid items.
* Cart totals must be calculated consistently.

---

# 10. Inventory Context

## Responsibility

The Inventory context owns stock quantities and reservations.

## Entity

```text
Inventory
```

## Core concepts

```text
Quantity on hand
Quantity reserved
Available to sell   (derived — on hand − reserved, never stored)
```

Example:

```text
Quantity on hand = 100

Quantity reserved = 20
Available to sell  = 80
```

## Invariants

```text
quantity_on_hand >= 0
quantity_reserved >= 0
quantity_reserved <= quantity_on_hand
```

Column names follow `database.md` §22 (divergence D17). `available_to_sell` is an expression, not a
column: storing it would allow the three values to disagree.

A reservation must be atomic.

---

# 11. Order Context

## Responsibility

The Order context owns orders and their lifecycle.

## Entities

```text
Order
OrderItem
```

## Order lifecycle

```text
PENDING
   │
   ▼
CONFIRMED
   │
   ▼
PROCESSING
   │
   ▼
SHIPPED
   │
   ▼
DELIVERED
```

Terminal and failure paths:

```text
PENDING ─────► CANCELLED

PENDING ─────► FAILED
```

`FAILED` is reachable **only** from `PENDING`. A confirmed order cannot fail: checkout authorizes
payment before the order is confirmed, so every payment failure is observed while the order is still
pending. See `failure-simulation.md` §46 for the compensation sequence that produces this transition.

> **See divergence A3.** `FAILED` was missing from an earlier draft of this diagram. It is
> **required**: the constitution's *Explicit Business Rules* principle (III) forbids a failed payment
> from silently becoming successful. The order must be able to record that outcome, or the
> compensation path is unobservable and untestable.

This diagram is a summary. The authoritative transition table is `database.md` §26, and a transition
absent from it is rejected with `409 INVALID_ORDER_TRANSITION`.

---

# 12. Payment Context

## Responsibility

The Payment context manages simulated payment operations.

## Entity

```text
Payment
```

## Payment lifecycle

```text
PENDING
   │
   ├────► AUTHORIZED
   │          │
   │          ├────► CAPTURED
   │          │          │
   │          │          └────► REFUNDED
   │          │
   ├────► DECLINED
   │
   └────► FAILED
```

> **See divergence A4.** `CAPTURED` was missing from an earlier draft, which showed
> `AUTHORIZED → REFUNDED` directly. `CAPTURED` is **required** to match the capture and refund
> endpoints in `API.md` §22.7–22.8 (`authorized → captured → refunded`), which mirror real card
> authorization/capture settlement flows.

Payment behavior is implemented through a provider abstraction.

---

# 13. Context Relationships

The major relationships are:

```text
                 Product Catalog
                       │
              ┌────────┼────────┐
              │        │        │
              ▼        ▼        ▼
            Cart    Inventory   Order
              │        ▲        │
              │        │        │
              └────────┘        ▼
                           Payment

Customer ───────► Cart
Customer ───────► Order
```

More explicitly:

```text
Customer
   │
   ├──── owns ────► Cart
   │
   └──── owns ────► Order

Cart
   │
   └──── references ────► Product

Cart
   │
   └──── checkout ────► Order

Order
   │
   ├──── references ────► Product
   │
   └──── creates ────► Payment

Order
   │
   └──── reserves ────► Inventory
```

Note that the last two relationships are *orchestrated by the Application layer* (see §23), not
performed by the Order context itself. The Order context does not call Inventory; `CheckoutCart`
coordinates both.

---

# 14. Project Structure

The target structure is:

```text
fake-ecommerce-server/
│
├── pyproject.toml
├── uv.lock
│
├── README.md
├── CHANGELOG.md
│
├── .specify/
│   └── memory/
│       └── constitution.md        # authoritative (divergence A1)
│
├── specs/                          # canonical Spec Kit output location
│   ├── 001-foundation/
│   ├── 002-product-catalog/
│   ├── 003-customers/
│   ├── 004-cart/
│   ├── 005-orders/
│   ├── 006-inventory/
│   ├── 007-payments/
│   ├── 008-fake-data-generator/
│   ├── 009-failure-simulation/
│   ├── 010-authentication/
│   └── 011-observability/
│
├── docs/
│   ├── PRD.md
│   ├── plan.md
│   ├── architecture.md             # this file
│   ├── API.md
│   ├── database.md
│   ├── generator.md
│   ├── configuration.md
│   ├── testing.md
│   ├── failure-simulation.md
│   └── development.md
│
├── alembic.ini
├── .env.example
├── .gitignore
├── Makefile
│
├── data/
│   └── ecommerce.db
│
├── migrations/
│   ├── env.py
│   ├── script.py.mako
│   └── versions/
│
├── src/
│   └── ecommerce/
│       │
│       ├── __init__.py
│       ├── main.py
│       │
│       ├── api/
│       │   ├── dependencies.py
│       │   └── v1/
│       │       ├── products.py
│       │       ├── customers.py
│       │       ├── carts.py
│       │       ├── orders.py
│       │       ├── inventory.py
│       │       └── payments.py
│       │
│       ├── application/
│       │   ├── products/
│       │   ├── customers/
│       │   ├── carts/
│       │   ├── orders/
│       │   ├── inventory/
│       │   └── payments/
│       │
│       ├── domain/
│       │   ├── products/
│       │   ├── customers/
│       │   ├── carts/
│       │   ├── orders/
│       │   ├── inventory/
│       │   └── payments/
│       │
│       ├── infrastructure/
│       │   ├── database/
│       │   ├── repositories/
│       │   └── external/
│       │
│       ├── config/
│       │   └── settings.py
│       │
│       └── seed/
│           ├── __init__.py
│           ├── __main__.py        # required for `python -m` (divergence A2)
│           ├── cli.py
│           ├── generator.py
│           ├── factories/
│           └── profiles/
│
├── scripts/
│
└── tests/
    ├── unit/
    ├── integration/
    └── api/
```

> **Divergences A1 and A2 (resolved).** An earlier draft placed `constitution.md`, `PRD.md`,
> `architecture.md`, `API.md`, `database.md`, `generator.md`, `configuration.md`, `testing.md`,
> `failure-simulation.md`, and `development.md` at the repository **root**. They live under
> `docs/`, matching the established documentation layout and every existing cross-link. The
> constitution is read from `.specify/memory/constitution.md` by the Spec Kit tooling.
>
> `seed/__main__.py` is included because `python -m ecommerce.seed` (the constitution's *Simple Local Development*, V) will not run
> without it.

---

# 15. Domain Module Structure

Each bounded context should follow a consistent structure.

Example:

```text
domain/products/
├── __init__.py
├── entities.py
├── value_objects.py
├── exceptions.py
├── repositories.py
└── services.py
```

For a more explicit structure as the project grows:

```text
domain/products/
├── entities/
├── value_objects/
├── services/
├── repositories/
├── exceptions.py
└── events.py
```

The project should start simple and expand only when necessary.

---

# 16. Application Module Structure

Example:

```text
application/products/
├── commands/
├── queries/
├── services.py
└── dto.py
```

For the initial implementation, this may be simplified to:

```text
application/products/
├── create_product.py
├── get_product.py
├── list_products.py
└── update_product.py
```

The architecture should avoid creating empty abstraction layers merely for organizational purposes.

---

# 17. API Module Structure

API routes are organized by bounded context.

Example:

```text
api/v1/
├── products.py
├── customers.py
├── carts.py
├── orders.py
├── inventory.py
└── payments.py
```

Routes should primarily perform:

```text
HTTP request
    ↓
Validate request
    ↓
Resolve dependencies
    ↓
Call application use case
    ↓
Translate result
    ↓
HTTP response
```

Routes should not contain substantial domain logic.

---

# 18. Pydantic Schemas

Pydantic models belong to the API boundary.

Example:

```text
api/v1/schemas/
├── products.py
├── customers.py
├── carts.py
├── orders.py
└── payments.py
```

Example conceptual flow:

```text
HTTP JSON
   ↓
Pydantic Request
   ↓
Application Command
   ↓
Domain
```

The domain must not use API request models directly.

---

# 19. Domain Entities

Domain entities represent business concepts.

Example:

```text
Product
Customer
Cart
Order
Inventory
Payment
```

Entities own behavior where appropriate.

Avoid creating an anemic domain model where all business rules are implemented exclusively in
application services.

For example:

```text
order.confirm()
```

is preferable to:

```text
order.status = "CONFIRMED"
```

when confirmation contains meaningful business rules.

---

# 20. Value Objects

Value objects should be introduced where they clarify domain rules.

Potential examples:

```text
Money
SKU
EmailAddress
Quantity
ProductId
CustomerId
OrderId
PaymentId
```

Example:

```text
Money
 ├── amount      # int, minor units (2499 = 24.99)
 └── currency    # ISO-4217 code
```

Money calculations must avoid floating-point errors. **`amount` is an integer count of minor units;
`float` and `Decimal` are both prohibited as the representation.** See §20.1 for the decision and
its rationale.

---

## 20.1 Money representation — divergence A5 (RESOLVED)

An earlier draft of this section specified `Decimal`. **Integer minor units are authoritative.**

**Decision:** money is an **integer count of minor units** plus an ISO-4217 currency code.
`2499 USD` means $24.99. Float is never used for money, and `Decimal` is not used as the
representation.

This is normative across:

* `API.md` §12.2 — wire format `{"amount": 2499, "currency": "USD"}`
* `database.md` §13 — `price_amount` INTEGER NOT NULL CHECK (price_amount >= 0)`
* `generator.md` — generated prices and totals in minor units
* `domain/` — the `Money` value object holds `amount: int` + `currency: str`

### Why not `Decimal`

| | **Integer minor units** (chosen) | `Decimal` (rejected) |
|---|---|---|
| Wire format | `{"amount": 2499, "currency": "USD"}` | `{"amount": "24.99", "currency": "USD"}` |
| Storage | `INTEGER` column | `NUMERIC` column |
| DB constraint | `CHECK (amount >= 0)` | `CHECK (amount >= 0)` |
| Arithmetic | Exact integer ops | Exact decimal arithmetic |
| Multi-currency FX | Requires an explicit rounding policy | More natural |
| SQLite fit | Native `INTEGER` — exact | `NUMERIC` has no exact decimal type; degrades toward `REAL` |
| JSON readability | `2499` (needs a comment to read) | `"24.99"` |

The decisive factor is SQLite. It has **no exact decimal type** — `NUMERIC` affinity falls back to
`REAL` for values that cannot be represented exactly, which reintroduces the floating-point error
that a money type exists to eliminate. Integer minor units are exact at every layer, and they make
the non-negotiable "amounts are never negative" rule a trivial `CHECK` rather than a domain
invariant that also needs a database backstop.

`Decimal` remains a reasonable choice if this project ever needs multi-currency conversion with
per-rate rounding. That is not a current requirement, and revisiting it later is a contained change
because `Money` is a value object behind a single constructor.

### Consequences

* **Line totals** — `unit_price_amount * quantity`, both integers. Exact, no rounding step.
* **Discounts/tax (future)** — must define an explicit rounding policy (e.g. banker's rounding at
  the line level) rather than relying on implicit float behavior.
* **Currency mixing** — a single order is single-currency. Multi-currency orders are out of scope
  and would require an FX conversion policy.
* **Sub-unit currencies** — `JPY` has no minor unit; represent it as `amount` in the currency's
  own unit and document the exponent per currency rather than assuming 2 decimal places.

---

# 21. Repository Pattern

Repositories provide persistence abstractions.

Example domain interface:

```python
class ProductRepository: ...
```

The infrastructure layer implements it:

```text
ProductRepository
        ▲
        │
SQLAlchemyProductRepository
```

The application layer should depend on the repository abstraction rather than directly on
SQLAlchemy.

Design rules:

* One repository per aggregate root, not a single generic repository for the whole system.
* Repositories return domain objects or plain projections — never live ORM instances.
* Filtering, sorting, and pagination are named, intention-revealing methods, not a generic
  `find(where=...)` escape hatch.
* Repositories contain **no business workflows** (see §61, Constraint 4). Business rules belong to
  the domain, even when the domain calls back into a repository to load an aggregate.

---

# 22. Unit of Work

Transactional workflows should use an explicit transaction boundary.

A Unit of Work abstraction may be introduced:

```text
UnitOfWork
 ├── products
 ├── customers
 ├── carts
 ├── orders
 ├── inventory
 └── payments
```

Example:

```text
with unit_of_work:
    reserve inventory
    create order
    create payment
    commit
```

The implementation may initially use SQLAlchemy sessions underneath.

The domain should not depend on SQLAlchemy sessions directly.

The Unit of Work groups the per-aggregate repositories behind one shared transaction; it is not a
generic repository. It also provides the seam at which idempotency (§46) can be introduced without
redesigning the domain.

---

# 23. Checkout Architecture

Checkout is the most important application workflow.

The application layer orchestrates the process.

```text
CheckoutCart
     │
     ├── Load cart
     │
     ├── Validate cart
     │
     ├── Calculate total
     │
     ├── Reserve inventory
     │
     ├── Create order
     │
     ├── Authorize payment
     │
     ├── Confirm order
     │
     └── Commit
```

Conceptually:

```text
              Checkout
                  │
       ┌──────────┼───────────┐
       ▼          ▼           ▼
     Cart     Inventory     Payment
       │          │           │
       └──────────┼───────────┘
                  ▼
                Order
```

---

# 24. Checkout Transaction Boundary

The checkout workflow must protect consistency.

A simplified successful transaction:

```text
BEGIN
  │
  ├── Validate cart
  ├── Reserve inventory
  ├── Create order
  ├── Create payment
  ├── Authorize payment
  ├── Confirm order
  │
COMMIT
```

Failure:

```text
BEGIN
  │
  ├── Validate cart
  ├── Reserve inventory
  ├── Create order
  ├── Payment fails
  │
  └── ROLLBACK / COMPENSATE
```

The implementation must explicitly define which operations are transactional and which require
compensation.

---

# 25. Payment Provider Abstraction

Payment processing should use a port/adapter design.

```text
              PaymentProvider
                    ▲
                    │
          ┌─────────┴─────────┐
          │                   │
FakePaymentProvider     FutureRealProvider
```

Initial implementation:

```text
FakePaymentProvider
```

It may simulate:

* Authorization success
* Decline
* Timeout
* Provider error

The rest of the application should not need to know how the provider is implemented.

---

# 26. Failure Simulation Architecture

Failure simulation belongs primarily in infrastructure adapters.

Example:

```text
Checkout
   │
   ▼
PaymentProvider
   │
   ▼
FailureSimulator
   │
   ├── success
   ├── decline
   ├── timeout
   └── provider error
```

Failure behavior should be configurable.

```text
FAKE_FAILURE_RATE
FAKE_LATENCY_MS
```

Failure simulation must not be embedded directly into domain entities.

---

# 27. Database Architecture

SQLite is the initial persistence technology.

Database:

```text
data/ecommerce.db
```

SQLAlchemy provides:

* Engine
* Session
* ORM mappings
* Transactions
* Query construction

Alembic provides:

* Schema versioning
* Migration generation
* Migration execution

Architecture:

```text
Application
    ↓
Repository Interface
    ↓
Repository Implementation
    ↓
SQLAlchemy
    ↓
SQLite
```

---

# 28. SQLAlchemy Boundary

SQLAlchemy models should remain infrastructure concerns.

Conceptually:

```text
Domain Entity
      ▲
      │ mapping
      ▼
SQLAlchemy Model
```

Do not make domain entities inherit from SQLAlchemy declarative base unless there is a deliberate
architectural reason.

The initial design should keep persistence models separate from domain entities.

---

# 29. Mapping Layer

Where domain and persistence representations differ, explicit mapping should be used.

Example:

```text
SQLAlchemy Product
        │
        ▼
Product Mapper
        │
        ▼
Domain Product
```

And reverse:

```text
Domain Product
        │
        ▼
Product Mapper
        │
        ▼
SQLAlchemy Product
```

This protects the domain from persistence concerns.

---

# 30. Database Sessions

Database sessions should be created and managed by infrastructure.

FastAPI dependencies may obtain a session through an infrastructure dependency.

Conceptually:

```text
HTTP Request
    ↓
FastAPI dependency
    ↓
Session
    ↓
Application service
    ↓
Repository
```

The API should not manually manage SQL transactions inside individual routes.

---

# 31. API Error Architecture

Internal errors should be translated at the API boundary.

Example:

```text
Domain Error
    ↓
Application Error
    ↓
API Exception Handler
    ↓
HTTP Response
```

Example:

```text
InsufficientInventory
        ↓
InventoryUnavailableError
        ↓
HTTP 409 Conflict
```

The exact HTTP mapping will be documented in `API.md`.

---

# 32. Error Taxonomy

The system should distinguish:

## Domain Errors

Examples:

```text
InvalidOrderState
InsufficientInventory
InvalidQuantity
InvalidPaymentTransition
```

## Application Errors

Examples:

```text
CartNotFound
CheckoutFailed
CustomerNotFound
```

## Infrastructure Errors

Examples:

```text
DatabaseUnavailable
PaymentProviderTimeout
ExternalProviderError
```

## API Errors

Examples:

```text
400 Bad Request
404 Not Found
409 Conflict
422 Unprocessable Entity
500 Internal Server Error
503 Service Unavailable
```

---

# 33. Dependency Injection

FastAPI dependency injection should be used at the API boundary.

Example:

```text
Route
  ↓
Application Service
  ↓
Repository
```

Dependencies should be assembled centrally.

Avoid constructing repositories directly inside route functions.

---

# 34. Composition Root

Application dependencies should be assembled in a controlled location.

Potential structure:

```text
api/dependencies.py
```

The composition root creates:

* Database session
* Repositories
* Unit of Work
* Application services
* Providers

Conceptually:

```text
main.py
   │
   ▼
Composition Root
   │
   ├── Database
   ├── Repositories
   ├── Providers
   └── Application Services
```

---

# 35. Configuration Architecture

Configuration should be represented by typed settings.

Example:

```text
config/settings.py
```

Potential settings:

```text
AppSettings
 ├── app_env
 ├── database_url
 ├── log_level
 ├── fake_failure_rate
 ├── fake_latency_ms
 └── random_seed
```

Environment variables provide runtime configuration.

---

# 36. Fake Data Generator Architecture

The generator is a separate application capability.

```text
CLI
 │
 ▼
Generator
 │
 ├── Category Factory
 ├── Product Factory
 ├── Customer Factory
 ├── Inventory Factory
 ├── Cart Factory
 ├── Order Factory
 └── Payment Factory
 │
 ▼
Repositories / Unit of Work
 │
 ▼
SQLite
```

The generator should reuse domain rules rather than bypassing them.

---

# 37. Generator Ordering

Data generation follows dependency order:

```text
1. Categories
2. Products
3. Inventory
4. Customers
5. Carts
6. Cart Items
7. Orders
8. Order Items
9. Payments
```

This ensures foreign-key relationships can be constructed correctly.

---

# 38. Generator Profiles

Profiles provide predefined configurations.

```text
seed/profiles/
├── test.py
├── development.py
└── large.py
```

The CLI may override profile values.

Example:

```bash
uv run python -m ecommerce.seed \
    --profile development \
    --customers 200
```

CLI overrides should take precedence over profile defaults.

---

# 39. Deterministic Generation

The generator must support an explicit seed.

Example:

```bash
uv run python -m ecommerce.seed --seed 42
```

The seed controls pseudo-random generation.

The goal is reproducibility:

```text
seed = 42
configuration = X

        ↓

dataset X
```

Running the same configuration again should produce equivalent synthetic data.

---

# 40. Authentication Architecture

Authentication should be implemented at the API boundary.

Conceptually:

```text
HTTP Request
     │
     ▼
Authentication
     │
     ▼
Current User
     │
     ▼
Authorization
     │
     ▼
Application Service
```

Roles:

```text
customer
admin
service
```

Authorization rules should remain explicit.

Example:

```text
Customer
 └── can access own cart

Admin
 └── can manage products

Service
 └── can perform service operations
```

---

# 41. Observability Architecture

Logging should be centralized.

```text
Request
   ↓
Middleware
   ↓
Request ID
   ↓
Application
   ↓
Domain
   ↓
Infrastructure
   ↓
Response
```

Useful fields:

```text
request_id
method
path
status_code
duration_ms
operation
error_code
```

Sensitive information must never be logged.

---

# 42. Health Check

The application exposes:

```text
GET /health
```

The health endpoint should provide basic application status.

A future readiness endpoint could inspect:

* Database connectivity
* Required infrastructure
* Provider configuration

The initial implementation should keep health checks simple.

---

# 43. API Versioning

The initial API version is:

```text
/api/v1
```

Versioning allows future API changes without immediately breaking clients.

Example:

```text
/api/v1/products
/api/v2/products
```

Future versions should be introduced only when required.

---

# 44. Pagination Architecture

Collection queries should support pagination.

Conceptual application request:

```text
ListProductsQuery
 ├── page
 ├── page_size
 ├── filters
 └── sorting
```

Repository handles efficient database retrieval.

API converts the result into a stable response schema.

---

# 45. Concurrency Considerations

SQLite has limitations around concurrent writes.

The initial application should:

* Keep transactions short.
* Avoid unnecessary long-running transactions.
* Use appropriate SQLite configuration (`PRAGMA foreign_keys = ON`, WAL).
* Avoid holding database sessions longer than necessary.
* Keep checkout transactions bounded.

Because SQLite serializes writers and provides no row-level `SELECT ... FOR UPDATE`, inventory
reservation MUST use a conditional update and check the affected row count, e.g.
`UPDATE inventory SET quantity_reserved = quantity_reserved + :n
WHERE product_id = :id AND quantity_on_hand - quantity_reserved >= :n`. A zero rowcount means the
reservation lost the race and must be rejected as `409 INSUFFICIENT_INVENTORY`.

The architecture should make future migration to PostgreSQL possible.

---

# 46. Idempotency

Critical operations should eventually consider idempotency.

The primary candidate is checkout/payment processing.

A future API may support:

```text
Idempotency-Key
```

The initial implementation should establish the application boundary so idempotency can be added
without redesigning the domain. The Unit of Work (§22) is the natural seam.

---

# 47. Security Boundary

Security responsibilities are distributed.

## API

Responsible for:

* Authentication
* Authorization
* Input validation
* HTTP security

## Application

Responsible for:

* Authorization-aware use cases
* Business permissions

## Domain

Responsible for:

* Business invariants

## Infrastructure

Responsible for:

* Secure persistence
* Parameterized queries
* Provider isolation
* Secret handling

---

# 48. Testing Architecture

Tests mirror the architecture.

```text
tests/
├── unit/
│   ├── domain/
│   └── application/
│
├── integration/
│   ├── repositories/
│   ├── database/
│   └── providers/
│
└── api/
    ├── products/
    ├── customers/
    ├── carts/
    ├── orders/
    ├── inventory/
    ├── payments/
    └── failure_simulation/
```

## Unit Tests

Should avoid:

* SQLite
* FastAPI
* HTTP calls

They test business behavior directly.

## Integration Tests

Test infrastructure boundaries.

## API Tests

Test HTTP behavior end-to-end within the application.

The `unit` / `integration` / `api` split is mandated by the constitution. The sub-directories
above are a refinement of it, not a replacement.

---

# 49. Test Database Strategy

Tests should not depend on the developer's normal database.

A test database should be isolated.

Possible initial strategy:

```text
tests
  ↓
temporary SQLite database
```

For unit tests:

```text
No database
```

For integration/API tests:

```text
Temporary SQLite database
```

The exact fixture strategy belongs in `testing.md`.

---

# 50. Transaction Strategy

Transactions belong primarily to application workflows.

Simple CRUD:

```text
Request
  ↓
Application
  ↓
Repository
  ↓
Commit
```

Complex workflow:

```text
Application Use Case
        ↓
     BEGIN
        ↓
   Multiple operations
        ↓
     COMMIT
```

Checkout should be treated as a transaction boundary.

---

# 51. Domain Events

Domain events are not required in the initial implementation.

Potential future events include:

```text
ProductCreated
OrderCreated
OrderConfirmed
PaymentAuthorized
PaymentFailed
InventoryReserved
InventoryReleased
```

The project deliberately avoids introducing event sourcing or a message broker initially.

Events may be added later if a concrete use case requires them.

---

# 52. Caching

Caching is not part of the initial architecture.

Do not introduce Redis or another cache prematurely.

If performance requirements eventually justify caching, it should be added behind an
application/infrastructure abstraction.

---

# 53. Messaging

The initial system does not use:

* Kafka
* RabbitMQ
* NATS
* Redis Streams
* Cloud queues

Synchronous application workflows are sufficient for the first version.

Messaging may be introduced later for concrete asynchronous requirements.

---

# 54. Microservices

The system must remain a modular monolith initially.

Do not split the bounded contexts into independently deployed services.

The internal structure should make future extraction possible:

```text
Modular Monolith
      ↓
Clear Context Boundaries
      ↓
Potential Future Service Extraction
```

The goal is architectural modularity without distributed-system complexity.

---

# 55. External Provider Architecture

External integrations must use adapters.

Example:

```text
Application
     │
     ▼
PaymentProvider
     ▲
     │
     └── FakePaymentProvider
```

Future:

```text
PaymentProvider
     ▲
     ├── FakePaymentProvider
     ├── StripeProvider
     └── OtherProvider
```

The application should depend on the interface, not the implementation.

---

# 56. Main Application Startup

The application lifecycle should be approximately:

```text
Process starts
     │
     ▼
Load configuration
     │
     ▼
Configure logging
     │
     ▼
Create database engine
     │
     ▼
Create application dependencies
     │
     ▼
Create FastAPI application
     │
     ▼
Register middleware
     │
     ▼
Register exception handlers
     │
     ▼
Register API routers
     │
     ▼
Start server
```

Database schema creation is handled by Alembic before application startup. The application MUST
NOT create, alter, or drop schema at startup.

---

# 57. Request Lifecycle

A normal request should follow:

```text
HTTP Request
     │
     ▼
FastAPI Router
     │
     ▼
Pydantic Validation
     │
     ▼
Authentication
     │
     ▼
Authorization
     │
     ▼
Application Use Case
     │
     ▼
Domain Logic
     │
     ▼
Repository / Provider
     │
     ▼
Result
     │
     ▼
Response DTO
     │
     ▼
HTTP Response
```

---

# 58. Example: Create Product

```text
POST /api/v1/products
          │
          ▼
ProductCreateRequest
          │
          ▼
CreateProductUseCase
          │
          ▼
Product domain entity
          │
          ▼
ProductRepository
          │
          ▼
SQLAlchemy repository
          │
          ▼
SQLite
          │
          ▼
ProductResponse
          │
          ▼
HTTP 201
```

---

# 59. Example: Add Cart Item

```text
POST /api/v1/carts/{id}/items
          │
          ▼
Validate request
          │
          ▼
AddCartItemUseCase
          │
          ├── Load cart
          │
          ├── Validate product
          │
          ├── Validate quantity
          │
          └── Add item
                    │
                    ▼
                 Cart
                    │
                    ▼
               Repository
```

The route itself should not perform these business decisions.

---

# 60. Example: Checkout

```text
POST /api/v1/checkout
                │
                ▼
       CheckoutCartUseCase
                │
        ┌───────┴────────┐
        ▼                ▼
      Cart           Customer
        │
        ▼
    Validate
        │
        ▼
   Calculate total
        │
        ▼
 Reserve inventory
        │
        ▼
   Create order
        │
        ▼
 Authorize payment
        │
        ▼
 Confirm order
        │
        ▼
      Commit
        │
        ▼
 Checkout response
```

---

## 60.1 Checkout route shape — divergence A6 (DEFERRED)

**Status: deferred to `specs/005-orders`.** The route shape is recorded here as an explicit
trade-off so the decision is made deliberately during specification, with the full context
available, rather than settled implicitly by whoever writes the route first.

**Interim contract:** `POST /api/v1/checkout` with `{"cart_id": "...", "payment_method": "..."}`.
This remains the published contract in `API.md` §21, `README.md`, and `failure-simulation.md` §46
until `specs/005-orders` resolves it. If the spec chooses the path-based form, those four documents
must be updated in the same commit.

An earlier draft of this section used `POST /api/v1/carts/{cart_id}/checkout`.

| | `POST /api/v1/checkout` (interim) | `POST /api/v1/carts/{cart_id}/checkout` |
|---|---|---|
| Request | `{"cart_id": "...", "payment_method": "fake_card"}` | `{"payment_method": "fake_card"}` |
| Cart identity | In the body | In the path |
| Retries | Caller must resend `cart_id`; duplicates possible | Path makes the target unambiguous |
| Idempotency (§46) | Needs a key to be safe | Naturally scoped to one cart |
| Resource semantics | A checkout is arguably a sub-resource of cart | |
| Extensibility | Could grow into a `POST /api/v1/orders` style | Ties checkout to cart forever |

**Lean:** `POST /api/v1/checkout`. It is already the published contract in four documents, and
separating the cart from the action keeps the route open to `Idempotency-Key` (§46) without
implying checkout is permanently a sub-resource of the cart.

**Required in `specs/005-orders`:** the spec MUST state the chosen route, justify it, and — if it
diverges from the interim contract — enumerate every document requiring an update.

---

# 61. Architecture Constraints

The following rules are mandatory.

### Constraint 1

Domain code must not import FastAPI.

### Constraint 2

Domain code must not import SQLAlchemy.

### Constraint 3

API routes must not contain complex business logic.

### Constraint 4

Repositories must not contain business workflows.

### Constraint 5

Application services orchestrate use cases.

### Constraint 6

Domain entities enforce their own invariants.

### Constraint 7

Database schema changes must use Alembic.

### Constraint 8

Fake providers must implement provider abstractions.

### Constraint 9

Synthetic data must not contain real sensitive information.

### Constraint 10

New infrastructure dependencies must have an explicit architectural justification.

---

# 62. Architectural Trade-Offs

## SQLite

### Advantages

* Zero external database dependency
* Simple local development
* Easy test environments
* Portable database file
* Easy reset

### Limitations

* Limited concurrent writes
* Less suitable for production-scale workloads
* Different operational characteristics from PostgreSQL

The project prioritizes local simplicity.

---

## Modular Monolith

### Advantages

* Simple deployment
* Simple debugging
* Low operational overhead
* Clear module boundaries
* Easy local development

### Limitations

* Less deployment isolation
* Requires discipline to maintain boundaries

The architecture prioritizes modularity before distribution.

---

## Separate Domain and ORM Models

### Advantages

* Strong domain isolation
* Easier testing
* Infrastructure replaceability
* Clear architecture

### Limitations

* Additional mapping code
* More classes

The project accepts this complexity because domain isolation is an explicit goal.

---

# 63. Evolution Strategy

The architecture should evolve incrementally.

```text
Phase 1
Simple FastAPI application

        ↓

Phase 2
Database + repositories

        ↓

Phase 3
Bounded contexts

        ↓

Phase 4
Application use cases

        ↓

Phase 5
Checkout orchestration

        ↓

Phase 6
Failure simulation

        ↓

Phase 7
Authentication

        ↓

Phase 8
Observability

        ↓

Phase 9
Hardening
```

Do not introduce advanced architectural patterns before the underlying use case requires them.

---

# 64. Future PostgreSQL Migration

Although SQLite is the initial database, the architecture should avoid SQLite-specific business
logic.

Future migration:

```text
Application
     │
     ▼
Repository Interface
     │
     ▼
PostgreSQL Repository
     │
     ▼
PostgreSQL
```

The domain model should remain unchanged.

Only infrastructure and database-specific configuration should require significant modification.

SQLite-specific behavior that must be isolated to infrastructure (never leaking into the domain):
`PRAGMA foreign_keys` must be enabled per connection; foreign-key columns are not auto-indexed;
`BOOLEAN` is `INTEGER`; `DATETIME` is weakly typed; and writers are serialized (see §45).

---

# 65. Future AI-Agent Integration

The architecture intentionally exposes a clean API boundary that can later be consumed by:

```text
AI Agent
   │
   ▼
Tool Layer / MCP
   │
   ▼
Fake E-Commerce API
```

Potential tools:

```text
search_products
get_product
create_customer
create_cart
add_to_cart
get_inventory
checkout
get_order
get_payment
```

The e-commerce server itself remains independent of the AI layer.

---

# 66. Architecture Decision Summary

| Decision                | Choice                        |
| ----------------------- | ----------------------------- |
| Application style       | Modular monolith              |
| Domain approach         | DDD-inspired                  |
| API framework           | FastAPI                       |
| Validation              | Pydantic v2                   |
| ORM                     | SQLAlchemy 2.x                |
| Database                | SQLite                        |
| Migrations              | Alembic                       |
| Dependency management   | uv                            |
| Persistence abstraction | Repository                    |
| Transaction abstraction | Unit of Work where useful     |
| Domain/ORM models       | Separate, explicit mapping    |
| External integrations   | Ports/adapters                |
| Payment provider        | Fake provider initially       |
| Fake data               | Dedicated generator           |
| Testing                 | pytest + HTTPX                |
| API version             | `/api/v1`                     |
| Authentication          | Fake authentication initially |
| Enum wire format        | Lowercase snake_case          |
| Messaging               | Not initially                 |
| Microservices           | Not initially                 |
| Event sourcing          | Not initially                 |
| CQRS                    | Not initially                 |
| Caching                 | Not initially                 |
| Money representation    | Integer minor units (§20.1)   |
| Checkout route shape    | **See §60.1 — deferred to `005-orders`** |

> **Enum convention.** State diagrams in this document use `UPPER_SNAKE_CASE` because that is the
> Python enum member convention (`OrderStatus.CONFIRMED`). The **wire format is lowercase**
> (`"confirmed"`) per `API.md` §1. This is a serialization choice, not a modelling difference.

---

# 67. Final Architecture

The resulting architecture is:

```text
┌─────────────────────────────────────────────────────┐
│                    API Layer                        │
│                                                     │
│ FastAPI │ Pydantic │ Authentication │ HTTP Errors  │
└───────────────────────┬─────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│                Application Layer                    │
│                                                     │
│ Use Cases │ Workflows │ Transactions │ DTOs         │
└───────────────────────┬─────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│                   Domain Layer                      │
│                                                     │
│ Product │ Customer │ Cart │ Inventory │ Order      │
│ Payment │ Entities │ Rules │ Value Objects          │
│ Repository Interfaces │ Provider Interfaces        │
└───────────────────────┬─────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│               Infrastructure Layer                  │
│                                                     │
│ SQLAlchemy │ SQLite │ Repositories │ Providers      │
│ Failure Simulation │ Logging │ Database             │
└───────────────────────┬─────────────────────────────┘
                        │
                        ▼
                 ┌───────────────┐
                 │    SQLite     │
                 │ ecommerce.db │
                 └───────────────┘
```

The core architectural principle is:

```text
HTTP concerns stay in API.
Workflow orchestration stays in Application.
Business rules stay in Domain.
Technical implementation stays in Infrastructure.
```

This structure provides enough separation to make the project realistic and extensible while
deliberately avoiding unnecessary distributed-system complexity.

---

# 68. Determinism Architecture (divergence A8)

> **Added.** An earlier draft of this document omitted the determinism ports, which broke
> cross-references in `testing.md` §5 ("Injected `Clock` port"), `failure-simulation.md` §7, and
> `generator.md` §12.

Determinism is an architectural property, not a test convenience. It requires the same
non-determinism sources — time, randomness, and identity — to be behind ports, so tests can
substitute deterministic implementations.

| Port | Production adapter | Test / generator adapter | Replaces |
|---|---|---|---|
| `Clock` | `SystemClock` | `FixedClock` | `datetime.now()`, `time.time()` |
| `IdGenerator` | `Uuid4IdGenerator` | `SeededUuidIdGenerator(rng)` | bare `uuid4()` calls |
| `RandomSource` | `RandomSource(random.SystemRandom())` | `RandomSource(random.Random(seed))` | module-level `random.*` |

Rules:

* Domain code receives a `Clock` and never calls `datetime.now()` directly. Time-dependent rules
  (e.g. order-date vs. status coherence in the generator) become unit-testable without sleeping.
* The generator composes a single seeded `RandomSource`; no module-level `random.*` calls anywhere
  in `seed/`.
* Identifiers come from an injected generator, so fixtures are byte-stable across runs.
* The test and generator adapter must still emit **UUID-shaped** strings. Primary keys are
  `TEXT` UUIDs (`database.md` D3), so a counter — `SequentialIdGenerator` and similar — cannot be
  substituted: it would be deterministic and simultaneously violate the schema. `SeededUuidIdGenerator`
  draws UUID4 strings from the seeded `RandomSource`, which satisfies both constraints.
* `APP_ENV=test` additionally pins `FAKE_CLOCK` (see `configuration.md` §21).

This is what makes constitution Principle II (*Determinism and Reproducibility*) structurally
enforceable rather than aspirational.

---

# 69. Related Documents

| Document | Contents |
|---|---|
| `.specify/memory/constitution.md` | Binding engineering rules (v1.1.1) |
| `docs/PRD.md` | What the system does and why |
| `docs/plan.md` | Implementation phases, quality gates, open conflicts |
| `docs/API.md` | HTTP contract, error envelope, status codes |
| `docs/database.md` | Schema, constraints, indexes, SQLite caveats |
| `docs/generator.md` | Fake-data generator contract |
| `docs/configuration.md` | Environment variables |
| `docs/testing.md` | Test strategy and fixtures |
| `docs/failure-simulation.md` | Failure scenarios |
| `docs/development.md` | Developer workflow |
| `specs/README.md` | Feature roadmap |
