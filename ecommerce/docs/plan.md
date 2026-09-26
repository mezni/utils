# Fake E-Commerce Server — Implementation Plan

> **Status:** Draft — derived from the project constitution
> (`.specify/memory/constitution.md`, v1.1.1).
>
> **Conflicts (tracked per constitution's *Explicit Business Rules* (III) — none resolved silently):**
>
> | # | Conflict | Where | Resolution |
> |---|---|---|---|
> | C1 | Spec feature decomposition. An earlier draft of this section proposed a 16-feature list in which the fake-data generator is `010`. The `specs/` tree uses 11 features in which the generator is `008` and was identified as *the important feature*. | §30 | **Resolved** — 11 features authoritative. The 16-item list is retained below as a rejected alternative. |
> | C2 | Constitution path. §4 listed `constitution.md` at the repository root. | §4 | **Resolved** — authoritative path is `.specify/memory/constitution.md` |
> | C3 | Generator entrypoint. §4 listed `seed/` without `__main__.py`, which `python -m ecommerce.seed` requires. | §4 | **Resolved** — `__main__.py` included |
>
> **Architecture divergences (found reconciling `architecture.md` against the other documents;**
> **full tables in `architecture.md` §1, §20.1, §60.1):**
>
> | # | Divergence | Where | Resolution |
> |---|---|---|---|
> | A1 | Documentation at repo root vs. under `docs/`. | arch. §1 | **Resolved** — same class as C2 |
> | A2 | `seed/` omitted `__main__.py`. | arch. §1 | **Resolved** — same class as C3 |
> | A3 | Order lifecycle omitted `FAILED`. | arch. §1 | **Resolved** — added; required by the constitution's *Explicit Business Rules* (III) and by the compensation path failure simulation exercises |
> | A4 | Payment lifecycle omitted `CAPTURED`, showed `AUTHORIZED → REFUNDED`. | arch. §1 | **Resolved** — `AUTHORIZED → CAPTURED → REFUNDED`, matching `API.md` §22.7–22.7 |
> | A5 | Money as `Decimal` vs integer minor units. | arch. §20.1 | **Resolved** — integer minor units; `float`/`Decimal` prohibited as the representation. Consistent with `API.md` §12.2, `database.md` §13, `generator.md` |
> | A6 | `POST /api/v1/checkout` vs `POST /api/v1/carts/{cart_id}/checkout`. | arch. §60.1 | **Deferred** — becomes an acceptance criterion in `specs/005-orders`; interim contract is the published `POST /api/v1/checkout` |
> | A7 | Product Catalog claimed "availability" that Inventory owns. | arch. §7.1 | **Resolved** — Catalog owns `is_active`; Inventory owns availability |
> | A8 | Determinism ports (`Clock`, `IdGenerator`, `RandomSource`) absent, breaking cross-refs in `testing.md` §5, `failure-simulation.md` §7, `generator.md` §12. | arch. §68 | **Resolved** — §68 added |

---

## 1. Project Overview

The Fake E-Commerce Server is a modular monolith implemented with Python and FastAPI.

The system provides a realistic e-commerce API backed by SQLite and supports:

* Product catalog
* Customers
* Shopping carts
* Orders
* Inventory
* Payments
* Fake-data generation
* Failure simulation
* Authentication
* Testing
* Observability

The implementation will proceed incrementally.

The initial implementation MUST remain simple enough to understand while establishing architectural
boundaries that can support future expansion.

---

## 2. Technology Stack

| Area               | Technology          |
| ------------------ | ------------------- |
| Language           | Python 3.12+        |
| API                | FastAPI             |
| Validation         | Pydantic v2         |
| ORM                | SQLAlchemy 2.x      |
| Database           | SQLite              |
| Migrations         | Alembic             |
| Package management | uv                  |
| Testing            | pytest              |
| HTTP testing       | HTTPX               |
| Fake data          | Faker or equivalent |
| Server             | Uvicorn             |

The project MUST remain a modular monolith during the initial implementation.

---

## 3. Architectural Approach

The system will use:

```text
API
 ↓
Application
 ↓
Domain
 ↓
Infrastructure
 ↓
SQLite
```

The dependency direction MUST remain inward.

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

The domain MUST NOT depend on FastAPI or SQLAlchemy.

---

## 4. Project Structure

The target structure is:

```text
fake-ecommerce-server/
│
├── pyproject.toml
├── uv.lock
├── alembic.ini
├── Makefile
├── .env.example
├── .gitignore
├── README.md
├── CHANGELOG.md
│
├── .specify/
│   └── memory/
│       └── constitution.md        # authoritative constitution (conflict C2)
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
│   ├── plan.md                     # this file
│   ├── architecture.md
│   ├── API.md
│   ├── database.md
│   ├── generator.md
│   ├── configuration.md
│   ├── testing.md
│   ├── failure-simulation.md
│   └── development.md
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
│           ├── __main__.py         # required for `python -m ecommerce.seed` (conflict C3)
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

> **Conflict C2 (resolved):** documentation lives under `docs/`, and the constitution is read from
> `.specify/memory/constitution.md`. The root-level `constitution.md`, `PRD.md`, `architecture.md`,
> `API.md`, `database.md`, `generator.md`, `configuration.md`, `testing.md`,
> `failure-simulation.md`, `development.md` entries from the original draft are **not** used.
>
> **Conflict C3 (resolved):** `seed/__main__.py` is included because the constitution's *Simple Local Development* (V) mandates
> `uv run python -m ecommerce.seed`.

The exact structure MAY evolve when feature specifications expose better boundaries.

---

## 5. Implementation Phases

The project will be implemented through the following phases:

```text
Phase 0  — Project Foundation
Phase 1  — FastAPI Foundation
Phase 2  — Database Foundation
Phase 3  — Product Catalog
Phase 4  — Customers
Phase 5  — Inventory
Phase 6  — Cart
Phase 7  — Orders
Phase 8  — Payments
Phase 9  — Fake Data Generator
Phase 10 — Checkout Workflow
Phase 11 — Failure Simulation
Phase 12 — Authentication
Phase 13 — Observability
Phase 14 — Testing Hardening
Phase 15 — Documentation and Developer Experience
```

---

## 6. Phase 0 — Project Foundation

## Objectives

Create the initial Python project and development environment.

## Tasks

1. Initialize the repository.
2. Initialize the project using `uv`.
3. Configure Python 3.12+.
4. Create `pyproject.toml`.
5. Add runtime dependencies.
6. Add development dependencies.
7. Configure `.gitignore`.
8. Create `.env.example`.
9. Create the source package.
10. Create the initial README.
11. Create the initial Makefile.
12. Configure formatting/linting if selected.
13. Verify the development environment.

## Dependencies

Initial dependencies SHOULD include:

```text
fastapi
uvicorn
pydantic
pydantic-settings
sqlalchemy
alembic
pytest
httpx
faker
```

## Completion Criteria

The project MUST install successfully with:

```bash
uv sync
```

and the test environment MUST execute successfully.

---

## 7. Phase 1 — FastAPI Foundation

## Objectives

Create the minimal FastAPI application.

## Tasks

1. Create `main.py`.
2. Create the FastAPI application.
3. Configure application metadata.
4. Add `/health`.
5. Add API version prefix.
6. Configure routers.
7. Configure development server.
8. Verify OpenAPI documentation.

Expected routes:

```text
GET /health
```

and eventually:

```text
/api/v1/*
```

## Completion Criteria

The server starts successfully:

```bash
uv run uvicorn ecommerce.main:app --reload
```

The following must work:

```text
GET /health
GET /docs
GET /openapi.json
```

---

## 8. Phase 2 — Database Foundation

## Objectives

Introduce SQLite, SQLAlchemy, and Alembic.

## Tasks

1. Configure SQLite.
2. Create the database session.
3. Create SQLAlchemy declarative base.
4. Configure engine creation.
5. Configure session management.
6. Configure Alembic.
7. Connect Alembic to SQLAlchemy metadata.
8. Create the initial migration.
9. Add database dependency to FastAPI.
10. Create test database configuration.

Database location:

```text
data/ecommerce.db
```

## Completion Criteria

The project MUST be able to:

```bash
uv run alembic upgrade head
```

and connect successfully to SQLite.

---

## 9. Phase 3 — Product Catalog

## Objectives

Implement the product domain.

## Domain

Initial entities:

```text
Category
Product
```

## Product capabilities

* Create product
* Retrieve product
* List products
* Update product
* Delete/deactivate product
* Search products
* Filter products
* Pagination

## Tasks

1. Define domain entities.
2. Define business rules.
3. Define SQLAlchemy models.
4. Create migration.
5. Create repository interface.
6. Implement repository.
7. Create application services.
8. Create Pydantic schemas.
9. Create FastAPI routes.
10. Add unit tests.
11. Add integration tests.
12. Add API tests.

---

## 10. Phase 4 — Customers

## Objectives

Implement customers.

## Capabilities

* Create customer
* Retrieve customer
* Update customer
* List customers
* Deactivate customer

## Tasks

1. Define Customer domain entity.
2. Define customer states.
3. Create database model.
4. Create migration.
5. Implement repository.
6. Implement application services.
7. Implement API.
8. Add validation.
9. Add tests.

Synthetic customer information MUST be used throughout development.

---

## 11. Phase 5 — Inventory

## Objectives

Implement product inventory.

## Capabilities

* View inventory
* Add stock
* Remove stock
* Reserve stock
* Release stock
* Check availability

## Core rules

Inventory MUST NOT become negative.

Reservation MUST fail when insufficient stock exists.

## Tasks

1. Define Inventory entity.
2. Define inventory rules.
3. Create database model.
4. Create migration.
5. Implement repository.
6. Implement inventory application service.
7. Implement API.
8. Add tests.
9. Add concurrency-related tests where practical.

---

## 12. Phase 6 — Cart

## Objectives

Implement shopping carts.

## Entities

```text
Cart
CartItem
```

## Capabilities

* Create cart
* Retrieve cart
* Add product
* Update quantity
* Remove product
* Clear cart
* Calculate cart total

## Tasks

1. Define domain entities.
2. Define cart rules.
3. Create database models.
4. Create migration.
5. Implement repositories.
6. Implement application services.
7. Implement API.
8. Add validation.
9. Add tests.

---

## 13. Phase 7 — Orders

## Objectives

Implement order management.

## Entities

```text
Order
OrderItem
```

## Initial order states

```text
pending
confirmed
processing
shipped
delivered
cancelled
failed
```

The exact state model MAY evolve based on the detailed specification.

## Tasks

1. Define Order entity.
2. Define OrderItem.
3. Define state transitions.
4. Define order-total calculation.
5. Create database models.
6. Create migration.
7. Implement repositories.
8. Implement application services.
9. Implement API.
10. Add tests.

Invalid state transitions MUST be rejected.

---

## 14. Phase 8 — Payments

## Objectives

Implement a simulated payment subsystem.

## Payment states

Example:

```text
pending
authorized
captured
declined
failed
refunded
```

## Capabilities

* Authorize payment
* Capture payment
* Fail payment
* Refund payment
* Retrieve payment

The payment system MUST use synthetic payment information.

It MUST NOT process real payment credentials.

## Tasks

1. Define Payment entity.
2. Define payment state transitions.
3. Create database model.
4. Create migration.
5. Implement payment repository.
6. Implement fake payment provider.
7. Implement application service.
8. Implement API.
9. Add deterministic payment failures.
10. Add tests.

---

## 15. Phase 9 — Fake Data Generator

## Objectives

Create the first-class database population system.

The generator MUST be independently executable.

Example:

```bash
uv run python -m ecommerce.seed
```

## Generated entities

```text
Categories
Products
Inventory
Customers
Carts
Cart Items
Orders
Order Items
Payments
```

## Tasks

1. Create generator package.
2. Create generator configuration.
3. Create deterministic random generator.
4. Create category factory.
5. Create product factory.
6. Create customer factory.
7. Create inventory factory.
8. Create cart factory.
9. Create order factory.
10. Create payment factory.
11. Implement dependency-aware generation.
12. Implement transaction handling.
13. Implement validation.
14. Implement reset support.
15. Implement CLI.
16. Implement generator profiles.
17. Add tests.

## CLI

The generator SHOULD support:

```bash
uv run python -m ecommerce.seed \
    --seed 42 \
    --customers 100 \
    --products 500 \
    --orders 1000
```

Profiles SHOULD include:

```text
test
development
large
```

See `docs/generator.md` for the full contract.

---

## 16. Phase 10 — Checkout Workflow

## Objectives

Implement the complete purchase workflow.

The target workflow is:

```text
Customer
   ↓
Cart
   ↓
Checkout
   ↓
Validate cart
   ↓
Reserve inventory
   ↓
Create order
   ↓
Authorize payment
   ↓
Confirm order
```

## Failure workflow

For example:

```text
Reserve inventory
       ↓
Payment declined
       ↓
Release inventory
       ↓
Mark order failed
```

## Tasks

1. Define checkout use case.
2. Validate cart.
3. Validate product availability.
4. Reserve inventory.
5. Create order.
6. Authorize payment.
7. Confirm order.
8. Implement compensation behavior.
9. Define transaction boundaries.
10. Add integration tests.
11. Add failure tests.

---

## 17. Phase 11 — Failure Simulation

## Objectives

Make the fake server useful for resilience and integration testing.

## Failure scenarios

Initial scenarios:

```text
payment_declined
payment_timeout
payment_provider_error
inventory_unavailable
service_timeout
internal_server_error
```

## Tasks

1. Define failure configuration.
2. Define deterministic failure mechanisms.
3. Implement fake provider failures.
4. Implement artificial latency.
5. Implement timeout simulation.
6. Implement configurable failure rates.
7. Add request-level failure overrides.
8. Add tests.
9. Document failure scenarios.

Failure simulation MUST be disabled or controlled by default.

See `docs/failure-simulation.md` for the full contract.

---

## 18. Phase 12 — Authentication

## Objectives

Add realistic but fake authentication.

## Initial roles

```text
customer
admin
service
```

## Tasks

1. Define authentication model.
2. Create synthetic credentials.
3. Implement authentication dependency.
4. Implement authorization checks.
5. Protect appropriate endpoints.
6. Add tests.
7. Document authentication behavior.

Authentication MUST remain separate from domain logic.

---

## 19. Phase 13 — Observability

## Objectives

Make server behavior easy to understand and debug.

## Tasks

1. Configure structured logging.
2. Add request IDs.
3. Log request duration.
4. Log important domain events.
5. Add consistent error logging.
6. Ensure sensitive information is excluded.
7. Add health endpoint.
8. Add readiness endpoint if required.
9. Add basic metrics if justified.

---

## 20. Phase 14 — Testing Hardening

## Objectives

Establish confidence in the complete system.

## Unit testing

Test:

```text
Domain rules
State transitions
Price calculations
Inventory rules
Payment rules
Application services
```

## Integration testing

Test:

```text
SQLite
Repositories
Transactions
Migrations
Checkout
Generator
```

## API testing

Test:

```text
Products
Customers
Cart
Orders
Inventory
Payments
Authentication
Failures
```

## Generator testing

Test:

```text
Deterministic output
Record counts
Foreign keys
Order totals
Payment relationships
Reset
Profiles
```

---

## 21. Phase 15 — Documentation and Developer Experience

## Objectives

Make the project easy to understand and run.

Documentation MUST include:

```text
README.md
CHANGELOG.md

docs/
├── PRD.md
├── plan.md
├── architecture.md
├── API.md
├── database.md
├── generator.md
├── configuration.md
├── testing.md
├── failure-simulation.md
└── development.md

.specify/memory/constitution.md
```

## Developer workflow

The target workflow SHOULD be:

```bash
uv sync
uv run alembic upgrade head
uv run python -m ecommerce.seed --profile development
uv run uvicorn ecommerce.main:app --reload
```

Testing:

```bash
uv run pytest
```

The project SHOULD provide convenient Makefile commands.

---

## 22. Database Migration Strategy

Every schema change MUST have an Alembic migration.

Example:

```bash
uv run alembic revision --autogenerate -m "create products"
uv run alembic upgrade head
```

Migration files MUST be reviewed before being applied.

The application MUST NOT silently modify the schema during startup.

---

## 23. Repository Strategy

Repositories SHOULD expose domain-oriented operations.

Avoid repositories that merely expose arbitrary SQLAlchemy operations.

Prefer:

```text
ProductRepository
CustomerRepository
CartRepository
OrderRepository
InventoryRepository
PaymentRepository
```

rather than a single generic repository for the entire system.

---

## 24. Application Service Strategy

Application services SHOULD represent use cases.

Examples:

```text
CreateProduct
AddProductToCart
CheckoutCart
ReserveInventory
AuthorizePayment
CancelOrder
GenerateFakeData
```

Application services MUST coordinate domain behavior rather than becoming a second domain model.

---

## 25. API Versioning Strategy

The initial API MUST use:

```text
/api/v1/
```

Routes SHOULD be grouped by bounded context.

Example:

```text
/api/v1/products
/api/v1/customers
/api/v1/carts
/api/v1/orders
/api/v1/inventory
/api/v1/payments
```

Breaking API changes SHOULD result in a new version.

---

## 26. Performance Considerations

The initial implementation MUST prioritize correctness and maintainability.

Performance optimization SHOULD be introduced based on measured behavior.

The system SHOULD eventually be tested with:

```text
Small dataset
Medium dataset
Large dataset
```

The fake-data generator MAY be used to create large datasets for testing.

Unnecessary optimization MUST be avoided.

---

## 27. Security Considerations

The system MUST:

* Validate all input.
* Use parameterized database operations.
* Avoid SQL injection.
* Avoid logging sensitive data.
* Avoid real payment information.
* Protect authenticated endpoints.
* Validate authorization.
* Avoid trusting client-calculated totals.

Security behavior MUST be covered by tests where applicable.

---

## 28. Quality Gates

Before a feature is considered complete:

```text
[ ] Specification exists
[ ] Architecture is defined
[ ] Domain rules are defined
[ ] Database changes have migrations
[ ] Implementation is complete
[ ] Unit tests exist
[ ] Integration tests exist where needed
[ ] API tests exist where needed
[ ] Error behavior is defined
[ ] Documentation is updated
```

A feature MUST NOT be considered complete merely because it works manually.

---

## 29. Implementation Order

The implementation MUST generally follow this dependency order:

```text
Foundation
    ↓
FastAPI
    ↓
SQLite
    ↓
Alembic
    ↓
Products
    ↓
Customers
    ↓
Inventory
    ↓
Cart
    ↓
Orders
    ↓
Payments
    ↓
Fake Data Generator
    ↓
Checkout
    ↓
Failure Simulation
    ↓
Authentication
    ↓
Observability
    ↓
Testing Hardening
    ↓
Documentation
```

Some independent work MAY proceed in parallel when it does not violate architectural
dependencies.

---

## 30. SpecKit Feature Mapping

> **CONFLICT C1 — RESOLVED.** The **11-feature** breakdown is authoritative, and the
> fake-data generator is **`008-fake-data-generator`**, explicitly identified as *the important
> feature*. This matches the `specs/` tree on disk and the constitution
> (*Speckit Development Process*). The 16-feature list that appeared in an earlier draft of this
> section is **rejected** and retained below only for traceability.
>
> Do not create `012-`+ directories. If a capability genuinely outgrows its feature — most likely
> `checkout` or `testing-hardening` — that is a new Spec Kit proposal, not an implicit split.

**Authoritative — 11 features (in place under `specs/`):**

```text
001-foundation
002-product-catalog
003-customers
004-cart
005-orders
006-inventory
007-payments
008-fake-data-generator      <-- high priority, the important feature
009-failure-simulation
010-authentication
011-observability
```

**Rejected alternative — 16 features (earlier draft of this section; not adopted):**

```text
001-foundation
002-fastapi-foundation
003-database-foundation
004-product-catalog
005-customers
006-inventory
007-cart
008-orders
009-payments
010-fake-data-generator      <-- renumbered away from 008; not adopted
011-checkout
012-failure-simulation
013-authentication
014-observability
015-testing-hardening
016-developer-experience
```

**Why it was rejected:**

* It renumbered the fake-data generator from `008` to `010`, contradicting both the existing
  `specs/` tree and the decision that the generator is the project's most important feature.
* Its finer slicing is **not** better justified: `fastapi-foundation` and `database-foundation`
  are both trivially small and are better absorbed by `001-foundation`; splitting them would
  produce three near-empty features and more ceremony than value — contrary to the *Simplicity*
  principle.
* `011-checkout` is real work but is genuinely a cross-context *workflow* over cart, inventory,
  order, and payment. It is better specified inside `005-orders` (with `006-inventory` and
  `007-payments` as dependencies) than as a feature that owns none of the entities it touches.
* `015-testing-hardening` and `016-developer-experience` are not independent features. Testing is
  a *requirement of every* feature, and DX is a consequence of the whole system — neither can be
  meaningfully "completed" in isolation.

**Cross-reference**, for anyone revisiting this decision:

| Authoritative (11) | Rejected (16) | Relationship |
|---|---|---|
| `001-foundation` | `001` + `002` + `003` | merged into one |
| `002-product-catalog` | `004` | 1:1 |
| `003-customers` | `005` | 1:1 |
| `004-cart` | `007` | 1:1 (position differs) |
| `005-orders` | `008` + `011` | merged; checkout absorbed |
| `006-inventory` | `006` | 1:1 (position differs) |
| `007-payments` | `009` | 1:1 (position differs) |
| `008-fake-data-generator` | `010` | 1:1 (**numbering conflict — 008 wins**) |
| `009-failure-simulation` | `012` | 1:1 (position differs) |
| `010-authentication` | `013` | 1:1 (position differs) |
| `011-observability` | `014` + `015` + `016` | merged; testing/DX absorbed |

Each feature SHOULD have its own specification and implementation plan.

---

## 31. Definition of Done

The project is considered complete when:

* FastAPI provides the versioned e-commerce API.
* SQLite persists application data.
* Alembic manages database schema changes.
* Products can be managed.
* Customers can be managed.
* Carts can be managed.
* Inventory can be reserved and released.
* Orders can be created and transitioned.
* Payments can be simulated.
* Checkout works as a complete workflow.
* Failure scenarios can be reproduced.
* Fake data can populate the database.
* Fake data generation is deterministic.
* API authentication is available.
* Logging and health checks are available.
* Unit, integration, and API tests exist.
* Documentation describes how to run and use the system.
* A new developer can initialize and populate the system without manually inserting database
  records.

---

## 32. Final Implementation Principle

The project MUST evolve from a small working FastAPI application into a realistic e-commerce
backend incrementally.

Do not implement the entire architecture before validating the first use case.

Each phase MUST leave the application in a runnable and testable state.

The preferred progression is:

```text
Small
  ↓
Working
  ↓
Tested
  ↓
Structured
  ↓
Realistic
  ↓
Extensible
```

Complexity MUST be introduced only when the corresponding business requirement justifies it.
