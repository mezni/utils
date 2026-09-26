# Testing

## Fake E-Commerce Server

> **Status:** Draft
> **Authority:** `.specify/memory/constitution.md` v1.1.1, *Testability and Isolation*
> **Framework:** `pytest`, with `pytest-cov`, `httpx`, and `alembic` for schema setup

## Divergences from other documents

Reconciled against the constitution, `architecture.md`, `database.md`, `API.md`, `generator.md`, and
`configuration.md`. Every conflict is recorded and resolved explicitly (the constitution's *Explicit Business Rules*, III).

| # | Divergence in the incoming draft | Resolution | Now in |
|---|---|---|---|
| T1 | §8, §35, §42, §43 use a **fourth** inventory naming scheme: `quantity` / `reserved_quantity` / `available_quantity`, with the invariant `reserved_quantity <= quantity`. | **Resolved** — `quantity_on_hand` / `quantity_reserved` / `available_to_sell`, invariant `quantity_reserved <= quantity_on_hand` (`database.md` D17). | §8, §35, §42, §43 |
| T2 | §17, §37 use `"currency": "CAD"`. | **Rejected** — `USD` (`database.md` D4, `API.md` E3, `generator.md` F2, `configuration.md` G2). | §17, §37 |
| T3 | §17, §37 use `"price": "29.99"` as a **string**. | **Rejected** — money is integer minor units in a nested object: `{ "amount": 2999, "currency": "USD" }` (A5). | §17, §37 |
| T4 | §17 uses `"id": 1`. | **Resolved** — opaque UUID **strings** stored as `TEXT` (`database.md` D3, `API.md` E1). | §17, §37 |
| T5 | §17 uses `"status": "ACTIVE"`. | **Rejected** — lowercase (`D6`). Uppercase survives only in state-machine diagrams, which is a documented readability convention. | §9, §10, §17 |
| T6 | §5.1, §6.1 use `Product.activate()` / `deactivate()` and an `ACTIVE / INACTIVE / DISCONTINUOUS` lifecycle. | **Rejected** — `is_active BOOLEAN` is authoritative; the `DISCONTINUOUS` state is deferred (`D8`, `F4`). | §5.1, §6.1 |
| T7 | §9 order lifecycle **omits `failed`**. | **Rejected** — `failed` is required (A3) and is what the compensation path exists to exercise. | §9 |
| T8 | §10 payment lifecycle **omits `captured` and `refunded`**. | **Rejected** — `authorized → captured → refunded` (A4). | §10 |
| T9 | §41 mandates `Decimal` for monetary assertions. | **Rejected** — this directly contradicts A5, which prohibits `Decimal` *and* `float` as the money representation. The draft recommends the very thing the corpus prohibits. | §41 |
| T10 | §25 uses `FAKE_FAILURE_SEED=42`. | **Rejected** — not adopted (`G11`); `RANDOM_SEED` governs all determinism. | §25 |
| T11 | §14 puts the test database at `/tmp/fake-ecommerce-test-<uuid>.db`. | **Rejected** — `./.tmp/test.db` or in-memory, with a startup guard (`G14`, `configuration.md` §14.2). | §13, §14 |
| T12 | §44 uses `POST /carts/{id}/checkout`. | **Rejected for now** — the interim contract is `POST /api/v1/checkout` with `cart_id` in the body (A6, deferred to `specs/005-orders`). | §44 |
| T13 | §44 lists routes without the `/api/v1` prefix, and treats `POST /payments/{id}/authorize` as published. | **Rejected** — the `API_V1_PREFIX` is mandatory, and that route is an **E19 proposal** requiring sign-off in `specs/007-payments`. | §44 |
| T14 | §59 uses `.specify/specs/007-cart/`. | **Rejected on two counts** — the path is `specs/`, not `.specify/specs/` (C2), and the cart is feature `004`, not `007` (`007` is payments). | §59 |
| T15 | §19, §20 use `CHECKED_OUT` for the cart. | **Rejected** — lowercase `checked_out` (D6). | §19, §20 |
| T16 | §28 says "if database health is included". | **Clarified** — `GET /ready` is planned, not optional-and-conditional (E19, `API.md` §25.2). | §28 |
| T17 | §23, §40 depend on injected clocks and stable IDs but never name the ports. | **Resolved** — `Clock`, `IdGenerator`, and `RandomSource` (`architecture.md` §68, A8). This also makes `architecture.md` §68's quotation of "testing.md §5 — Injected `Clock` port" accurate. | §5, §23, §40 |
| T18 | §49 CI pipeline omits `FAKE_CLOCK`. | **Resolved** — added, per `configuration.md` §40. | §49 |
| T19 | §39 says "the production application should not depend on test-specific random seeds", which sits awkwardly with a shipped `RANDOM_SEED=42` default. | **Resolved** — the distinction is made explicit: the default is a documented, overridable value, not a test-only hook. | §39 |
| T20 | §2 principle 11 forbids test-specific behavior in production code, but failure simulation *is* test-specific behavior shipped in production code. | **Resolved** — a tension, resolved explicitly rather than papered over. See §2. | §2 |
| T21 | §12 says "Delete where supported", which is vague about soft delete. | **Resolved** — soft-delete semantics are published (E15); the vagueness is replaced. | §12 |
| T22 | §34 places factories in `tests/factories/`, but the §4 tree omits that directory. | **Resolved** — the tree now includes it. | §4, §34 |
| T23 | §24 omits `orders.cart_id` from the referential-integrity checklist. | **Resolved** — `orders.cart_id` is a nullable unique FK to `carts.id` (`database.md` D11); the checklist now covers it plus the address snapshot (D12). | §24 |

**This draft's genuinely new material** — the §57 per-feature minimum-coverage matrix, §43
property-based testing, §44 contract regression tests, §45 naming conventions, §46 markers, §48
coverage policy, §50 migration testing, §52 synthetic-data rules, §53 provider testing, §54 dependency
injection, §55 bug-to-regression-test process, §58 workflow, and §60 summary — is retained in full.

## Test architecture

This feature uses three test layers, each with a dedicated in-process `TestClient` so no external
server is required. The layers are:

1. **Unit** (`tests/unit/`) — domain rules, state transitions, calculations, validation. No database,
   no FastAPI, no HTTPX. Pure Python objects exercising business invariants. Fast, isolated, order-
   independent.

2. **Integration** (`tests/integration/`) — repositories, SQLite persistence, transactions, database
   relationships. Starts a real `alembic`-managed database in a temp directory. Verifies that the
   application correctly persists and reads data through the repository pattern.

3. **API** (`tests/api/`) — HTTP contract, workflows, failures, authentication. Starts the real
   FastAPI application via `create_app()` and uses `httpx.AsyncClient` against `http://127.0.0.1:8000`.
   Each test gets its own isolated test database. The `TestClient` approach (in-process, no OS process)
   is used for the unit and integration layers; the API layer uses `httpx.AsyncClient` against the
   running server because it tests the full HTTP pipeline (routing, error envelope, request ID echo,
   description switches).

The in-process `TestClient` approach means the next feature can extend the same structure — add
unit tests without spinning up a server, add integration tests that test the repository pattern, and
add API tests that test the full HTTP pipeline — without cross-layer contamination. Each layer has
its own test database, seed, and determinism controls.

---

# 1. Purpose

This document defines the testing strategy for the Fake E-Commerce Server.

Testing is a first-class architectural concern. The system must be testable at multiple levels without
requiring external services or production infrastructure.

The testing strategy must verify:

* Domain business rules
* Application use cases
* Repository behavior
* Database integrity
* API contracts
* Checkout workflows
* Payment behavior
* Inventory consistency
* Fake-data generation
* Failure simulation
* Configuration behavior
* Error handling
* Authentication boundaries
* Deterministic behavior

The primary testing framework is `pytest`.

---

# 2. Testing Principles

The project follows these principles:

1. Tests must be deterministic.
2. Tests must be isolated.
3. Tests must be fast where possible.
4. Domain tests must not require a database.
5. API tests must not require external services.
6. Integration tests must use an isolated SQLite database.
7. Tests must verify business invariants rather than only implementation details.
8. Failure scenarios must be explicitly tested.
9. Generated test data must be synthetic.
10. Tests must be repeatable locally and in CI.
11. Production code must not contain test-specific behavior.
12. Tests should favor observable behavior over internal implementation details.

## 2.1 Principle 11 is not absolute, and the exception is deliberate

Failure simulation (§25) is genuinely test-specific behavior that ships in the production codebase.
This is a real tension, not an oversight, and it is resolved by constraints rather than by wishful
thinking:

* It is **off by default** and requires an explicit master switch (`configuration.md` §23).
* A "production"-shaped environment **refuses to start** with simulation enabled.
* It is reachable only through infrastructure middleware, never from the domain.

The same reasoning applies to the determinism ports (§5): injecting a `Clock` is test-oriented, and
it ships, because the alternative is domain code that reads the wall clock and cannot be tested at
all. The rule is therefore better stated as: *production code must not contain behavior that is
**active by default** only for testing's benefit.* Anything gated and inert by default is acceptable;
anything that silently changes production semantics is not.

---

# 3. Testing Pyramid

```text
                    ┌─────────────────────┐
                    │     API / E2E       │
                    │      Tests          │
                    └──────────┬──────────┘
                               │
                  ┌────────────▼────────────┐
                  │     Integration         │
                  │        Tests            │
                  └──────────┬──────────────┘
                               │
             ┌─────────────────▼─────────────────┐
             │            Unit Tests             │
             │       Domain + Application        │
             └───────────────────────────────────┘
```

The expected distribution is:

* Many unit tests
* A moderate number of integration tests
* A smaller number of API/end-to-end tests

The project should avoid making every test an HTTP-level test.

---

# 4. Test Directory Structure

Tests are located outside `src/`.

```text
tests/
├── conftest.py
│
├── unit/
│   ├── domain/
│   │   ├── products/
│   │   ├── customers/
│   │   ├── carts/
│   │   ├── orders/
│   │   ├── inventory/
│   │   └── payments/
│   │
│   ├── application/
│   │   ├── products/
│   │   ├── customers/
│   │   ├── carts/
│   │   ├── orders/
│   │   ├── inventory/
│   │   └── payments/
│   │
│   └── seed/
│       ├── test_generator.py
│       ├── test_factories.py
│       └── test_profiles.py
│
├── integration/
│   ├── repositories/
│   │   ├── test_product_repository.py
│   │   ├── test_customer_repository.py
│   │   ├── test_cart_repository.py
│   │   ├── test_order_repository.py
│   │   ├── test_inventory_repository.py
│   │   └── test_payment_repository.py
│   │
│   ├── workflows/
│   │   ├── test_checkout.py
│   │   ├── test_inventory_reservation.py
│   │   └── test_payment_flow.py
│   │
│   └── database/
│       ├── test_constraints.py
│       └── test_migrations.py
│
├── api/
│   ├── test_health.py
│   ├── test_products.py
│   ├── test_customers.py
│   ├── test_inventory.py
│   ├── test_carts.py
│   ├── test_orders.py
│   ├── test_payments.py
│   └── test_checkout.py
│
└── factories/
    ├── __init__.py
    ├── product_factory.py
    ├── customer_factory.py
    ├── cart_factory.py
    ├── order_factory.py
    └── payment_factory.py
```

Tests must not be placed under:

```text
src/ecommerce/
```

The `src/` directory contains application code; `tests/` contains test code. `tests/factories/` is
listed explicitly because §34 depends on it.

---

# 5. Test Categories

## 5.1 Unit Tests

Unit tests test one component or business rule in isolation.

They should normally avoid:

* SQLite
* SQLAlchemy
* FastAPI
* HTTPX
* environment variables
* network calls
* filesystem dependencies
* the wall clock and unseeded randomness

The last two are the reason the determinism ports exist. A unit test that calls `datetime.now()` or
`random.random()` is not isolated no matter how few other dependencies it has.

Examples:

```text
Product.deactivate()
Cart.add_item()
Cart.remove_item()
Order.confirm()
Order.cancel()
Inventory.reserve()
Inventory.release()
Payment.authorize()
Payment.fail()
```

Unit tests should be the fastest tests in the project.

## 5.2 Injected determinism ports

Unit tests substitute the three determinism ports (`architecture.md` §68, divergence A8) rather than
monkey-patching the standard library:

| Port | Test substitute | Used for |
|---|---|---|
| `Clock` | `FrozenClock(fixed_instant)` | `created_at`, expiry, "recent" filters |
| `IdGenerator` | `SeededUuidIdGenerator(rng)` | Deterministic UUID primary keys |
| `RandomSource` | `SeededRandomSource(seed)` | Reproducible distributions |

```text
Inventory(quantity_reserved=0, clock=frozen, ids=seeded-uuid, rng=seeded)
```

Injecting a `Clock` port is the mechanism that makes §40's "avoid depending on the wall clock"
actionable rather than aspirational. Substituting a port is also why the domain layer is allowed to
depend on an interface: this is the paying customer for that dependency inversion.

---

# 6. Domain Testing

Domain tests verify business rules.

## 6.1 Product Tests

Test:

* Product creation
* Positive prices
* Valid currency
* `is_active` defaulting to `true`
* Deactivation via `is_active = false`
* Reactivation
* Price as integer minor units with a currency

```text
is_active = true   → listed by default catalog filter
is_active = false  → excluded by default catalog filter
```

A `DISCONTINUOUS` state is **not** tested, because it does not exist in the model. `is_active` is a
boolean, so there is no third state to assert; if `specs/002-product-catalog` adopts `DISCONTINUOUS`,
it becomes a derived field and the tests move there (`database.md` D8, `generator.md` F4).

---

# 7. Cart Testing

Cart tests must verify:

* Adding an item
* Updating quantity
* Removing an item
* Positive quantities
* Duplicate product behavior
* Cart totals
* Empty cart behavior
* Checkout eligibility

Example:

```text
quantity = 1    → valid
quantity = 5    → valid
quantity = 0    → invalid
quantity = -1   → invalid
```

Adding the same product twice should follow the defined application behavior rather than creating
duplicate product entries when the cart-item uniqueness rule applies.

---

# 8. Inventory Testing

Inventory is one of the most important areas of the test suite.

## 8.1 Behavioral invariants

Four documents described the same two inventory columns three different ways (`database.md` D17), and
D17 is now **resolved**: `quantity_on_hand` and `quantity_reserved`, with
`available_to_sell = quantity_on_hand - quantity_reserved` derived, never stored. The behavioral
invariants, which hold regardless of the naming question and are the part worth protecting:

```text
available-to-sell is never negative
reserving more than available-to-sell fails
releasing more than reserved fails
restocking increases available-to-sell
a reservation does not change on-hand stock
a release does not change on-hand stock
```

The last two are the tests that actually catch the class of bug D17 was about, and they are why the
naming mattered: a column called `quantity_available` invites an implementation that *decrements it on
reservation*, which would make a reservation look like a sale.

## 8.2 Concrete assertions

D17 is settled, so the assertions are concrete:

| | Column | Type | Constraint |
|---|---|---|---|
| On-hand | `quantity_on_hand` | `INTEGER NOT NULL` | `CHECK (>= 0)` |
| Reserved | `quantity_reserved` | `INTEGER NOT NULL` | `CHECK (>= 0)` |
| Available | *derived* | — | `quantity_on_hand - quantity_reserved` |

The binding invariant is `quantity_reserved <= quantity_on_hand`. There is no third stored column to
assert against: asserting `available >= 0` where `available` is a stored value would be asserting a
tautology, because nothing enforces that the three copies agree. The subtraction is the invariant.

Test:

* Increasing inventory
* Decreasing inventory
* Reserving inventory
* Releasing inventory
* Insufficient inventory
* Negative quantity attempts
* Releasing more than reserved
* Multiple reservations
* Boundary conditions

```text
quantity_on_hand = 10
quantity_reserved = 3

available_to_sell = 7
```

Attempting to reserve 8 must fail with `409 INSUFFICIENT_INVENTORY`.

---

# 9. Order Testing

Order tests must verify:

* Order creation
* At least one order item
* Quantity validation
* Total calculation
* Order status transitions
* Cancellation rules
* Historical product information

Expected lifecycle — **stored lowercase**, per `database.md` §26:

```text
pending → confirmed → processing → shipped → delivered
pending → cancelled
pending → failed
```

```text
PENDING
   │
   ├──► CONFIRMED ──► PROCESSING ──► SHIPPED ──► DELIVERED
   ├──► CANCELLED
   └──► FAILED
```

> The diagram is uppercase for readability only. The **stored and transported** values are lowercase.
> This convention is recorded in `database.md` D6, and it is not a contradiction to be "fixed" when
> it appears in a diagram.

`failed` is not optional. It is required by the constitution's *Explicit Business Rules* (III) and is the state the checkout
compensation tests (§20) assert against; a lifecycle without it cannot express "payment failed after
inventory was reserved" (`A3`).

Invalid state transitions must be rejected with `409 INVALID_ORDER_TRANSITION`.

---

# 10. Payment Testing

Payment tests must verify:

* Payment creation
* Amount validation
* Payment method validation
* Authorization
* Capture
* Decline
* Refund
* Provider error
* Timeout
* Retry
* Invalid state transitions

Expected lifecycle — **stored lowercase**, per `API.md` §24:

```text
pending → authorized → captured → refunded
pending → declined
pending → failed
```

```text
PENDING
   │
   ├──► AUTHORIZED ──► CAPTURED ──► REFUNDED
   ├──► DECLINED
   └──► FAILED
```

`captured` exists because capture and refund are separate endpoints (`API.md` §22.7–22.8). Without it,
a capture followed by a refund is indistinguishable from an authorization that was never captured,
and authorized-but-uncaptured exposure cannot be reported (`A4`).

Retry behavior must be explicitly tested. Retry applies to `pending` or `failed` only; retrying a
`captured` payment is `409 INVALID_PAYMENT_TRANSITION` (`API.md` E11).

---

# 11. Application Layer Testing

Application tests verify use cases and orchestration.

Examples:

```text
CreateProduct
CreateCustomer
AddCartItem
UpdateCartItem
ReserveInventory
CreateOrder
AuthorizePayment
CheckoutCart
CancelOrder
```

Application tests may use fake repository implementations.

For example:

```text
CheckoutApplicationService
        │
        ├── CartRepository
        ├── InventoryRepository
        ├── OrderRepository
        ├── PaymentRepository
        └── UnitOfWork
```

The test can replace these dependencies with in-memory fakes.

This allows application behavior to be tested without SQLite.

---

# 12. Repository Testing

Repository tests verify that infrastructure correctly implements domain repository contracts.

Test:

* Create
* Get by ID
* List
* Update
* Delete where supported
* Filtering
* Pagination
* Not-found behavior
* Constraint violations
* Transaction behavior

Example:

```text
ProductRepository
    create()
    get_by_id()
    list()
    update()
```

"Delete where supported" is a real distinction, not hedging (`E15`). The soft-delete semantics are:

* `DELETE` on a customer **deactivates**; it does not erase. The row persists and `status` becomes
  `inactive` (`database.md` §39).
* A resource with dependents is never hard-deleted: the dependent rows would be destroyed by
  `ON DELETE CASCADE`, and the `409 CONFLICT` response exists to say so.
* Hard delete is therefore only exercised for resources with no dependents, and only in tests that
  assert the refusal otherwise.

Repository tests use an isolated SQLite database.

---

# 13. Database Testing

Database integration tests must verify:

* Tables exist after migrations
* Foreign keys work
* Unique constraints work
* Check constraints work
* Indexes exist where required
* Relationships are correct
* Transactions commit correctly
* Transactions roll back correctly

SQLite foreign keys must be enabled during testing — `PRAGMA foreign_keys = ON` on **every**
connection, or every FK and `ON DELETE` assertion in this section silently passes for the wrong
reason (`database.md` §34). A test suite that verifies referential integrity against a connection with
FK enforcement off verifies nothing.

The test database must not be:

```text
data/ecommerce.db
```

The test suite must use a separate database, and `APP_ENV=test` **refuses to start** against a
non-test DSN (`configuration.md` §14.2).

---

# 14. Test Database

The preferred test database is an isolated SQLite database:

```text
./.tmp/test.db
```

or an in-memory database:

```text
sqlite+aiosqlite:///:memory:
```

The in-memory form is valid but carries a caveat: each connection gets its own database, so it must
be paired with a shared static pool or a single held connection, or the second connection will see an
empty schema (`database.md` §53).

The test database should be created automatically.

A test must never modify the developer's normal database. `/tmp/fake-ecommerce-test-<uuid>.db` is
**not adopted** (`T11`): a per-run unique file defeats reuse between tests, leaves orphaned databases
behind, and makes a failure hard to inspect after the fact. One known path, recreated per run, is
easier to clean up and easier to look at.

---

# 15. Database Fixture Lifecycle

The typical lifecycle is:

```text
pytest starts
     │
     ▼
Create temporary database
     │
     ▼
Apply Alembic migrations
     │
     ▼
Create test session
     │
     ▼
Run test
     │
     ▼
Rollback / cleanup
     │
     ▼
Dispose database
```

Tests should not depend on execution order.

---

# 16. API Testing

API tests use:

```text
FastAPI
HTTPX
pytest
```

The application should be tested through its public HTTP interface.

Example:

```text
POST  /api/v1/products
GET   /api/v1/products
GET   /api/v1/products/{id}
PATCH /api/v1/products/{id}
```

API tests verify:

* HTTP status codes
* Response structure
* Request validation
* Response validation
* Error responses
* Pagination
* Filtering
* Request IDs
* Authentication behavior
* Content types

Every route in this section carries the `API_V1_PREFIX` (`/api/v1`). `/health` and `/ready` are the
only exceptions, because a probe must not need to know the API version (`API.md` §25).

---

# 17. API Contract Testing

API tests must verify that responses conform to the documented API contract.

For example:

```json
{
  "id": "a1b2c3d4-0000-4000-8000-000000000001",
  "sku": "AUDIO-0001",
  "name": "Wireless Earbuds",
  "price": { "amount": 2999, "currency": "USD" },
  "is_active": true
}
```

Note what this replaces from the draft (`T2`–`T5`): the currency is `USD`, the price is an **integer**
`2999` in a nested object rather than the string `"29.99"`, the ID is a UUID **string** rather than
`1` (D3/E1, resolved), and status-like fields are lowercase.

Tests should verify:

* Required fields
* Field types
* Nullable fields
* Enum values
* HTTP status codes
* Error structure
* Money is an integer plus a currency, never a float or a decimal string

The OpenAPI specification should remain consistent with the implementation.

---

# 18. Error Testing

Every major error category must have tests.

Examples:

```text
ProductNotFound
CustomerNotFound
CartNotFound
OrderNotFound
InsufficientInventory
InvalidOrderState
InvalidPaymentState
PaymentDeclined
ProviderTimeout
ValidationError
AuthenticationError
AuthorizationError
```

The API should convert internal errors into the standard structure:

```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "Product was not found.",
    "details": [],
    "request_id": "7d3f4a2e-..."
  }
}
```

`details` is always an **array** of field issues, never an object (`API.md` E17). A not-found error
carries no field issues, so the array is empty.

Tests must verify that internal implementation details are not accidentally exposed.

---

# 19. Checkout Testing

Checkout is a critical integration workflow.

The normal workflow is:

```text
Cart
 │
 ▼
Validate cart
 │
 ▼
Validate customer
 │
 ▼
Validate inventory
 │
 ▼
Reserve inventory
 │
 ▼
Create order
 │
 ▼
Create payment
 │
 ▼
Authorize payment
 │
 ▼
Confirm order
 │
 ▼
Mark cart as checked out
```

Tests must cover both successful and unsuccessful paths.

## Successful checkout

Verify:

* Cart contains items
* Inventory is available
* Inventory is reserved
* Order is created
* Order items contain product snapshots
* Payment is authorized
* Order becomes `confirmed` (lowercase — `T15`)
* Cart becomes `checked_out` (lowercase)

---

# 20. Checkout Failure Testing

Checkout must be tested when:

```text
inventory is unavailable
payment is declined
payment provider times out
payment provider fails
cart is empty
cart does not exist
customer does not exist
product becomes unavailable
```

The system must leave the database in a consistent state.

For example, if payment fails after inventory reservation, the defined compensation strategy must
execute.

A test must verify:

```text
payment failure
      ↓
inventory reservation released
      ↓
order marked appropriately
      ↓
cart remains usable
```

"Marked appropriately" means the order reaches `failed` with a `failure_reason` — not `cancelled`,
and not left at `pending` (`A3`).

The exact behavior must follow the checkout specification, and the pricing authority question is
deferred to `specs/005-orders` (D15), so tests must not assume the cart snapshot is authoritative.

---

# 21. Transaction Testing

Transaction boundaries must be explicitly tested.

Example:

```text
BEGIN
   reserve inventory
   create order
   create payment
   authorize payment
COMMIT
```

If an operation fails:

```text
BEGIN
   reserve inventory
   create order
   payment fails
ROLLBACK
```

Tests must verify that partial state is not left behind.

---

# 22. Fake Data Generator Testing

The generator is a first-class component and requires its own test suite.

Test:

* Default profiles
* Custom counts
* Deterministic seeds
* Reset behavior
* Referential integrity
* Business invariants
* Relationship distribution
* Invalid configuration
* Transaction rollback

Example:

```bash
uv run python -m ecommerce.seed \
    --profile test \
    --seed 42 \
    --reset
```

---

# 23. Generator Determinism

The same seed and configuration should produce equivalent generated data.

Example:

```text
seed = 42
customers = 50
products = 100
orders = 100
```

Running the generator twice with the same configuration should produce equivalent logical data.

Tests should compare stable fields such as:

```text
SKU
product names
customer email
order relationships
generated statuses
```

Database-generated IDs should not be treated as the only definition of equality. IDs come from the
injected `IdGenerator` (§5.2), and a comparison keyed on surrogate identity rather than on content
would pass even if the generator produced different data every run.

---

# 24. Generator Integrity Tests

After generation, validate:

```text
Every product references an existing category.
Every inventory row references an existing product.
Every cart references an existing customer.
Every cart item references an existing cart.
Every cart item references an existing product.
Every order references an existing customer.
Every order with a non-null `cart_id` references an existing cart.
Every order item references an existing order.
Every payment references an existing order.
```

The generator must never intentionally create broken foreign-key relationships.

Two further assertions follow from D11/D12 and belong here:

```text
orders.cart_id is unique across non-null values   (one cart yields at most one order)
every order has a non-empty shipping_address_json (D12 — NOT NULL)
```

The first is the assertion that makes checkout idempotent for a given cart; the second is why the
`large` profile cannot generate a "direct" order without supplying an address.

---

# 25. Failure Simulation Testing

Failure simulation must be deterministic when a seed is configured.

Tests must cover:

```text
payment decline
payment timeout
payment provider error
inventory unavailable
provider latency
HTTP 500 simulation
```

Failure simulation should be disabled by default.

Example:

```dotenv
APP_ENV=test
FAKE_FAILURE_ENABLED=true
FAKE_FAILURE_RATE=0.25
RANDOM_SEED=42
```

Note the substitution: the draft's `FAKE_FAILURE_SEED=42` is **not adopted** (`T10`, `G11`). There is
one seed, `RANDOM_SEED`, and it governs both data generation and randomized failure injection. With
two seeds, a failure that reproduces locally and not in CI has no defined cause.

`FAKE_FAILURE_RATE=0.25` is **not** what most tests should use. Per `configuration.md` §23.1, a rate
above zero makes outcomes probabilistic, so an assertion that a request fails can lose a coin flip.
The default for a deterministic test is:

```dotenv
FAKE_FAILURE_ENABLED=true
FAKE_FAILURE_RATE=0.0
```

which enables only explicitly requested scenarios via `X-Fake-Failure`, and is fully deterministic.
Non-zero rates belong in soak tests, not assertions.

Tests should verify that the configured behavior is actually applied.

---

# 26. Authentication Testing

When authentication is enabled, tests must cover:

```text
anonymous request
valid customer
valid admin
service identity
invalid credentials
expired credentials
insufficient permissions
```

Authorization must be tested independently from authentication.

Example:

```text
Customer
    → can access own cart

Customer
    → cannot modify another customer's cart

Admin
    → can manage products

Service
    → can perform service-level operations
```

`AUTH_ENABLED` defaults to `true` (`configuration.md` §24, `G5`), so these paths are exercised in the
default configuration. A test that needs an unauthenticated path must set it explicitly; discovering
that the suite only covers authorization when auth happens to be on is how a permission boundary ends
up untested in practice.

---

# 27. Configuration Testing

Configuration tests must verify:

* Default values
* Environment variable overrides
* Invalid values
* Required settings
* Test environment overrides
* Generator settings
* Failure simulation settings
* Database configuration

Invalid configuration should fail early rather than producing undefined runtime behavior.

The rules under test are exactly the table in `configuration.md` §29, including the two hard guards:

* `APP_ENV=production` with simulation enabled **refuses to start**
* `APP_ENV=test` against a non-test database **refuses to start**

A guard that is never exercised is a guard that does not exist.

---

# 28. Health Endpoint Testing

The health endpoint:

```text
GET /health
```

must be tested.

The test should verify:

```text
HTTP 200
valid JSON
expected status field
```

`/health` MUST NOT touch the database (§`API.md` §25.1), and there should be a test asserting exactly
that — a liveness probe that queries the database will restart healthy processes during a slow query.

Database health is the separate `GET /ready` endpoint, which is **planned and not required for the
initial implementation** (`API.md` §25.2, E19). It is not "included if database health is in scope"
(`T16`); its absence is a known scope decision. When it lands, it must be tested both ways:

```text
database available
database unavailable
```

---

# 29. Request ID Testing

The API supports:

```text
X-Request-ID
```

Tests should verify:

1. Client supplies a request ID.
2. Server preserves it.
3. Response contains the request ID.
4. Logs contain the request ID where logging is enabled.

When no request ID is supplied, the application should generate one according to the API specification.

The header name is **not configurable** (`configuration.md` §18, G9), so there is no configuration
test for changing it — the contract is the contract.

---

# 30. Pagination Testing

Pagination tests must verify:

* Default page size
* Custom page size
* First page
* Middle page
* Last page
* Empty page
* Maximum page size
* Invalid page values

Example:

```text
?page=1&page_size=20
```

Boundary conditions must be explicitly tested. Responses use the nested `meta` envelope
(`API.md` §9), so tests assert on `meta.total` / `meta.page` / `meta.page_size` rather than on
top-level pagination fields.

---

# 31. Filtering and Sorting Tests

Where endpoints support filtering and sorting, test:

```text
valid filter
invalid filter
multiple filters
ascending sort
descending sort
unknown sort field
empty result
```

Tests should verify both:

* result correctness
* deterministic ordering

An unknown sort field must be rejected rather than ignored. Silently falling back to a default sort
produces two clients with the same query and different orderings, which is the kind of bug that only
shows up as "the list looks wrong" months later.

---

# 32. Concurrency Testing

SQLite has specific concurrency characteristics.

The test suite should include targeted tests for:

* Concurrent inventory reservations
* Multiple checkout attempts
* Simultaneous cart updates
* Transaction conflicts

The goal is not to simulate massive production traffic.

The goal is to verify that important business invariants remain protected under competing operations.

SQLite allows a single writer at a time, so these tests exercise the conditional-`UPDATE`/`rowcount`
path rather than row-level locking (`database.md` §50). They are the tests that catch a reservation
implemented as a read-then-write, which is correct in every single-threaded test and wrong in
production.

---

# 33. Test Fixtures

Common fixtures should live in:

```text
tests/conftest.py
```

Typical fixtures include:

```text
settings
test_engine
test_session
client
product
customer
cart
inventory
order
payment
seeded_database
```

Fixtures should remain small and composable.

Avoid creating a huge global fixture containing the entire database for every test.

---

# 34. Factory Pattern for Tests

Test factories may be used to create entities quickly.

Example:

```text
ProductFactory
CustomerFactory
CartFactory
OrderFactory
PaymentFactory
```

Test factories are separate from production fake-data factories.

Production generator:

```text
src/ecommerce/seed/
```

Test factories:

```text
tests/factories/
```

The two systems have different responsibilities. The generator is slow, deterministic, and
relationship-complete; a test factory is fast, minimal, and creates exactly the one entity under test.
Reusing the generator to build a single cart would make unit tests slow and turn every unit test into
an integration test.

---

# 35. Unit Test Example Structure

A domain test should follow:

```text
Arrange
   ↓
Act
   ↓
Assert
```

```python
def test_inventory_reservation_reduces_available_to_sell():
    inventory = Inventory(quantity_on_hand=10, quantity_reserved=0)

    inventory.reserve(3)

    assert inventory.quantity_reserved == 3
    assert inventory.available_to_sell() == 7
```

Note also that `available_to_sell()` is written as a **method**, not a stored field. Deriving it is
what makes it impossible for the stored value and the components to disagree.

The rejected alternative is instructive. Under the draft's `quantity_available` naming, the same test
would read `Inventory(quantity_available=10, quantity_reserved=0)` and assert `available == 10` after
reserving 3 — which is indistinguishable from asserting that a reservation does nothing, because with
a single "available" column there is nothing left to observe. D17 exists because that version cannot
express the property the test is trying to check.

The test should focus on the business behavior rather than SQLAlchemy internals.

---

# 36. Integration Test Example Structure

An integration test may look conceptually like:

```text
Arrange
    create database records

Act
    call repository

Assert
    returned domain object is correct

Verify
    database state is correct
```

Integration tests are allowed to use:

```text
SQLAlchemy
SQLite
repositories
UnitOfWork
Alembic
```

---

# 37. API Test Example Structure

API tests should use HTTPX.

Conceptually:

```python
response = client.post(
    "/api/v1/products",
    json={
        "name": "Wireless Earbuds",
        "sku": "AUDIO-0001",
        "price": {"amount": 2999, "currency": "USD"},
        "category_id": category.id,
    },
)

assert response.status_code == 201

data = response.json()

assert data["sku"] == "AUDIO-0001"
assert data["price"]["amount"] == 2999
assert data["price"]["currency"] == "USD"
```

This replaces the draft's `CAD` and string price (`T2`, `T3`). Asserting on the nested money object
rather than a serialized string is also what lets a type change fail a test instead of silently
passing a string comparison.

The API test should interact with the application as a client would.

---

# 38. Test Isolation

Tests must not depend on:

```text
test execution order
global mutable state
developer database
network availability
external APIs
random uncontrolled values
local machine configuration
wall-clock time
```

Each test should establish the state it needs.

---

# 39. Randomness

Randomness must be controlled.

For tests involving random behavior:

```text
seed = 42
```

should produce reproducible behavior.

The draft's claim that "the production application should not depend on test-specific random seeds"
needs one clarification (`T19`), because the project ships `RANDOM_SEED=42` as a **default
configuration value**. The distinction is between a documented, overridable setting and a test-only
hook:

* `RANDOM_SEED` is a real configuration variable with a real default, present in every environment
  and documented in `configuration.md` §42. Production behavior does not change when it is set; only
  reproducibility does.
* What production code must not do is read a seed out of the test environment, or branch on
  `PYTEST_CURRENT_TEST`.

So the rule is: seeds are configuration, not a test backdoor. A production code path whose behavior
differs between "seeded" and "unseeded" — other than the reproducibility of its output — is a bug.

---

# 40. Time Handling

Tests involving timestamps should avoid depending on the actual wall clock whenever possible.

Prefer the injected `Clock` port (§5.2) or controlled timestamps for domain logic.

For example:

```text
created_at = fixed_datetime
```

This makes tests deterministic. The `FAKE_CLOCK` setting (`configuration.md` §21) is the
environment-level equivalent, and CI sets it (`§49`). Both exist because a test that reads
`datetime.now()` will eventually fail at midnight, on a leap second, or in a month when a
"created in the last 24 hours" filter crosses a day boundary.

---

# 41. Testing Monetary Values

> **The draft recommends `Decimal` here. Not adopted** (`T9`). That directly contradicts A5, which
> prohibits `Decimal` *and* `float` as the money representation. A test suite that asserts money with
> `Decimal` while the domain stores integers is asserting against a different representation than the
> one under test, and it will pass while the real rounding bug lives in production.

Money is stored and transported as **integer minor units plus a currency code**. Assertions use
integers:

```python
assert order.total.amount == 3998
assert order.total.currency == "USD"
```

Not:

```python
assert order.total == 39.98  # float — prohibited
assert order.total == Decimal("39.98")  # Decimal — prohibited
```

A price of `$39.98` is the integer `3998`. Comparing integers is exact; there is no rounding step
that could differ between the test and the implementation, which is the entire reason A5 chose
integers in the first place.

---

# 42. Testing Business Invariants

Critical invariants should have dedicated tests.

### Inventory

Per D17 (§8):

```text
quantity_reserved <= quantity_on_hand
available_to_sell = quantity_on_hand - quantity_reserved  (derived, >= 0)
reserving more than available_to_sell fails
a reservation does not change on-hand stock
```

### Cart

```text
quantity > 0
```

### Order

```text
order contains at least one item
order total equals the sum of its item line totals
```

The second is a real cross-check: it catches a total recomputed from a stale cart snapshot, which is
exactly the D15 pricing-authority risk deferred to `specs/005-orders`.

### Payment

```text
amount > 0
valid status transition
```

### Product

```text
valid price
is_active is a boolean
```

These tests protect the domain model from regression.

---

# 43. Property-Based Testing

Property-based testing may be introduced later using `hypothesis`.

Potential properties:

```text
available-to-sell is never negative after any sequence of reserve/release/restock operations.
Cart quantities are always positive.
Order totals equal the sum of order item totals.
Generated relationships always reference existing entities.
No sequence of operations drives a status outside its enum.
```

The inventory property is stated as `quantity_reserved <= quantity_on_hand` per D17 (§8) rather than
as a named `available` column, because the value is derived and there is no stored field to drift.

This is optional during the initial implementation.

The first implementation should use conventional pytest tests.

---

# 44. Contract Regression Tests

The project should maintain regression tests for important API behavior.

Once an endpoint is implemented, its behavior should be protected against accidental changes.

Examples:

```text
POST /api/v1/products
GET  /api/v1/products
POST /api/v1/carts/{cart_id}/items
POST /api/v1/checkout
GET  /api/v1/orders/{order_id}
```

Two corrections to the draft (`T12`, `T13`):

* Every route carries the `API_V1_PREFIX`.
* The checkout route is `POST /api/v1/checkout` with `cart_id` in the body — the interim contract.
  `POST /carts/{id}/checkout` is the A6 alternative, deferred to `specs/005-orders`, and is not
  something to enshrine in a regression test before the spec settles it.
* `POST /api/v1/payments/{payment_id}/authorize` is deliberately **absent** from this list. It is an
  E19 proposal requiring sign-off in `specs/007-payments`, and a regression test would freeze an
  unapproved contract.

Changes to the API contract should require intentional test updates.

---

# 45. Test Naming

Test names should describe behavior.

Prefer:

```text
test_inventory_reserve_fails_when_quantity_is_insufficient
```

over:

```text
test_inventory_1
```

Prefer:

```text
test_checkout_releases_inventory_when_payment_is_declined
```

over:

```text
test_checkout_error
```

A test name should make the expected behavior obvious. When a test fails in CI six months from now,
its name is the first thing anyone reads, and `test_checkout_error` tells them nothing about what was
expected or what actually happened.

---

# 46. Test Markers

Pytest markers may be used:

```text
unit
integration
api
slow
generator
```

Example:

```bash
uv run pytest -m unit
```

```bash
uv run pytest -m integration
```

```bash
uv run pytest -m api
```

```bash
uv run pytest -m "not slow"
```

The project should keep the marker set small and meaningful. The draft's `checkout` marker is
**not adopted** — checkout tests are integration tests, and a second marker for a subset of them
means every CI command has to remember the combination.

---

# 47. Local Test Commands

Run the entire suite:

```bash
uv run pytest
```

Run unit tests:

```bash
uv run pytest tests/unit
```

Run integration tests:

```bash
uv run pytest tests/integration
```

Run API tests:

```bash
uv run pytest tests/api
```

Run a specific test file:

```bash
uv run pytest tests/unit/domain/inventory/test_inventory.py
```

Run a specific test:

```bash
uv run pytest \
    tests/unit/domain/inventory/test_inventory.py \
    -k reserve
```

Run with verbose output:

```bash
uv run pytest -v
```

---

# 48. Coverage

Coverage should be measured regularly.

Recommended command:

```bash
uv run pytest --cov=src/ecommerce --cov-report=term-missing
```

Coverage should be treated as a diagnostic metric rather than the only measure of test quality.

High coverage does not guarantee correct business behavior.

Critical domain rules should have explicit tests even if overall coverage is already high.

---

# 49. CI Testing

CI should execute at least:

```text
dependency installation
linting
type checking
unit tests
integration tests
API tests
coverage
migration validation
```

A typical pipeline is:

```text
Push
  │
  ▼
Install dependencies
  │
  ▼
Lint
  │
  ▼
Type check
  │
  ▼
Run unit tests
  │
  ▼
Run integration tests
  │
  ▼
Run API tests
  │
  ▼
Run migration checks
  │
  ▼
Generate coverage
```

CI must never depend on the developer's local SQLite database, and it pins the environment
(`configuration.md` §40) — note `FAKE_CLOCK`, which the draft omitted (`T18`) and without which
timestamp-dependent tests drift:

```bash
APP_ENV=test
DATABASE_URL=sqlite+aiosqlite:///./.tmp/test.db
RANDOM_SEED=42
FAKE_CLOCK=2026-01-01T00:00:00Z
FAKE_FAILURE_ENABLED=false
AUTH_ENABLED=true
LOG_LEVEL=WARNING
```

---

# 50. Migration Testing

Every database migration must be tested.

At minimum verify:

```text
upgrade from previous revision
upgrade to latest revision
database schema exists
foreign keys exist
constraints exist
indexes exist
```

Migration tests should also detect accidental destructive changes.

Tests build the schema with `alembic upgrade head`, never `create_all()` (`database.md` §41) — a
migration that is never applied is not tested, and `create_all()` builds whatever the ORM models
happen to say today rather than what the migration chain actually produces.

---

# 51. Seeded Integration Environment

A seeded integration environment may be created using:

```bash
uv run python -m ecommerce.seed \
    --profile test \
    --seed 42 \
    --reset
```

This is useful for:

* manual API testing
* integration tests
* agent experiments
* debugging
* local development

The seed process must not use production data.

---

# 52. Test Data Rules

All test data must be synthetic.

Do not use:

* real customer information
* real addresses
* real phone numbers belonging to people
* real payment credentials
* real API credentials
* production database exports

Fake payment transaction IDs should look realistic but must never represent actual transactions.
They are prefixed `faketxn_` so a synthetic identifier can never be mistaken for a real one at a
glance (`generator.md` F13).

Reserved example domains are used for synthetic email addresses (`customer1@example.invalid`, per the
`.invalid` TLD reserved by RFC 2606) rather than `example.com`, so a stray address in a log or
fixture cannot accidentally resolve.

---

# 53. External Provider Testing

External providers are represented by interfaces.

Example:

```text
PaymentProvider
       │
       ├── FakePaymentProvider
       │
       └── FutureRealPaymentProvider
```

Tests should normally use:

```text
FakePaymentProvider
```

rather than making network calls.

The fake provider must support controlled outcomes:

```text
success
declined
timeout
provider error
```

The fake provider's behavior is driven through the same `X-Fake-Failure` header as HTTP-level
failure simulation, so a provider timeout and an HTTP 500 are triggered the same way and tested the
same way (`docs/failure-simulation.md`).

---

# 54. Testing Dependency Injection

FastAPI dependencies should be overrideable in tests.

For example:

```text
production dependency
        ↓
real repository
```

can become:

```text
test dependency
        ↓
test repository
```

This allows API tests to control infrastructure without changing production code.

This is the pay-off of the composition-root rule (`configuration.md` §33): because the only place
configuration meets concrete implementations is `main.py`, a test can substitute an implementation
without the production code containing any test-specific branch.

---

# 55. Regression Testing

Every production bug discovered during development should result in a regression test when practical.

The process is:

```text
Bug
 ↓
Reproduce
 ↓
Write failing test
 ↓
Fix implementation
 ↓
Test passes
 ↓
Keep regression test
```

This prevents the same defect from silently returning.

---

# 56. Definition of Test Completion

A feature is considered adequately tested when:

* Domain rules have unit tests.
* Application use cases have unit tests.
* Repository behavior has integration tests.
* API behavior has API tests.
* Important failure paths are tested.
* Database constraints are tested.
* Relevant configuration is tested, including the startup guards.
* Generator behavior is tested.
* Critical invariants are tested.
* Tests are deterministic.
* Tests pass locally.
* Tests pass in CI.
* No test depends on production data.
* No test depends on wall-clock time or unseeded randomness.

---

# 57. Minimum Testing Requirements by Feature

| Feature            |    Unit | Integration |      API |
| ------------------ | ------: | ----------: | -------: |
| Categories         |     Yes |         Yes |      Yes |
| Products           |     Yes |         Yes |      Yes |
| Customers          |     Yes |         Yes |      Yes |
| Inventory          |     Yes |         Yes |      Yes |
| Cart               |     Yes |         Yes |      Yes |
| Orders             |     Yes |         Yes |      Yes |
| Payments           |     Yes |         Yes |      Yes |
| Checkout           |     Yes |         Yes |      Yes |
| Generator          |     Yes |         Yes | Optional |
| Failure Simulation |     Yes |         Yes |      Yes |
| Authentication     |     Yes |         Yes |      Yes |
| Observability      | Limited |         Yes |      Yes |

Checkout, inventory, and payments require particularly strong integration coverage because they
involve cross-context consistency.

The Generator's API coverage is optional because it is a command-line tool, not an HTTP surface; its
behavior is verified through the CLI instead. "Optional" here does not weaken §22 — the generator has
its own dedicated unit suite, which is where most of its risk actually lives.

---

# 58. Testing Workflow

For each new feature:

```text
1. Define behavior
       ↓
2. Write specification
       ↓
3. Write domain tests
       ↓
4. Implement domain logic
       ↓
5. Write application tests
       ↓
6. Implement application service
       ↓
7. Write repository integration tests
       ↓
8. Implement persistence
       ↓
9. Write API tests
       ↓
10. Implement endpoint
       ↓
11. Test failure paths
       ↓
12. Run full suite
       ↓
13. Update documentation
```

This workflow should be used for every Spec Kit feature.

---

# 59. Testing in Spec Kit

Each Spec Kit feature should define:

```text
Functional requirements
Acceptance criteria
Test scenarios
Failure scenarios
Definition of done
```

Example:

```text
specs/004-cart/
├── spec.md
├── plan.md
└── tasks.md
```

The draft's `.specify/specs/007-cart/` is wrong twice over (`T14`): the path is `specs/` at the
repository root, not `.specify/specs/` (the constitution's *Speckit Development Process*), and the cart is feature `004` — `007`
is payments.

The specification defines expected behavior.

The tests provide executable verification of that behavior.

---

# 60. Testing Strategy Summary

The Fake E-Commerce Server uses a layered testing strategy:

```text
                 API Tests
                    │
                    ▼
             Integration Tests
                    │
                    ▼
              Application Tests
                    │
                    ▼
                Domain Tests
```

The most important principles are:

```text
Fast
Deterministic
Isolated
Synthetic
Behavior-focused
Invariant-driven
Failure-aware
CI-compatible
```

The test suite is not simply a validation mechanism. It is part of the architecture and provides
executable documentation for the domain, application workflows, persistence layer, and public API.
