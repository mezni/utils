# Fake Data Generator

## Fake E-Commerce Server

> **Status:** Draft — highest-priority feature (`specs/008-fake-data-generator`).
> **Authority:** `.specify/memory/constitution.md` v1.1.1
> **Invocation:** `uv run python -m ecommerce.seed`

The generator is a **first-class project capability**, not an ad-hoc collection of SQL statements.
Nobody should ever need to hand-insert rows to get a useful environment, and the generator must be
executable without the HTTP server running.

---

## Divergences from other documents

Reconciled against the constitution, `architecture.md`, `database.md`, `API.md`, and
`configuration.md`. Every conflict is recorded and resolved explicitly (the constitution's *Explicit Business Rules*, III).

| # | Divergence in this draft | Where | Resolution |
|---|---|---|---|
| F1 | Prices use `Decimal("89.99")`. | §17, §26 | **Resolved** — integer minor units. Would have regressed decision A5. |
| F2 | Default currency `CAD`. | §62 | **Resolved** — `USD`. |
| F3 | Uppercase statuses (`ACTIVE`, `PENDING`). | throughout | **Resolved** — lowercase snake_case. |
| F4 | `products.status` with `ACTIVE`/`INACTIVE`/`DISCONTINUOUS`. | §15, §18 | **Resolved** — `is_active` boolean. |
| F5 | Customers split into `first_name`/`last_name`/`phone`, with `address_line2`/`state`. | §21 | **Resolved** — `full_name` + `address_*` (`database.md` D9). |
| F6 | Inventory invariant `reserved_quantity <= quantity`. | §19, §39, §51 | **Resolved** — **wrong invariant**; see §19. |
| F7 | Payment methods `CARD`/`BANK_TRANSFER`/`CASH_ON_DELIVERY`. | §34 | **Resolved** — `fake_card`. |
| F8 | Emails on `@example.test`. | §5, §21 | **Resolved** — `example.invalid`, the domain used everywhere else. Both are RFC 2606 reserved; consistency matters more than the choice. |
| F9 | Transaction IDs `txn_test_000001`. | §5, §36 | **Resolved** — `faketxn_<NNNN>`, already published in `API.md` §6.1 and `database.md` §33. |
| F10 | Env vars `GENERATOR_SEED`, `GENERATOR_PROFILE`, `GENERATOR_*`. | §47 | **Resolved** — `RANDOM_SEED` and `SEED_PROFILE`, as published in `configuration.md`. |
| F11 | Different profile counts (`test` 3/10/5/5/10, `large` 50/5000/1000/10000). | §9–§11 | **Resolved** — published profile table kept. `database.md` §55 already quotes it. |
| F12 | CLI drops `--validate/--no-validate`, `--quiet`, `--output`. | §44, §45 | **Resolved** — all three restored; `--carts` added. |
| F13 | "For very large datasets, generation may be performed in bounded batches." | §40 | **Resolved** — single transaction is preserved. Batching is deferred and must not weaken all-or-nothing. |
| F14 | `payments.provider_code` column. | §32 | **Recorded** — not in the published schema. Proposed; see §32. |
| F15 | Tree omits `__main__.py`; renames `config.py`/`rng.py` to `profiles/`. | §3 | **Resolved** — published layout kept (`__main__.py` is required by the constitution's *Simple Local Development* (V)). |
| F16 | Failure reasons `CARD_DECLINED`, `PROVIDER_UNAVAILABLE`, `TIMEOUT`. | §37 | **Resolved** — aligned with `failure-simulation.md` scenario names. |
| F17 | `GENERATOR_START_DATE` / `GENERATOR_END_DATE`. | §61 | **Recorded** — expressed through the `Clock` port instead. See §61. |
| F18 | "auto-increment behavior should be handled consistently". | §12 | **Resolved** — there is no auto-increment; identifiers come from `IdGenerator` (see `database.md` D3). |

---

# 1. Purpose

The Fake Data Generator creates realistic, synthetic e-commerce data for the Fake E-Commerce
Server.

It exists to support:

* Local development
* Integration testing
* API testing
* Demo environments
* Frontend development
* AI-agent experiments
* Failure and resilience testing
* Performance testing
* Reproducible test scenarios

The generator MUST create internally consistent data satisfying every domain and database invariant,
and MUST NEVER require real customer information, real payment credentials, or other real-world
sensitive data.

---

# 2. Design Goals

The generator must be:

1. **Deterministic** for a given seed.
2. **Configurable** by flag, environment, and profile.
3. **Reproducible** from the command line alone.
4. **Fast** enough for everyday local development.
5. Able to generate a **1/1/1** dataset for unit tests and a **20,000-order** dataset for load tests.
6. **Referentially consistent** — no orphans, ever.
7. **Domain-rule compliant** — it reuses domain code rather than reimplementing it (§56).
8. **Transaction-safe** — all or nothing.
9. **Reusable** from both the CLI and automated tests.
10. **Independent of HTTP** — never calls its own API.
11. **Extensible** as new entities are introduced.

---

# 3. Architecture

```text
src/ecommerce/seed/
├── __init__.py
├── __main__.py        # enables `python -m ecommerce.seed` (the constitution's *Simple Local Development*, V)
├── cli.py             # argument parsing, summary output, exit codes
├── config.py          # GeneratorConfig + profile definitions
├── generator.py       # orchestration, transactions, validation
├── rng.py             # seeded RNG + IdGenerator + Clock sources
├── factories/         # one per entity
│   ├── __init__.py
│   ├── category.py
│   ├── product.py
│   ├── inventory.py
│   ├── customer.py
│   ├── cart.py
│   ├── order.py
│   └── payment.py
└── profiles/
    ├── __init__.py
    ├── test.py
    ├── development.py
    └── large.py
```

```text
CLI
 │
 ▼
GeneratorConfig  (Pydantic)
 │
 ▼
EcommerceGenerator
 │
 ├── CategoryFactory ──┐
 ├── ProductFactory ───┤
 ├── InventoryFactory ─┤
 ├── CustomerFactory ──┼──► domain services / repositories
 ├── CartFactory ──────┤         │
 ├── CartItemFactory ──┤         ▼
 ├── OrderFactory ─────┤    Unit of Work
 ├── OrderItemFactory ─┤         │
 └── PaymentFactory ───┘         ▼
                            SQLite
```

**The generator MUST NOT call HTTP endpoints to create data.** It uses application and
infrastructure components directly. Going through the API would make seeding depend on a running
server, on authentication, and on the API's own validation — three ways for a data tool to acquire
reasons to fail that have nothing to do with data.

---

# 4. Technology

* Python 3.12+
* Faker (synthetic values)
* Pydantic v2 (configuration)
* SQLAlchemy 2.x (persistence)
* SQLite
* Alembic
* uv

```bash
uv add faker
```

---

# 5. Synthetic Data Requirements

All generated information MUST be synthetic.

```text
Name:           Alex Morgan
Email:          customer1@example.invalid
SKU:            AUDIO-0001
Transaction ID: faketxn_0001
```

The generator MUST NOT generate:

* Real payment card numbers
* Real banking credentials
* Real authentication credentials or API keys
* Real passwords
* Real personal contact information
* Real production transaction IDs

`example.invalid` is reserved by RFC 2606 and can never resolve, so generated email addresses cannot
accidentally reach a real mail server. `faketxn_` is a prefix, not just a value, so a synthetic
transaction id cannot be mistaken for a real one in a log line or a screenshot.

---

# 6. Dataset Generation Order

Generation MUST respect entity dependencies:

```text
1. Categories      no dependencies
2. Products        -> categories
3. Inventory       -> products
4. Customers       no dependencies
5. Carts           -> customers
6. Cart Items      -> carts, products
7. Orders          -> customers
8. Order Items     -> orders, products
9. Payments        -> orders
```

```text
Category
   │
   ▼
Product ──► Inventory
   │
   ├──────────────┐
   ▼              ▼
Cart            Order
   │              │
   ▼              ├──────► OrderItem
CartItem         └──────► Payment
```

The sequence MAY change when domain dependencies require it. The invariant does not: **no record is
created before everything it references exists.**

---

# 7. Generator Configuration

Configuration is a Pydantic model, validated before the database is touched:

```python
class GeneratorConfig(BaseModel):
    seed: int | None = None

    categories: int = 12
    products: int = 200
    customers: int = 50
    carts: int | None = None       # None = derived from customers
    orders: int = 300

    min_cart_items: int = 1
    max_cart_items: int = 5

    min_order_items: int = 1
    max_order_items: int = 6

    min_inventory: int = 0
    max_inventory: int = 100

    reset: bool = False
    validate: bool = True
```

Negative or zero counts for a required entity are a **configuration error**, rejected before any
connection is opened — a typo should not cost a full generation run to discover.

The model will evolve as capabilities are added; the fields above are the current contract.

---

# 8. Generator Profiles

```text
test
development   (default)
large
```

Profiles provide sensible defaults while remaining fully overridable (§46).

---

# 9. Test Profile

| Entity     | Count |
| ---------- | ----: |
| Categories |      5 |
| Products   |     20 |
| Customers  |     10 |
| Carts      |    derived |
| Orders     |     25 |

Purpose: unit and integration development, API tests, manual debugging, fast database recreation.

```bash
uv run python -m ecommerce.seed --profile test --seed 42 --reset
```

---

# 10. Development Profile

| Entity     | Count |
| ---------- | ----: |
| Categories |     12 |
| Products   |    200 |
| Customers  |     50 |
| Carts      |    derived |
| Orders     |    300 |

The default. A realistic local dataset.

```bash
uv run python -m ecommerce.seed --profile development --seed 42
```

---

# 11. Large Profile

| Entity     | Count |
| ---------- | ----: |
| Categories |     40 |
| Products   |  5,000 |
| Customers  |  2,000 |
| Carts      |    derived |
| Orders     | 20,000 |

For performance experiments, pagination testing, search testing, and API load testing.

```bash
uv run python -m ecommerce.seed --profile large --seed 42
```

The largest profile is deliberately bounded at 20,000 orders because of the single-transaction
strategy (§40): SQLite has one writer, and a maintenance command holding that lock for minutes is
acceptable while doing it during a request is not.

---

# 12. Determinism

The generator MUST be deterministic.

```bash
uv run python -m ecommerce.seed --seed 42
```

The same **seed + configuration + application version + starting database state** MUST produce
equivalent data:

```text
Seed: 42, run 1 → customer 1 = "Alex Morgan", product 1 = "Wireless Keyboard"
Seed: 42, run 2 → customer 1 = "Alex Morgan", product 1 = "Wireless Keyboard"
```

### 12.1 Rules

* All randomness flows through **one explicitly seeded RNG** (`random.Random(seed)`). No module-level
  `random.*`, no `uuid4()`, no `datetime.now()` in generation code.
* Identifiers come from the seeded `IdGenerator` port (`architecture.md` §68), not from `uuid4()`.
* Time values are drawn relative to a **configurable anchor instant**, supplied by the `Clock` port —
  never the wall clock. A dataset generated today and tomorrow is identical.
* Faker is seeded per instance via `seed_instance(seed)`, never globally.

```python
rng = random.Random(seed)
faker = Faker()
faker.seed_instance(seed)
```

There is **no auto-increment behavior to reconcile** (divergence F18) — identifiers are UUIDs drawn
from the seeded `IdGenerator`, stored as `TEXT` (`database.md` D3, **resolved**). Nothing in this
section depends on resetting a database sequence.

### 12.2 Verification

```bash
uv run python -m ecommerce.seed --seed 42 --output json > a.json
uv run python -m ecommerce.seed --seed 42 --output json > b.json
diff a.json b.json   # must be identical
```

Because the application version participates in reproducibility, the summary records it.
Regenerating with a different version MAY legitimately differ.

---

# 13. Random Number Generation

The generator MUST use a controlled random source — a generator-specific instance, never global
randomness (§12.1). All factories receive the same seeded RNG; none constructs their own.

Mixing an unseeded source into the chain makes a failure that reproduces locally impossible to
reproduce anywhere else, which is the one thing a seed exists to prevent.

---

# 14. Category Generation

Categories are generated first, as they have no dependencies.

```text
Electronics   Computers    Home & Kitchen   Books
Sports        Office       Accessories      Gaming
Outdoor
```

Generated fields:

```text
id  name  slug  parent_id  description  created_at  updated_at
```

Category `name` and `slug` MUST be unique. `parent_id` is `NULL` for roots; the draft's
`description`-only shape is not used, because `API.md` §6.1 publishes `slug` and `parent_id`.

---

# 15. Product Generation

Products reference categories.

```json
{
  "id": "uuid",
  "category_id": "uuid",
  "sku": "AUDIO-0001",
  "name": "Wireless Keyboard",
  "description": "Synthetic product description",
  "price": { "amount": 8999, "currency": "USD" },
  "is_active": true,
  "created_at": "2026-09-25T14:03:00Z",
  "updated_at": "2026-09-25T14:03:00Z"
}
```

Money is `{amount, currency}` with an **integer** amount in minor units (divergence F1).

---

# 16. Product SKU Generation

Every product MUST have a unique SKU.

```text
{CATEGORY-PREFIX}-{NNNN}
```

`{NNNN}` is a zero-padded 4-digit sequence **scoped to the category**, so the `large` profile's
5,000 products across 40 categories (125 each) stay well inside the range. Uniqueness is enforced by
`UNIQUE (sku)` on `products`, not by the format.

```text
AUDIO-0001
AUDIO-0002
ELEC-0001
```

Requirements: unique, deterministic, human-readable, stable. A globally unique deterministic format
is acceptable if the category set changes — a SKU that must be recomputed when a category is renamed
is not stable.

---

# 17. Product Pricing

Prices MUST be realistic but synthetic, in integer minor units:

```text
500      ($5.00)
1599     ($15.99)
4999     ($49.99)
8999     ($89.99)
24999    ($249.99)
99999    ($999.99)
```

**This draft specifies `Decimal("89.99")`. Not adopted** — `float` and `Decimal` are both prohibited
as the money representation (`architecture.md` §20.1, `database.md` §13). A price of `$89.99` is the
integer `8999`, and `$0.50` is `50`, not `0.5`.

A category-appropriate range is used, so a laptop is not priced at the same magnitude as a book
strap.

---

# 18. Product Status — divergence F4

This draft proposes a three-value lifecycle:

```text
ACTIVE: 85%
INACTIVE: 10%
DISCONTINUED: 5%
```

**Not adopted.** `is_active` is the published boolean field (`API.md` §6.1, `database.md` D8), and a
boolean is generated from a probability, not a distribution:

```text
is_active = true   90%
is_active = false  10%
```

A minority of inactive products makes catalog filtering realistic without inventing a
`DISCONTINUED` state that no other document defines. If `specs/002-product-catalog` adopts that
state, the distribution moves there.

---

# 19. Inventory Generation

Every generated product MUST have an inventory record.

```text
product_id  quantity_on_hand  quantity_reserved  updated_at
```

### 19.1 The invariant — divergence F6, resolved by D17

This draft states:

```text
quantity >= 0
reserved_quantity >= 0
reserved_quantity <= quantity
```

**The third line names the wrong thing.** Under the D17 resolution the columns are:

```text
quantity_on_hand  = total physical stock
quantity_reserved = the portion committed to carts
```

so a reservation may never exceed on-hand — the draft's *intent* was right, and only its vocabulary
was wrong. The earlier rejection of this rule in this file claimed the third line "would make the
final unit of any product unreservable"; that claim was itself an artifact of reading
`quantity_available` as "stock not yet allocated", which is not what the column holds. The rule is
correct once the column is named for what it is.

The invariants the generator MUST satisfy:

```text
quantity_on_hand >= 0
quantity_reserved >= 0
quantity_reserved <= quantity_on_hand
```

```text
available_to_sell = quantity_on_hand - quantity_reserved
```

`database.md` §23.

> This file previously carried the tautology `quantity_reserved <= quantity_on_hand +
> quantity_reserved` alongside prose describing the column as "stock not yet allocated" — two
> mutually inconsistent readings of one column, in the same section. Resolved by D17; the tautology
> is gone corpus-wide.

---

# 20. Inventory Scenarios

The generator SHOULD deliberately produce varied inventory states, because uniform stock makes
inventory endpoints untestable:

| Scenario          | `quantity_on_hand` | `quantity_reserved` | Available |
| ----------------- | -------------------: | ------------------: | --------: |
| Well stocked      |                  100 |                  10 |        90 |
| Low stock         |                   10 |                   1 |         9 |
| Almost out        |                    2 |                   1 |         1 |
| Out of stock      |                    0 |                   0 |         0 |
| Fully reserved    |                   10 |                  10 |         0 |
| Partially reserved|                  50 |                  20 |        30 |

"Fully reserved" and "out of stock" are different states with the same available count and different
consequences: the first is transient, the second needs a restock. A dataset containing both is what
makes `INSUFFICIENT_INVENTORY` testable in its two real forms.

---

# 21. Customer Generation

Customers use Faker for name and address components.

```json
{
  "id": "uuid",
  "email": "customer1@example.invalid",
  "full_name": "Alex Morgan",
  "status": "active",
  "address": {
    "line1": "1 Example Street",
    "city": "Springfield",
    "postal_code": "00000",
    "country_code": "US"
  }
}
```

**This draft splits the name into `first_name`/`last_name` and adds `phone`, `address_line2`, and
`state`. Not adopted** — `full_name` and the four-column address are published (`database.md` D9).
Faker still generates a first and last name internally; they are composed into `full_name` rather
than stored separately, which keeps the schema and the wire format aligned.

All addresses are synthetic.

---

# 22. Customer Email Uniqueness

Customer emails MUST be unique. A collision is resolved deterministically:

```text
alex.morgan@example.invalid
alex.morgan.0001@example.invalid
alex.morgan.0002@example.invalid
```

The generator MUST NOT knowingly produce a duplicate and rely on the database to reject it — a
`UNIQUE` violation halfway through a 20,000-order run costs the whole transaction, and the fix
belongs in the generator.

The same rule applies to SKU (§16) and to `(cart_id, product_id)` (§25).

---

# 23. Customer Status

```text
active
inactive
suspended
```

Weighted heavily toward `active`, with a minority of `inactive` and `suspended`. Non-active customers
are what make the checkout precondition and the authorization rules testable.

---

# 24. Cart Generation

Carts reference customers.

```text
customer_id  status  created_at  updated_at
```

```text
active: 50%
checked_out: 35%
abandoned: 15%
```

A `checked_out` cart MUST have a corresponding order (§59). An `abandoned` cart MUST NOT.

---

# 25. Cart Item Generation

```text
cart_id  product_id  quantity  unit_price_amount  currency  created_at  updated_at
```

Rules:

* `quantity > 0`
* No duplicate `(cart_id, product_id)` pairs
* `unit_price_amount` is the product price at generation time (§26)

Cart items are generated for `active` and `abandoned` carts. A `checked_out` cart's items are
consumed by the order (§59).

---

# 26. Cart Price Snapshot

`cart_items.unit_price_amount` snapshots the product price at the moment the item was added. It
exists so the cart can display a stable subtotal.

Line arithmetic is exact integer multiplication:

```text
line_total_amount = quantity * unit_price_amount
```

**This draft specifies `Decimal` arithmetic here. Not adopted** (divergence F1) — with integer minor
units there is no rounding step to perform, which is precisely the property that made decision A5
worth making.

The snapshot is display-only. Whether checkout honours it or the current catalog price is deferred
to `specs/005-orders` (`database.md` D15).

---

# 27. Order Generation

```text
customer_id  status  total_amount  currency  shipping_address_json  failure_reason  created_at  updated_at
```

`shipping_address_json` is a snapshot of the customer's address at the time the order is created.
For direct orders (no cart), a generated address should be used. This column is NOT NULL
(`database.md` D12).

Every order MUST contain at least one order item. `total_amount` is computed from the items, never
assigned independently (§31).

`failure_reason` is populated when `status = 'failed'`.

---

# 28. Order Status

```text
pending
confirmed
processing
shipped
delivered
cancelled
failed
```

```text
pending: 10%   confirmed: 20%   processing: 20%
shipped: 20%   delivered: 20%   cancelled: 7%   failed: 3%
```

A realistic mixture across the lifecycle makes order-history and filtering endpoints meaningful. A
dataset where every order is `confirmed` tests nothing.

---

# 29. Order Date Generation

Orders SHOULD have realistic historical timestamps spread over a configurable window:

```text
today        yesterday     last week
last month   several months ago
```

The generator MUST ensure:

```text
created_at <= updated_at
```

Timestamps are drawn relative to the `Clock` anchor (§61), never the wall clock.

State and time MUST agree: an order `shipped` cannot have been created after the anchor instant, and
a `delivered` order cannot have a `created_at` later than its `updated_at`. A dataset whose
timestamps contradict its statuses is worse than one with uniform timestamps, because it produces
test failures that look like application bugs.

If per-status timestamp history is later introduced, the ordering MUST be monotonic through the
transition path — `delivered` is never earlier than `pending`.

---

# 30. Order Item Generation

Each order contains between `1` and `6` items by default (`min_order_items` / `max_order_items`).

```text
order_id  product_id  product_name  sku
quantity  unit_price_amount  line_total_amount  currency
```

`product_name` and `sku` are copied from the product as a **historical snapshot** (§60), so a later
catalog change cannot rewrite order history.

---

# 31. Order Total Calculation

```text
order_total_amount = SUM(order_item.line_total_amount)
```

```text
Item 1:  2 × 5000  = 10000
Item 2:  1 × 2500  =  2500
Order:               12500
```

The generator MUST NOT assign an order total independently of its items. A random total is the
easiest way to ship a dataset that fails its own validation, and it hides real total-calculation
bugs because nothing ever compares the two.

---

# 32. Payment Generation

```text
order_id  status  amount  currency  method
transaction_id  failure_reason  created_at  updated_at
```

One payment per order (`database.md` D13), so the relation is 1:1.

This draft adds a `provider_code` column (divergence F14). It is **not** in the published schema
(`database.md` §30). A provider code is genuinely useful for a fake provider — it is how a client
distinguishes "declined by issuer" from "provider unreachable" after the fact — but adding a column
requires a migration, so it is recorded as a proposal for `specs/007-payments` rather than adopted
here. The information can live in `failure_reason` until then.

---

# 33. Payment Amount

For a successful payment:

```text
payment.amount == order.total_amount
```

For a failed or declined attempt, the **requested** amount is retained, so the attempt still records
what was tried.

---

# 34. Payment Methods — divergence F7

```text
fake_card
```

This draft proposes `CARD`/`BANK_TRANSFER`/`CASH_ON_DELIVERY`. Not adopted — `fake_card` is
published, and the prefix is deliberate. No real payment credentials are ever generated, accepted, or
stored.

`fake_bank_transfer` and `fake_wallet` are the natural extensions if `specs/007-payments` adds them.

---

# 35. Payment Statuses

```text
pending
authorized
captured
declined
failed
refunded
```

```text
pending ──► authorized ──► captured ──► refunded
   ├──► declined
   └──► failed
```

Generated states MUST respect valid transitions. A `refunded` payment implies a prior `captured`, and
a `captured` payment implies a prior `authorized`; generating a terminal state in isolation produces
data that could never arise through the API.

---

# 36. Transaction IDs

```text
faketxn_0001
faketxn_0002
faketxn_0003
```

Deterministic when a seed is supplied, unique within the database.

This draft's `txn_test_000001` (divergence F9) is not adopted — `faketxn_<NNNN>` is already
published in `API.md` §6.1 and `database.md` §33, and changing the prefix would orphan every
document that shows one.

---

# 37. Payment Failure Data

Failed payments MAY carry a synthetic `failure_reason`:

```text
card_declined
provider_unavailable
timeout
payment_validation_failed
```

**This draft's uppercase `CARD_DECLINED`/`PROVIDER_UNAVAILABLE`/`TIMEOUT` is not adopted** (F16).
Failure reasons are stored lowercase like every other enum, and align with the scenario names in
`docs/failure-simulation.md`:

| Scenario                   | Stored `failure_reason`   |
| -------------------------- | ------------------------ |
| `payment_declined`         | `card_declined`          |
| `payment_provider_error`   | `provider_unavailable`   |
| `payment_timeout`          | `timeout`                 |
| `inventory_unavailable`    | *(order-level reason)*    |

A generated dataset that records a failure reason matching the injectable scenario means a test can
assert on stored state as well as on the response.

---

# 38. Referential Integrity

Every generated relationship MUST reference an existing entity:

```text
product.category_id     -> existing category
inventory.product_id    -> existing product
cart.customer_id        -> existing customer
cart_item.cart_id       -> existing cart
cart_item.product_id    -> existing product
order.customer_id       -> existing customer
order_item.order_id     -> existing order
order_item.product_id   -> existing product
payment.order_id        -> existing order
```

The generator tracks the ids it has created and never invents one, so a misconfigured profile cannot
produce an orphan.

---

# 39. Domain Invariants

The generator MUST NOT intentionally produce invalid domain state.

### Inventory

```text
quantity_on_hand >= 0
quantity_reserved  >= 0
quantity_reserved <= quantity_on_hand
```

### Cart

```text
quantity > 0
no duplicate (cart_id, product_id)
```

### Order

```text
at least one order item
quantity > 0
total_amount == SUM(line_total_amount)
```

### Payment

```text
amount >= 0
amount == order.total_amount
```

### Product

```text
price_amount >= 0
```

---

# 40. Transaction Strategy

A complete generation run executes inside a single transaction:

```text
BEGIN
    Generate categories
    Generate products
    Generate inventory
    Generate customers
    Generate carts
    Generate cart items
    Generate orders
    Generate order items
    Generate payments
    Validate
COMMIT
```

On any failure:

```text
ROLLBACK
```

The database MUST NOT be left partially populated. Validation runs **inside** the transaction, before
commit, so an invariant violation rolls back the entire dataset rather than committing a broken one.

**This draft allows "bounded batches" for very large datasets (divergence F13). Not adopted as
written.** Batching means intermediate commits, which means a failure at batch 9 of 10 leaves 8
batches of data behind — exactly the partial state this section exists to prevent. Batching remains a
legitimate future optimization, but it requires a resumable design (a generation journal, or an
idempotent per-entity key) and cannot be a transparent change to this loop. It is deferred
(`database.md` §68).

A single transaction is acceptable at the documented scale because the generator is a maintenance
command, not a request handler: holding SQLite's single write lock for the duration of a seed run
blocks nothing that matters.

---

# 41. Reset Behavior

```bash
uv run python -m ecommerce.seed --profile development --seed 42 --reset
```

Reset removes generated application data before generating the new dataset. It MUST NOT delete:

* Migration files
* Source code
* Configuration
* **The database schema**

Reset does **not** run `alembic downgrade` and does **not** drop tables. To rebuild the schema
itself:

```bash
make db-reset      # drop file + migrate + seed — a separate, explicit action
```

Rows the generator did not create are left alone; a marker column or dedicated schema scope
identifies generated data when a partial reset is requested.

---

# 42. Reset Ordering

Deletion follows FK dependency order, children before parents:

```text
payments
order_items
orders
cart_items
carts
inventory
products
customers
categories
```

`orders` is deleted before `carts` because `orders.cart_id` is a `RESTRICT` foreign key
(`database.md` D11). Deleting in the wrong order produces a `FOREIGN KEY constraint failed` error
that looks like a generator bug.

---

# 43. CLI

```bash
uv run python -m ecommerce.seed
```

```bash
uv run python -m ecommerce.seed --profile development --seed 42
```

```bash
make seed          # default profile
make seed-test     # small, fast profile
make reset         # wipe + regenerate
```

---

# 44. CLI Options

| Option | Type | Default | Notes |
|---|---|---|---|
| `--seed` | int | `RANDOM_SEED` env, else `42` | Seeds the explicit RNG |
| `--profile` | choice | *(none)* | `test`, `development`, `large` |
| `--categories` | int | profile | Number of categories |
| `--products` | int | profile | Number of products |
| `--customers` | int | profile | Number of customers |
| `--carts` | int | derived | Number of carts; omitted means a realistic subset of customers |
| `--orders` | int | profile | Number of orders |
| `--reset` | flag | off | Remove generated records first |
| `--validate/--no-validate` | flag | on | Post-generation validation (§50) |
| `--quiet` | flag | off | Suppress progress output |
| `--output` | choice | `text` | `text` or `json` for the summary |

`--carts` is new in this draft. It is additive and safe **provided** that omitting it still derives a
realistic minority of active carts (§24) — an explicit `--carts 10000` on a 50-customer dataset is
a valid request, and a confusing one, which is why the summary always prints the effective
configuration.

`--validate/--no-validate`, `--quiet`, and `--output` were dropped by this draft (divergence F12) and
are restored: `--no-validate` is what makes a 20,000-order run practical, and `--output json` is what
makes the determinism check in §12.2 scriptable.

---

# 45. CLI Help

```bash
uv run python -m ecommerce.seed --help
```

```text
Usage: python -m ecommerce.seed [OPTIONS]

  Synthetic e-commerce data generator. Deterministic for a given seed.

Options:
  --seed INTEGER           Seed for the deterministic RNG  [env: RANDOM_SEED]
  --profile [test|development|large]
                           Profile providing default counts  [env: SEED_PROFILE]
  --categories INTEGER      Number of categories
  --products INTEGER        Number of products
  --customers INTEGER       Number of customers
  --carts INTEGER           Number of carts (default: derived from customers)
  --orders INTEGER          Number of orders
  --reset                   Remove generated records before generating
  --validate / --no-validate
                           Validate the dataset before committing  [default: validate]
  --quiet                   Suppress progress output
  --output [text|json]      Summary format  [default: text]
  --help                    Show this message and exit.
```

---

# 46. Configuration Precedence

The generator uses the project's single precedence chain from `configuration.md` §8. It is not a
generator-specific chain:

```text
Explicit CLI flag / runtime argument
      ↓
Environment variable
      ↓
.env file
      ↓
Profile default
      ↓
Application default
```

The `.env` tier matters for reproducibility: if `.env` sets `RANDOM_SEED`, that value beats the
profile default, so a "profile default" run is not reproducible across machines with different
`.env` files. The effective value is echoed in the §49 summary for exactly this reason.

```text
--products 500
```

overrides:

```text
RANDOM_SEED=42  (unrelated; for illustration: SEED_PROFILE=development)
```

which overrides the `development` profile's 200.

The effective configuration is always printed in the summary, so any dataset is reproducible from the
command line alone.

---

# 47. Environment Variables

The generator reads the **already-published** configuration variables (divergence F10):

| Variable | Purpose |
|---|---|
| `RANDOM_SEED` | Default seed when `--seed` is omitted |
| `SEED_PROFILE` | Default profile when `--profile` is omitted |
| `DATABASE_URL` | Target database |

```env
RANDOM_SEED=42
SEED_PROFILE=development
```

**This draft's `GENERATOR_SEED`, `GENERATOR_PROFILE`, `GENERATOR_CATEGORIES`, `GENERATOR_PRODUCTS`,
`GENERATOR_CUSTOMERS`, `GENERATOR_CARTS`, and `GENERATOR_ORDERS` are not adopted.** `configuration.md`
publishes `RANDOM_SEED` and `SEED_PROFILE`, and per-entity counts are CLI flags. Adding a second
naming scheme for the same two settings means a user who sets `GENERATOR_SEED` gets a silent
no-op, because nothing reads it.

---

# 48. Generator Output

```text
Loading configuration...
Profile: development
Seed:    42
Reset:   true

Resetting generated data... done

Generating categories...  done   (12)
Generating products...    done   (200)
Generating inventory...   done   (200)
Generating customers...   done   (50)
Generating carts...       done   (31)
Generating cart items...  done   (94)
Generating orders...      done   (300)
Generating order items... done   (842)
Generating payments...    done   (300)

Validating counts...          done
Validating relationships...   done
Validating domain invariants... done
Validating totals...          done

Generation completed successfully.
```

This block is the canonical output. The four validation phases are printed separately because
validation runs **before** the commit (§40): a run that prints `done` four times and then fails has
told the truth about which check failed, and nothing was written. The `Resetting generated data...`
line appears only when `--reset` was passed; `Reset: false` omits both it and the value line.

`--quiet` suppresses progress; `--output json` emits only the summary (§49) so it can be diffed.

---

# 49. Generation Summary

Actual counts, not requested counts. The difference is the point of the summary — a profile that
asked for 300 orders and produced 298 is reporting a real problem.

```text
Generation Summary
------------------
Categories:    12
Products:     200
Inventory:    200
Customers:     50
Carts:         31
Cart Items:    94
Orders:       300
Order Items:  842
Payments:     300

Seed:            42
Profile:  development
Version:      0.1.0
Duration:     4.2s
```

```json
{
  "counts": { "categories": 12, "products": 200, "orders": 300 },
  "seed": 42,
  "profile": "development",
  "version": "0.1.0",
  "duration_seconds": 4.2
}
```

---

# 50. Generator Validation

Validation runs after generation, **inside the transaction** (§40), and by default cannot be
skipped.

### Counts

| Check | Assertion |
|---|---|
| Record counts | Actual rows match the requested configuration |
| Dependent counts | One inventory row per product; ≥ 1 order item per order |

### Relationships

| Check | Assertion |
|---|---|
| Foreign keys | Every FK resolves to an existing row |
| Orphans | No record points at a missing parent |
| Payments | Every payment references an existing order |
| Cart/order | Every `checked_out` cart has an order |

### Domain invariants

| Check | Assertion |
|---|---|
| Inventory | `quantity_on_hand >= 0`, `quantity_reserved >= 0` |
| Inventory | `quantity_reserved <= quantity_on_hand` |
| Cart items | `quantity > 0`, no duplicate `(cart_id, product_id)` |
| Order contents | Every order has ≥ 1 item |
| Order totals | `orders.total_amount == SUM(order_items.line_total_amount)` |
| Line totals | `line_total_amount == quantity * unit_price_amount` |
| Payments | `payment.amount == order.total_amount` |
| State coherence | Completed orders have valid payment states; failed orders have a reason |
| Prices | No negative prices or amounts |
| Uniqueness | No duplicate SKU, email, or transaction id |

Database constraints remain the final integrity boundary (`database.md` §40); application-level
validation happens before persistence wherever practical, so a violation is reported as a
comprehensible message rather than an `IntegrityError`.

If a relationship cannot be established, the generator **fails clearly** rather than writing a
partial record.

---

# 51. Business Validation

Illustrative assertions — the real checks are the §50 table expressed in the domain's own types:

```python
assert inv.quantity_on_hand >= 0
assert inv.quantity_reserved >= 0
assert inv.quantity_reserved <= inv.quantity_on_hand

assert order.total_amount == sum(i.line_total_amount for i in order.items)
assert len(order.items) >= 1

assert item.quantity > 0
```

These are written against domain objects, not ORM rows, so they assert the same thing the application
would.

---

# 52. Database Validation

The generator additionally relies on database constraints:

```text
UNIQUE SKU
UNIQUE customer email
UNIQUE (cart_id, product_id)
UNIQUE transaction_id
FOREIGN KEY relationships
CHECK (>= 0) constraints
```

SQLite foreign keys MUST be enabled for these to mean anything (`database.md` §34). A generator
running against a connection without `PRAGMA foreign_keys = ON` would happily create orphans, and
the §50 relationship checks are what would catch it.

---

# 53. Factory Responsibilities

Each factory produces data for one entity:

```text
CategoryFactory   → category data
ProductFactory    → product data
CustomerFactory   → customer data
InventoryFactory  → inventory data
CartFactory       → cart data
OrderFactory      → order data
PaymentFactory    → payment data
```

Factories MUST NOT orchestrate the dataset. That belongs to the generator (§54). A factory that
creates its own dependencies turns the generation order into an emergent property of import order.

---

# 54. Generator Orchestration

`generator.py` coordinates the factories:

```python
class EcommerceGenerator:
    def generate(self) -> GenerationResult:
        with self.uow:
            categories = self.factories.categories.create_many(n=self.config.categories)
            products = self.factories.products.create_many(n=..., categories=categories)
            inventory = self.factories.inventory.create_for_products(products)
            customers = self.factories.customers.create_many(n=...)
            carts = self.factories.carts.create_many(customers)
            cart_items = self.factories.cart_items.create_for(carts, products)
            orders = self.factories.orders.create_many(customers, products)
            payments = self.factories.payments.create_for(orders)

            self.validate()          # inside the transaction
            self.uow.commit()        # validation passed → commit

        return GenerationResult(...)
```

Note `commit()` after `validate()`. Validating and then committing in separate steps — or committing
before validating — is the single most likely way for this to ship broken.

---

# 55. Factory Data vs Persistence

Factories produce domain objects or creation-command inputs:

```text
Factory → generated data → domain service → repository → database
```

They SHOULD NOT contain SQLAlchemy session logic. A bulk-insert optimization MAY bypass the ORM, but
it must be an explicit, measured exception with the invariants re-checked afterwards (§50) — not the
default shape of a factory.

---

# 56. Generator and Domain Services

The generator MUST respect domain rules by **calling the same code the API calls**:

```text
OrderFactory → OrderCreationCommand → OrderService → Order
```

The generator MUST NOT bypass a business invariant to simplify seeding. A duplicated rule drifts, and
the generator then becomes capable of producing data the API itself would reject — which defeats the
purpose of having realistic fixtures.

Fast, purely synthetic fields (names, descriptions, emails) MAY be produced directly, since no
business rule governs them.

---

# 57. Generated Scenario Diversity

The dataset SHOULD deliberately contain every state that matters for testing:

```text
Products      active · inactive · out of stock · low stock · fully reserved
Customers     active · inactive · suspended
Carts         active · checked out · abandoned
Orders        pending · confirmed · processing · shipped · delivered
              cancelled · failed
Payments      authorized · captured · declined · failed · refunded
```

A uniform dataset makes a large fraction of the API untestable: if no product is out of stock, the
`409 INSUFFICIENT_INVENTORY` path never executes.

---

# 58. Relationship Diversity

Customers SHOULD NOT all have identical relationships:

```text
Customer A → 1 cart,  0 orders     (new shopper)
Customer B → 1 active cart, 3 orders
Customer C → 1 abandoned cart, 10 orders (repeat buyer who walked away)
```

This produces realistic query results for "most recent order", "customers with no orders", and
pagination over unevenly distributed data — three queries that behave very differently when every
customer has the same history.

---

# 59. Cart and Order Relationship

A `checked_out` cart MUST correspond to an order, and every order that came from a cart MUST record
`cart_id`:

```text
ACTIVE CART → CHECKOUT → ORDER   (orders.cart_id set)
DIRECT CREATE → ORDER            (orders.cart_id = NULL)
```

The generator MUST NOT create impossible states — a checked-out cart with no order is exactly the
inconsistency that `orders.cart_id` exists to detect (`database.md` D11), and generating one would
make the constraint look broken when it is working. Conversely, the `large` profile creates orders
with `cart_id = NULL`, which is why the column is nullable; and it MUST NOT create two orders for
one cart, because the column is `UNIQUE`.

An `abandoned` cart MUST NOT have an order.

---

# 60. Historical Price Behavior

Orders MUST preserve historical prices. The generator SHOULD be able to produce orders whose
snapshotted price differs from the current catalog price:

```text
Product current price:  9999
Historical order price: 8999
```

```text
order_item.unit_price_amount != products.price_amount
```

This exercises the snapshot logic rather than merely asserting it. Without this, a catalog that
silently rewrote historical prices would pass every generated-dataset check.

---

# 61. Generator and Time — divergence F17

The generator MUST support a controlled time range. This draft proposes `GENERATOR_START_DATE` and
`GENERATOR_END_DATE` environment variables; **not adopted as env vars.** Time is supplied through the
`Clock` port (`architecture.md` §68), which is the same mechanism the application uses:

```python
clock = FixedClock(anchor=datetime(2026, 9, 25, tzinfo=timezone.utc))
```

All generated timestamps are drawn relative to the anchor, within a configurable window. When no
range is configured, the anchor is the current application time — read through the port, never
`datetime.now()` called directly, so a test can substitute a fixed instant.

This keeps time injectable in exactly one place. A second env-var mechanism would mean the generator
and the application could disagree about what "now" is, and a dataset generated for a test could
contain timestamps the application would consider future-dated.

---

# 62. Generator and Currency

```text
USD
```

Currency is configurable at the application level. The initial implementation does NOT introduce
multi-currency conversion logic; all amounts in one dataset share a single currency
(`database.md` §14).

---

# 63. Performance

The generator should support larger datasets without per-row overhead. Candidate optimizations:

* Batch inserts
* SQLAlchemy bulk operations where the invariants allow
* Reduced Faker overhead (Faker is the usual hot spot)
* Pre-generated lookup collections (category names, adjectives)
* Reused random generators
* Bulk commit — bounded by §40

**Correctness takes priority over premature optimization.** Any optimization that bypasses a domain
service or a validation check must re-assert those checks afterwards, and the determinism test (§66)
must still pass byte-for-byte.

---

# 64. SQLite Considerations

* Foreign keys MUST be enabled (`PRAGMA foreign_keys = ON`) or every §50 relationship check is
  vacuous.
* Transactions MUST be used carefully — one long write transaction is acceptable for a maintenance
  command and unacceptable inside a request.
* Large runs SHOULD avoid unnecessary commits; within the constraints of §40, that means one commit.
* Database locking SHOULD be minimized — the generator takes the single write lock, so it should not
  run against a database a server is serving.
* The generator MUST NOT assume PostgreSQL-specific features.

---

# 65. Testing the Generator

```text
tests/unit/
└── test_generator_config.py
    test_generator_rng.py

tests/integration/
└── test_generator_counts.py
    test_generator_relationships.py
    test_generator_invariants.py
    test_generator_determinism.py
    test_generator_reset.py
    test_generator_profiles.py
    test_generator_cli.py
```

---

# 66. Determinism Test

```text
Generate into database A with seed 42
Generate into database B with seed 42

Compare every table
```

Expected: equivalent datasets, byte-for-byte. In practice, compare the `--output json` summaries
(§12.2) for the fast check, and a full table comparison for the integration test.

The comparison MUST cover identifiers and timestamps, not just counts — two datasets with the same
row counts and different ids are not deterministic.

---

# 67. Different Seed Test

```text
seed 42  →  dataset A
seed 100 →  dataset B
```

The datasets MUST differ while both remaining valid.

The test MUST NOT assert a specific Faker output — that couples the suite to a library version and
fails on an unrelated upgrade. It should assert *meaningful* difference (different values present) and
that both datasets satisfy every invariant.

---

# 68. Count Test

```bash
uv run python -m ecommerce.seed \
    --categories 5 \
    --products 20 \
    --customers 10 \
    --orders 25 \
    --seed 42
```

Expected:

```text
categories = 5
products   = 20
inventory  = 20      (one per product)
customers  = 10
orders     = 25
payments   = 25      (one per order)
```

Dependent counts are derived, so they are asserted rather than requested — that is the property worth
testing.

---

# 69. Referential Integrity Test

```text
for every product:      category exists
for every inventory:    product exists
for every cart:         customer exists
for every cart item:    cart exists, product exists
for every order:        customer exists
for every order item:   order exists, product exists
for every payment:      order exists
for every checked_out cart: an order exists
```

---

# 70. Invariant Test

```text
No negative quantities
No reserved quantity exceeding available + reserved
No negative prices
No duplicate SKUs
No duplicate customer emails
No duplicate (cart_id, product_id)
No order without items
order.total_amount == SUM(line_total_amount)
No invalid payment state
No order in an invalid state for its timestamps
```

---

# 71. Reset Test

```text
Generate a dataset          → count rows
Run with --reset            → count rows again
Verify: no duplication, expected counts, valid relationships
```

Also verify that reset did **not** drop the schema: querying `alembic_version` afterwards must still
show `head`.

---

# 72. Failure Handling

On failure the generator MUST:

```text
1. Capture the exception
2. Roll back the active transaction
3. Log useful diagnostic information
4. Return a non-zero exit code
5. NOT report success
```

```text
Generation failed.

Reason:
  INVENTORY_INVARIANT_VIOLATED
  product AUDIO-0042: quantity_reserved (15) exceeds
  available-to-sell (10)

Transaction rolled back. 200 products, 50 customers discarded.
```

Naming the invariant and the offending record is what makes a failed seed debuggable. "Generation
failed" sends the reader back to the start.

> The message is phrased behaviorally because the underlying column arithmetic is unsettled
> (`database.md` D17). An earlier draft of this file phrased it as `quantity_reserved (15) exceeds
> quantity_on_hand (10) + quantity_reserved (15)`, which is the tautological invariant restated as
> prose — it can never be violated, so it can never be the reason a run fails.

---

# 73. Exit Codes

| Code | Meaning |
| ---: | ------- |
| `0` | Success |
| `1` | Configuration error — invalid counts, unknown profile |
| `2` | Database error — missing, not migrated, or locked |
| `3` | Generation error — a factory or domain service raised |
| `4` | Validation error — data generated but failed §50 |

Distinct codes let CI distinguish "you passed a bad flag" from "your generator produced invalid
data", which need different fixes.

---

# 74. CLI Safety

`--reset` MUST be explicit. The generator MUST NEVER silently delete existing data.

```bash
uv run python -m ecommerce.seed
```

on an already-seeded database MUST refuse and tell the user to pass `--reset` — it MUST NOT
implicitly replace the dataset.

The CLI SHOULD announce reset clearly before performing it, since it is the one destructive operation
in the tool.

---

# 75. Development Workflow

```bash
uv sync
uv run alembic upgrade head
uv run python -m ecommerce.seed --profile development --seed 42
uv run uvicorn ecommerce.main:app --reload
```

Reset and regenerate:

```bash
uv run python -m ecommerce.seed --profile development --seed 42 --reset
```

---

# 76. Test Workflow

```bash
uv run python -m ecommerce.seed --profile test --seed 42 --reset
uv run pytest
```

Generator tests only:

```bash
uv run pytest tests/unit/test_generator*
uv run pytest tests/integration/test_generator*
```

---

# 77. Future Extensions

Possible later additions:

```text
Product reviews          Coupons and discounts      Product images
Shipping addresses      Shipping methods            Warehouses
Multiple stock locations Returns and refunds        Payment attempts
Order status history    Audit events                Promotions
Product variants        Multi-currency datasets     Customer segments
```

None may compromise determinism (§12) or domain integrity (§39). Any addition that introduces a new
entity or column requires a migration and a corresponding entry in `database.md` §68.

---

# 78. AI-Agent Dataset Scenarios

The generated dataset should support agent tool-calling experiments, where each scenario is
answerable by a documented endpoint:

```text
Find products under $50.
Find products that are out of stock.
Find customers with cancelled orders.
Find failed payments.
Find a customer's orders.
Find orders containing a particular product.
Determine whether a product can currently be purchased.
```

These are only answerable if the dataset contains inactive products, zero-stock products, cancelled
orders, and failed payments — which is the argument for §57.

---

# 79. Completion Criteria

The generator feature is complete when:

* The CLI exists with every §44 option.
* All three profiles exist with the §9–§11 values.
* Seeding is deterministic and proven by a byte-identical rerun (§66).
* `--reset` works and preserves the schema.
* All nine entity types are generated with valid relationships.
* Domain invariants hold for every generated record.
* Monetary calculations are exact and verified against line items.
* Generation is transactional: a mid-run failure leaves an empty dataset, never a partial one.
* Validation runs before commit and cannot be silently skipped.
* Scenario diversity (§57) and relationship diversity (§58) are present in every profile.
* Failure paths exit non-zero with a diagnostic naming the invariant (§72).
* Tests cover determinism, counts, relationships, invariants, reset, profiles, and the CLI.
* The effective configuration is printed so any dataset is reproducible from the command line.

---

# 80. Example Complete Run

```bash
uv run python -m ecommerce.seed --profile development --seed 42 --reset
```

```text
Loading configuration...
Profile: development
Seed:    42
Reset:   true

Resetting generated data... done

Generating categories...  done   (12)
Generating products...    done   (200)
Generating inventory...   done   (200)
Generating customers...   done   (50)
Generating carts...       done   (31)
Generating cart items...  done   (94)
Generating orders...      done   (300)
Generating order items... done   (842)
Generating payments...    done   (300)

Validating counts...          done
Validating relationships...   done
Validating domain invariants... done
Validating totals...          done

Generation Summary
------------------
Categories:    12
Products:     200
Inventory:    200
Customers:     50
Carts:         31
Cart Items:    94
Orders:       300
Order Items:  842
Payments:     300

Seed: 42   Profile: development   Version: 0.1.0   Duration: 4.2s

Generation completed successfully.
```

This is the canonical output block from §48 with the §49 summary inserted before the completion
line. The two sections are not independent formats: §48 defines the format, and this run shows the
summary that `--output json` also emits.

---

# 81. Final Generator Architecture

```text
                         ┌──────────────────────┐
                         │    Generator CLI     │
                         │ --profile --seed     │
                         │ --reset --output     │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │ GeneratorConfig      │
                         │ Pydantic             │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │ EcommerceGenerator   │
                         │ seeded RNG + Clock   │
                         │ + IdGenerator        │
                         └──────────┬───────────┘
                                    │
             ┌──────────────────────┼──────────────────────┐
             │                      │                      │
             ▼                      ▼                      ▼
      CategoryFactory        ProductFactory        CustomerFactory
             │                      │                      │
             └──────────────┬───────┴──────────────┬───────┘
                            │                      │
                            ▼                      ▼
                    InventoryFactory         CartFactory
                            │                      │
                            │                      ▼
                            │                CartItemFactory
                            │                      │
                            └──────────┬───────────┘
                                       ▼
                                OrderFactory
                                       │
                                       ▼
                                OrderItemFactory
                                       │
                                       ▼
                                PaymentFactory
                                       │
                                       ▼
                              Validation (§50)
                                       │
                                       ▼
                            Commit  /  Rollback
                                       │
                                       ▼
                                   SQLite
```

The generator is a **synthetic data generation subsystem**: deterministic orchestration,
domain-aware factories, transactional persistence, and post-generation validation that runs before
the commit rather than after it.
