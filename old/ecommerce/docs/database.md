# Database Architecture

## Fake E-Commerce Server

> **Status:** Draft — the schema is specified but not yet migrated.
> **Engine:** SQLite (default) · **ORM:** SQLAlchemy 2.x · **Migrations:** Alembic
> **Dev file:** `data/ecommerce.db` · **Authority:** `.specify/memory/constitution.md` v1.1.1

---

## Divergences from other documents

This document was reconciled against the constitution and the other ten documents. Every conflict
was resolved explicitly — none silently (the constitution's *Explicit Business Rules*, III). Full rationale for each is in the
referenced section.

| # | Divergence in this draft | Where | Resolution |
|---|---|---|---|
| D1 | Money as `Decimal` in `NUMERIC(12,2)` columns, e.g. `price = 1299.99`. | §13 | **Resolved** — integer minor units. This would have regressed decision A5, which was made after this draft was started. See §13. |
| D2 | Payment status omits `CAPTURED`; shows `AUTHORIZED → REFUNDED`. | §31 | **Resolved** — `CAPTURED` restored. This would have regressed decision A4, which `API.md` §22.7–22.8 requires. See §31. |
| D3 | Integer primary keys; "UUIDs are not required for the first version". | §7, §8 | **Resolved** — UUID stored as `TEXT`. `API.md` §5.1 publishes `"id": "uuid"` (14 occurrences) and architecture §68 defines an `IdGenerator` port for byte-stable fixtures. See §7. |
| D4 | Default currency `CAD`. | §14 | **Resolved** — `USD`, the only currency published in `API.md` and `architecture.md`. |
| D5 | Payment methods `CARD` / `BANK_TRANSFER` / `DIGITAL_WALLET`. | §32 | **Resolved** — `fake_card`, the synthetic value published in `API.md` §23. |
| D6 | Status values uppercase (`ACTIVE`, `PENDING`). | §12, §16, §18, §26, §31 | **Resolved** — lowercase. `API.md` publishes `"status": "confirmed"`, `"status": "active"`. Uppercase survives only in state-machine diagrams, for readability. |
| D7 | `categories` loses `slug` and `parent_id`. | §10 | **Resolved** — both kept; `API.md` §6.1 publishes them in the category payload. |
| D8 | `products.status` with `ACTIVE`/`INACTIVE`/`DISCONTINUOUS`. | §11, §12 | **Resolved** — `is_active BOOLEAN` kept, per `API.md` §7 and divergence A7. The `DISCONTINUOUS` lifecycle is deferred — see §12. |
| D9 | `customers` splits `first_name`/`last_name` and flattens `address_line1`…`country`. | §15 | **Resolved** — `full_name` + `address_*` columns kept, matching the `address: { … }` object in `API.md` §7. |
| D10 | `inventory` gets a surrogate `id` PK plus `UNIQUE(product_id)`. | §22 | **Resolved** — `product_id` is the PK. The relation is strictly 1:1, so a surrogate key adds a second unique index for no gain. |
| D11 | `orders` has no link to the `carts` row it was created from. | §25 | **Resolved** — `cart_id TEXT NULL UNIQUE REFERENCES carts(id)`. Real gap; see §25. |
| D12 | `orders` snapshots product data but not the shipping address. | §25, §28 | **Resolved** — `shipping_address_json TEXT NOT NULL` write-once snapshot. Real gap; see §25. |
| D13 | `payments` is 1:N per order. | §30 | **Resolved** — 1:1 via `UNIQUE (order_id)`, so the order↔payment relation is unambiguous and capture/refund have one obvious target. |
| D14 | Timestamps as `DATETIME`. | §9 | **Resolved** — ISO-8601 `TEXT`. See §9 and §67. |
| D15 | Checkout pricing authority left undecided. | §21 | **Deferred** — becomes an acceptance criterion in `specs/005-orders`, alongside divergence A6. See §21. |
| D16 | No `idempotency_keys` table. | §68 | **Resolved** — deferred, consistent with architecture §46. Listed in §68. |
| D17 | `inventory` stores `quantity_available` as a column and asserts the tautology `quantity_reserved <= quantity_available + quantity_reserved`, while prose calls it "stock not yet allocated". | §23 | **Resolved** — columns are `quantity_on_hand` and `quantity_reserved`; available-to-sell is the derived expression `quantity_on_hand - quantity_reserved`, never stored. The load-bearing invariant is `quantity_reserved <= quantity_on_hand`, which can actually fail. `generator.md`, `architecture.md`, and `testing.md` each described the pair differently; all now use the column names. See §23. |

---

# 1. Purpose

This document defines the persistence architecture for the Fake E-Commerce Server.

It describes:

* Database technology
* Schema organization
* Identifier strategy
* Tables, columns, relationships, constraints, and indexes
* Foreign keys and delete behavior
* Enumerated values
* Transaction boundaries
* Migration strategy
* Generator and checkout interaction
* SQLite-specific constraints that affect application semantics
* Testing database strategy
* Data integrity rules

The database is designed to support a realistic e-commerce workflow while remaining simple enough
for local development and testing.

---

# 2. Database Goals

The database must provide:

* Referential integrity
* Transactional consistency
* Deterministic development environments
* Simple local setup
* Explicit schema migrations
* Appropriate indexing
* Clear relationships
* Historical order integrity
* Inventory consistency
* Payment state tracking
* Portability toward a future PostgreSQL implementation (§66)

The schema should avoid unnecessary complexity.

---

# 3. Database Technology

```text
SQLite
   +
SQLAlchemy 2.x
   +
Alembic
```

Default database:

```text
data/ecommerce.db
```

Configuration:

```env
DATABASE_URL=sqlite+aiosqlite:///./data/ecommerce.db
```

---

# 4. Database Responsibilities

The database is responsible for:

* Persisting domain state
* Enforcing structural constraints
* Maintaining foreign-key relationships
* Supporting transactions
* Supporting queries
* Providing indexes
* Preserving historical order data

The database should **not** be responsible for implementing complex business workflows. Business
rules remain in the domain and application layers (§40).

---

# 5. Schema Overview

The initial schema contains nine tables:

```text
categories
products
customers
carts
cart_items
inventory
orders
order_items
payments
```

Relationship overview:

```text
categories
    │
    └───< products ───┬── 1:1 ──> inventory
                      ├───< cart_items >── carts >── customers
                      └───< order_items >── orders >── customers
                                                  │
                                                  └── 1:1 ──> payments
```

---

# 6. Entity Relationship Diagram

```text
┌──────────────────────────────────────────────┐
│                  categories                   │
├──────────────────────────────────────────────┤
│ id           PK (TEXT)                       │
│ name         NOT NULL UNIQUE                 │
│ slug         NOT NULL UNIQUE                 │
│ parent_id    FK -> categories.id, NULL        │
│ description  NULL                            │
│ created_at   NOT NULL                        │
│ updated_at   NOT NULL                        │
└───────────────────┬──────────────────────────┘
                    │ 1:N
                    ▼
┌──────────────────────────────────────────────┐
│                   products                   │
├──────────────────────────────────────────────┤
│ id            PK (TEXT)                      │
│ category_id   FK -> categories.id, NOT NULL   │
│ sku           NOT NULL UNIQUE                │
│ name          NOT NULL                       │
│ description   NULL                           │
│ price_amount  NOT NULL, CHECK (>= 0)         │  minor units
│ currency      NOT NULL, ISO-4217             │
│ is_active     NOT NULL, default 1            │
│ created_at    NOT NULL                       │
│ updated_at    NOT NULL                       │
└───────┬──────────────────────┬───────────────┘
        │ 1:1                  │ 1:N
        ▼                      ▼
┌──────────────────────┐  ┌──────────────────────────┐
│      inventory       │  │        cart_items        │
├──────────────────────┤  ├──────────────────────────┤
│ product_id  PK, FK   │  │ id             PK (TEXT) │
│ quantity_on_hand     │  │ cart_id        FK        │
│ quantity_reserved    │  │ product_id     FK       │
│ updated_at  NOT NULL │  │ quantity       CHECK >0 │
└──────────────────────┘  │ unit_price_amount        │
                          │ currency                 │
                          │ created_at   NOT NULL    │
                          │ updated_at   NOT NULL    │
                          │ UNIQUE (cart_id,         │
                          │          product_id)     │
                          └─────────────┬────────────┘
                                        │ N:1
                                        ▼
                          ┌──────────────────────────┐
                          │          carts           │
                          ├──────────────────────────┤
                          │ id           PK (TEXT)   │
                          │ customer_id  FK, NOT NULL│
                          │ status       NOT NULL    │
                          │ created_at   NOT NULL    │
                          │ updated_at   NOT NULL    │
                          └─────────────┬────────────┘
                                        │ N:1
                                        ▼
┌──────────────────────────────────────────────┐
│                  customers                   │
├──────────────────────────────────────────────┤
│ id                  PK (TEXT)                │
│ email               NOT NULL UNIQUE          │
│ full_name           NOT NULL                  │
│ status              NOT NULL                  │
│ address_line1       NULL                      │
│ address_city        NULL                      │
│ address_postal_code NULL                      │
│ address_country_code NULL                     │
│ created_at          NOT NULL                  │
│ updated_at          NOT NULL                  │
└───────────────────┬──────────────────────────┘
                    │ 1:N
                    ▼
┌──────────────────────────────────────────────────────────┐
│ orders                                                   │
├──────────────────────────────────────────────────────────┤
│ id                                             PK (UUID) │
│ customer_id                 FK -> customers.id, NOT NULL │
│ cart_id              FK -> carts.id, NULL, UNIQUE  (D11) │
│ status                                          NOT NULL │
│ total_amount         NOT NULL, CHECK (>= 0)  minor units │
│ currency                                        NOT NULL │
│ shipping_address_json           NOT NULL  snapshot (D12) │
│ failure_reason                                      NULL │
│ created_at                                      NOT NULL │
│ updated_at                                      NOT NULL │
└────────────────┬──────────────────┬────────────────────┘
                 │ 1:N              │ 1:1
                 ▼                  ▼
┌──────────────────────────┐  ┌──────────────────────────┐
│       order_items        │  │         payments         │
├──────────────────────────┤  ├──────────────────────────┤
│ id                PK     │  │ id             PK (TEXT) │
│ order_id    FK, CASCADE  │  │ order_id       FK, UNIQUE│
│ product_id  FK, RESTRICT │  │ status         NOT NULL  │
│ product_name  NOT NULL   │  │ amount         NOT NULL  │
│ sku          NOT NULL    │  │ currency       NOT NULL  │
│ quantity     CHECK > 0    │  │ method         NOT NULL  │
│ unit_price_amount        │  │ transaction_id UNIQUE,   │
│ line_total_amount        │  │ failure_reason NULL      │
│ currency     NOT NULL    │  │ created_at     NOT NULL  │
└──────────────────────────┘  │ updated_at     NOT NULL  │
                               └──────────────────────────┘
```

The `orders.cart_id` edge back to `carts` (D11) is omitted from the diagram to keep the cart → order
flow readable, and is stated explicitly instead: `orders.cart_id` is `NULL`-able and `UNIQUE`, giving
**0:1 orders → carts** — each cart produces at most one order, and many orders have no cart at all
(direct creation by the generator or a future admin flow). See §25.

---

# 7. Identifier Strategy — divergence D3 (RESOLVED)

**This draft proposes integer primary keys. That is not what the rest of the project publishes.**

`API.md` §5.1 specifies:

| | Value |
|---|---|
| IDs | Opaque strings (UUID). Never client-generated. |

and every payload example uses `"id": "uuid"`, `"product_id": "uuid"`, and so on — 14 occurrences.
`architecture.md` §68 defines an `IdGenerator` port precisely so that generated fixtures are
**byte-stable across runs**, which requires injectable, non-database-generated identifiers.

**Decision: primary keys are opaque UUID strings stored as `TEXT`.** Every table in §10–§30 uses
`id TEXT PRIMARY KEY`. This document will not silently convert a published API contract.

| | **UUID stored as TEXT** (adopted) | Integer `PRIMARY KEY` (rejected) |
|---|---|---|
| Wire format | `"id": "3f7c…"` | `"id": 12` |
| Matches `API.md` | Yes (14 published occurrences) | No — breaking change |
| Deterministic fixtures | Yes, via `IdGenerator` (§68) | Yes, but only if the sequence is reset |
| Inspectable by hand | No | Yes |
| Index size | Larger (16-byte B-tree key) | Smaller (1–8 bytes) |
| Cross-system generation | Possible | Not possible |
| PostgreSQL portability | `UUID` native | `SERIAL`/`BIGSERIAL` |
| Enumeration risk | None | Sequential IDs leak volume/ordering |

The draft's "easy to inspect" benefit was real but small — a SQLite file is already trivial to
inspect with any SQL client, and the cost of switching is a published-contract change across
`API.md`, `architecture.md`, `generator.md`, and every future spec. Integer keys would also have
made generated fixtures depend on insertion order being reproducible across tables, which is a
stronger determinism obligation than the project accepts.

Storage note: the value is a canonical, lowercase, hyphenated UUID-4 string in `TEXT`. The column
carries no `UUID` affinity because SQLite has none (§13, §67) and no application-level parsing is
required — identifiers are opaque and are never decomposed.

---

# 8. Integer primary keys and `rowid` — rejected

The draft's rationale for integer keys is recorded here so the decision in §7 is traceable:

* The project is local and the database is SQLite.
* Integer IDs are simple to inspect and debug.
* They simplify some development and testing tasks.
* A future migration to PostgreSQL remains possible.

All four are outweighed by `API.md` publishing UUIDs and by the generator's determinism contract
being better served by injectable identifiers than by autoincrement sequences. If the project ever
revisits this, `API.md` must be updated in the same commit, and `IdGenerator` becomes a
sequence-reset port rather than a UUID port.

One point from the draft is retained regardless of the key type: the application MUST NOT depend on
SQLite's implicit `rowid` behavior. Any future integer key must be an explicit, declared
`INTEGER PRIMARY KEY`.

---

# 9. Timestamps — divergence D14

Entities with a lifecycle carry:

```text
created_at
updated_at
```

**Stored as ISO-8601 UTC `TEXT`** (e.g. `2026-09-25T14:03:00Z`), not `DATETIME` and not Python
`datetime` objects. Rationale in §67: SQLite has a weak `DATETIME` affinity and no native temporal
type, and ISO-8601 text sorts lexicographically in chronological order, so `ORDER BY created_at` is
correct without conversion.

Application code MUST use timezone-aware `datetime` objects and convert at the persistence boundary.

---

# 10. Categories Table

Purpose: stores the product taxonomy.

| Column      | Type         | Nullable | Constraints |
| ----------- | ------------ | -------: | ----------- |
| id          | TEXT         |       No | Primary key (UUID) |
| name        | VARCHAR(100) |       No | Unique, not empty |
| slug        | VARCHAR(120) |       No | Unique |
| parent_id   | TEXT         |      Yes | FK → `categories.id`, self-referencing |
| description | TEXT         |      Yes | |
| created_at  | TEXT         |       No | ISO-8601 |
| updated_at  | TEXT         |       No | ISO-8601 |

`parent_id` is nullable to represent a root category, and is what `API.md` §6.1 publishes. Category
depth limits belong to the domain, not to a `CHECK`.

Deleting a category that still has products is rejected (§35).

---

# 11. Products Table

Purpose: stores products in the catalog.

| Column       | Type         | Nullable | Constraints |
| ------------ | ------------ | -------: | ----------- |
| id           | TEXT         |       No | Primary key (UUID) |
| category_id  | TEXT         |       No | FK → `categories.id` |
| sku          | VARCHAR(64)  |       No | Unique |
| name         | VARCHAR(200) |       No | |
| description  | TEXT         |      Yes | |
| price_amount | INTEGER      |       No | `CHECK (price_amount >= 0)` — minor units |
| currency     | VARCHAR(3)   |       No | `CHECK (length(currency) = 3)` |
| is_active    | BOOLEAN      |       No | Default `1` |
| created_at   | TEXT         |       No | ISO-8601 |
| updated_at   | TEXT         |       No | ISO-8601 |

Foreign key:

```text
products.category_id → categories.id
```

`price` is exposed on the wire as a nested object — `{ "amount": 7999, "currency": "USD" }` — and
flattened into two columns here. The mapping is explicit (§63), not implicit.

---

# 12. Product Status — divergence D8

This draft proposed a three-value lifecycle:

```text
ACTIVE
INACTIVE
DISCONTINUOUS
```

`API.md` §6.1 instead publishes a boolean:

```json
"is_active": true
```

and divergence A7 established that the Product Catalog context owns **only** the catalog-level
active flag, while stock availability belongs to Inventory.

**Resolved: `is_active BOOLEAN` is authoritative.** The `DISCONTINUOUS` state is deferred — it
implies a product that remains orderable-by-lookup but must not be sold, which is a genuine
distinction from `is_active = false`, but nothing in `API.md`, `PRD.md`, or `architecture.md`
currently requires it. It belongs in `specs/002-product-catalog` if it is ever wanted, at which
point `is_active` becomes a derived field rather than the stored one.

Storage is `INTEGER` (`0`/`1`), not a `true`/`false` literal — see §67.

---

# 13. Money Storage — divergence D1

**Money is an integer count of minor units plus an ISO-4217 currency code.** `2499 USD` is $24.99.

```text
price = 2499
currency = USD
```

Every monetary column in this schema is `INTEGER`:

| Column                  | Table        |
|-------------------------|--------------|
| `price_amount`          | `products`   |
| `unit_price_amount`     | `cart_items` |
| `total_amount`          | `orders`     |
| `unit_price_amount`     | `order_items`|
| `line_total_amount`     | `order_items`|
| `amount`                | `payments`   |

**`float` is prohibited. `Decimal` is prohibited as the stored representation. `NUMERIC` and
`DECIMAL` column types are prohibited.** SQLite has no exact decimal type: `NUMERIC` affinity falls
back to `REAL` for values it cannot represent exactly, which reintroduces the floating-point error a
money type exists to eliminate. A `NUMERIC(12, 2)` column therefore gives the *appearance* of exact
decimal money while silently storing binary floating point.

This draft proposed `NUMERIC(12, 2)` columns with `Decimal` application values. That was correct
before decision A5 was made and wrong after it; the decision is recorded in `architecture.md` §20.1
and is normative on the wire (`API.md` §12.2), in storage (this section), and in generated data
(`generator.md`).

Line arithmetic is exact integer multiplication — `unit_price_amount * quantity` — with no rounding
step. Sub-unit currencies are represented in the currency's own unit (JPY has no minor unit); the
exponent is a property of the currency, not a hardcoded `2`.

---

# 14. Currency — divergence D4

Every monetary aggregate carries a `currency` column. This draft proposed `CAD` as the default; the
rest of the project publishes `USD` in every example (`API.md` §6.1 occurrences, `architecture.md`,
`generator.md`).

**Resolved: `USD` is the default and the only currency in the initial dataset.** `CAD` may be
generated as non-default currency data, but the default is `USD`.

The domain MUST NOT assume every currency shares a formatting rule or a two-decimal exponent. A
single order is single-currency; multi-currency orders are out of scope (§68).

---

# 15. Customers Table

Purpose: stores synthetic customer profiles.

| Column                 | Type         | Nullable | Constraints |
| ---------------------- | ------------ | -------: | ----------- |
| id                     | TEXT         |       No | Primary key (UUID) |
| email                  | VARCHAR(255) |       No | Unique |
| full_name              | VARCHAR(200) |       No | |
| status                 | VARCHAR(32)  |       No | See §16 |
| address_line1          | VARCHAR(255) |      Yes | |
| address_city           | VARCHAR(100) |      Yes | |
| address_postal_code    | VARCHAR(20)  |      Yes | |
| address_country_code   | VARCHAR(2)   |      Yes | ISO-3166-1 alpha-2 |
| created_at             | TEXT         |       No | ISO-8601 |
| updated_at             | TEXT         |       No | ISO-8601 |

This draft proposed splitting `full_name` into `first_name`/`last_name` and adding
`address_line2`/`state` columns. **Not adopted:** `API.md` §6.1 publishes a single `full_name` and a
nested `address: { line1, city, postal_code, country_code }` object. The flat `address_*` columns
here are the persistence form of that object.

`address_line2` and `state`/`province` are a reasonable addition for address realism; they are
deferred to `specs/003-customers` because nothing currently consumes them.

All generated customer data is synthetic (§52).

---

# 16. Customer Status

```text
active
inactive
suspended
```

Stored lowercase, as published in `API.md` §6.1. Enforced as `TEXT` plus a `CHECK (status IN (...))`
where practical; transition rules (may a `suspended` customer check out?) belong to the domain
(§40).

---

# 17. Carts Table

Purpose: represents a customer's shopping cart.

| Column      | Type        | Nullable | Constraints |
| ----------- | ----------- | -------: | ----------- |
| id          | TEXT        |       No | Primary key (UUID) |
| customer_id | TEXT        |       No | FK → `customers.id` |
| status      | VARCHAR(32) |       No | See §18 |
| created_at  | TEXT        |       No | ISO-8601 |
| updated_at  | TEXT        |       No | ISO-8601 |

---

# 18. Cart Status

```text
active
checked_out
abandoned
```

A cart has exactly one status at a time. "One active cart per customer" is a domain invariant, not
a database constraint — a partial unique index is possible but is deferred until
`specs/004-cart` states the rule precisely.

---

# 19. Cart Items Table

Purpose: stores products currently in a cart.

| Column           | Type        | Nullable | Constraints |
| ---------------- | ----------- | -------: | ----------- |
| id               | TEXT        |       No | Primary key (UUID) |
| cart_id          | TEXT        |       No | FK → `carts.id`, `ON DELETE CASCADE` |
| product_id       | TEXT        |       No | FK → `products.id` |
| quantity         | INTEGER     |       No | `CHECK (quantity > 0)` |
| unit_price_amount| INTEGER     |       No | `CHECK (>= 0)` — minor units, snapshot at add time |
| currency         | VARCHAR(3)  |       No | ISO-4217 |
| created_at       | TEXT        |       No | ISO-8601 |
| updated_at       | TEXT        |       No | ISO-8601 |

---

# 20. Cart Item Uniqueness

```text
UNIQUE (cart_id, product_id)
```

A product appears at most once per cart. Adding a product already in the cart increments the existing
row's `quantity` rather than inserting a second row; that behavior is the application layer's
responsibility, and the unique constraint is what makes it safe.

---

# 21. Cart Price Snapshot — divergence D15 (DEFERRED)

`cart_items.unit_price_amount` is a snapshot of the catalog price at the moment the item was added.
It exists so the cart can display a stable subtotal between visits.

The snapshot is **display-only**. This is stated here because the draft correctly identified that
checkout must not inherit it accidentally, and because the rest of the project has not yet decided
the rule. Open question:

```text
Does checkout use the cart's captured price, or the current catalog price?
```

| | Cart's captured price | Current catalog price |
|---|---|---|
| Customer trust | Cart total is what they agreed to pay | Cart total can change without warning |
| Realism | Unusual — real carts re-price | Matches real e-commerce |
| Detects price changes | No | Yes |
| Refund/dispute handling | Simpler | Needs a price-delta decision |

**Deferred to `specs/005-orders`**, which must state the rule and handle the case where the two
prices differ (accept, reject with `409 PRICE_CHANGED`, or absorb the difference). Listed as an
acceptance criterion in `specs/README.md`.

Until it is decided, cart display uses the snapshot and nothing else consumes it.

---

# 22. Inventory Table

Purpose: stores stock information for products. One row per product.

| Column              | Type    | Nullable | Constraints |
| ------------------- | ------- | -------: | ----------- |
| product_id          | TEXT    |       No | **Primary key** and FK → `products.id` (1:1) |
| quantity_on_hand  | INTEGER |       No | Default `0`, `CHECK (>= 0)` |
| quantity_reserved   | INTEGER |       No | Default `0`, `CHECK (>= 0)` |
| updated_at          | TEXT    |       No | ISO-8601 |

This draft proposed a surrogate `id` PK plus `UNIQUE (product_id)`. Not adopted: the relation is
strictly 1:1, so a surrogate key buys nothing and costs a second unique index on every write.

Naming uses `quantity_on_hand` / `quantity_reserved` rather than `quantity` / `reserved`, because
`quantity` alone is ambiguous between on-hand, available, and reserved.

---

# 23. Inventory Invariants

The application MUST enforce:

```text
quantity_on_hand >= 0
quantity_reserved >= 0
quantity_reserved <= quantity_on_hand
```

Available-to-sell is:

```text
available_to_sell = quantity_on_hand - quantity_reserved
```

The third invariant is the load-bearing one: **a reservation may never exceed on-hand stock.**
`quantity_on_hand` is total physical stock; `quantity_reserved` is the portion committed to carts;
the difference is what may still be sold. Reserving 25 of 100 leaves 75 available and does not
change on-hand.

> **Resolved — D17.** An earlier draft of this document used `quantity_available` and the tautological
> `quantity_reserved <= quantity_available + quantity_reserved`, alongside prose claiming
> `quantity_available` was "stock not yet allocated" — which contradicts the subtraction in the same
> section. `generator.md`, `architecture.md`, and `testing.md` each described the pair differently.
> The column is now named for what it holds, and the invariant is a real constraint that can fail.

Example:

```text
quantity_on_hand = 100
quantity_reserved =  25

available to sell = 75
```

The `CHECK` constraints are a **backstop**; the real rule lives in the domain, because a constraint
can reject a write but cannot explain, compensate for, or unit-test it (§40).

---

# 24. Inventory Reservation

To reserve `n` units:

```text
1. BEGIN
2. Read the inventory row
3. Verify (quantity_on_hand - quantity_reserved) >= n
4. quantity_reserved += n
5. COMMIT
```

If step 3 fails, the reservation fails with `409 INSUFFICIENT_INVENTORY` and nothing is written.

The operation MUST be atomic, and the availability re-check MUST happen inside the transaction
(§50) — checking before beginning the transaction is a time-of-check/time-of-use bug.

---

# 25. Orders Table

Purpose: stores customer orders.

| Column                | Type        | Nullable | Constraints |
| --------------------- | ----------- | -------: | ----------- |
| id                    | TEXT        |       No | Primary key (UUID) |
| customer_id           | TEXT        |       No | FK → `customers.id` |
| cart_id               | TEXT        |      Yes | FK → `carts.id`, `UNIQUE` — **divergence D11** |
| status                | VARCHAR(32) |       No | See §26 |
| total_amount          | INTEGER     |       No | `CHECK (>= 0)` — minor units, derived (§29) |
| currency              | VARCHAR(3)  |       No | ISO-4217 |
| shipping_address_json | TEXT        |       No | Write-once snapshot — **divergence D12** |
| failure_reason        | TEXT        |      Yes | Populated when `status = 'failed'` |
| created_at            | TEXT        |       No | ISO-8601 |
| updated_at            | TEXT        |       No | ISO-8601 |

`total_amount` is denormalized for query convenience. It MUST always equal the sum of the order's
`order_items.line_total_amount`; the invariant is verified by integration tests and by the
generator's post-generation validation (§48, §56).

### D11 — the cart link (RESOLVED)

**This draft has no column connecting an order to the cart it was created from.** Checkout converts
a cart into an order, but without a link the resulting order is only reachable through
`customer_id`, so:

* A customer with several carts cannot tell which cart produced which order.
* The `carts.status = 'checked_out'` transition cannot be verified against a real order.
* Retrying checkout has nothing to inspect for an already-created order, which weakens the
  idempotency story in `architecture.md` §46.
* Abandoned-cart analysis cannot exclude carts that were converted.

**Decision: `cart_id TEXT NULL UNIQUE REFERENCES carts(id)`.** All three properties are load-bearing:

* `NULL`-able — an order created directly (by the generator, or by a future admin flow) is still
  expressible. The generator's large profile creates orders that were never checked out.
* `UNIQUE` — a cart yields **at most one** order. This is what makes checkout idempotent for a given
  cart: a retried `POST /checkout` for the same `cart_id` finds the existing order instead of creating
  a second one. Without it, the nullable FK answers "where did it come from" but not "was it already
  converted", and the whole idempotency argument collapses.
* FK with `ON DELETE RESTRICT` — a cart that produced an order cannot be deleted, which the generator
  validates explicitly (`generator.md` §38 (referential integrity), §40 (validation before commit)).

`UNIQUE` in SQLite permits multiple `NULL`s, so the constraint does not interfere with direct order
creation.

### D12 — the shipping-address snapshot (RESOLVED)

§28 correctly snapshots product name, SKU, and price on order items, because history must not
change. The same argument applies to the shipping address, and this draft omits it: if the customer
later edits their profile address, an order created from that profile loses the address it was
actually shipped to.

**Decision: `orders.shipping_address_json TEXT NOT NULL`**, a write-once snapshot taken at checkout
holding the same shape `API.md` §6.1 publishes for `address`.

| Choice | Reasoning |
|---|---|
| `NOT NULL` | An order is a promise to ship somewhere. An order with no address is not a representable state, and the generator and the API both have a source for the value. |
| JSON text, not normalized columns | It is a write-once snapshot, never queried by field. Normalizing it would let a customer's address edit reach back and mutate order history — the exact failure the snapshot exists to prevent. |
| Not indexed | Nothing queries orders by address. |
| Immutable after insert | Application-enforced. The order flow never updates it. |

Because it is a snapshot rather than a reference, the `address` shape is duplicated from
`customers.address_*` by design. That duplication is the point: a later profile change must not
rewrite history.

---

# 26. Order Status

```text
pending
confirmed
processing
shipped
delivered
cancelled
failed
```

Stored lowercase, as published in `API.md` §6.1. Valid transitions are controlled by domain logic:

```text
PENDING
   ├──► CONFIRMED
   ├──► CANCELLED
   └──► FAILED

CONFIRMED ──► PROCESSING ──► SHIPPED ──► DELIVERED
```

Invalid transitions MUST be rejected with `409 INVALID_ORDER_TRANSITION`. A database `CHECK` cannot
express a transition table, so this rule is domain-only (§40).

`failed` is populated with `failure_reason`. Its existence is required by the constitution's *Explicit Business Rules* (III) and by
the compensation path that failure simulation exists to exercise.

---

# 27. Order Items Table

Purpose: stores the products included in an order.

| Column             | Type         | Nullable | Constraints |
| ------------------ | ------------ | -------: | ----------- |
| id                 | TEXT         |       No | Primary key (UUID) |
| order_id           | TEXT         |       No | FK → `orders.id`, `ON DELETE CASCADE` |
| product_id         | TEXT         |       No | FK → `products.id`, `ON DELETE RESTRICT` |
| product_name       | VARCHAR(200) |       No | **Snapshot** (§28) |
| sku                | VARCHAR(64)  |       No | **Snapshot** (§28) |
| quantity           | INTEGER      |       No | `CHECK (quantity > 0)` |
| unit_price_amount  | INTEGER      |       No | `CHECK (>= 0)` — **snapshot**, minor units |
| line_total_amount  | INTEGER      |       No | `CHECK (>= 0)` — minor units (§29) |
| currency           | VARCHAR(3)   |       No | ISO-4217 |

Order items have no `created_at`: they are created with their order, and the order carries the
timestamp.

---

# 28. Order Snapshot Data

Order items intentionally duplicate product information:

```text
product_name
sku
unit_price_amount
```

This is deliberate. An order is a historical record. If the product later changes:

```text
Product name:  Laptop Pro → Laptop Pro 2
```

the historical order still shows what was ordered. Prices must never be rewritten by a catalog
change.

`product_id` is retained alongside the snapshot purely as a back-reference for analytics; the
snapshot columns are what the API returns. See D12 for the address equivalent of this rule.

---

# 29. Order Total

Line totals are exact integer products:

```text
line_total_amount = quantity * unit_price_amount
```

and the order total is:

```text
total_amount = SUM(line_total_amount)
```

The application computes this during checkout and stores it on `orders` (§25). Because both operands
are integers, no rounding step exists and no rounding policy is needed — the property that
`Decimal` would have required (A5, `architecture.md` §20.1).

The stored total MUST equal the sum of the line totals. Divergence is a bug, not a tolerated state,
and is checked by the generator's validation pass (§48, §56) and by integration tests.

---

# 30. Payments Table

Purpose: stores simulated payment attempts.

| Column          | Type          | Nullable | Constraints |
| --------------- | ------------- | -------: | ----------- |
| id              | TEXT          |       No | Primary key (UUID) |
| order_id        | TEXT          |       No | FK → `orders.id`, `UNIQUE` (1:1 — divergence D13) |
| status          | VARCHAR(32)   |       No | See §31 |
| amount          | INTEGER       |       No | `CHECK (>= 0)` — minor units |
| currency        | VARCHAR(3)    |       No | ISO-4217 |
| method          | VARCHAR(32)   |       No | Synthetic only (§32) |
| transaction_id  | VARCHAR(128)  |      Yes | UNIQUE, synthetic (§33) |
| failure_reason  | VARCHAR(255)  |      Yes | |
| created_at      | TEXT          |       No | ISO-8601 |
| updated_at      | TEXT          |       No | ISO-8601 |

This draft proposed 1:N payments per order. **Resolved as 1:1** via `UNIQUE (order_id)`: it keeps the
order↔payment relation unambiguous, and capture/refund always have exactly one target. Multiple
attempts against one order (retry after decline) would need a retry-attempt table and a
`payments` relaxation — a real requirement if `specs/007-payments` adopts retry semantics, but not
one the current documents describe.

---

# 31. Payment Status — divergence D2

```text
pending
authorized
captured
declined
failed
refunded
```

Stored lowercase, as published in `API.md` §24. Valid transitions belong to the domain:

```text
PENDING
   ├──► AUTHORIZED ──► CAPTURED ──► REFUNDED
   ├──► DECLINED
   └──► FAILED
```

**This draft omitted `CAPTURED` and showed `AUTHORIZED → REFUNDED`. That was correct before decision
A4 was made and wrong after it.** `API.md` §22.7–22.8 expose separate capture and refund endpoints, which
requires a distinct captured state — otherwise a capture followed by a refund is
indistinguishable from an authorization that was never captured, and the ledger cannot report
"authorized but not yet captured" exposure.

`CAPTURED → REFUNDED` is the only refund path. A refund from `AUTHORIZED` is a void/cancel and
requires a different flow.

---

# 32. Payment Methods — divergence D5

This draft proposed:

```text
CARD
BANK_TRANSFER
DIGITAL_WALLET
```

`API.md` §23 publishes `"method": "fake_card"`.

**Resolved: `fake_card` is the initial method.** The naming principle is preserved from the draft —
every value is explicitly fake — and `fake_bank_transfer` / `fake_wallet` are the natural extensions
if `specs/007-payments` adds them.

No real payment credentials, card numbers, or tokens are ever stored, in any column (§52, §60).

---

# 33. Transaction IDs

Fake payment transactions carry generated transaction identifiers:

```text
faketxn_0001
```

matching `generator.md` §5 (F9). They MUST NOT represent real payment-provider transactions, and they are
UNIQUE within the database. The `faketxn_` prefix is deliberate: it makes accidental confusion with
a real provider reference impossible, both in the database and in a log line.

---

# 34. Foreign Key Strategy

SQLite does not enforce foreign keys unless they are explicitly enabled:

```sql
PRAGMA foreign_keys = ON;
```

SQLAlchemy MUST set this on **every** connection, not once at startup — a `NullPool` or a
`StaticPool` that opens a connection without the pragma silently disables all referential integrity
and all `ON DELETE CASCADE` behavior. This is the single easiest way to end up with a corrupt
development database, so it is asserted in the test suite rather than assumed.

---

# 35. Foreign Key Delete Behavior

Delete behavior is conservative. Historical data is not destroyed accidentally.

| Relationship | Behavior | Reason |
|---|---|---|
| `cart_items.cart_id → carts.id` | `ON DELETE CASCADE` | Cart items have no meaning without their cart |
| `order_items.order_id → orders.id` | `ON DELETE CASCADE` | Order items have no meaning without their order |
| `order_items.product_id → products.id` | `ON DELETE RESTRICT` | A product in an order cannot be hard-deleted (§28) |
| `products.category_id → categories.id` | `ON DELETE RESTRICT` | Deleting a category with products is rejected |
| `customers.id → orders.customer_id` | `ON DELETE RESTRICT` | Customers with order history are never physically deleted |
| `payments.order_id → orders.id` | `ON DELETE RESTRICT` | Payment records are financial history |
| `categories.parent_id → categories.id` | `ON DELETE RESTRICT` | Prevents cyclic category trees |

Restricting a hard delete does not mean the row is unusable:

| Entity | Soft-delete column | Notes |
|---|---|---|
| `products` | `is_active = 0` | A deactivated product is no longer purchasable; existing orders keep it |
| `customers` | `status = 'inactive'` | A deactivated customer cannot check out; order history is retained |
| `categories` | *(none)* | `categories` has no soft-delete column. A category with dependents can only be emptied of products, or have its `parent_id` re-pointed. Its delete is rejected, not softened |

See §39 for the deactivation endpoints.

---

# 36. Referential Integrity

The following relationships MUST always remain valid:

```text
Product        → Category
Inventory      → Product
Cart           → Customer
CartItem       → Cart
CartItem       → Product
Order          → Customer
OrderItem      → Order
OrderItem      → Product
Payment        → Order
Category       → Category (parent)
```

Every generated foreign key MUST reference an existing row. The generator MUST fail loudly rather
than silently produce an orphan (the constitution's *Explicit Business Rules*, III).

---

# 37. Index Strategy

Indexes support the queries published in `API.md` §6.1. Index selection is driven by those queries,
not by guesswork — an index SQLite cannot use still costs a write on every insert.

| Index                        | Table        | Columns        | Rationale |
| ---------------------------- | ------------ | -------------- | --------- |
| `ix_categories_slug`         | categories   | `slug`         | Unique lookup; serves the unique constraint |
| `ix_products_sku`            | products     | `sku`          | Unique lookup |
| `ix_products_category_id`    | products     | `category_id`  | Category listing/filter |
| `ix_products_is_active`      | products     | `is_active`    | Default catalog query filter |
| `ix_products_name`           | products     | `name`         | Search / sort |
| `ix_customers_email`         | customers    | `email`        | Unique lookup |
| `ix_carts_customer_id`       | carts        | `customer_id`  | "My cart" lookup |
| `ix_cart_items_cart_id`      | cart_items   | `cart_id`      | Item listing; serves the unique constraint |
| `ix_orders_customer_id`      | orders       | `customer_id`  | Customer order history |
| `ix_orders_status`           | orders       | `status`       | Operational filtering by state |
| `ix_orders_created_at`       | orders       | `created_at`   | Chronological paging |
| `ix_order_items_order_id`    | order_items  | `order_id`     | Order detail; serves cascade |
| `ix_payments_order_id`       | payments     | `order_id`     | Order→payment lookup; serves unique |
| `ix_payments_status`         | payments     | `status`       | Reconciliation queries |

Every foreign-key column is indexed explicitly — SQLite does **not** index foreign keys
automatically (§67), so an unindexed FK turns a parent delete into a full table scan.

The draft's index list additionally named `customers.status`, `carts.status`, `cart_items.product_id`,
`order_items.product_id`, and `payments.transaction_id`. `payments.transaction_id` is already covered
by its `UNIQUE` constraint. The other four are deferred until a query in `API.md` §6.1 or
`specs/00X` actually filters on them.

---

# 38. Composite Indexes

Composite indexes are justified by an actual query, not added speculatively.

| Candidate                            | Supports |
| ------------------------------------ | -------- |
| `products (category_id, is_active)`  | Category listing restricted to active products |
| `orders (customer_id, created_at)`   | Paged order history for one customer |
| `carts (customer_id, status)`        | "Find my active cart" |
| `payments (order_id, created_at)`    | Payment history for an order |

Note that `ix_orders_customer_id` and `ix_orders_created_at` (§37) are both subsumed by
`orders (customer_id, created_at)` for the paged-history query. The composite index replaces them
when it is added, rather than living alongside them. Adding an index requires a stated query, and
removing a superseded index is part of the same migration.

---

# 39. Soft Deletion

Lifecycle status is preferred over physical deletion for business entities that have dependents:

```text
Product   → is_active = 0
Customer  → status = 'inactive'
Cart      → status = 'abandoned'
```

Hard deletion is reserved for rows with no dependents (§35) — principally `cart_items` of a deleted
cart, and generated rows removed by the generator's `--reset` (§47).

Dedicated soft-delete columns (`deleted_at`) are deferred (§68): status already expresses the
states this project needs, and a second deletion mechanism would double the query logic.

---

# 40. Database Constraints vs Domain Rules

Not every business rule belongs in the database.

### Database constraints

```text
PRIMARY KEY
FOREIGN KEY
UNIQUE
NOT NULL
CHECK (numeric ranges)
CHECK (status IN (...))
```

### Domain rules

```text
Can this order transition to shipped?
Can this payment be refunded?
Can this cart be checked out?
Can this inventory be reserved?
Is the order total still correct?
```

A `CHECK` constraint can reject a write. It cannot explain the failure, compensate for it, or be
unit-tested without a database. Constraints are a backstop that catches bugs in application code
that should already have been impossible.

| Rule | Enforced by | Backstop |
|---|---|---|
| Inventory never negative | Domain `Inventory.reserve`/`release` | `CHECK (quantity_* >= 0)` |
| Reservation fails when short | Domain | `409 INSUFFICIENT_INVENTORY` |
| Cart quantity > 0 | Domain | `CHECK (quantity > 0)` |
| Order has ≥ 1 item | Application (checkout) | Integration test |
| Order total = Σ line totals | Application + generator validation | Integration test |
| Valid state transitions | Domain transition table | `409 INVALID_*_TRANSITION` |
| Price never negative | Domain `Money` value object | `CHECK (price_amount >= 0)` |
| One payment per order | Application | `UNIQUE (order_id)` |
| One active cart per customer | Domain | Deferred (partial unique index) |
| Every order has a shipping address | Application (checkout) | Integration test (D12) |

---

# 41. Database Initialization

The schema is initialized with Alembic:

```bash
uv run alembic upgrade head
```

`Base.metadata.create_all()` MUST NOT be the primary initialization strategy, and the application
MUST NOT create, alter, or drop schema at startup. `create_all()` is acceptable **only** inside a
throwaway test database whose schema is defined by the models under test (§53) — never as a
shortcut around a missing migration, because it silently diverges from migration history.

---

# 42. Alembic Structure

```text
migrations/
├── env.py
├── script.py.mako
└── versions/
    ├── 0001_initial_schema.py
    ├── 0002_add_indexes.py
    └── ...

alembic.ini
```

Migration names describe the change:

```text
0001_initial_schema
0002_add_order_failure_reason
0003_add_order_indexes
```

---

# 43. Migration Rules

Every schema change MUST arrive as a reviewed Alembic revision. The chain is:

```text
Change SQLAlchemy model
        ↓
Generate migration
        ↓
Review migration
        ↓
Apply migration
        ↓
Run tests
```

The database MUST NOT be modified manually as part of the normal development workflow. A schema
that only exists because someone edited the `.db` file is not a schema anyone can reproduce.

* One logical change per revision; the message states the change.
* Schema migrations and data migrations are separate, clearly labelled revisions.
* `downgrade()` is implemented wherever practical.
* Destructive changes (drop or rename a column) additionally require a documented strategy, updated
  tests, updated docs, and a decision recorded in `CHANGELOG.md`.

---

# 44. Migration Development Workflow

```bash
uv run alembic revision --autogenerate -m "create products"
```

Review the generated file, then:

```bash
uv run alembic upgrade head
uv run alembic current
uv run alembic history
```

Generated migrations MUST be reviewed before committing.

---

# 45. Migration Safety

Autogenerate compares models to the database; it does not understand intent. Inspect every
generated revision for:

* Column additions, drops, and renames
* Constraint changes — especially dropped `CHECK` or `UNIQUE`
* Index additions and removals
* Foreign keys and their delete behavior
* Data migrations
* Destructive operations (`drop_column`, `drop_table`) that autogenerate emits silently

SQLite cannot `ALTER COLUMN`, so a column type change is a table rebuild; autogenerate's output for
these cases is not trustworthy and must be hand-written. Migration behavior MUST be tested against a
real database, not only against a fresh in-memory one, because a fresh database never exercises the
rebuild path.

---

# 46. Seed Generator Interaction

The generator operates against a **migrated** database:

```text
Create database file
      ↓
alembic upgrade head
      ↓
Generate data
```

Not:

```text
Generator → create arbitrary tables
```

The generator MUST go through the application's repositories and session, never through raw SQL that
could bypass constraints or produce data the models would reject (§60).

---

# 47. Database Reset

Development supports resetting:

```bash
uv run python -m ecommerce.seed --reset --profile development
```

or, to rebuild the file from nothing:

```bash
make db-reset
```

Reset MUST be explicit. It MUST NEVER happen during normal application startup — a server that
wipes its own data on boot is indistinguishable from a server with a bug, and the failure mode is
silent data loss.

---

# 48. Generator Transaction Strategy

Data generation runs inside a single transaction:

```text
BEGIN
  categories
  products
  inventory
  customers
  carts
  cart_items
  orders
  order_items
  payments
COMMIT
```

On failure:

```text
ROLLBACK
```

The database MUST NOT be left partially populated. Post-generation validation (§56) runs
**inside** the transaction, before commit, so an invariant violation rolls back the entire dataset
rather than committing a broken one. This matches `generator.md` §40.

A single transaction is acceptable at the documented scale (`generator.md` §11, largest profile
40 categories / 5,000 products / 2,000 customers / 20,000 orders) precisely because SQLite has a
single writer — one long transaction blocks all other writers, which is acceptable for a
maintenance command and unacceptable for a request.

---

# 49. Checkout Transaction Strategy

Checkout is the critical transactional workflow. It spans four tables: `orders`, `order_items`,
`inventory`, and `payments`.

Success:

```text
BEGIN
   ├── Validate cart (non-empty, all items still exist)
   ├── Resolve pricing (see §21 — UNRESOLVED)
   ├── Reserve inventory
   ├── Create order
   ├── Create order items (with snapshots)
   ├── Create payment
   ├── Authorize payment
   ├── Confirm order
   └── Mark cart checked_out
COMMIT
```

Failure:

```text
BEGIN
   ├── Reserve inventory
   ├── Create order
   ├── Payment declines or fails
   └── ROLLBACK  (or COMPENSATE, if the failure is post-commit)
```

The precise boundary and compensation strategy is owned by the application layer and specified in
`specs/005-orders` / `specs/009-failure-simulation`; `architecture.md` §6 defines the sequence and
§49's concern here is only that it is atomic and does not commit a half-built order.

A post-commit failure (for example, a failure injected after payment authorization) is handled by
compensation, not rollback — see `docs/failure-simulation.md`.

---

# 50. Inventory Concurrency

SQLite permits one writer at a time, so concurrent reservations are the real contention point.

* Use short transactions.
* Avoid unnecessary reads before writes.
* Perform the reservation atomically.
* Re-check availability **inside** the transaction.

Preferred, because it needs no read at all:

```sql
UPDATE inventory
   SET quantity_reserved = quantity_reserved + :n,
       updated_at        = :now
 WHERE product_id = :pid
   AND quantity_on_hand - quantity_reserved >= :n
```

and the application checks `rowcount`. Zero rows updated means insufficient stock — the check and the
write cannot be interleaved, which a read-then-write pair allows.

A read-then-write inside `BEGIN IMMEDIATE` is the correct fallback, and the transaction MUST be
`IMMEDIATE` rather than deferred, or the first write upgrades the lock and may fail with
`SQLITE_BUSY`.

Negative available inventory MUST be impossible, and the `CHECK` constraints (§23) are the last line
of defense if application logic is wrong.

---

# 51. Data Generation Relationships

Generated data follows this dependency order:

```text
categories → products → inventory

customers  → carts    → cart_items

customers  → orders   → order_items → payments
```

Every generated foreign key MUST reference an existing entity. The generator tracks the IDs it has
created and never invents one, so a generation run cannot produce an orphan even if a profile is
misconfigured.

---

# 52. Synthetic Data Requirements

The database MAY contain:

* Fake names and email addresses (`@example.invalid` — a reserved TLD that cannot resolve)
* Fake phone numbers
* Fake addresses
* Fake products and SKUs
* Fake transaction IDs (`faketxn_` prefix, §33)

It MUST NOT contain:

* Real customer information
* Real credit-card numbers
* Real bank credentials
* Real authentication secrets or tokens
* Real payment tokens
* Any data scraped from a real system

The project is designed to be safe to commit, screenshot, and share.

---

# 53. Test Database

Automated tests use an isolated database:

```text
data/ecommerce.db        # development — disposable, git-ignored
<temp>/test.db           # tests — per run, never committed
```

The test database MUST be temporary, isolated, deterministic, and recreated when required. Tests
MUST NOT modify `data/ecommerce.db`.

Recommended: one in-memory database per test session, shared via `StaticPool`, or a temp file per
test. Whichever is chosen, `PRAGMA foreign_keys = ON` (§34) and the connection-sharing mode must be
consistent — an in-memory database that is dropped when a connection closes produces a schema that
appears to vanish between tests.

---

# 54. Database Fixtures

Integration tests provide fixtures for:

```text
engine          session          connection
repositories    unit_of_work
sample_category sample_product  sample_inventory
sample_customer sample_cart
```

Fixtures create the minimum state a test needs, so each test is readable in isolation. The
`unit_of_work` fixture yields a committed or rolled-back session and is what makes test isolation
cheap.

---

# 55. Example Test Dataset

A minimal integration dataset:

```text
Categories: 2
Products:   5
Customers:  3
Inventory:  5
Carts:      2
Orders:     2
Payments:   2
```

expressible with the published generator flags:

```bash
uv run python -m ecommerce.seed \
    --categories 2 \
    --products 5 \
    --customers 3 \
    --orders 2 \
    --seed 42
```

Named profiles from `generator.md` §8–§11:

| Profile      | Categories | Products | Customers | Orders | Use                        |
| ------------ | ---------: | -------: | --------: | -----: | -------------------------- |
| `test`       |          5 |       20 |        10 |     25 | Fast unit/API fixtures     |
| `development`|         12 |      200 |        50 |    300 | Normal local dev (default) |
| `large`      |         40 |    5,000 |     2,000 | 20,000 | Performance / load testing |

---

# 56. Data Consistency Checks

The generator validates generated data before commit (§48) and fails the run rather than committing
a broken dataset:

```text
Every product has a category.
Every product has an inventory row.
Every cart belongs to a customer.
Every cart item references a valid product.
Every order belongs to a customer.
Every order contains at least one item.
orders.total_amount == SUM(order_items.line_total_amount)
line_total_amount == quantity * unit_price_amount
Every payment references an order.
quantity_on_hand >= 0
quantity_reserved <= quantity_on_hand
No order references a product that does not exist.
```

These are the same checks the integration tests assert, expressed once so the generator and the test
suite cannot drift apart.

---

# 57. Database Health Checks

`GET /health` reports application liveness only:

```json
{ "status": "ok" }
```

A readiness check that verifies database connectivity MAY follow:

```text
GET /ready
    └── database connection → success | failure
```

Not required for the initial implementation. It MUST NOT be conflated with liveness — a database
that is briefly unreachable should not cause a restart loop.

---

# 58. Backup

The application is intended for local development, so the database file is the backup:

```text
data/ecommerce.db
```

Copy it only when no writer is active, or use `sqlite3 .backup`, because SQLite locks the whole file
for the duration of a copy (§67). The dev database is disposable and regenerable, so a backup is a
convenience rather than a requirement. Production backup strategy is out of scope.

---

# 59. Database Performance

Expected dataset sizes are moderate (`generator.md` §8–§11). Relevant considerations:

* Index foreign keys and frequently filtered columns (§37)
* Paginate list endpoints — never return an unbounded result set
* Avoid N+1 queries; eager-load or project the columns actually needed
* Choose SQLAlchemy loading strategies deliberately
* Keep transactions short — a long write transaction blocks every other writer (§50)
* Avoid unnecessary commits; commit at transaction boundaries, not per statement

The application should not optimize prematurely. Premature index and query tuning on a
5,000-product SQLite database costs clarity and buys nothing measurable.

---

# 60. Database Security

Even though the database holds only synthetic data:

* Use SQLAlchemy's parameterized queries exclusively. Never build SQL with string concatenation of
  untrusted input.
* Never store real payment information, credentials, or secrets (§52).
* Do not log complete customer records.
* Do not expose raw SQL errors through the API — a `sqlite3.IntegrityError` message reveals table
  and column names, which is information disclosure even when the data is fake. Map integrity
  violations to domain errors and let the error envelope (`API.md` §12) describe them.
* Run the dev database with a filesystem mode that is not world-readable, since it contains
  synthetic-but-realistic PII-shaped data.

---

# 61. Example SQL Schema

The schema is generated through SQLAlchemy models and Alembic migrations. This section is
illustrative, not an executed script:

```sql
CREATE TABLE categories (
    id          TEXT         NOT NULL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL UNIQUE,
    slug        VARCHAR(120) NOT NULL UNIQUE,
    parent_id   TEXT         REFERENCES categories(id) ON DELETE RESTRICT,
    description TEXT,
    created_at  TEXT         NOT NULL,
    updated_at  TEXT         NOT NULL
);

CREATE TABLE products (
    id           TEXT         NOT NULL PRIMARY KEY,
    category_id  TEXT         NOT NULL REFERENCES categories(id) ON DELETE RESTRICT,
    sku          VARCHAR(64)  NOT NULL UNIQUE,
    name         VARCHAR(200) NOT NULL,
    description  TEXT,
    price_amount INTEGER      NOT NULL CHECK (price_amount >= 0),
    currency     VARCHAR(3)   NOT NULL CHECK (length(currency) = 3),
    is_active    INTEGER      NOT NULL DEFAULT 1,
    created_at   TEXT         NOT NULL,
    updated_at   TEXT         NOT NULL,
    CHECK (is_active IN (0, 1))
);
```

Note the money column type: `INTEGER` minor units, not `NUMERIC(12, 2)` (§13).

The complete schema is implemented incrementally through reviewed migrations rather than
maintained as a hand-executed SQL script.

---

# 62. Database Naming Conventions

Tables and columns are `snake_case`:

```text
cart_items
order_items
customer_id
created_at
quantity_reserved
```

Primary key:

```text
id
```

Foreign keys:

```text
<entity>_id
```

```text
customer_id
product_id
order_id
cart_id
category_id
parent_id
```

Money columns are suffixed `_amount` and paired with a sibling `currency` column, so a bare money
column is always identifiable by its name and never ambiguous about its unit:

```text
price_amount + currency
total_amount + currency
line_total_amount + currency
```

This draft's `price`, `unit_price`, `total_amount`, `line_total`, and `amount` columns are renamed
accordingly — `amount` on `payments` is the sole exception, kept because it is unambiguous on a
table whose every other numeric column is a timestamp or a status.

---

# 63. SQLAlchemy Naming Conventions

A single metadata naming convention keeps Alembic diffs readable:

```text
pk_<table>
fk_<table>_<column>_<referenced_table>
uq_<table>_<column>
ix_<table>_<columns>
ck_<table>_<name>
```

```text
pk_products
fk_products_category_id_categories
uq_products_sku
ix_products_category_id
ck_products_price_amount_nonneg
```

This makes generated migrations reviewable at a glance, which is what makes the §45 review step
practical rather than ceremonial.

---

# 64. Database Layer Structure

Persistence lives under the infrastructure layer, matching `architecture.md` §10:

```text
src/ecommerce/infrastructure/
├── database/
│   ├── __init__.py
│   ├── base.py           # DeclarativeBase + naming convention (§63)
│   ├── engine.py         # engine, PRAGMA setup (§34)
│   ├── session.py        # session factory, Unit of Work
│   └── models/
│       ├── __init__.py
│       ├── category.py
│       ├── product.py
│       ├── customer.py
│       ├── cart.py
│       ├── cart_item.py
│       ├── inventory.py
│       ├── order.py
│       ├── order_item.py
│       └── payment.py
└── repositories/
    ├── categories.py
    ├── products.py
    ├── customers.py
    ├── carts.py
    ├── inventory.py
    ├── orders.py
    └── payments.py
```

Domain entities and ORM models are **separate types** (`architecture.md` §16, §19). The models here
are a persistence representation; mapping between them is explicit rather than inherited, so the
domain never imports SQLAlchemy and a schema change cannot leak into business logic.

---

# 65. Database Architecture Summary

```text
                    Application
                         │
                         ▼
                Repository Interface        (domain-defined port)
                         │
                         ▼
                Repository Adapter         (infrastructure)
                         │
                         ▼
                    SQLAlchemy
                         │
                         ▼
                      SQLite
                         │
                         ▼
                 data/ecommerce.db
```

Schema lifecycle:

```text
SQLAlchemy Models
        │
        ▼
Alembic Migration        (reviewed, §45)
        │
        ▼
SQLite Schema
```

Data lifecycle:

```text
Fake Generator
      │
      ▼
Repositories + Unit of Work
      │
      ▼
Single transaction (§48)
      │
      ▼
Validation (§56) then commit
```

---

# 66. Final Schema

```text
categories
    │
    └── products
           │
           ├── 1:1 inventory
           │
           └── cart_items
                    │
                    └── carts
                           │
                           └── customers
                                  │
                                  └── orders
                                         │
                                         ├── order_items
                                         │
                                         └── payments (1:1)
```

The key persistence principles:

```text
1. SQLite is the initial database.
2. Alembic owns schema evolution; the app never mutates schema at startup.
3. SQLAlchemy handles persistence; domain and ORM models are separate types.
4. Foreign keys are enforced on every connection, and every FK column is indexed.
5. Money is an integer count of minor units. float, Decimal, and NUMERIC columns are prohibited.
6. Domain logic enforces business invariants; database constraints are a backstop.
7. Historical order data is preserved through snapshots (§28, D12).
8. Transactions protect multi-step workflows; the generator validates before it commits.
9. Generated data is synthetic and deterministic; tests use isolated databases.
10. The schema stays portable enough for a future PostgreSQL implementation.
```

---

# 67. SQLite-Specific Considerations

These behaviors affect application-visible semantics and MUST be respected:

| Behavior | Consequence for the application |
|---|---|
| **Single writer** | One write transaction at a time; concurrent writers get `SQLITE_BUSY`. Long transactions block everything (§48, §50). |
| **No row-level locking** | Read-modify-write on `inventory` can interleave. Use a conditional `UPDATE ... WHERE` and check `rowcount` (§50), or `BEGIN IMMEDIATE`. |
| **Foreign keys OFF by default** | `PRAGMA foreign_keys = ON` MUST be set on **every** connection, or `ON DELETE CASCADE` and all FK enforcement silently do nothing (§34). |
| **Weak `DATETIME` type** | Store timestamps as ISO-8601 `TEXT`, never as Python `datetime` objects and never as integer epoch seconds (§9). The lexicographic-sort argument in §9 is the reason epoch seconds are excluded, not merely disfavored. |
| **Boolean is INTEGER** | `0`/`1`, not `true`/`false` literals (§12). |
| **No native `BOOLEAN` or `ENUM`** | Use `INTEGER`/`TEXT` plus `CHECK`, not a dialect-specific type. |
| **`NUMERIC` is not decimal** | `NUMERIC` affinity degrades toward `REAL`; money stays `INTEGER` (§13). |
| **No `ALTER COLUMN`** | Column type changes require a table rebuild; autogenerate's output is untrustworthy for these (§45). |
| **No `RETURNING` before 3.35** | Feature-detect the SQLite version before relying on it. |
| **WAL recommended** | `PRAGMA journal_mode = WAL` greatly improves read-during-write for a dev/test server. |
| **FKs are not auto-indexed** | Index every FK column explicitly (§37). |
| **Whole-file locking on copy** | Copy the file only when no writer is active, or use `sqlite3 .backup` (§58). |

---

# 68. Reserved / Not Yet Implemented

Deferred until a requirement justifies them, per the *Simplicity* principle. Each item names the
trigger that would bring it back.

| Item | Trigger | Notes |
|---|---|---|
| `idempotency_keys` table | `architecture.md` §46 idempotency is implemented | Divergence D16. `POST /checkout` is the only route that needs it, and it is already the route A6 defers. |
| Audit log table | Compliance or debugging requires change history | |
| Outbox / event table | A real message broker is introduced | A transactional outbox is only correct alongside a broker. |
| Soft-delete columns | A status field no longer expresses the required state | §39 explains why status is preferred today. |
| Multi-currency conversion tables | Orders may span currencies | §14 keeps orders single-currency. |
| Full-text search (FTS5) | `LIKE` search is measurably too slow | At 5,000 products (§55) it is not. |
| Payment attempt history | `specs/007-payments` adopts retry-after-decline | Requires relaxing `UNIQUE (order_id)` (§30). |
| `address_line2`, `state` columns | `specs/003-customers` requires richer addresses | §15. |
| `DISCONTINUED` product state | `specs/002-product-catalog` requires it | §12. |
| Partial unique index on active cart | `specs/004-cart` states the rule precisely | §18. |
| Audit columns (`created_by`) | Multiple actors write the same rows | |
