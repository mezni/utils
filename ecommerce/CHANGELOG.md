# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Pre-release.** The project has no released version. Everything below is unreleased work in
> progress.

---

## [Unreleased]

### Added

- **Liveness endpoint** — `GET /health` returns `{"status":"ok"}` (FR-002, FR-003)
- **Description switches** — `API_DOCS_ENABLED` and `API_REDOC_ENABLED` independently gate `/docs`, `/redoc`, `/openapi.json`; not a security boundary (FR-004, research D-05)
- **Error envelope** — consistent four-field envelope (`code`, `message`, `details`, `request_id`) on all failures (FR-008)
- **Dependency declaration correction** — runtime deps moved to `[project] dependencies`, dev deps to `[dependency-groups]` uv.lock re-locked (39 packages, P0 blocker)

#### Governance

- **Constitution v1.1.1** — two PATCH clarifications, no principle added or removed. The core
  status-code list is now explicitly illustrative, with `docs/API.md` §13 named as authoritative
  (it additionally uses 402, 415, 429, 502, 503, 504). The error-envelope example now includes the
  mandatory `details` array and `request_id`, matching `API.md` §12 and divergence E17. The
  amendment log in the constitution records the reason and affected areas for both.

#### Documentation

- **Cross-document consistency pass.** Resolved the contradictions that would have produced
  disagreeing implementations:
  - Order lifecycle: removed the `CONFIRMED → FAILED` transition from `architecture.md`, which
    contradicted `database.md` §26, and made §26 the named authority.
  - Payment retry: `testing.md` and this changelog said `failed`/`declined`; `API.md` §22.6 (was
    `API.md` §22.6) says `pending`/`failed`. Corrected the two, and the citation `E12` → `E11`.
  - `orders.shipping_address_json` is `NOT NULL` (D12) but was absent from `generator.md`, so the
    generator could not satisfy its own schema. Added, with the snapshot rule.
  - `SequentialIdGenerator` → `SeededUuidIdGenerator`: a counter cannot satisfy UUID-`TEXT` keys
    (D3), so the deterministic substitute was specified to emit UUID4 from the seeded RNG.
  - `VALIDATION_ERROR` mapped to 400 in `failure-simulation.md`; corrected to 422 per `API.md` §12.1.
  - `testing.md`'s error example used the unpublished code `PRODUCT_NOT_FOUND` and an object for
    `details`; corrected to `NOT_FOUND` and an array (E17).
  - Pre-D17 vocabulary purged from `architecture.md`, `testing.md`, and `generator.md`, including a
    tautological invariant and a claim that the column arithmetic was "unsettled".
  - Category deletion: `categories` has no `is_active`, so the delete is rejected outright rather
    than softened. `database.md` §35 now states soft-delete per entity; `API.md` §15.5 matches.
  - Admin routes: `API.md` §44 claimed they are unauthenticated until `specs/010`, which contradicts
    `AUTH_ENABLED=true` (G5). Corrected, and the README now carries the "auth boundary is fake"
    callout the section required.
  - Added the missing `D17` row to the `database.md` divergence table; five documents cited it
    while no row existed.
  - 22 references to `constitution §N` were dangling — the constitution has no numbered sections.
    All now name the clause (`*Explicit Business Rules*, III` and similar).
  - Corrected mis-attributed divergence summaries in this changelog: `D14`, and the `E`, `F`, and
    `S` series had content mapped to the wrong IDs.
  - Corrected wrong section targets: `API.md` §7 (HTTP Methods) → `API.md` §6.1 for resource schemas, and
    the `configuration.md`/`generator.md`/`testing.md` targets that pointed at unrelated sections.
  - `W21` claimed §23 and §58 were merged; both existed. §58 is now a pointer to §23.
  - Documented the undocumented `GET /api/v1/payments` collection in `API.md` §22.2 and §48.
  - SKU examples reconciled to the published 4-digit `{NNNN}` format.
  - The generator's output format was specified twice and inconsistently; §48 is now canonical and
    §80 is a verified superset that adds the §49 summary.
  - README's generator option table was missing `--carts`, `--validate/--no-validate`, `--quiet`,
    and `--output`; all eleven published options are now listed.
  - All cross-document `§N` references verified to resolve (0 unresolved, from 20+).

### Added

#### Governance

- **Constitution v1.1.0** — replaced all template placeholders with a concrete governance baseline
  for the Fake E-Commerce Server:
  - Five core principles — Domain Independence, Determinism and Reproducibility, Explicit Business
    Rules, Testability and Isolation, Simple Local Development.
  - Technology stack and API design constraints (Python 3.12+, FastAPI, Pydantic v2, SQLAlchemy
    2.x, Alembic, SQLite, pytest, HTTPX, uv).
  - Database and data management rules (Alembic-only schema management; no
    `Base.metadata.create_all()` as the primary mechanism).
  - Development workflow and feature-completion criteria.
  - **Speckit Development Process** — mandates that all Spec Kit output lives under `specs/`, with
    a sequential `NNN-slug/` layout and a roadmap of independently implementable features.
  - Project documentation layout requirement.
  - Security, observability, error-envelope, and configuration requirements.
  - Governance: amendment procedure, semantic versioning policy, compliance expectations.
- **Constitution v1.0.0** — initial substantive ratification of the governance baseline.

#### Specifications

- Feature roadmap scaffolded under `specs/`:
  `001-foundation`, `002-product-catalog`, `003-customers`, `004-cart`, `005-orders`,
  `006-inventory`, `007-payments`, `008-fake-data-generator`, `009-failure-simulation`,
  `010-authentication`, `011-observability`.
- `specs/README.md` — feature roadmap, per-directory artifact layout, and the working workflow.

#### Documentation

- `README.md` — project entry point: purpose, features, technology stack, installation, running
  migrations, generating fake data, running tests, and example API requests.
- `CHANGELOG.md` — this file.
- `docs/PRD.md` — product requirements: users and roles, functional requirements per bounded
  context, checkout workflow, fake-data generation, failure simulation, non-functional
  requirements, and explicit out-of-scope items.
- `docs/plan.md` — implementation plan: technology stack, architectural approach, project
  structure, 16 implementation phases with objectives/tasks/completion criteria, migration and
  repository strategy, quality gates, and definition of done.
- `docs/architecture.md` — comprehensive architecture: goals, architectural style, high-level
  system, layer boundaries, dependency rule, all six bounded contexts with their entities and
  invariants, context relationships, project and module structure per layer, Pydantic schema
  boundary, entities and value objects, repository pattern, Unit of Work, checkout architecture and
  transaction boundaries, payment provider abstraction, failure simulation architecture, database
  and SQLAlchemy boundaries, mapping layer, session management, error taxonomy and API error
  translation, dependency injection and composition root, typed configuration, generator
  architecture, authentication, observability, health checks, API versioning, pagination,
  concurrency, idempotency, security boundary, testing architecture, transaction strategy, and
  explicit non-goals for domain events, caching, messaging, and microservices. Ends with
  architectural trade-offs, evolution strategy, PostgreSQL migration path, AI-agent integration,
  and a decision summary. Includes ten mandatory architecture constraints.
- `docs/API.md` — HTTP contract: conventions, money representation, error format and stable error
  codes, status codes, pagination/filtering/sorting, authentication and authorization, resource
  schemas, and the full endpoint table per bounded context.
- `docs/database.md` — SQLite persistence model: table-by-table columns and constraints, entity
  relationships, constraint strategy, index rationale, transaction guidance, migration strategy,
  SQLite-specific behavioral caveats, and test database policy.
- `docs/generator.md` — fake-data generator: scope, CLI options, dataset profiles, determinism
  guarantees, generation order, validation checks, realistic data distributions, reset semantics,
  transaction handling, test-data reuse, architecture, and failure behavior.
- `docs/configuration.md` — environment variables, per-environment behavior for
  development/test/production, `.env.example` contract, precedence rules, failure-simulation
  defaults, and startup validation.
- `docs/testing.md` — testing strategy: `unit`/`integration`/`api` layout, test database policy,
  fixtures, generator reuse, determinism controls, per-tier coverage, failure scenario matrix, and
  isolation rules.
- `docs/failure-simulation.md` — failure simulation: principles, scenario table, triggering
  mechanisms, determinism guarantee, per-scenario behavior and required end state, implementation
  boundaries, safety rules, and test matrix.
- `docs/development.md` — developer workflow: prerequisites, setup, database, fake data, running
  the server, calling the API, testing, Makefile targets, feature workflow, and troubleshooting.

### Resolved Conflicts

Tracked per constitution's *Explicit Business Rules* (III) — identified explicitly, never resolved silently. See `docs/plan.md`
for the full write-up.

- **C1 — Spec feature decomposition.** `docs/plan.md` §30 originally proposed a 16-feature
  breakdown with the fake-data generator at `010`, conflicting with the `specs/` tree that places
  it at `008`. **Resolved: the 11-feature breakdown is authoritative**, and
  `008-fake-data-generator` remains the project's highest-priority feature. The 16-item list is
  retained in `docs/plan.md` §30 as a rejected alternative with its rationale, so the decision
  remains auditable and reversible if circumstances change.
- **C2 — Constitution path.** The plan originally listed `constitution.md` at the repository root.
  **Resolved:** the authoritative path is `.specify/memory/constitution.md`, matching the Spec Kit
  tooling. Documentation lives under `docs/`.
- **C3 — Generator entrypoint.** The plan's project tree omitted `seed/__main__.py`, which
  `python -m ecommerce.seed` (the constitution's *Simple Local Development*, V) requires to run. **Resolved:** `__main__.py` is
  part of the target structure.
- **A-series — architecture divergences.** Found when reconciling `docs/architecture.md` against
  the constitution and the other nine documents. All are annotated in that file's header and
  inline. Resolved without loss of intent:
  - **A1** — Documentation was listed at the repository root; retained under `docs/`. Same class as
    C2.
  - **A2** — `seed/__main__.py` missing from the project tree. Same class as C3.
  - **A3** — Order lifecycle omitted `FAILED`. **Added**; required by the constitution's *Explicit Business Rules* (III) and by
    the compensation path that failure simulation exists to exercise.
  - **A4** — Payment lifecycle omitted `CAPTURED` and showed `AUTHORIZED → REFUNDED`.
    **Corrected** to `authorized → captured → refunded` to match the capture/refund endpoints in
    `API.md` §22.7–22.8.
  - **A7** — Product Catalog claimed "product availability". **Clarified:** availability is owned by
    the Inventory context; Product Catalog owns only the catalog-level `is_active` flag.
  - **A8** — Determinism ports (`Clock`, `IdGenerator`, `RandomSource`) were absent, breaking
    cross-references in `testing.md` §5, `failure-simulation.md` §7, and `generator.md` §12.
    **Added** as `architecture.md` §68.

### Open Items

**No design decision is blocked.** The four that were open have been decided; two remain deliberately
deferred to a later feature spec.

- **A6 — Checkout route shape**, deferred to `specs/005-orders` by decision. Not blocking
  `001-foundation` through `004-cart`. Interim contract is `POST /api/v1/checkout` with `cart_id` in
  the body.
- **D15 — Checkout pricing authority** (cart snapshot vs. authoritative reprice at checkout),
  deferred to `specs/005-orders`. Cart totals remain display-only until decided.

#### Resolved since the last entry

All four belonged to the `001-foundation` migration and are now settled.

- **D3 / E1 — Identifier type: RESOLVED — opaque UUID strings stored as `TEXT`.** The `IdGenerator`
  port is already a published determinism port, and integer keys would have made generated fixtures
  depend on insertion order being reproducible across tables. `database.md` §7, `API.md` §5.1.
- **D11 — `orders.cart_id`: RESOLVED — `TEXT NULL UNIQUE REFERENCES carts(id)`.** Nullable, because
  the generator creates orders that were never checked out; `UNIQUE`, because that is what makes
  checkout idempotent for a given cart; `RESTRICT` on delete, which the generator validates.
  `database.md` §25.
- **D12 — Shipping-address snapshot: RESOLVED — `shipping_address_json TEXT NOT NULL`,** written once
  at checkout. `NOT NULL` because an order without a destination is not a representable state; JSON
  text because a snapshot must not be reachable by a later profile edit. `database.md` §25.
- **D17 — Inventory quantity semantics: RESOLVED — resolution (a).** `quantity_on_hand` and
  `quantity_reserved`, with `available_to_sell` **derived, never stored**, and the invariant
  `quantity_reserved <= quantity_on_hand`. The tautology
  `quantity_reserved <= quantity_available + quantity_reserved` is gone from all five documents, as
  is the generator failure message that had inherited it.

#### D17 — the inconsistency that was fixed

The same two columns were described three incompatible ways:

| Document | Reading | Consistent? |
|---|---|---|
| `database.md` §23 | `quantity_available` is **on-hand**; derives `available = quantity_available - quantity_reserved` | Yes |
| `generator.md` §19 | Same formula, but `quantity_available` is "stock not yet allocated" | No — double-counts |
| `architecture.md` | `WHERE quantity_available >= :n` | No — matches neither reading |
| `testing.md` | A **fourth** scheme: `quantity` / `reserved_quantity` / `available_quantity` | No |

Resolution (b) — keep the published name and fix the prose to the "unallocated" reading — was
available and cheaper, and it was rejected. It leaves a column named `quantity_available` that is
not what a reader assumes "available" means, and it makes the property untestable: with a single
"available" column there is no second quantity to observe, so "a reservation does not change on-hand
stock" cannot be asserted at all (`testing.md` §35). The rename costs one column and one API field; the
alternative costs a test.

Propagated to `API.md` §18.1, `database.md` §23, `generator.md` §19.1, `architecture.md` §45, and
`testing.md` §8, plus the conditional `UPDATE` statement in `architecture.md` §45 and the
`architecture.md` §60.1 reference to `failure-simulation.md` §46.

### Reconciled Drafts (D / E / F / G / T / S / W series)

Six incoming drafts were reconciled against the constitution and the settled corpus rather than
adopted wholesale. Each draft's divergences are tracked in its own document header; the resolutions
are summarized here.

#### D-series — `docs/database.md`

- **D1** — Money as `NUMERIC`/`DECIMAL` → **resolved: integer minor units** (consistent with A5).
- **D2** — Restoration of `captured` and `refunded` payment states → **resolved**, consistent with A4.
- **D4** — `DEFAULT_CURRENCY` → **resolved: `USD`**.
- **D5** — Payment method → **resolved: `fake_card`**.
- **D6** — Status casing → **resolved: lowercase** snake_case.
- **D7** — Category `slug` + self-referential `parent_id` → **resolved**, adopted.
- **D8** — Product availability flag → **resolved: `is_active`**; stock levels belong to Inventory (A7).
- **D9** — Customer naming (`full_name`) and address shape → **resolved**, adopted.
- **D10** — Inventory keyed by `product_id` as PK (1:1 with `products`) → **resolved**, no surrogate key.
- **D13** — Payment 1:1 with order → **resolved**, adopted.
- **D14** — Timestamps as `DATETIME` → **resolved: ISO-8601 UTC `TEXT`**. SQLite has a weak
  `DATETIME` affinity and no native temporal type; ISO-8601 text sorts lexicographically in
  chronological order, so `ORDER BY created_at` is correct without conversion. `database.md` §9.
- **D16** — Idempotency → **resolved as reserved**, columns deferred pending `005-orders`.
- **D3, D11, D12, D17** → **resolved**; see *Resolved since the last entry* above.
- **D15** → **deferred** to `specs/005-orders`.

#### E-series — `docs/API.md`

- **E2** — Money as decimal numbers (`"price": 89.99`) → **rejected**; integer minor units, consistent
  with D1 and A5.
- **E3** — Default currency `CAD` → **rejected: `USD`**, consistent with D4.
- **E4** — Uppercase enum values (`ACTIVE`, `CONFIRMED`, `PENDING`) → **rejected**; lowercase
  snake_case, consistent with D6.
- **E5** — `products.status` with a `POST /products/{id}/status` route → **rejected**; `is_active
  BOOLEAN` is authoritative and there is no status-transition endpoint (D8, F4, A7).
- **E6** — Payment methods `CARD` / `BANK_TRANSFER` / `CASH_ON_DELIVERY` → **rejected: `fake_card`**,
  consistent with D5 and F7.
- **E7, E8** — The draft dropped category `slug`/`parent_id` and split the customer into
  `first_name`/`last_name` + `phone` with a flattened address → **rejected; all restored**. Category
  keeps `slug` and self-referential `parent_id`; customers keep `full_name` and the four-column
  address, and there is no `phone` (D7, D9, F5).
- **E9** — The draft's `reserved_quantity > quantity` rejection rule → **the rejection is kept, the
  stated reason is corrected.** The draft called it invalid for the wrong reason (`quantity_available`
  being "stock not yet allocated"); it is invalid because it breaks
  `quantity_reserved <= quantity_on_hand`. See D17 and `API.md` §18.1.
- **E10** — Checkout route shape → **deferred** (A6).
- **E11** — Payment retry semantics → **resolved**: retry applies to `pending`/`failed` only, and a
  retry of `captured` is `409 INVALID_PAYMENT_TRANSITION`.
- **E12** — Failure config example set `FAKE_FAILURE_RATE=0.10` alone → **rejected**; the example was
  incomplete, since a rate is meaningless without `FAKE_FAILURE_ENABLED=true` (G10, `API.md` §41).
- **E13** — `AUTHORIZED`, `DECLINED`, `TIMEOUT`, `PROVIDER_ERROR` returned as one flat list → **rejected**;
  the provider outcomes are `402`/`502`/`504` and are kept separate from the order/payment state
  machines. Consistent with S6.
- **E14** — Flat pagination envelope (`page`, `total`, `pages` at top level) → **rejected; the nested
  `meta` envelope is published**.
- **E15, E16** — `sort_by`/`sort_order` and a `search` query parameter → **rejected**; a single
  `sort` parameter with a `-` prefix for descending, and `q` for search.
- **E17** — `error.details` shown as an object → **rejected**; it is an **array** of field issues.
- **E18** — The draft narrowed an existing contract (dropped soft-delete, `/customers/me`, cart
  clear-all, inventory restock, `capture`/`refund`, and both standard headers) → **rejected; all
  restored**. The draft was not additive.
- **E19** — Four genuinely new routes (`GET /api/v1/health`, `POST /api/v1/orders`,
  `POST /api/v1/payments`, `POST /api/v1/payments/{id}/authorize`) → **retained as proposals**,
  each marked `★` and requiring sign-off in its own feature spec.
- **E20** — `402` appeared in the draft's status table but was missing from the previously published
  list → **resolved: added**. `402 PAYMENT_DECLINED` is part of the published status set.
- **E1** → **resolved**, coupled to D3: opaque UUID strings stored as `TEXT` (`API.md` §5.1).

#### F-series — `docs/generator.md`

- **F1–F18** — Restored the full published generator contract: single-transaction commit, validation
  before commit, deterministic `RandomSource` seeding, UUID-capable `IdGenerator` and `Clock` ports,
  the `test`/`development`/`large` profiles, `__main__.py` as the entrypoint, `faketxn_` prefixing,
  idempotent reset, and a documented generator ownership rule for rows it did not create.
- **F1** — Prices as `Decimal("89.99")` → **rejected**; integer minor units, consistent with A5.
- **F2, F7** — `CAD` currency and `CARD`/`BANK_TRANSFER`/`CASH_ON_DELIVERY` payment methods →
  **rejected: `USD` and `fake_card`**, consistent with D4 and D5.
- **F8** — Emails on `@example.test` → **rejected: `.invalid`**, the reserved TLD for exactly this
  purpose.
- **F9** — Transaction IDs `txn_test_000001` → **rejected: `faketxn_` prefixing**, so generated
  transactions are identifiable as synthetic.
- **F10** — The draft's per-entity `GENERATOR_*` environment variables → **rejected**; counts are CLI
  flags, and `RANDOM_SEED` / `SEED_PROFILE` are the only generator settings.
- **F11** — Different profile counts → **rejected; the published `test`/`development`/`large` counts
  are authoritative.**
- **F12** — CLI drops `--validate/--no-validate`, `--quiet`, `--output` → **rejected; all restored.**
- **F13** — "For very large datasets, generation may be performed in bounded batches" → **rejected**.
  Generation is a single transaction with validation before commit; a partial batch commit would
  produce a dataset that violates its own invariants.
- **F14** — A `payments.provider_code` column → **rejected**; the fake provider's outcome is
  expressed through the published state machine and error codes, not a new column.
- **F16** — Failure reasons `CARD_DECLINED`, `PROVIDER_UNAVAILABLE`, `TIMEOUT` → **rejected; aligned
  to the published codes** `PAYMENT_DECLINED`, `PAYMENT_PROVIDER_ERROR`, `PAYMENT_PROVIDER_TIMEOUT`
  (S6).
- **F17** — `GENERATOR_START_DATE` / `GENERATOR_END_DATE` → **rejected; the date window is a CLI
  setting** governed by the seeded `Clock`, not environment variables.

#### G-series — `docs/configuration.md`

- **G1** — `GENERATOR_*` variable family → **rejected**; superseded by F10.
- **G2** — `DEFAULT_CURRENCY=CAD` → **rejected: `USD`**, consistent with D4.
- **G3** — Synchronous `sqlite:///` DSN → **rejected: `sqlite+aiosqlite:///`**, the driver already
  published throughout the corpus.
- **G4** — `API_PREFIX` → **resolved: `API_V1_PREFIX`**.
- **G5** — `AUTH_ENABLED=false` as the development/test default → **rejected: `true`**, so the
  permission boundary is real in the common case.
- **G6** — `SQLITE_FOREIGN_KEYS` as a setting → **rejected as not configurable**; `PRAGMA
  foreign_keys = ON` is mandatory on every connection (`database.md` §34).
- **G7** — `GET /health/ready` → **rejected: `GET /ready`**, as published in `API.md` §25.2.
- **G8** — `HEALTH_ENABLED` → **rejected as not configurable**.
- **G9** — `REQUEST_ID_HEADER` → **rejected as not configurable**; `X-Request-ID` is contract.
- **G10** — `FAKE_LATENCY_ENABLED` → **rejected**: `FAKE_FAILURE_ENABLED` is the single master switch.
- **G11** — `FAKE_FAILURE_SEED` → **rejected**: `RANDOM_SEED` already governs all determinism.
- **G12** — `APP_TIMEZONE` → **rejected**: timestamps are ISO-8601 UTC text. `FAKE_CLOCK`, which the
  draft omitted entirely despite three documents depending on it, was **restored**.
- **G13** — `DEFAULT_LOCALE=en_CA` → **rejected: `en_US`**, consistent with the `USD` default.
- **G14** — Test database under `data/` → **rejected: `./.tmp/test.db`**, with a hard startup guard.
- **G15** — "Production" example reusing the development path → **rejected**: the DSN comes from the
  environment and is never committed.
- **G16** — Production hard guards and the startup validation table → **restored**.
- **G17** — `PAYMENT_DECLINE_RATE` / `PAYMENT_TIMEOUT_RATE` / `PAYMENT_ERROR_RATE` /
  `PAYMENT_TIMEOUT_MS` / `PAYMENT_PROVIDER` → **recorded as reasonable but deferred** to
  `specs/009-failure-simulation`, because they would add a second randomization mechanism alongside
  `FAKE_FAILURE_RATE`.
- The draft's genuinely additive material was retained: `APP_NAME`, `APP_VERSION`, `DEBUG`,
  `HOST`/`PORT`, `API_DOCS_ENABLED`/`API_REDOC_ENABLED`, `CORS_ENABLED`,
  `OBSERVABILITY_ENABLED`/`METRICS_ENABLED`/`TRACING_ENABLED`, `SQLITE_BUSY_TIMEOUT_MS`,
  `AUTH_PROVIDER`, `AUTH_DEFAULT_ROLE`, the nested settings model, configuration immutability,
  `data/` directory creation, the shared Alembic configuration-source rule, Docker, and CI.

#### T-series — `docs/testing.md`

- **T2, T5** — Draft examples used `"currency": "CAD"` and `"status": "ACTIVE"`. **Rejected** — `USD`
  and lowercase, consistent with D4/D6. Uppercase survives only in state-machine diagrams, which is a
  documented readability convention (`database.md` D6).
- **T3** — Draft used `"price": "29.99"` as a string. **Rejected** — money is integer minor units in a
  nested object, consistent with A5.
- **T6** — Draft used `Product.activate()`/`deactivate()` and an `ACTIVE`/`INACTIVE`/`DISCONTINUOUS`
  lifecycle. **Rejected** — `is_active BOOLEAN` is authoritative; `DISCONTINUOUS` stays deferred
  (D8, F4).
- **T7** — Draft order lifecycle **omitted `failed`**. **Rejected** — required by A3; it is the state
  the checkout compensation tests assert against.
- **T8** — Draft payment lifecycle **omitted `captured` and `refunded`**. **Rejected** — required by
  A4 and by the capture/refund endpoints.
- **T9** — Draft §41 **mandated `Decimal` for monetary assertions**, directly contradicting A5, which
  prohibits `Decimal` and `float` alike. **Rejected** — assertions use integer minor units. This was
  the most consequential conflict in the draft: adopting it would have produced a test suite asserting
  against a different money representation than the one under test.
- **T10** — Draft used `FAKE_FAILURE_SEED`. **Rejected**, consistent with G11.
- **T11** — Draft used a per-run unique `/tmp` database. **Rejected** — `./.tmp/test.db` or in-memory
  (G14).
- **T12** — Draft regression-tested `POST /carts/{id}/checkout`. **Rejected for now** — the interim
  contract is `POST /api/v1/checkout` (A6).
- **T13** — Draft listed routes without the `API_V1_PREFIX` and treated
  `POST /payments/{id}/authorize` as published. **Rejected** — the prefix is mandatory, and that route
  is an E19 proposal that a regression test would freeze prematurely.
- **T14** — Draft referenced `.specify/specs/007-cart/`, wrong in two ways: the path is `specs/` at
  the repository root (C2), and the cart is feature `004`, not `007`. **Rejected.**
- **T15** — Draft used `CHECKED_OUT`. **Rejected** — lowercase `checked_out` (D6).
- **T17** — Draft relied on injected clocks and stable IDs without naming the ports. **Resolved** —
  `Clock`, `IdGenerator`, `RandomSource` (`architecture.md` §68, A8). This also makes
  `architecture.md` §68's quotation of "testing.md §5 — Injected `Clock` port" accurate.
- **T18** — Draft CI pipeline omitted `FAKE_CLOCK`. **Resolved** — added.
- **T20** — Draft principle 11 ("production code must not contain test-specific behavior") conflicts
  with shipped failure simulation and the determinism ports. **Resolved** by restating the rule:
  nothing may be *active by default* only for testing's benefit; anything gated and inert by default
  is acceptable.
- **T21, T22** — Soft-delete semantics made explicit; `tests/factories/` added to the directory tree.
- **T23** — The `orders.cart_id` referential check → **resolved and added**, plus a uniqueness check
  and the D12 address check (`testing.md` §24).
- **T1, T4** — **Resolved**, no longer deferred: D17 (inventory naming) and D3/E1 (identifier type).
- **T16, T19** — Clarified respectively.
- The draft's genuinely additive material was retained in full: the §57 per-feature minimum-coverage
  matrix, property-based testing, contract regression tests, naming conventions, markers, coverage
  policy, migration testing, synthetic-data rules, provider testing, dependency injection, the
  bug-to-regression-test process, the feature workflow, and the summary.

#### S-series — `docs/failure-simulation.md`

- **S1** — The draft's `FAKE_LATENCY_ENABLED` as a separate switch → **rejected**;
  `FAKE_FAILURE_ENABLED` is the single master switch, including latency, so there is no configuration
  state in which latency is simulated but failures are not. Consistent with G10.
- **S2, S19** — The draft's `FAKE_FAILURE_SEED`, used both ad hoc and in the four failure profiles →
  **rejected**; `RANDOM_SEED` already governs every source of nondeterminism in the project. A second
  seed would make "reproducible" ambiguous. Consistent with G11.
- **S3, S4** — Draft examples used `POST /api/v1/carts/1/checkout` and
  `POST /api/v1/payments/{id}/authorize` → **rejected for now**; the interim checkout contract is
  `POST /api/v1/checkout` with `cart_id` in the body (A6), and the authorize route is an E19 proposal
  that an example would freeze prematurely.
- **S5** — Draft expected `error.code = PAYMENT_TIMEOUT` → **rejected**; the published code is
  `PAYMENT_PROVIDER_TIMEOUT` at `504`, keeping the provider outcome distinct from the payment state
  machine (S6).
- **S6** — Draft collapsed every simulated failure onto the generic error code → **rejected**. The
  three payment scenarios now map to specific codes, because a client that receives
  `402 PAYMENT_DECLINED` can act on it in a way it cannot act on `SIMULATED_FAILURE`:
  `payment_declined` → `402 PAYMENT_DECLINED`,
  `payment_timeout` → `504 PAYMENT_PROVIDER_TIMEOUT`,
  `payment_provider_error` → `502 PAYMENT_PROVIDER_ERROR`.
  The generic `SIMULATED_FAILURE` remains for scenarios with no specific taxonomy code.
- **S7** — Draft mapped "provider unavailable" to `503` → **rejected: `502`**, matching the published
  scenario table, where `503` is reserved for genuine service unavailability (see S10).
- **S8** — Draft said the order "becomes FAILED **or** remains PENDING" → **rejected as indecisive**;
  the contract is fixed at order `→ failed` with a `failure_reason`, inventory released, payment
  terminal. Required by A3.
- **S10** — Draft implied an "inventory service unavailable" condition distinct from insufficient
  inventory → **rejected**; the only inventory failure is insufficient stock, so
  `INSUFFICIENT_INVENTORY` has exactly one cause (S7 keeps `503` for real unavailability).
- **S11** — Draft treated `Idempotency-Key` as an active mechanism → **rejected as premature**; the
  columns are deferred pending `specs/005-orders` (D16).
- **S12** — Draft proposed a future `python -m ecommerce.failure` CLI → **deferred** to
  `specs/009-failure-simulation`; it is not a current interface, and the only CLI entrypoint the
  constitution requires is the generator's.
- **S13** — A `Sleeper` port for latency tests → **recorded as a proposal, not adopted**, pending
  `architecture.md` §68. The port is needed for a latency test not to actually sleep.
- **S14** — Draft asserted `inventory.reserved <= inventory.quantity` → **rejected; pre-D17 naming.**
  Corrected to `quantity_reserved <= quantity_on_hand` (§47).
- **S15** — Draft used uppercase statuses in **prose** (`Order becomes FAILED`) → **rejected**;
  lowercase in prose, consistent with D6.
- **S16** — Draft invented a `FailurePolicy` type and a `FailureSimulator` class API → **rejected**;
  behavior is configuration plus a request header. This is also why the administrative injection
  endpoint proposed alongside it was rejected: an endpoint that turns failure injection on is a
  failure-injection endpoint an attacker would want.
- **S17** — Draft logged `failure_type=TIMEOUT` and `seed=42` → **rejected**; log fields follow the
  published error codes and carry no seed value, so logs stay deterministic and free of the seed.
- **S18** — Draft listed "Failure rate 1 → every eligible operation fails" while also saying
  probability-based failure was for experiments → **rejected as indecisive**; `FAKE_FAILURE_RATE` is
  a probability in `[0, 1]`, and deterministic failure is expressed through the
  `X-Fake-Failure` header.
- **S20** — §46 was cited by `architecture.md` §60.1 as holding the checkout route contract (previously
  §5) → **resolved**: the scenario matrix was repointed at `docs/testing.md` §25, and the
  `architecture.md` §60.1 reference to `failure-simulation.md` §46 was corrected.

#### W-series — `docs/development.md`

- **W1** — Draft placed ten documents and the constitution at the repository **root** → **rejected**;
  `docs/` and `.specify/memory/` (C2, A1).
- **W2–W9** — The draft's configuration block contradicted settled decisions on eight variables:
  `API_PREFIX`, a synchronous `sqlite:///` DSN, `GENERATOR_PROFILE`/`GENERATOR_SEED`,
  `FAKE_LATENCY_ENABLED`, `AUTH_ENABLED=false`, `CAD`, `en_CA`, and `APP_TIMEZONE` → **all rejected**
  and replaced with the G-series contract. It also omitted `FAKE_CLOCK`, `RANDOM_SEED`, and
  `SEED_PROFILE` — the three variables that make a run reproducible.
- **W10** — Draft recommended a `.gitignore` entry of `data/*.db` → **rejected**; ignore `data/`
  wholesale, because SQLite also creates `-wal` and `-shm` sidecars that a `*.db` pattern misses.
- **W11** — Draft said "if database health is included" → **clarified**; `GET /ready` is planned, not
  conditional (E19).
- **W12, W13** — Draft placed specs at `.specify/specs/` and published a **16-feature** roadmap →
  **rejected**; `specs/` at the root, and the 11-feature breakdown is authoritative. The 16-item list
  is retained in `plan.md` §30 as a rejected alternative.
- **W14** — Draft debugged failures with `FAKE_FAILURE_SEED` and `FAKE_FAILURE_RATE=1.0` → **rejected
  on both counts**. A 100% rate makes *every* operation fail randomly, burying the one provider call
  under investigation; naming the scenario with `X-Fake-Failure` and leaving the rate at `0.0` is
  both deterministic and targeted.
- **W15** — Draft's "add simulator policy" step → **rejected**; there is no policy object (S12, S16).
- **W16** — Draft's "production" section claimed a "real authentication boundary" and "appropriate
  external providers" → **rejected**; both contradict the project's premise. The fake provider *is*
  the product, and swapping in a real payment processor is an explicit non-goal.
- **W17** — Draft treated `development`/`test`/`large` as one set of "profiles" → **resolved** into two
  independent axes (`APP_ENV` and `SEED_PROFILE`). Their names partially overlap, which is a standing
  trap worth documenting rather than a single list.
- **W18** — The draft introduces `ruff` and `mypy` → **recorded as new**, filling the slot
  `plan.md` deliberately left open ("configure formatting/linting *if selected*"). This is the one
  place where the draft adds a tool rather than reconciling one; `make lint`, `make format`, and
  `make typecheck` are now concrete targets instead of "(if configured)".
- **W19, W20** — Routes written without the `API_V1_PREFIX` → **rejected**; and `docs/`, `specs/`, and
  `tests/factories/` were missing from the project tree → **added**.
- **W21** — §16/§17 and §23/§58 were duplicates → **resolved**: §16/§17 were already consolidated, and
  §58 is now a short pointer to §23 rather than a second copy of the reset procedure. The
  quick-reference command table in §61 remains the single place commands are listed side by side.
- The draft's genuinely additive material was retained, in most cases near-verbatim: the development
  loop and lifecycle sections, the Makefile target list, the lint/format/typecheck workflow, the
  debug-by-layer recipe, the Alembic and SQLite debugging recipes, the generator and failure
  debugging recipes, the domain-first rule, the API/repository/database layering recipes, the "adding
  a new X" sequences, the git and commit workflow, the pull-request checklist, the agent workflows,
  the development rules, and the completion criteria.

---

## Future Releases

Planned, in roadmap order. See [`specs/README.md`](specs/README.md).

- `001-foundation` — project scaffolding, configuration, logging, error envelope, `GET /health`,
  Alembic baseline.
- `002-product-catalog` — categories, products, search, filtering, pagination, CRUD.
- `003-customers` — registration, profile, status lifecycle.
- `004-cart` — cart lifecycle, cart items, quantity rules, pricing.
- `005-orders` — order lifecycle, order items, state transitions.
- `006-inventory` — stock levels, reservation, release, conflict detection.
- `007-payments` — payment states, synthetic fake payment provider.
- `008-fake-data-generator` — deterministic fake-data generator and CLI.
- `009-failure-simulation` — deterministic and randomized failure injection.
- `010-authentication` — fake authentication and explicit authorization rules.
- `011-observability` — request IDs, structured logging, domain events, health checks.

[Unreleased]: https://example.invalid/ecommerce/compare/v0.0.0...HEAD
