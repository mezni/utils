# Development

## Fake E-Commerce Server

> **Status:** Complete — FastAPI foundation implemented, `make check` passes, 55/55 tasks recorded.
> Feature: `002-fastapi-foundation` (P3). `make bootstrap` verified, liveness `/health`,
> description switches (`API_DOCS_ENABLED`, `API_REDOC_ENABLED`), error envelope, version
> consistency, dependency declaration correction.
> **Authority:** `.specify/memory/constitution.md` v1.1.1, *Simple Local Development*
> **Goal:** clean checkout → populated server, in a handful of commands

## Divergences from other documents

Reconciled against the constitution, `architecture.md`, `configuration.md`, `generator.md`,
`testing.md`, `failure-simulation.md`, and `plan.md`. Every conflict is recorded and resolved
explicitly (the constitution's *Explicit Business Rules*, III).

| # | Divergence in the incoming draft | Resolution | Now in |
|---|---|---|---|
| W1 | §4 places `constitution.md`, `PRD.md`, `architecture.md`, `API.md`, `database.md`, `generator.md`, `configuration.md`, `testing.md`, `failure-simulation.md`, and `development.md` at the repository **root**. | **Rejected** — documentation lives under `docs/`; the constitution is at `.specify/memory/constitution.md` (C2, A1). | §4, §8 |
| W2 | §11 uses `API_PREFIX`. | **Rejected** — `API_V1_PREFIX` (G4). | §11 |
| W3 | §11 uses `DATABASE_URL=sqlite:///…` (synchronous). | **Rejected** — `sqlite+aiosqlite:///…` (G3). | §11 |
| W4 | §11 uses `GENERATOR_PROFILE` / `GENERATOR_SEED`. | **Rejected** — `RANDOM_SEED` / `SEED_PROFILE` (G1, F10). | §11 |
| W5 | §11 uses `FAKE_LATENCY_ENABLED`. | **Rejected** — not adopted; `FAKE_FAILURE_ENABLED` is the single master switch (G10, S1). | §11 |
| W6 | §11 uses `AUTH_ENABLED=false`. | **Rejected** — `true` (G5). | §11, §52 |
| W7 | §11 uses `DEFAULT_CURRENCY=CAD`. | **Rejected** — `USD` (G2, D4). | §11 |
| W8 | §11 uses `DEFAULT_LOCALE=en_CA`. | **Rejected** — `en_US` (G13). | §11 |
| W9 | §11 uses `APP_TIMEZONE=UTC` and omits `FAKE_CLOCK`. | **Rejected** — timezone is not configurable (G12); `FAKE_CLOCK` restored. | §11 |
| W10 | §12 recommends `.gitignore` entry `data/*.db`. | **Rejected** — ignore `data/` wholesale; SQLite creates `-wal` and `-shm` sidecars that a `*.db` pattern misses (G-series, `configuration.md` §37). | §12 |
| W11 | §18 says "if database health is included". | **Resolved** — `GET /ready` is planned, not conditional (E19, T16, `API.md` §25.2). | §18 |
| W12 | §33 places specs at `.specify/specs/`. | **Rejected** — `specs/` at the repository root (C2, A1). | §33 |
| W13 | §34 lists a **16-feature** roadmap with entirely different numbering (`002-fastapi-foundation`, `003-database-foundation`, `011-checkout`, `016-developer-experience`). | **Rejected** — the 11-feature breakdown is authoritative (C1), and it places the fake-data generator at `008`, not `010`. The 16-item list is retained in `plan.md` §30 as a rejected alternative. | §34 |
| W14 | §46 uses `FAKE_FAILURE_SEED` and enables simulation with `FAKE_FAILURE_RATE=1.0` to debug one operation. | **Rejected** — `RANDOM_SEED` only (G11, S2); debugging one operation uses the `X-Fake-Failure` header with `FAKE_FAILURE_RATE=0.0`, not a 100% failure rate. | §46 |
| W15 | §41 step 4 says "add simulator policy". | **Rejected** — there is no policy object (S16); behavior is configuration plus a request header. | §41 |
| W16 | §52 "production" lists "real authentication boundary" and "appropriate external providers". | **Rejected** — both contradict the project's premise. There is no real authentication (the provider is `fake` by design) and no real external provider; swapping in a real payment processor is an explicit non-goal. | §52 |
| W17 | §53 conflates the `development`/`test`/`large` **generator** profiles with application **environment** profiles. | **Resolved** — two distinct axes: `APP_ENV` ∈ {`development`, `test`, `production`} and `SEED_PROFILE` ∈ {`test`, `development`, `large`}. They are set independently and the names only partially overlap, which is a standing trap. | §53 |
| W18 | §28–§30, §50, §59, §61 introduce `ruff` and `mypy`. | **Recorded as new** — no linter or type checker is named anywhere in the corpus; `plan.md` deliberately left the slot open ("configure formatting/linting **if selected**"). This draft selects them. `make lint` / `make format` become real targets rather than "(if configured)". | §28–§30, §9 |
| W19 | §54 and §56 write API paths without the `/api/v1` prefix. | **Rejected** — the prefix is part of the contract (`API.md` §3). | §54, §56 |
| W20 | §8 project tree omits `docs/`, `specs/`, and `tests/factories/`. | **Resolved** — all three added. | §8 |
| W21 | §16 and §17 are the same section; §23 and §58 substantially overlap. | **Resolved** — §16/§17 were already consolidated, and §58 is now a short pointer to §23 rather than a second copy of the reset procedure. §23 holds the full text; the quick-reference command table is in §61 for scanning. | §16, §23, §61 |

**This draft's genuinely new material** — the development-loop and lifecycle sections (§32, §57, §59,
§63), the make target list (§31), the lint/format/typecheck workflow (§28–§30), the debug-by-layer
recipe (§43), the Alembic and SQLite debugging recipes (§44), the generator and failure-simulation
debugging recipes (§45, §46), the domain-first rule (§35), the API/repository/database layering
recipes (§36–§38), the "adding a new X" sequences (§39–§42), the git and commit workflow (§49, §50),
the pull-request checklist (§51), the agent workflows (§54–§56), the development rules (§60), and
the completion criteria (§62) — is retained, in most cases near-verbatim.

Carried over from the previous revision of this document: the symptom→cause→fix lookup table
(§46.1) and the code conventions (§60.1). Both were unique to it and are not derivable from the
incoming draft. Everything else in that revision is either present above or has a canonical home
elsewhere — the documentation map lives in `README.md`.

---

# 1. Purpose

This document defines the local development workflow for the Fake E-Commerce Server.

The goal is to make the project easy to:

* Set up
* Run
* Modify
* Test
* Seed with synthetic data
* Debug
* Validate
* Extend
* Use with AI agents and integration experiments

The project is a local-first modular monolith. A new developer goes from a clean checkout to a
working populated server with a small number of commands, and nobody inserts database rows by hand.

---

# 2. Development Stack

| Tool              | Purpose                                      |
| ----------------- | -------------------------------------------- |
| Python 3.12+      | Application runtime                          |
| uv                | Python dependency and environment management |
| FastAPI           | HTTP API                                     |
| Pydantic v2       | Validation and schemas                       |
| Pydantic Settings | Configuration                                |
| SQLAlchemy 2.x    | Persistence                                  |
| Alembic           | Database migrations                          |
| SQLite            | Local database                               |
| pytest            | Testing                                      |
| HTTPX             | API testing                                  |
| Faker             | Synthetic data generation                    |
| Uvicorn           | ASGI server                                  |
| ruff              | Lint and format (W18)                        |
| mypy              | Static type checking (W18)                   |
| Spec Kit          | Specification-driven development             |

`uv.lock` is committed. Every other tool is reachable through `make`.

---

# 3. Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Python | 3.12+ | Managed by `uv` |
| `uv` | latest | Package management, lockfile, venv |
| make | any | Convenience targets (optional — every target maps to a plain command) |
| Git | any | Version control |

Verify:

```bash
uv --version
python --version
git --version
```

Expected: `Python 3.12.x` or newer. Activation is unnecessary — every command below runs through
`uv run`.

---

# 4. Clone the Repository

```bash
git clone <repository-url>
cd ecommerce
```

The project root contains:

```text
ecommerce/
├── .specify/
│   └── memory/
│       └── constitution.md
├── docs/                      # all documentation (W1)
│   ├── PRD.md
│   ├── plan.md
│   ├── architecture.md
│   ├── API.md
│   ├── database.md
│   ├── generator.md
│   ├── configuration.md
│   ├── testing.md
│   ├── failure-simulation.md
│   └── development.md
├── specs/                     # Spec Kit output (W12)
│   └── NNN-slug/
├── src/
├── tests/
├── migrations/
│   └── versions/
├── scripts/
├── data/                      # created at runtime, git-ignored
├── pyproject.toml
├── uv.lock
├── alembic.ini
├── .env.example
├── README.md
└── CHANGELOG.md
```

The draft's version of this tree put ten documents and the constitution at the repository root. They
live under `docs/` and `.specify/memory/` (C2, A1) — the same correction applied to
`architecture.md`.

---

# 5. Python Environment

`uv` manages the environment:

```bash
uv venv                  # optional; uv run creates it on demand
source .venv/bin/activate  # optional
```

The recommended workflow uses `uv run <command>` and does not require activation.

---

# 6. Install Dependencies

```bash
uv sync          # or: make install
```

This installs exactly what `uv.lock` pins. The lockfile MUST be committed.

---

# 7. Dependency Management

```bash
uv add <package>          # runtime dependency
uv add --dev <package>    # dev dependency
uv remove <package>
uv sync                   # re-sync after editing pyproject.toml
```

Do not hand-edit `uv.lock`. If `uv sync` fails on a lockfile mismatch, `pyproject.toml` changed
without a re-lock.

---

# 8. Project Structure

```text
ecommerce/
├── .specify/memory/constitution.md
├── docs/                        # documentation
├── specs/                       # 11 feature specs (W13, W20)
│   ├── README.md
│   ├── 001-foundation/
│   └── … 011-observability/
├── data/                        # git-ignored runtime database
├── migrations/
│   └── versions/                # Alembic revisions
├── scripts/
├── src/
│   └── ecommerce/
│       ├── api/
│       ├── application/
│       ├── config/
│       │   └── settings.py
│       ├── domain/
│       ├── infrastructure/
│       │   ├── database/
│       │   ├── repositories/
│       │   └── external/
│       └── seed/
│           ├── __init__.py
│           ├── __main__.py      # required for `python -m` (A2)
│           ├── cli.py
│           ├── generator.py
│           ├── factories/
│           └── profiles/
├── tests/
│   ├── unit/{domain,application,seed}/
│   ├── integration/{repositories,workflows,database}/
│   ├── api/
│   └── factories/               # (W20) test factories, `testing.md` §34
├── pyproject.toml
├── uv.lock
├── alembic.ini
└── .env.example
```

The draft omitted `docs/`, `specs/`, and `tests/factories/`. The first two are the authoritative
locations for two whole categories of artifact, so a tree that omits them invites putting a spec in
the wrong place.

---

# 9. Source Code Organization

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

The dependency direction is inward. The domain must not import:

```text
FastAPI
SQLAlchemy
HTTPX
pydantic-settings
os.environ
```

Infrastructure may depend on the domain. The API layer may depend on application services. Business
logic lives in the domain/application layers and is **never** duplicated across endpoints
(`architecture.md` §6).

---

# 10. Configuration Setup

```bash
cp .env.example .env   # optional; sensible defaults apply without it
```

`.env` is local configuration and MUST NOT be committed. Confirm `.gitignore` contains `.env`
(`configuration.md` §37).

Settings are read **once** at startup and validated then; invalid values fail immediately rather
than on first use. Full reference: `configuration.md`.

---

# 11. Default Development Configuration

```dotenv
# --- Application ---
APP_ENV=development
APP_NAME=Fake E-Commerce Server
APP_VERSION=0.1.0
DEBUG=true

# --- API ---
API_V1_PREFIX=/api/v1
API_DOCS_ENABLED=true
API_REDOC_ENABLED=true
CORS_ENABLED=false
CORS_ORIGINS=[]

# --- Server ---
HOST=127.0.0.1
PORT=8000

# --- Database ---
DATABASE_URL=sqlite+aiosqlite:///./data/ecommerce.db
SQLITE_BUSY_TIMEOUT_MS=5000

# --- Logging ---
LOG_LEVEL=INFO
LOG_FORMAT=text

# --- Determinism ---
RANDOM_SEED=42
# FAKE_CLOCK=2026-01-01T00:00:00Z

# --- Failure simulation (dev/test only) ---
FAKE_FAILURE_ENABLED=false
FAKE_FAILURE_RATE=0.0
FAKE_LATENCY_MS=0

# --- Auth ---
AUTH_ENABLED=true
AUTH_PROVIDER=fake
AUTH_DEFAULT_ROLE=customer
# AUTH_FAKE_TOKEN_SECRET=change-me-not-a-real-secret

# --- Generator ---
SEED_PROFILE=development

# --- Observability ---
OBSERVABILITY_ENABLED=true

# --- Data defaults ---
DEFAULT_CURRENCY=USD
DEFAULT_LOCALE=en_US
```

The draft's version of this block contained eight variables that contradict settled decisions
(W2–W9): `API_PREFIX`, a synchronous DSN, `GENERATOR_PROFILE`/`GENERATOR_SEED`,
`FAKE_LATENCY_ENABLED`, `AUTH_ENABLED=false`, `CAD`, `en_CA`, and `APP_TIMEZONE`. It also omitted
`FAKE_CLOCK`, `RANDOM_SEED`, and `SEED_PROFILE` — the three variables that make a run reproducible.

`AUTH_ENABLED=true` is the default **everywhere**, including development, so the permission boundary
is real in the common case. The fake tokens make it free.

Authoritative reference: `configuration.md`.

---

# 12. Database Directory

The development SQLite database lives at:

```text
data/ecommerce.db
```

Create the directory if needed:

```bash
mkdir -p data
```

In practice you do not have to: startup creates it (`configuration.md` §19).

Recommended `.gitignore` entry:

```text
data/
```

**Not** `data/*.db` (W10). SQLite also creates `ecommerce.db-wal` and `ecommerce.db-shm` while the
server runs, and a `*.db` pattern does not match them — so a developer who runs the server and
commits ends up committing a corrupt database fragment. Ignoring the directory removes the class of
mistake rather than one instance of it.

The dev database is disposable and can be deleted at any time.

---

# 13. Database Migrations

Alembic is the authoritative mechanism for schema evolution.

```bash
uv run alembic upgrade head
uv run alembic current
uv run alembic history
uv run alembic downgrade -1
```

Or via make: `make migrate`, `make migration-status`.

---

# 14. Creating a Migration

```bash
uv run alembic revision --autogenerate -m "add product status"
```

or `make migrate-new m="add product status"`.

Review the generated migration manually. Autogenerate **cannot** detect data migrations, server
defaults, or column renames, and a rename autogenerated as drop-plus-add silently destroys data.

Verify tables, columns, constraints, indexes, foreign keys, nullable behavior, default values, and
any data migration — then apply.

---

# 15. Migration Rules

1. Alembic owns schema evolution.
2. Migrations MUST be committed.
3. Autogenerated migrations MUST be reviewed.
4. Destructive changes require explicit consideration and a documented decision.
5. "Production" schema changes must be backward-aware.
6. Tests MUST verify important migrations.
7. `Base.metadata.create_all()` is **not** the schema mechanism — the application never migrates on
   boot (`database.md` §41).
8. Alembic reads the same `DATABASE_URL` as the application (`configuration.md` §20).

---

# 16. Running the Application

```bash
make run           # uvicorn --reload
uv run uvicorn ecommerce.main:app --reload
```

`--reload` restarts the server on source changes. Never use it in a "production"-shaped
environment.

The API is normally at <http://127.0.0.1:8000>.

| URL | Purpose |
|---|---|
| <http://127.0.0.1:8000/health> | Liveness |
| <http://127.0.0.1:8000/docs> | Interactive OpenAPI docs |
| <http://127.0.0.1:8000/openapi.json> | Machine-readable schema |
| <http://127.0.0.1:8000/api/v1/...> | Versioned API |

Useful variants:

```bash
uv run uvicorn ecommerce.main:app --reload --port 9000
uv run uvicorn ecommerce.main:app --log-level debug
```

---

# 17. Health Check

```bash
curl http://127.0.0.1:8000/health
```

Expected:

```json
{ "status": "ok" }
```

`/health` reports process liveness and MUST NOT touch the database — a liveness probe that queries
the database restarts healthy processes during a slow query.

Database connectivity is the separate `GET /ready` endpoint, which is **planned and not required for
the initial implementation** (`API.md` §25.2, E19). The draft's "if database health is included"
(W11) implies a conditional; it is a known scope decision, not a pending question.

---

# 18. Calling the API

```bash
BASE=http://127.0.0.1:8000

curl -s $BASE/health
curl -s "$BASE/api/v1/products?is_active=true&page=1&page_size=5"

curl -s -X POST $BASE/api/v1/customers \
  -H 'Content-Type: application/json' \
  -d '{"email":"dev1@example.invalid","full_name":"Dev One"}'
```

End-to-end purchase:

```bash
BASE=http://127.0.0.1:8000
TOKEN=<fake token>

# 1. Pick a product
PRODUCT_ID=$(curl -s "$BASE/api/v1/products?is_active=true&page_size=1" | jq -r '.items[0].id')

# 2. Create a cart
CART=$(curl -s -X POST $BASE/api/v1/carts \
  -H "Authorization: Bearer $TOKEN" | jq -r '.id')

# 3. Add an item
curl -s -X POST $BASE/api/v1/carts/$CART/items \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"product_id":"'"$PRODUCT_ID"'","quantity":2}'

# 4. Checkout — interim contract, cart_id in the body (A6)
curl -s -X POST $BASE/api/v1/checkout \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"cart_id":"'"$CART"'","payment_method":"fake_card"}'
```

Failure path:

```bash
curl -s -X POST $BASE/api/v1/checkout \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H 'X-Fake-Failure: payment_declined' \
  -d '{"cart_id":"'"$CART"'","payment_method":"fake_card"}'
# {"error":{"code":"PAYMENT_DECLINED", ...}}   -> order failed, stock released
```

---

# 19. Generate Development Data

The fake-data generator is a first-class development tool.

```bash
uv run python -m ecommerce.seed --profile development --seed 42
```

or `make seed`.

Regenerate from scratch:

```bash
uv run python -m ecommerce.seed --profile development --seed 42 --reset
```

The generator does **not** require the HTTP server to be running. Full contract: `docs/generator.md`.

---

# 20. Generate a Custom Dataset

```bash
uv run python -m ecommerce.seed \
    --seed 42 \
    --categories 20 \
    --products 500 \
    --customers 100 \
    --carts 100 \
    --orders 250
```

Counts are CLI flags, **not** `GENERATOR_*` environment variables (F10, G1) — see §11.

Useful for pagination, filtering, search, checkout, agent workflows, and performance work.

---

# 21. Test Dataset

```bash
uv run python -m ecommerce.seed --profile test --seed 42 --reset   # or: make seed-test
```

Small, fast, deterministic. Published counts: 5 categories, 20 products, 10 customers, 25 orders.

---

# 22. Large Dataset

```bash
uv run python -m ecommerce.seed --profile large --seed 42 --reset  # or: make seed-large
```

Published counts: 40 categories, 5,000 products, 2,000 customers, 20,000 orders. For pagination,
query performance, indexing, and realistic agent retrieval — still small enough to run locally.

---

# 23. Resetting the Database

Prefer the generator's `--reset`, which clears generated rows and regenerates deterministically:

```bash
uv run python -m ecommerce.seed --profile development --seed 42 --reset   # make reset
```

For a full rebuild including the schema:

```bash
make db-reset     # drop the DB file, migrate, seed
```

which is equivalent to:

```bash
rm -f data/ecommerce.db
uv run alembic upgrade head
uv run python -m ecommerce.seed --profile development --seed 42
```

`--reset` is preferable in the common case: it is faster, and it preserves the schema, so a reset
cannot mask a broken migration.

---

# 24. Running Tests

```bash
make test
uv run pytest
uv run pytest -v
uv run pytest tests/unit
uv run pytest tests/integration
uv run pytest tests/api
```

---

# 25. Running a Specific Test

```bash
uv run pytest tests/unit/domain/inventory/test_inventory.py
uv run pytest -k "inventory"
uv run pytest tests/unit/domain/inventory/test_inventory.py -k reserve
```

---

# 26. Coverage

```bash
uv run pytest --cov=src/ecommerce --cov-report=term-missing   # or: make coverage
uv run pytest --cov=src/ecommerce --cov-report=html
```

Coverage identifies untested areas. It does not replace meaningful behavioral tests: a suite can
reach 100% line coverage while asserting nothing about the invariants that matter
(`testing.md` §48).

---

# 27. Test Database

Tests MUST NOT depend on `data/ecommerce.db`. The suite creates its own isolated database:

```text
pytest
  │
  ▼
temporary SQLite DB (./.tmp/test.db or in-memory)
  │
  ▼
Alembic migrations
  │
  ▼
test
  │
  ▼
cleanup
```

`APP_ENV=test` **refuses to start** against a non-test DSN (`configuration.md` §14.2), so a
misconfigured test run fails loudly instead of truncating a developer's data.

---

# 28. Linting

```bash
uv run ruff check .        # or: make lint
uv run ruff format .       # or: make format
uv run ruff format --check .
```

`ruff` and `mypy` are newly selected tooling (W18). `plan.md` left this slot open
("configure formatting/linting **if selected**"); the choice is recorded here so the Makefile
targets are concrete rather than conditional. Configuration lives in `pyproject.toml`.

---

# 29. Type Checking

```bash
uv run mypy src            # or: make typecheck
```

Type hints are used throughout. Checking covers `domain`, `application`, `infrastructure`, `api`,
`config`, and `seed`. The domain layer should have the strongest coverage — it is the layer with the
most invariants and the fewest external types to lean on.

---

# 30. Code Quality Workflow

Before committing:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```

Or `make check` (W18), which runs all four.

---

# 31. Makefile Targets

| Target | Does |
|---|---|
| `make install` | `uv sync` |
| `make bootstrap` | install → migrate → seed |
| `make migrate` | `alembic upgrade head` |
| `make migrate-new m="..."` | autogenerate a revision |
| `make migration-status` | `alembic current` |
| `make seed` | seed with the development profile |
| `make seed-test` | seed with the test profile |
| `make seed-large` | seed with the large profile |
| `make seed-args SEED=42` | explicit seed and counts |
| `make reset` | clear generated rows and regenerate |
| `make db-reset` | drop the DB file, migrate, seed |
| `make run` | `uvicorn --reload` |
| `make test` | full pytest suite |
| `make test-unit` | unit tests only |
| `make coverage` | tests with a coverage report |
| `make lint` | `ruff check` |
| `make format` | `ruff format` |
| `make typecheck` | `mypy src` |
| `make check` | lint + format check + typecheck + test |
| `make clean` | remove caches and the dev database |

Every target wraps a plain command, so the Makefile is a convenience and never a dependency. A
developer with no `make` installed loses nothing.

---

# 32. Recommended Developer Loop

```text
Understand requirement
       ↓
Update Spec Kit specification
       ↓
Update design/plan
       ↓
Write tests
       ↓
Implement domain behavior
       ↓
Implement application service
       ↓
Implement persistence
       ↓
Implement API
       ↓
Run tests
       ↓
Run lint/type checks
       ↓
Update documentation
       ↓
Commit
```

This prevents implementation from drifting away from the specification. Note the ordering: tests
before implementation, and documentation before commit.

---

# 33. Spec Kit Workflow

Feature development follows:

```text
Specification → Design → Implementation Plan → Tasks → Implementation → Testing → Validation → Documentation
```

```text
/specify  -> specs/NNN-slug/spec.md    (observable behavior)
/plan     -> specs/NNN-slug/plan.md    (design, layers, tradeoffs)
/tasks    -> specs/NNN-slug/tasks.md   (ordered work items)
   ... implement, test, validate ...
   ... update docs/ and CHANGELOG.md ...
```

The constitution lives at `.specify/memory/constitution.md`. All feature output lives under
`specs/` at the repository root (W12) — the single canonical location the Spec Kit tooling resolves
to (the constitution's *Speckit Development Process*, C2).

---

# 34. Feature Specifications

The authoritative roadmap is `specs/README.md` — **11 features**:

```text
specs/
├── 001-foundation/
├── 002-product-catalog/
├── 003-customers/
├── 004-cart/
├── 005-orders/
├── 006-inventory/
├── 007-payments/
├── 008-fake-data-generator/     # highest priority
├── 009-failure-simulation/
├── 010-authentication/
└── 011-observability/
```

The draft's 16-feature list (W13) is **not** adopted. It conflicts on structure — it splits
foundation into `002-fastapi-foundation` and `003-database-foundation`, pushes the fake-data
generator to `010`, and adds `015-testing-hardening` and `016-developer-experience`. C1 resolved
this: the 11-feature breakdown is authoritative, and the 16-item version is retained in
`plan.md` §30 as a rejected alternative so the decision stays auditable.

Each feature defines: problem, user scenarios, functional requirements, acceptance criteria,
constraints, failure cases, test scenarios, and a definition of done.

Definition of done — an endpoint returning `200 OK` is **not** sufficient:

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

---

# 35. Domain-First Development

New business behavior begins in the domain layer.

```text
Requirement: Reserve inventory
        ↓
Domain rule
        ↓
Inventory entity
        ↓
Application service
        ↓
Repository
        ↓
API endpoint
```

Do not begin by placing business logic in a FastAPI route handler. A rule that lives in a route body
is duplicated the moment a second entry point needs it — the CLI, a background job, or a test — and
the copies drift.

---

# 36. API Development

```text
Request
  ↓
Pydantic request schema
  ↓
API dependency injection
  ↓
Application command/use case
  ↓
Domain behavior
  ↓
Repository
  ↓
Database
  ↓
Domain result
  ↓
Application response
  ↓
Pydantic response schema
  ↓
HTTP response
```

FastAPI routes stay thin.

---

# 37. Repository Development

Repositories implement domain-defined persistence contracts:

```text
Domain
  │
  └── ProductRepository        (interface, owned by the domain)
          ▲
          │
Infrastructure
  │
  └── SqlAlchemyProductRepository   (implementation)
```

The domain MUST NOT import the SQLAlchemy implementation. The interface belongs to the domain so the
dependency points inward.

---

# 38. Database Development

```text
1. Update the domain model if required.
2. Update the persistence model.
3. Generate the migration.
4. Review the migration.
5. Apply the migration.
6. Add repository tests.
7. Add API/integration tests.
8. Update docs/database.md.
```

Schema changes are never made by hand outside a throwaway test database.

---

# 39. Adding a New Entity

```text
1. Define the business purpose.
2. Define the bounded context.
3. Define the domain entity.
4. Define invariants.
5. Define the repository interface.
6. Define the SQLAlchemy model.
7. Create the Alembic migration.
8. Implement the repository.
9. Implement application use cases.
10. Define Pydantic schemas.
11. Implement API endpoints.
12. Add tests.
13. Update documentation.
```

Steps 3–4 before 6 is deliberate. Writing the SQLAlchemy model first produces a table that mirrors
whatever the ORM felt like, and the invariants get retrofitted as `CHECK` constraints nobody fully
trusts.

---

# 40. Adding a New API Endpoint

```text
Requirement
    ↓
API contract (docs/API.md)
    ↓
Pydantic schema
    ↓
Application use case
    ↓
Domain behavior
    ↓
Persistence
    ↓
Endpoint
    ↓
API test
```

The API specification is updated before or alongside the implementation.

---

# 41. Adding a New Failure Scenario

```text
1. Define the failure semantics and required end state.
2. Add the scenario name to docs/failure-simulation.md §5.
3. Map it to an error code and HTTP status (API.md §12.1, §13).
4. Implement the provider behavior.
5. Wire the scenario into the simulator.
6. Document any configuration (configuration.md §23).
7. Add the scenario to the test matrix (testing.md §25).
8. Verify the production guard still refuses to start.
```

There is no "policy object" to add (W15, S16) — behavior comes from the scenario name in
`X-Fake-Failure` plus configuration. Failure injection stays outside the domain layer, and no
administrative endpoint is introduced (S12, `failure-simulation.md` §34).

---

# 42. Adding a New Generator Entity

```text
1. Define the database entity.
2. Define the domain representation.
3. Create a factory.
4. Define generation order.
5. Define relationships.
6. Add validation.
7. Add deterministic tests.
8. Update docs/generator.md.
```

Generated records MUST always respect foreign-key relationships, and the generation order must be
topological — a cart cannot be generated before its customer.

---

# 43. Debugging the API

When a request fails, inspect in this order:

```text
1. HTTP request        (method, path, headers, X-Request-ID)
2. Pydantic validation
3. API dependency      (authn/authz, DI resolution)
4. Application service (use-case orchestration)
5. Domain rule         (the actual invariant)
6. Repository
7. SQL query
8. SQLite state
```

The order mirrors the architecture, so each step rules out a whole layer. Start at the top because a
validation error at step 2 makes steps 5–8 irrelevant.

---

# 44. Debugging Database Problems

```bash
uv run alembic current
uv run alembic history
ls -lh data/
```

```bash
sqlite3 data/ecommerce.db
```

```sql
.tables
.schema products
PRAGMA foreign_keys;      -- expect 1
PRAGMA journal_mode;      -- expect wal
```

Never modify the schema manually as a substitute for a migration. A hand-edited local database
diverges from the migration chain and produces failures that do not reproduce for anyone else.

---

# 45. Debugging Generator Problems

Reset first, then shrink:

```bash
uv run python -m ecommerce.seed --profile test --seed 42 --reset

uv run python -m ecommerce.seed \
    --seed 42 \
    --categories 2 \
    --products 5 \
    --customers 5 \
    --carts 5 \
    --orders 5
```

A 5-product run that fails points at the invariant; a 5,000-product run that fails only tells you
something failed. If output differs between two identical runs, the cause is unseeded randomness or
a `datetime.now()` somewhere outside the injected ports.

---

# 46. Debugging Failure Simulation

Confirm normal behavior first:

```dotenv
FAKE_FAILURE_ENABLED=false
FAKE_LATENCY_MS=0
```

Then enable **deterministic** injection and name the one scenario being debugged:

```dotenv
FAKE_FAILURE_ENABLED=true
FAKE_FAILURE_RATE=0.0
RANDOM_SEED=42
```

```bash
curl -s -X POST "$BASE/api/v1/checkout" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H 'X-Fake-Failure: payment_timeout' \
  -d '{"cart_id":"'"$CART"'","payment_method":"fake_card"}'
```

The draft's approach (W14) was `FAKE_FAILURE_RATE=1.0` with `FAKE_FAILURE_SEED=42`. Both halves are
wrong for debugging: there is no `FAKE_FAILURE_SEED` (G11, S2), and a 100% rate makes *every*
operation fail randomly, so the one provider call under investigation is buried in noise from every
other. Naming the scenario is deterministic and targets exactly one boundary.

Reset afterward:

```dotenv
FAKE_FAILURE_ENABLED=false
```

Failure simulation must never remain enabled accidentally.

## 46.1 Symptom Reference

A lookup table for the failures that actually recur. The sections above explain *why*; this one
answers "I am staring at this error, what now".

| Symptom | Cause | Fix |
|---|---|---|
| `no such table: products` | Migrations never applied | `make migrate` |
| Server starts, every list is empty | Database exists but was never seeded | `make seed` |
| `database is locked` | SQLite allows a single writer; a second process holds the write lock | Stop the other process (a forgotten `uvicorn` or a `sqlite3` shell in the same directory), confirm `PRAGMA journal_mode` is `wal`, and raise `SQLITE_BUSY_TIMEOUT_MS` if contention is expected |
| Foreign keys appear to be ignored | `PRAGMA foreign_keys` is **off by default and per connection** | Enable it on every connection, not once at startup — see `database.md` |
| Generator output differs between two identical runs | Unseeded randomness, or wall-clock time read outside the injected ports | Pass `--seed` / set `RANDOM_SEED`; hunt for a `datetime.now()` that bypasses the `Clock` port |
| Tests pass alone but fail in a full run | Shared state between tests | Fix the isolation. Reordering tests hides the bug; it does not remove it |
| Tests modified my development data | Test suite is pointing at the dev database | `APP_ENV=test` plus a dedicated `DATABASE_URL` (`testing.md` §14) |
| A failure occurs when none was requested | `FAKE_FAILURE_*` left enabled, or `FAKE_FAILURE_RATE > 0` | Restore the defaults in §11 — `FAKE_FAILURE_ENABLED=false`, `FAKE_FAILURE_RATE=0.0` |
| A route returns `401` unexpectedly | `AUTH_ENABLED=true` everywhere, including development (W6) | Send a fake token; the default is deliberate, not a misconfiguration |
| `500` with an empty or non-standard body | An unhandled exception escaped the error envelope | Read the server log for the real trace and request ID; internal exceptions are never returned to the client |
| A `ecommerce.db-wal` or `-shm` file was committed | `.gitignore` matches `data/*.db`, which does not match SQLite sidecars (W10) | Ignore `data/` wholesale, then `git rm --cached` the stray files |
| `uv sync` fails against the lockfile | `pyproject.toml` changed without re-locking | `uv sync`, then commit the updated `uv.lock` |
| `ModuleNotFoundError: ecommerce` | Environment not synced, or the `src/` layout is not on the path | `uv sync` — the project is installed editable by default |

Never respond to a schema-shaped symptom by editing the database by hand. The development database
is disposable: `make db-reset` is faster than reasoning about a database that has drifted from the
migration chain, and it costs nothing to regenerate.

---

# 47. Logging During Development

Development logging should expose enough to understand:

* Request ID
* HTTP method and path
* Status and duration
* Application errors
* Simulated failures
* Domain events where applicable

Never log passwords, authentication tokens, payment credentials, secrets, or sensitive customer
information. `LOG_FORMAT=json` in "production" makes the same fields aggregatable.

---

# 48. Interactive API Development

FastAPI's OpenAPI interface is the primary tool while developing the API layer:

| Path | Use |
|---|---|
| `/docs` | Swagger UI — submit requests, inspect schemas |
| `/redoc` | ReDoc — read-only reference |
| `/openapi.json` | Machine-readable schema |

Route metadata MUST be filled in — `summary`, `description`, `response_model`, `status_code`, and
documented error responses — so the generated document is usable without reading `docs/API.md`
(`API.md` §26).

---

# 49. Git Workflow

```bash
git checkout -b feature/product-catalog
```

Focused commits, conventional prefixes:

```text
feat: add product domain
feat: add product repository
feat: add product API
test: add product integration tests
docs: document product API
```

Avoid mixing unrelated changes into one commit.

---

# 50. Commit Validation

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
git status
git diff
```

Then commit. `.env` and `data/` must not appear in the staged set.

---

# 51. Pull Request Checklist

```text
[ ] Specification updated (specs/NNN-slug/spec.md)
[ ] Implementation plan updated
[ ] Domain rules implemented
[ ] Application use case implemented
[ ] Persistence implemented
[ ] API implemented
[ ] Unit tests added
[ ] Integration tests added where needed
[ ] API tests added where needed
[ ] Failure paths tested
[ ] Migrations reviewed
[ ] Documentation updated (docs/ and CHANGELOG.md)
[ ] Lint passes
[ ] Type checking passes
[ ] Full test suite passes
[ ] No `.env` or `data/` files staged
```

---

# 52. Environment Separation

Three application environments, selected with `APP_ENV`:

| `APP_ENV` | Database | Auth | Failure simulation | Docs | Logging |
|---|---|---|---|---|---|
| `development` | `data/ecommerce.db` | `true` (fake tokens) | off by default | on | `text`, `DEBUG` |
| `test` | `./.tmp/test.db` | `true` | off; opt in per test | on | `text`, `WARNING` |
| `production` | from environment | `true` (mandatory) | **refuses to start** | off | `json`, `INFO` |

"Production" means production-*shaped configuration* for realistic deployments and client testing.
There is no real payment provider and no real identity provider, and adding them is an explicit
non-goal — the draft's "real authentication boundary" and "appropriate external providers" (W16)
would defeat the project's purpose. The fake provider *is* the product.

Two hard guards are enforced at startup, not merely documented:

```text
APP_ENV=production + FAKE_FAILURE_ENABLED=true   → refuse to start
APP_ENV=test + non-test DATABASE_URL              → refuse to start
```

Full reference: `configuration.md` §28–§29.

---

# 53. Development Profiles

Two **independent** axes, often confused because their names partially overlap (W17):

| Axis | Variable | Values |
|---|---|---|
| Application environment | `APP_ENV` | `development`, `test`, `production` |
| Generator profile | `SEED_PROFILE` | `test`, `development`, `large` |

They are set separately: a `production`-shaped server happily runs against a `test`-profile dataset,
and a development server can be seeded with the `large` profile. The draft's third list — treating
`development`/`test`/`large` as one set of "configuration profiles" — collapses these and implies
`production` is a generator profile, which it is not.

Profiles are configuration and data differences only, never separate application implementations.

Published generator counts:

| Profile | Categories | Products | Customers | Orders |
|---|---:|---:|---:|---:|
| `test` | 5 | 20 | 10 | 25 |
| `development` | 12 | 200 | 50 | 300 |
| `large` | 40 | 5,000 | 2,000 | 20,000 |

---

# 54. AI-Agent Development

The fake server is deliberately suitable for AI-agent experiments. An agent can interact with:

```text
GET  /api/v1/products
GET  /api/v1/inventory
POST /api/v1/carts
POST /api/v1/carts/{cart_id}/items
POST /api/v1/checkout
GET  /api/v1/orders
GET  /api/v1/payments
```

The deterministic generator makes experiments reproducible: `--seed 42` recreates the same logical
dataset, and `FAKE_CLOCK` pins timestamps (W19 — the draft omitted the prefix on these paths).

---

# 55. Agent Experiment Workflow

```text
Reset database
      ↓
Apply migrations
      ↓
Generate deterministic data (--seed 42)
      ↓
Start API
      ↓
Run agent
      ↓
Capture API interactions
      ↓
Inject failures (X-Fake-Failure)
      ↓
Run agent again
      ↓
Compare behavior
```

This makes the server a controlled environment for tool-calling and agent reliability experiments:
same seed, same clock, same injected scenario — so any difference in agent behavior is attributable
to the agent.

---

# 56. Failure Testing Workflow

```text
Normal
  ↓
Payment decline        X-Fake-Failure: payment_declined
  ↓
Payment timeout        X-Fake-Failure: payment_timeout
  ↓
Provider error         X-Fake-Failure: payment_provider_error
  ↓
Inventory unavailable  X-Fake-Failure: inventory_unavailable
  ↓
Latency                FAKE_LATENCY_MS=500
```

Each experiment uses a known seed and configuration, making failures reproducible. Walk the scenarios
in order: the decline path and the timeout path share compensation behavior but differ in how the
application learns the payment failed, and running them adjacently is what surfaces a handler that
only covers one.

Full matrix: `failure-simulation.md` §38.

---

# 57. Local Development Lifecycle

A new developer, from nothing:

```bash
git clone <repository-url>
cd ecommerce

make bootstrap        # install → migrate → seed

uv run uvicorn ecommerce.main:app --reload
```

Then, in another terminal:

```bash
uv run pytest
```

`make bootstrap` is the whole setup. If you prefer the explicit form:

```bash
uv sync
cp .env.example .env      # optional
mkdir -p data             # created automatically
uv run alembic upgrade head
uv run python -m ecommerce.seed --profile development --seed 42
uv run uvicorn ecommerce.main:app --reload
```

---

# 58. Clean Development Reset

The full procedure — generator `--reset`, `make db-reset`, and the explicit three-command form — is
in **§23**. This section is the quick-reference entry point for it.

```bash
make db-reset
```

Prefer `make reset` (generator `--reset`) when the schema is not the suspect: it is faster and
cannot mask a broken migration.

---

# 59. Recommended Daily Workflow

Start of day:

```bash
uv sync
uv run alembic upgrade head
```

While working:

```bash
uv run pytest tests/unit -q      # fast inner loop, no database
```

Before committing:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```

---

# 60. Development Rules

1. Use `uv` for dependency management; commit `uv.lock`.
2. Use Alembic for schema changes; never `create_all()`.
3. Keep domain logic framework-independent.
4. Keep API routes thin.
5. Keep repositories behind domain-owned interfaces.
6. Use synthetic data only — `example.invalid`, `faketxn_` prefixes.
7. Keep tests deterministic: injected `Clock`/`IdGenerator`/`RandomSource`, no `datetime.now()`.
8. Keep failure simulation disabled by default.
9. Add tests with every feature.
10. Update documentation when behavior changes.
11. Do not commit `.env`.
12. Do not commit local SQLite databases — ignore `data/` wholesale.
13. Do not introduce unnecessary infrastructure.
14. Prefer simple solutions over premature abstraction.
15. Record conflicts explicitly; never resolve a contradiction silently (the constitution's *Explicit Business Rules*, III).

## 60.1 Code Conventions

Rules §1–§15 govern process. These govern the code itself:

* Type hints on all public functions and methods — enforced by `mypy` (§29).
* Small functions with one responsibility. A function that needs a comment to explain its second half
  is two functions.
* Explicit dependency injection. No service locators, no module-level mutable state — tests must be
  able to substitute a `Clock` or `Repository` without monkey-patching.
* Business logic lives in the domain and application layers and is written **once**. Two endpoints
  computing the same total differently is the defect this rule exists to prevent.
* No magic values. Prices, timeouts, and state names are named constants or domain enumerations.
* Comments explain *why*, not *what*. The code already says what it does.
* Formatting and linting are the tools' decision, not a reviewer's (§28–§30).

These are defaults, not absolutes. A deviation is fine when it is justified in the pull request —
an undocumented deviation is not.

---

# 61. Common Commands

### Install

```bash
uv sync                    # make install
```

### Run server

```bash
uv run uvicorn ecommerce.main:app --reload    # make run
```

### Migrate

```bash
uv run alembic upgrade head                   # make migrate
```

### Create migration

```bash
uv run alembic revision --autogenerate -m "description"   # make migrate-new m="..."
```

### Seed

```bash
uv run python -m ecommerce.seed --profile development --seed 42        # make seed
uv run python -m ecommerce.seed --profile test --seed 42 --reset       # make seed-test
uv run python -m ecommerce.seed --profile large --seed 42 --reset      # make seed-large
uv run python -m ecommerce.seed --products 500 --customers 100 --seed 42
```

### Reset

```bash
uv run python -m ecommerce.seed --profile development --seed 42 --reset  # make reset
make db-reset                                                            # drop + migrate + seed
```

### Test

```bash
uv run pytest                     # make test
uv run pytest tests/unit          # make test-unit
uv run pytest --cov=src/ecommerce --cov-report=term-missing   # make coverage
```

### Lint / format / typecheck

```bash
uv run ruff check .               # make lint
uv run ruff format .              # make format
uv run mypy src                   # make typecheck
```

---

# 62. Development Completion Criteria

The development workflow is complete when a new developer can:

* Install the project with `uv`.
* Create local configuration.
* Apply database migrations.
* Generate synthetic data.
* Start the FastAPI server.
* Access the API and its OpenAPI docs.
* Run the full test suite, and each tier independently.
* Run linting, formatting checks, and type checking.
* Reset the local database.
* Reproduce a deterministic dataset from a seed.
* Enable a specific, deterministic failure scenario.
* Locate a defect by layer using §43–§46.
* Understand the Spec Kit workflow and the 11-feature roadmap.
* Know which decisions are settled and which are deferred.

---

# 63. Final Development Workflow

```text
                    Requirement
                         │
                         ▼
                   Spec Kit Spec
                         │
                         ▼
                    Design / Plan
                         │
                         ▼
                    Write Tests
                         │
                         ▼
                 Domain Implementation
                         │
                         ▼
              Application Implementation
                         │
                         ▼
                Infrastructure / DB
                         │
                         ▼
                     API Layer
                         │
                         ▼
                 Integration Testing
                         │
                         ▼
                    API Testing
                         │
                         ▼
              Lint / Format / Typecheck
                         │
                         ▼
                   Documentation
                         │
                         ▼
                       Commit
```

The project remains a modular monolith throughout the initial development phases. Microservices,
distributed messaging, CQRS, event sourcing, and external infrastructure should be introduced only
when a concrete requirement justifies it — and for a fake server, none has yet.
