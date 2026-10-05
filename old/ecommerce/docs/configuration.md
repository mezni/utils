# Configuration

## Fake E-Commerce Server

> **Status:** Draft
> **Authority:** `.specify/memory/constitution.md` v1.1.1, *Security and Observability*
> **Loader:** `pydantic-settings` in `src/ecommerce/config/settings.py`

Configuration is **externalized**. Environment variables carry environment-specific settings, so the
same checkout behaves correctly in development, test, and production without code changes.

Secrets MUST NOT be committed. `.env.example` documents every variable and contains no real secret —
and this project has no real secret to contain, because every credential is synthetic by design.

---

## Divergences from other documents

Reconciled against the constitution, `architecture.md`, `database.md`, `API.md`, `generator.md`, and
`development.md`. Every conflict is recorded and resolved explicitly (the constitution's *Explicit Business Rules*, III).

| # | Divergence in the incoming draft | Resolution | Now in |
|---|---|---|---|
| G1 | `GENERATOR_PROFILE`, `GENERATOR_SEED`, `GENERATOR_CATEGORIES`, `GENERATOR_PRODUCTS`, `GENERATOR_CUSTOMERS`, `GENERATOR_CARTS`, `GENERATOR_ORDERS`. | **Resolved** — `RANDOM_SEED` and `SEED_PROFILE`. Per-entity counts are CLI flags, not environment variables. Matches `generator.md` F10. | §21, §22 |
| G2 | `DEFAULT_CURRENCY=CAD`. | **Resolved** — `USD` (`database.md` D4). | §27 |
| G3 | `DATABASE_URL=sqlite:///…` (synchronous driver). | **Resolved** — `sqlite+aiosqlite:///…`. The async driver is already published throughout the corpus. | §14 |
| G4 | `API_PREFIX` instead of `API_V1_PREFIX`. | **Resolved** — `API_V1_PREFIX`, as published. | §9, §11 |
| G5 | `AUTH_ENABLED=false` as the development and test default. | **Resolved** — `true`. Auth on by default keeps the permission boundary real; the fake tokens make it free. | §24, §28.1, §28.2 |
| G6 | `SQLITE_FOREIGN_KEYS=true` as a configurable setting. | **Resolved** — **not configurable.** | §15, §42.1 |
| G7 | `GET /health/ready` as the readiness path. | **Resolved** — `GET /ready`, as published in `API.md` §25.2. | §26 |
| G8 | `HEALTH_ENABLED` as a configurable setting. | **Resolved** — not configurable. | §26, §42.1 |
| G9 | `REQUEST_ID_HEADER` as a configurable setting. | **Resolved** — not configurable. `X-Request-ID` is part of the published contract (`API.md` §8.1). | §18, §42.1 |
| G10 | `FAKE_LATENCY_ENABLED` as a second master switch. | **Resolved** — not adopted. `FAKE_FAILURE_ENABLED` is the single master switch for all simulation. | §23, §23.1 |
| G11 | `FAKE_FAILURE_SEED` as a separate seed. | **Resolved** — not adopted. `RANDOM_SEED` already governs all determinism; a second seed creates a "which one wins?" question. | §21, §41 |
| G12 | `APP_TIMEZONE=UTC` as a configurable setting. | **Resolved** — dropped. Timestamps are stored as ISO-8601 UTC text (`database.md` §9); there is nothing to configure. `FAKE_CLOCK` is restored instead — it was **missing entirely**, and three documents depend on it. | §21, §42.1 |
| G13 | `DEFAULT_LOCALE=en_CA`. | **Resolved** — `en_US`, consistent with the USD default (G2) and the published example addresses. | §27 |
| G14 | Test database at `data/test.db`. | **Resolved** — `./.tmp/test.db`, and `APP_ENV=test` **refuses to start** against a non-test database. | §14.2, §28.2 |
| G15 | "Production" example reuses the development SQLite path. | **Resolved** — DSN comes from the environment and is never committed. | §14.3, §28.3 |
| G16 | Drops the "production" hard guards and the startup validation table. | **Resolved** — both restored. | §28.3, §29 |
| G17 | Adds `PAYMENT_DECLINE_RATE`, `PAYMENT_TIMEOUT_RATE`, `PAYMENT_ERROR_RATE`, `PAYMENT_TIMEOUT_MS`, `PAYMENT_PROVIDER`. | **Recorded** — reasonable, but they introduce a second randomization mechanism alongside `FAKE_FAILURE_RATE`. Deferred to `specs/009-failure-simulation`. | §23.1 |

**This draft's genuinely new material** — `APP_NAME`, `APP_VERSION`, `DEBUG`, `HOST`/`PORT`,
`API_DOCS_ENABLED`/`API_REDOC_ENABLED`, `CORS_ENABLED`, `OBSERVABILITY_ENABLED`, `METRICS_ENABLED`,
`TRACING_ENABLED`, `SQLITE_BUSY_TIMEOUT_MS`, `AUTH_PROVIDER`, `AUTH_DEFAULT_ROLE`, the nested settings
model, configuration immutability, `data/` directory creation, the Alembic configuration-source rule,
Docker, and CI — is retained. See §42 for where each is filed.

---

# 1. Purpose

This document defines the configuration model for the Fake E-Commerce Server.

Configuration controls:

* Application runtime and environment selection
* The SQLite database
* API behavior
* Logging and request correlation
* Determinism (seed and clock)
* Failure simulation
* Authentication and authorization
* Observability
* The fake-data generator

Configuration is **centralized, validated, typed, and environment-aware**. It is part of the contract,
not an implementation detail (§7).

---

# 2. Configuration Principles

1. Configuration is externalized from application code.
2. Environment variables are the primary runtime mechanism.
3. `pydantic-settings` validates at startup.
4. Secrets are never hard-coded — and this project has no real secrets.
5. **Domain code never reads environment variables.** Only the composition root does.
6. Configuration is injected into application and infrastructure components.
7. `development`, `test`, and `production` are explicit, documented profiles.
8. Safe defaults are preferred over required values.
9. Failure simulation is off by default.
10. Generator configuration is separate from server runtime configuration, but shares
    `DATABASE_URL` (`docs/generator.md` §59).
11. Invalid configuration **fails fast at startup**, never on first use.
12. Sensitive values never appear in logs, health responses, or API errors.
13. Configuration is immutable once startup completes (§15).

---

# 3. Configuration Architecture

```text
Environment variables / .env
        │
        ▼
pydantic-settings  →  Settings  (validated once, at startup)
        │
        ▼
┌───────────────┬───────────────┬──────────────────┐
│ AppSettings   │ DatabaseSettings│ LoggingSettings │
│ ApiSettings   │ GeneratorSettings│ FailureSettings│
│ AuthSettings  │ ObservabilitySettings              │
└───────────────┴───────────────┴──────────────────┘
        │
        ▼
Composition root (main.py)
        │
        ▼
Application services / repositories / providers
```

The domain layer has no access to configuration. A domain entity that reads an environment variable
cannot be constructed deterministically in a test, which is the whole reason the layer boundary
exists.

---

# 4. Configuration Module

```text
src/ecommerce/config/
├── __init__.py
└── settings.py
```

```python
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    app_env: AppEnv = AppEnv.DEVELOPMENT
    log_level: LogLevel = LogLevel.INFO
    # …
```

The model is **nested logically** (§13) while the **environment variable names stay flat** (§15).
That combination is deliberate: grouping in the code makes the settings object navigable, and flat
names keep `.env` greppable and free of prefix conventions nobody remembers.

---

# 5. Required Dependency

```bash
uv add pydantic-settings
```

Managed through `uv`; the lockfile is committed.

---

# 6. Environment Files

```text
.env          # local, git-ignored
.env.example  # committed, no real values
```

`.env` is read after environment variables, so an exported variable always wins over the file
(§8) — which is what makes CI and one-off test runs work without editing a file.

---

# 7. Environment Names

```text
development
test
production
```

```env
APP_ENV=development
```

An unrecognized value **fails validation at startup** rather than falling back to a default. A typo
in `APP_ENV` that silently yields `development` means `DEBUG=true` and a dev database in front of
something that believed it was in production.

> "production" here means *production-shaped configuration for realistic deployments and client
> testing*, not a real commercial deployment. This is a fake server.

---

# 8. Configuration Precedence

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

```bash
uv run python -m ecommerce.seed --seed 7
```

`--seed 7` beats `RANDOM_SEED=42`. The effective value is echoed in the generator summary so a run is
reproducible from its own recorded output.

---

# 9. Core Application Configuration

| Variable | Type | Default | Purpose |
|---|---|---|---|
| `APP_ENV` | enum | `development` | `development` \| `test` \| `production` |
| `LOG_LEVEL` | enum | `INFO` | `DEBUG` \| `INFO` \| `WARNING` \| `ERROR` \| `CRITICAL` |
| `LOG_FORMAT` | enum | `text` | `text` (dev) \| `json` (prod) |
| `RANDOM_SEED` | int | `42` | Default seed for reproducible data |
| `FAKE_FAILURE_RATE` | float | `0.0` | Probability of **randomized** failure injection, `0.0`–`1.0` |
| `FAKE_LATENCY_MS` | int | `0` | Artificial latency added per request |
| `FAKE_FAILURE_ENABLED` | bool | `false` | **Master switch** for all failure simulation |
| `FAKE_CLOCK` | ISO-8601 str | *(unset)* | Fixed instant for deterministic time |
| `AUTH_ENABLED` | bool | `true` | Enforce authentication/authorization |
| `CORS_ORIGINS` | list | `[]` | Allowed origins |
| `API_V1_PREFIX` | str | `/api/v1` | Route prefix |
| `DATABASE_URL` | str | `sqlite+aiosqlite:///./data/ecommerce.db` | SQLAlchemy DSN |
| `SEED_PROFILE` | enum | `development` | Default profile when `--profile` is omitted |

Settings are read **once** at startup. Invalid values fail fast rather than at first use.

## 9.1 The critical ones

```bash
DATABASE_URL=sqlite+aiosqlite:///./data/ecommerce.db
APP_ENV=development
LOG_LEVEL=INFO
RANDOM_SEED=42
FAKE_FAILURE_ENABLED=false
FAKE_FAILURE_RATE=0.0
FAKE_LATENCY_MS=0
```

---

# 10. Application Identity

| Variable | Type | Default | Purpose |
|---|---|---|---|
| `APP_NAME` | str | `Fake E-Commerce Server` | FastAPI metadata, logs, `/ready` |
| `APP_VERSION` | str | `0.1.0` | `/ready` payload; recorded in the generator summary |
| `DEBUG` | bool | `false` | Verbose errors, auto-reload |

```env
APP_NAME=Fake E-Commerce Server
APP_VERSION=0.1.0
DEBUG=true
```

`APP_VERSION` participates in the determinism contract (`docs/generator.md` §12.2): regenerating with
a different version MAY legitimately produce different data, so the summary records it.

`DEBUG=true` **MUST NOT** expose sensitive information. It enables verbose logging and interactive
docs — never a stack trace in an API response (`API.md` §12).

---

# 11. API Configuration

| Variable | Type | Default | Purpose |
|---|---|---|---|
| `API_V1_PREFIX` | str | `/api/v1` | Route prefix for all versioned routes |
| `API_DOCS_ENABLED` | bool | `true` | Serve `/docs` and `/openapi.json` |
| `API_REDOC_ENABLED` | bool | `true` | Serve `/redoc` |
| `CORS_ENABLED` | bool | `false` | Apply CORS middleware |
| `CORS_ORIGINS` | list | `[]` | Allowed origins |

```env
API_V1_PREFIX=/api/v1
API_DOCS_ENABLED=true
API_REDOC_ENABLED=true
CORS_ENABLED=true
CORS_ORIGINS=["http://localhost:3000","http://localhost:5173"]
```

CORS is **off by default** and, when enabled, requires explicit origins. `"*"` is never the default:
the server holds synthetic-but-realistic PII-shaped data, and a wildcard origin on a local server is
how that data ends up on a page it should not be on.

API_DOCS_ENABLED=false in production (§28.3) hides /docs and /openapi.json, as it is a convenience switch, not a security control.

---

# 12. Host and Port

| Variable | Type | Default | Purpose |
|---|---|---|---|
| `HOST` | str | `127.0.0.1` | Bind address |
| `PORT` | int | `8000` | Bind port |

```env
HOST=127.0.0.1
PORT=8000
```

`127.0.0.1` by default, not `0.0.0.0`. A fake-server bound to all interfaces on a laptop or shared
host is reachable by every device on the network, and its auth boundary is fake.

---

# 13. Settings Model Shape

```python
class Settings(BaseSettings):
    app: AppSettings
    api: ApiSettings
    database: DatabaseSettings
    logging: LoggingSettings
    determinism: DeterminismSettings
    generator: GeneratorSettings
    failure: FailureSettings
    auth: AuthSettings
    observability: ObservabilitySettings
```

```python
class AppSettings(BaseModel):
    name: str = "Fake E-Commerce Server"
    env: AppEnv = AppEnv.DEVELOPMENT
    version: str = "0.1.0"
    debug: bool = False


class DatabaseSettings(BaseModel):
    url: str = "sqlite+aiosqlite:///./data/ecommerce.db"
    sqlite_busy_timeout_ms: int = 5000


class ApiSettings(BaseModel):
    v1_prefix: str = "/api/v1"
    docs_enabled: bool = True
    redoc_enabled: bool = True
    cors_enabled: bool = False
    cors_origins: list[str] = []


class DeterminismSettings(BaseModel):
    seed: int = 42
    clock: datetime | None = None  # FAKE_CLOCK
```

Grouping by concern means a reader looking for "how do I control time" finds one section rather than
grepping. The flat environment names in §15 preserve greppability in `.env`.

---

# 14. Database Configuration

| Variable | Type | Default | Purpose |
|---|---|---|---|
| `DATABASE_URL` | str | `sqlite+aiosqlite:///./data/ecommerce.db` | SQLAlchemy async DSN |
| `SQLITE_BUSY_TIMEOUT_MS` | int | `5000` | How long SQLite waits for a lock before `SQLITE_BUSY` |

```env
DATABASE_URL=sqlite+aiosqlite:///./data/ecommerce.db
SQLITE_BUSY_TIMEOUT_MS=5000
```

The **async** driver is used throughout, so the DSN is `sqlite+aiosqlite:///`, not `sqlite:///`
(divergence G3). A synchronous driver here would block the event loop on every query.

### 14.1 Database path

The default database is `data/ecommerce.db`, and the `data/` directory MUST exist before the engine
opens a file there (§19).

### 14.2 Test database

Tests MUST use a dedicated database:

```env
APP_ENV=test
DATABASE_URL=sqlite+aiosqlite:///./.tmp/test.db
```

An in-memory database (`sqlite+aiosqlite:///:memory:`) is also valid, with the connection-sharing
caveat in `docs/database.md` §53.

`APP_ENV=test` **refuses to start** if `DATABASE_URL` points at the development database. A test suite
that silently targets a developer's real data is a data-loss incident that looks like a passing build.

### 14.3 "Production"

`APP_ENV=production` takes `DATABASE_URL` from the environment. It is never committed, and never
hard-coded to a path. The database boundary is isolated (`docs/database.md`) so a future PostgreSQL
migration does not touch domain logic.

---

# 15. SQLite Configuration

| Setting | Value | Configurable? |
|---|---|---|
| `PRAGMA foreign_keys` | `ON` | **No** — see below |
| `PRAGMA journal_mode` | `WAL` | **No** |
| `PRAGMA busy_timeout` | `SQLITE_BUSY_TIMEOUT_MS` | Yes |
| Connection pooling | SQLAlchemy default | Not PostgreSQL-tuned |

**Foreign-key enforcement is not configurable** (divergence G6). `PRAGMA foreign_keys = ON` MUST be
set on **every** connection, per `docs/database.md` §34. A `SQLITE_FOREIGN_KEYS=false` setting would
be a switch that silently disables referential integrity and every `ON DELETE CASCADE` in the schema,
in exchange for nothing — no legitimate reason exists to run this schema without FK enforcement.

`WAL` is likewise not configurable, because read-during-write behavior is a property of the engine
setup rather than a deployment preference.

`SQLITE_BUSY_TIMEOUT_MS` **is** configurable, because the right value genuinely depends on whether
the server is running alongside a seed process.

---

# 16. Alembic Configuration

Alembic reads the **same** `DATABASE_URL` as the application:

```bash
uv run alembic upgrade head
```

The schema MUST NOT be created with `Base.metadata.create_all()` outside a throwaway test database
(`docs/database.md` §41).

Alembic must never target a different database than the application (§20).

---

# 17. Logging Configuration

| Variable | Type | Default | Purpose |
|---|---|---|---|
| `LOG_LEVEL` | enum | `INFO` | Minimum level emitted |
| `LOG_FORMAT` | enum | `text` | `text` (human) \| `json` (structured) |

```text
DEBUG  INFO  WARNING  ERROR  CRITICAL
```

### 17.1 Development

```env
LOG_LEVEL=DEBUG
LOG_FORMAT=text
```

```text
2026-09-25T20:00:01Z INFO request completed method=GET path=/api/v1/products status=200
```

### 17.2 Production

```env
LOG_LEVEL=INFO
LOG_FORMAT=json
```

Structured fields:

```text
timestamp  level  request_id  method  path
status_code  duration_ms  event  error_code
```

`request_id` is what makes a request traceable across the log stream, and it is the same value the
client sees in the `X-Request-ID` header and the error envelope (`API.md` §8.1).

### 17.3 Redaction

The application MUST NOT log:

* Passwords
* API keys
* Authentication tokens
* `Authorization` header values
* Payment credentials
* Database credentials
* Full customer records

Sensitive values are redacted at the logging boundary. A config value is marked sensitive in
`settings.py`, and the logging formatter redacts it by key — redaction by convention fails the first
time someone logs a whole settings object.

---

# 18. Request Correlation

The correlation header is **not configurable** (divergence G9). Middleware:

1. Reads `X-Request-ID` if supplied.
2. Generates one if absent.
3. Attaches it to the request context.
4. Echoes it on the response.
5. Includes it in the error envelope and in every log line.

`X-Request-ID` is part of the published contract (`API.md` §8.1). A configurable header name means a
client's correlation header silently stops working.

---

# 19. Directory Initialization

Before the engine opens `data/ecommerce.db`, `data/` MUST exist. Directory creation belongs to
infrastructure startup code, **not** the domain — and not to a migration.

---

# 20. Configuration and Alembic

Alembic obtains `DATABASE_URL` from the same settings object as the application, so the following can
never happen:

```text
Application → database A
Alembic     → database B
```

The migration target and the runtime target are the same value, read from the same place. A migration
applied to the wrong database is one of the few failures that is both silent and unrecoverable.

---

# 21. Determinism Configuration

| Variable | Type | Default | Purpose |
|---|---|---|---|
| `RANDOM_SEED` | int | `42` | Seeds the generator RNG **and** randomized failure injection |
| `FAKE_CLOCK` | ISO-8601 str | *(unset)* | Fixed instant; unset means real time |
| `SEED_PROFILE` | enum | `development` | Generator profile when `--profile` is omitted |

```env
RANDOM_SEED=42
FAKE_CLOCK=2026-01-01T00:00:00Z
```

One seed governs all determinism (divergence G11). A separate `FAKE_FAILURE_SEED` would mean two
seeds, and the question of which wins when they disagree — with a test that passes locally and fails
in CI depending on the answer.

`FAKE_CLOCK` is what makes timestamped behavior reproducible: order dates, token expiry, and
"created in the last 24 hours" filters all become assertable. It is a **test affordance** and is unset
in development.

Timestamps are stored as ISO-8601 UTC text. There is no timezone setting, because there is nothing to
configure: everything is UTC, and a variable named `APP_TIMEZONE` invites someone to set it to
something the storage format cannot represent (divergence G12).

---

# 22. Generator Configuration

The generator shares `DATABASE_URL` and `RANDOM_SEED` with the application, and owns exactly one
setting of its own:

```env
SEED_PROFILE=development
```

Per-entity counts are **CLI flags**, not environment variables (divergence G1):

```bash
uv run python -m ecommerce.seed --categories 10 --products 100 --customers 50 --orders 100
```

**This draft's `GENERATOR_CATEGORIES=10` style is not adopted.** Seven `GENERATOR_*` variables for
counts that are almost always overridden on the command line is a second place to look, and a second
place to forget. A user who exports `GENERATOR_PRODUCTS=500` and runs `--profile development` gets 200
products and no warning.

Full precedence: `docs/generator.md` §46.

---

# 23. Failure Simulation Configuration

| Variable | Type | Default | Purpose |
|---|---|---|---|
| `FAKE_FAILURE_ENABLED` | bool | `false` | **Master switch.** `false` ignores everything, including `X-Fake-Failure` |
| `FAKE_FAILURE_RATE` | float | `0.0` | Probability of **randomized** injection, `0.0`–`1.0` |
| `FAKE_LATENCY_MS` | int | `0` | Artificial latency per request |

```env
FAKE_FAILURE_ENABLED=false
FAKE_FAILURE_RATE=0.0
FAKE_LATENCY_MS=0
```

### 23.1 The three-way interaction

| `FAKE_FAILURE_ENABLED` | `FAKE_FAILURE_RATE` | Behavior |
|---|---|---|
| `false` | any | All simulation off. `X-Fake-Failure` is **ignored**. |
| `true` | `0.0` | Only **deterministic**, explicitly requested failures via `X-Fake-Failure`. Recommended even when enabled. |
| `true` | `> 0.0` | Adds **randomized** injection for resilience testing. |

Deterministic-first is deliberate: a test asserting "this request fails" must never lose a coin flip.
Randomized injection is for soak testing only.

`FAKE_LATENCY_MS` is covered by the same master switch (divergence G10). A separate
`FAKE_LATENCY_ENABLED` would permit latency with simulation nominally off, contradicting the table
above — and "latency that nobody turned on" is a very slow test suite.

`FAKE_LATENCY_MS >= 0`; `FAKE_FAILURE_RATE` in `[0.0, 1.0]`. Both are validated at startup.

Full behavior: `docs/failure-simulation.md`.

---

# 24. Authentication Configuration

| Variable | Type | Default | Purpose |
|---|---|---|---|
| `AUTH_ENABLED` | bool | `true` | Enforce authentication and authorization |
| `AUTH_PROVIDER` | enum | `fake` | Only `fake` is supported |
| `AUTH_FAKE_TOKEN_SECRET` | str | *(none — synthetic)* | Signs fake tokens |
| `AUTH_DEFAULT_ROLE` | enum | `customer` | Role for a token with no explicit role |

```env
AUTH_ENABLED=true
AUTH_PROVIDER=fake
AUTH_DEFAULT_ROLE=customer
```

**`AUTH_ENABLED` defaults to `true`** (divergence G5), in every environment including development and
test. The permission boundary is the point of the project; leaving it off by default means the common
case is an API where `403` never happens, and the authorization logic is only exercised in the one
suite that remembers to turn it on. Fake tokens cost nothing, and the corpus publishes
`Authorization: Bearer <fake-token>` (`API.md` §30).

`AUTH_ENABLED=false` exists for tests that specifically need an unauthenticated path, and for a
quick manual poke at a running server. It is a deliberate exception, not a default.

Authorization logic lives in the application/API boundary, never in a route body
(`API.md` §31).

---

# 25. Observability Configuration

| Variable | Type | Default | Purpose |
|---|---|---|---|
| `OBSERVABILITY_ENABLED` | bool | `true` | Master switch for request correlation and access logs |
| `METRICS_ENABLED` | bool | `false` | Reserved |
| `TRACING_ENABLED` | bool | `false` | Reserved |

```env
OBSERVABILITY_ENABLED=true
METRICS_ENABLED=false
TRACING_ENABLED=false
```

The project focuses on structured logging and request correlation. Metrics and tracing are reserved
and off — an enabled flag with no implementation behind it produces empty dashboards and false
confidence.

---

# 26. Health Configuration

Whether health endpoints exist is **not configurable** (divergence G8). A client-facing deployment
that can turn its health endpoint off will eventually turn it off somewhere it matters.

```http
GET /health
GET /ready
```

| Endpoint | Required | Database access | Payload |
|---|---|---|---|
| `GET /health` | **Yes** | None | `{ "status": "ok" }` |
| `GET /ready` | No — planned (E19) | Connectivity check | `{ "status": "ok", "version": "…", "database": "ok" }` |

`/health` reports process liveness for a load balancer and MUST NOT touch the database — a liveness
probe that fails when the database is slow will restart a healthy process.

`/ready` checks database connectivity and is **planned, not required for the initial implementation**
(`API.md` §25.2). It is listed here because `APP_VERSION` feeds its `version` field; the absence of
`/ready` in an early build is a deliberate scope decision, not a misconfiguration.

The `version` value in the `/ready` example in `API.md` §25.2 is **illustrative**. The field is
populated from `APP_VERSION` (§10), so the two documents must not be read as pinning the same literal.

Neither endpoint is under `API_V1_PREFIX` — a probe must not need to know the API version
(`API.md` §25). This draft's `GET /health/ready` is not adopted (G7), nor is a versioned
`/api/v1/health` (E19).

---

# 27. Currency and Locale

| Variable | Type | Default | Purpose |
|---|---|---|---|
| `DEFAULT_CURRENCY` | str | `USD` | Currency for generated data |
| `DEFAULT_LOCALE` | str | `en_US` | Faker locale |

`DEFAULT_CURRENCY=CAD` is not adopted (divergence G2). `USD` is the currency published in every
example across `API.md`, `database.md`, and `generator.md`.

`DEFAULT_LOCALE=en_CA` is not adopted (G13): it contradicts the USD default and the published example
addresses (`US` / `Springfield`). Locale affects generated names, addresses, and phone formats, and a
mismatched locale is a quiet source of data that looks wrong without being invalid.

Currency is stored **per row**, not read from configuration at display time — a product's currency is
part of its data (`database.md` §14). `DEFAULT_CURRENCY` only tells the generator what to write.

---

# 28. Environment Differences

### 28.1 `development`

| Setting | Value | Rationale |
|---|---|---|
| `APP_ENV` | `development` | |
| `DEBUG` | `true` | Verbose; shows SQLAlchemy queries if enabled |
| `LOG_LEVEL` | `DEBUG` | |
| `LOG_FORMAT` | `text` | Human-readable |
| `FAKE_FAILURE_ENABLED` | `false` | Never surprise a developer mid-task |
| `FAKE_FAILURE_RATE` | `0.0` | No random failures |
| `FAKE_LATENCY_MS` | `0` | No artificial slowness |
| `FAKE_CLOCK` | *(unset)* | Real time |
| `AUTH_ENABLED` | `true` | Realistic boundary, easy fake credentials |
| `API_DOCS_ENABLED` | `true` | Interactive exploration |
| `DATABASE_URL` | `sqlite+aiosqlite:///./data/ecommerce.db` | Disposable dev file |
| Reload | enabled | `uvicorn --reload` |

The dev database is disposable. If it is corrupted: `make db-reset`.

### 28.2 `test`

| Setting | Value | Rationale |
|---|---|---|
| `APP_ENV` | `test` | |
| `DEBUG` | `false` | |
| `LOG_LEVEL` | `WARNING` | Keeps test output readable |
| `LOG_FORMAT` | `text` | |
| `FAKE_FAILURE_ENABLED` | `false` | Tests opt in explicitly |
| `FAKE_FAILURE_RATE` | `0.0` | Determinism over realism |
| `FAKE_LATENCY_MS` | `0` | No sleeping in tests |
| `FAKE_CLOCK` | fixed instant | Deterministic timestamps |
| `AUTH_ENABLED` | `true` | Auth paths are tested explicitly |
| `RANDOM_SEED` | fixed (e.g. `42`) | Reproducible fixtures |
| `DATABASE_URL` | `./.tmp/test.db` | **Never** the dev database |

```bash
APP_ENV=test
DATABASE_URL=sqlite+aiosqlite:///./.tmp/test.db
RANDOM_SEED=42
FAKE_CLOCK=2026-01-01T00:00:00Z
```

`APP_ENV=test` MUST NOT read ambient developer configuration, and MUST refuse to start against a
non-test database (§6).

### 28.3 `production`

| Setting | Value | Rationale |
|---|---|---|
| `APP_ENV` | `production` | |
| `DEBUG` | `false` | |
| `LOG_LEVEL` | `INFO` | No debug noise |
| `LOG_FORMAT` | `json` | Machine-parseable for aggregation |
| `FAKE_FAILURE_ENABLED` | `false` | Never inject failures |
| `FAKE_FAILURE_RATE` | `0.0` | |
| `FAKE_LATENCY_MS` | `0` | |
| `FAKE_CLOCK` | *(unset)* | |
| `AUTH_ENABLED` | `true` | Mandatory |
| `API_DOCS_ENABLED` | `false` | `/docs` and `/openapi.json` off |
| `DATABASE_URL` | from environment | Never committed |

**Hard guards:** with `APP_ENV=production` the application **refuses to start** if
`FAKE_FAILURE_ENABLED=true` or `FAKE_FAILURE_RATE > 0`. Simulated failure is a test affordance and
MUST NOT leak into a production-shaped environment, where it would look like an unreliable service
rather than a deliberate fault.

---

# 29. Validation at Startup

| Rule | Failure mode |
|---|---|
| `APP_ENV` in the allowed set | Startup error listing valid values |
| `LOG_LEVEL` in the allowed set | Startup error |
| `LOG_FORMAT` in the allowed set | Startup error |
| `FAKE_FAILURE_RATE` in `[0.0, 1.0]` | Startup error |
| `FAKE_LATENCY_MS >= 0` | Startup error |
| `SQLITE_BUSY_TIMEOUT_MS >= 0` | Startup error |
| `PORT` in `1..65535` | Startup error |
| `DEFAULT_CURRENCY` is 3 uppercase letters | Startup error |
| `DATABASE_URL` parses as a SQLAlchemy URL | Startup error |
| `APP_ENV=production` + failure simulation enabled | **Refuse to start** |
| `APP_ENV=test` + development database URL | **Refuse to start** |
| `data/` directory creatable | Startup error |

Failing at startup is always preferable to failing on the first request that happens to need the
value — and a startup failure is visible to whoever started the process, whereas a request failure
is visible to whoever happened to make the request.

---

# 30. Startup Sequence

```text
Load Settings
    ↓
Validate configuration          (§29)
    ↓
Ensure data/ exists             (§19)
    ↓
Build engine + session factory
    ↓
Verify database connectivity
    ↓
Verify schema is current (alembic head)
    ↓
Build repositories, UoW, providers, services
    ↓
Serve
```

A schema-version mismatch is a **startup error**, not a runtime surprise. The application never
migrates on boot (`docs/database.md` §41); `make migrate` is a separate, explicit step.

---

# 31. Adding a Variable

1. Add the field to `src/ecommerce/config/settings.py` with a type and a documented default.
2. Document it in this file **and** in `.env.example`.
3. Note any environment-specific difference in §28.
4. If it affects the generator, document it in `docs/generator.md`.
5. If it affects failure simulation, document it in `docs/failure-simulation.md` and update §23.1.
6. Mark it sensitive if it is, so the logging formatter redacts it (§17.3).
7. Add a startup validation rule if an invalid value is possible (§29).

Configuration that changes behavior MUST be documented. It is part of the contract.

---

# 32. Access Rule

Components receive configuration through dependency injection:

```text
Settings → Composition root → Service / Provider
```

**Never** this, inside a domain or application module:

```python
import os

API_KEY = os.getenv("API_KEY")
```

Environment access is centralized in the settings module. A module that reads `os.environ` cannot be
constructed with an explicit configuration in a test, which is what makes configuration injection
worth the indirection.

---

# 33. Composition Root

`src/ecommerce/main.py` builds:

```text
Settings
Database engine
Session factory
Repositories
Unit of Work
PaymentProvider
Failure-injection middleware
Application services
FastAPI dependencies
```

This is the **only** place where configuration meets concrete implementations. Everything else
receives what it needs.

---

# 34. Configuration Immutability

After startup completes, settings are **immutable**. Runtime code MUST NOT mutate global settings.

If runtime reconfiguration is ever needed, it goes through an explicit mechanism with its own
validation — not by assigning to a settings field. Mutable global configuration turns "what was
configured when this request ran?" into a question with no stable answer, which is precisely what
makes a failure unreproducible.

---

# 35. Configuration and Testing

Tests override settings explicitly:

```python
settings = Settings(
    app_env="test", database=DatabaseSettings(url="sqlite+aiosqlite:///:memory:")
)
```

FastAPI dependency overrides work for application services. Tests MUST NOT depend on a developer's
local `.env` — which is why `APP_ENV=test` ignores ambient configuration entirely.

---

# 36. Secrets

If a future feature introduces a secret, it is supplied externally:

```text
AUTH_FAKE_TOKEN_SECRET
EXTERNAL_API_KEY
```

Rules, unchanged from §28.3 and §17.3:

* Never committed
* Never in source code
* Never printed in logs
* Never returned by a health endpoint
* Never present in an API error response
* Never in `.env.example` with a real value

`.env.example` contains placeholders only. This project's fake secret is commented out, because
committing even a fake-looking secret trains the habit of uncommenting it.

---

# 37. `.gitignore`

```gitignore
.env
.venv/
data/
.tmp/
*.db
*.db-wal
*.db-shm
*.sqlite
__pycache__/
.pytest_cache/
.mypy_cache/
.ruff_cache/
```

`data/` is ignored wholesale rather than pattern-by-pattern: SQLite also creates `-wal` and `-shm`
sidecar files (`docs/database.md` §67), and a pattern list that misses them commits a corrupt
database file.

`.env.example` is **committed**.

---

# 38. `.env.example`

This is the committed **development** baseline: it sets `APP_ENV=development` and `DEBUG=true`, so
its values track the `development` profile in §28.1. A setting that differs between the two is a
defect in one of them, not a deliberate override.

```bash
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
LOG_LEVEL=DEBUG
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
METRICS_ENABLED=false
TRACING_ENABLED=false

# --- Data defaults ---
DEFAULT_CURRENCY=USD
DEFAULT_LOCALE=en_US
```

---

# 39. Docker

Configuration remains environment variables, so no Docker-specific configuration code is required:

```yaml
environment:
  APP_ENV: production
  DATABASE_URL: sqlite+aiosqlite:///./data/ecommerce.db
  LOG_LEVEL: INFO
  LOG_FORMAT: json
  AUTH_ENABLED: "true"
  FAKE_FAILURE_ENABLED: "false"
```

`.env` is not copied into the image.

---

# 40. CI

CI provides explicit configuration and depends on no local file:

```bash
APP_ENV=test
DATABASE_URL=sqlite+aiosqlite:///./.tmp/test.db
RANDOM_SEED=42
FAKE_CLOCK=2026-01-01T00:00:00Z
FAKE_FAILURE_ENABLED=false
AUTH_ENABLED=true
LOG_LEVEL=WARNING
```

Determinism is what makes CI reproducible: a fixed seed and a fixed clock mean a failure is
reproducible locally.

---

# 41. AI-Agent Experiments

For an agent experiment to be repeatable against the same data and the same timing:

```bash
APP_ENV=test
RANDOM_SEED=42
FAKE_CLOCK=2026-01-01T00:00:00Z
FAKE_FAILURE_ENABLED=false
```

One seed and one clock cover data **and** randomized failures — a single knob to pin for a
reproducible agent run. Failure scenarios are then enabled deliberately, by name:

```http
X-Fake-Failure: payment_declined
```

---

# 42. Variable Reference

| Variable | Type | Default | Environments | Sensitive | Purpose |
|---|---|---|---|---|---|
| `APP_ENV` | enum | `development` | all | No | Runtime environment |
| `APP_NAME` | str | `Fake E-Commerce Server` | all | No | Display name |
| `APP_VERSION` | str | `0.1.0` | all | No | Version; participates in determinism |
| `DEBUG` | bool | `false` | all | No | Verbose mode; never exposes secrets |
| `API_V1_PREFIX` | str | `/api/v1` | all | No | Route prefix |
| `API_DOCS_ENABLED` | bool | `true` | dev/test | No | Serve `/docs`, `/openapi.json` |
| `API_REDOC_ENABLED` | bool | `true` | dev/test | No | Serve `/redoc` |
| `CORS_ENABLED` | bool | `false` | all | No | Apply CORS middleware |
| `CORS_ORIGINS` | list | `[]` | all | No | Allowed origins |
| `HOST` | str | `127.0.0.1` | all | No | Bind address |
| `PORT` | int | `8000` | all | No | Bind port |
| `DATABASE_URL` | str | `sqlite+aiosqlite:///./data/ecommerce.db` | all | Maybe | SQLAlchemy DSN |
| `SQLITE_BUSY_TIMEOUT_MS` | int | `5000` | all | No | Lock wait before `SQLITE_BUSY` |
| `LOG_LEVEL` | enum | `INFO` | all | No | Minimum level |
| `LOG_FORMAT` | enum | `text` | all | No | `text` \| `json` |
| `RANDOM_SEED` | int | `42` | all | No | Deterministic data and failures |
| `FAKE_CLOCK` | ISO-8601 | *(unset)* | test | No | Fixed instant |
| `SEED_PROFILE` | enum | `development` | all | No | Default generator profile |
| `FAKE_FAILURE_ENABLED` | bool | `false` | dev/test | No | Master switch for simulation |
| `FAKE_FAILURE_RATE` | float | `0.0` | dev/test | No | Randomized failure probability |
| `FAKE_LATENCY_MS` | int | `0` | dev/test | No | Artificial latency |
| `AUTH_ENABLED` | bool | `true` | all | No | Enforce authn/authz |
| `AUTH_PROVIDER` | enum | `fake` | all | No | Only `fake` supported |
| `AUTH_FAKE_TOKEN_SECRET` | str | *(unset)* | all | **Yes** | Signs fake tokens |
| `AUTH_DEFAULT_ROLE` | enum | `customer` | all | No | Default role |
| `OBSERVABILITY_ENABLED` | bool | `true` | all | No | Correlation and access logs |
| `METRICS_ENABLED` | bool | `false` | all | No | Reserved |
| `TRACING_ENABLED` | bool | `false` | all | No | Reserved |
| `DEFAULT_CURRENCY` | str | `USD` | all | No | Currency for generated data |
| `DEFAULT_LOCALE` | str | `en_US` | all | No | Faker locale |

### 42.1 Deliberately not configurable

| Setting | Why not |
|---|---|
| `PRAGMA foreign_keys` | Switching it off silently disables all referential integrity (`database.md` §34) |
| `PRAGMA journal_mode` | Engine setup, not a deployment preference |
| `GET /health`, `GET /ready` | A probe must not be switchable off (`API.md` §25) |
| `X-Request-ID` header name | Part of the published contract (`API.md` §8.1) |
| Timezone | Everything is stored as ISO-8601 UTC text (`database.md` §9) |

---

# 43. Completion Criteria

Configuration is complete when:

* `Settings` exists in `src/ecommerce/config/settings.py` and is read once at startup.
* `pydantic-settings` validates every variable and fails fast on an invalid one.
* Every variable in §42 is documented here **and** in `.env.example`.
* `.env` is git-ignored; `.env.example` is committed and secret-free.
* Development, test, and "production" profiles work and are documented in §28.
* The `production` hard guards actually refuse to start.
* `APP_ENV=test` refuses to target a non-test database.
* Database configuration is centralized and shared with Alembic.
* Generator configuration works and follows `docs/generator.md` §46 precedence.
* Failure simulation is off by default and requires the master switch.
* Logging is configurable, structured in "production", and redacts secrets.
* `FAKE_CLOCK` makes timestamped behavior reproducible in tests.
* Domain code contains no `os.environ` access.
* Configuration is immutable after startup.
* The `data/` directory is created by infrastructure, not by the domain.
* CI configuration depends on no developer-local file.

---

# 44. Final Configuration Architecture

```text
                       ┌─────────────────────┐
                       │ Environment / .env  │
                       └──────────┬──────────┘
                                  │
                                  ▼
                       ┌─────────────────────┐
                       │  pydantic-settings  │
                       │    Validation       │
                       └──────────┬──────────┘
                                  │
             ┌────────────────────┼────────────────────┐
             │                    │                    │
             ▼                    ▼                    ▼
      Application Settings   Database Settings   Failure Settings
             │                    │                    │
             ▼                    ▼                    ▼
        FastAPI / API         SQLAlchemy          Failure middleware
             │                    │                    │
             └────────────────────┼────────────────────┘
                                  │
                                  ▼
                    Composition Root (main.py)
                                  │
                                  ▼
                       Application Services
                                  │
                  ┌───────────────┴───────────────┐
                  ▼                               ▼
               Domain                       Infrastructure
                                                  │
                                    ┌─────────────┴─────────────┐
                                    ▼                           ▼
                                 SQLite                  Fake Providers
```

A single typed configuration boundary, with the domain independent of environment variables,
deployment concerns, and infrastructure details.
