# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Version history

| Version | Feature Domain | Key Objectives |
|---------|----------------|----------------|
| 0.0.3   | Foundation | Database pool creation, WAL + foreign keys, health endpoint with DB verification |
| 0.0.2   | Foundation | Dependencies, Actix Web server, health endpoint |
| 0.0.1   | Foundation | Project scaffold, DDD structure, dependencies, config, entry point |
| 0.0.4   | Subscriber  | Repository port + SQLite persistence, subscribers table migration, domain reconstitution |

### Legend

- Feature Domain: bounded context (or set of contexts) delivered by the version.
- Key Objectives: main deliverables scoped to that version.

## [Unreleased]

### Added

- `src/domain/` — Domain layer bootstrap (Phase 2, Subscriber context, part 1):
  - `src/domain/mod.rs` — domain root module re-exporting the subscriber context.
  - `src/domain/subscriber/mod.rs` — module wiring and public re-exports
    (`Subscriber`, `SubscriberError`, `AccountNumber`, `Money`, `SubscriberId`,
    `SubscriberStatus`).
  - `src/domain/subscriber/error.rs` — `SubscriberError` (`thiserror`) with
    variants covering the state machine: `AlreadySuspended`, `AlreadyActive`,
    `Terminated`, `InvalidTermination`, `NegativeBalance`, `InvalidAccountNumber`.
  - `src/domain/subscriber/value_objects.rs` — `SubscriberStatus` enum,
    `SubscriberId` (UUID v4), `AccountNumber` (non-empty validation),
    `Money` with `zero()` / `from_cents()` (returns `Result` and rejects negative
    amounts via `NegativeBalance`) / `cents()` / `add()`.
  - `src/domain/subscriber/entity.rs` — `Subscriber` aggregate: `new()`
    (starts Active, zero balance), `suspend()` / `activate()` / `terminate()`
    transitions enforcing the state machine, and getters.
  - `src/domain/subscriber/entity.rs` — `Subscriber::reconstitute()` builds an
    aggregate from persisted values (bypasses `new()` invariants for reads).
  - `src/domain/subscriber/value_objects.rs` — `SubscriberId::from_uuid()` for
    reconstructing IDs from the database.
- Unit tests for the Subscriber state machine and value-object invariants
  (15 tests: entity transitions + account-number/money validation).

### Added

- `src/application/` — Application layer bootstrap (Phase 2, Subscriber context, part 2):
  - `src/application/mod.rs` and `src/application/subscriber/mod.rs` — module wiring.
  - `src/application/subscriber/repository.rs` — `SubscriberRepository` port
    (`async_trait`, `Send + Sync`): `create()`, `find_by_id()`,
    `find_by_account_number()`, `update()`.
  - `src/application/subscriber/service.rs` — `SubscriberService<R>` use-case
    layer over `SubscriberRepository`:
    - `new()` constructor taking the repository.
    - `create()` — validates `AccountNumber`, rejects duplicate account numbers,
      and persists a new Active subscriber.
    - `get()` — retrieves a subscriber by `Uuid`.
    - `suspend()` / `activate()` / `terminate()` — load, apply the state
      transition (mapping domain errors to `anyhow`), and persist via `update()`.
  - `src/application/subscriber/mod.rs` — re-exports `SubscriberRepository` and
    `SubscriberService`.
- `src/infrastructure/` — Infrastructure layer bootstrap:
  - `src/infrastructure/mod.rs` and `src/infrastructure/persistence/mod.rs` — module wiring.
  - `src/infrastructure/persistence/subscriber_repository.rs` —
    `SqliteSubscriberRepository` implementing the `SubscriberRepository` port on
    SQLx: `SubscriberRow` (`FromRow`) mapped to/from the domain aggregate via
    `into_domain()` and `reconstitute()`, with status string mapping
    (`active` / `suspended` / `terminated`) and field validation on reads.
- `migrations/20260920183725_create_subscribers.sql` — `subscribers` table
  (UUID id, unique account_number, status with CHECK constraint,
  non-negative balance_cents, nullable plan_id, timestamps) plus an index on
  `status`.
- `src/main.rs` — runs `sqlx::migrate!` on startup before the pool
  verification.
- `src/database.rs` — pool built with `SqlitePoolOptions::max_connections(10)`.
- Dependency: `async-trait`.

### Added

- `src/interfaces/` — HTTP interface layer (Phase 2, Subscriber context, part 3):
  - `src/interfaces/mod.rs` and `src/interfaces/http/mod.rs` — module wiring.
  - `src/interfaces/http/dto.rs` — `CreateSubscriberRequest` (Deserialize) and
    `SubscriberResponse` (Serialize) with `From<Subscriber>` conversion.
  - `src/interfaces/http/subscriber.rs` — Actix Web handlers on the
    `/subscribers` scope:
    - `POST /subscribers` — create (201 on success, 400 on error).
    - `GET /subscribers/{id}` — get (200, 404 if not found, 500 on error).
    - `POST /subscribers/{id}/suspend` | `/activate` | `/terminate` — state
      transitions (200 on success, 400 on error).
    - `configure()` registering the scoped routes.
- `src/main.rs` — wires `SqliteSubscriberRepository` → `SubscriberService`,
  injects the service via `web::Data` (cloned per worker), and configures the
  subscriber routes alongside `/health`.
- `SubscriberService` now derives `Clone` (and `SqliteSubscriberRepository`
  derives `Clone` via `SqlitePool`) so it can be shared across Actix workers.

### Added

- `src/application/error.rs` — `ApplicationError` enum (`thiserror`) with typed
  variants: `SubscriberNotFound`, `AccountNumberAlreadyExists`,
  `InvalidRequest`, `InvalidSubscriberState`, `Infrastructure` (+ `ApplicationResult<T>` alias).
- `src/application/subscriber/service.rs` — use-case methods now return
  `ApplicationResult` instead of `anyhow::Result`, mapping validation /
  duplicates / missing rows / state-machine / repository failures to the
  respective `ApplicationError` variants.
- `src/interfaces/http/error.rs` — centralized `ResponseError` for
  `ApplicationError`:
  - 404 for `SubscriberNotFound`.
  - 409 for `AccountNumberAlreadyExists` and `InvalidSubscriberState`.
  - 400 for `InvalidRequest`.
  - 500 with a generic `"internal server error"` body (error logged) for
    `Infrastructure`.
- `src/interfaces/http/subscriber.rs` — handlers simplified to
  `Result<HttpResponse, ApplicationError>` using `?`; `get_subscriber`
  returns 404 via `SubscriberNotFound` when no row exists.

### Added

- `src/lib.rs` — exposes the application as a library crate
  (`application`, `config`, `database`, `domain`, `infrastructure`,
  `interfaces`) so integration tests can import the modules.
- `src/main.rs` — drops local `mod` declarations and imports from the
  `telco_si` library crate.
- `tests/subscriber_integration.rs` — integration tests on an in-memory
  SQLite database (`sqlite::memory:` with `max_connections(1)` so a single
  connection backs the test DB, plus migrations on setup):
  - `create_and_get_subscriber` — full service → repository → SQLite round trip.
  - `duplicate_account_number_is_rejected` — duplicates surface as
    `ApplicationError::AccountNumberAlreadyExists`.
  - `subscriber_lifecycle_is_persisted` — suspend → activate → terminate
    transitions are persisted and reloadable.
  - `create_subscriber_through_http` — `POST /subscribers` via the Actix test
    harness returns 201.

### Added

- `src/database.rs` — SQLite connection pool via SQLx 0.9 `SqliteConnectOptions`:
  - `create_if_missing(true)` — fixes sqlx 0.9's default (error code 14, `unable to open database file`) which no longer auto-creates the DB file.
  - WAL journal mode and foreign keys enabled at the options level.
- `src/config.rs` — `AppConfig` reading `APP_HOST`, `APP_PORT`, `DATABASE_URL` from env (returning `anyhow::Result`).
- `src/main.rs`:
  - Initializes `tracing_subscriber` with env-filter (fallback `telco_si=info`).
  - Loads config, creates the DB pool, verifies connectivity with `SELECT 1` before binding.
  - Health endpoint returns `{"status": "ok"}`.
- `.gitignore` for `/target` and local `*.db` files.

### Fixed

- Server startup failed with `unable to open database file` (SQLite code 14) because sqlx 0.9 defaults `create_if_missing` to `false`; the pool is now built with explicit connect options.

### Changed

- `src/application/state.rs` — new `AppState` aggregate holding
  `subscriber_service` (a `SubscriberService<SqliteSubscriberRepository>`),
  exported from `src/application/mod.rs` as `AppState`.
- `src/interfaces/http/subscriber.rs` — all handlers
  (`create_subscriber`, `get_subscriber`, `suspend_subscriber`,
  `activate_subscriber`, `terminate_subscriber`) now receive
  `web::Data<AppState>` and access `state.subscriber_service` instead of
  receiving the concrete service directly; the `SubscriberAppService` type
  alias was removed.
- `src/main.rs` — builds `AppState` from the subscriber service and injects it
  via `web::Data::new(app_state.clone())` per worker.
- `tests/subscriber_integration.rs` — `create_subscriber_through_http` builds
  an `AppState` and injects it with the same dependency structure as
  production.

### Added

- Dependency: `validator` 0.20 (with `derive`).
- `src/interfaces/http/dto.rs` — `CreateSubscriberRequest` validates
  `account_number` via `#[validate(length(min = 3, max = 50))]`;
  `SubscriberResponse` now derives `Deserialize` so API responses can be
  decoded directly in integration tests.
- `src/application/error.rs` — new `ApplicationError::Validation` variant
  (`"validation failed"`).
- `src/interfaces/http/subscriber.rs` — `create_subscriber` runs
  `request.validate()` before creating the subscriber.
- `src/interfaces/http/error.rs` — `Validation` maps to HTTP 400. The full
  error mapping is now: 404 `SubscriberNotFound`, 409
  `AccountNumberAlreadyExists` / `InvalidSubscriberState`, 400
  `Validation` / `InvalidRequest`, 500 `Infrastructure`.
- `tests/subscriber_integration.rs`:
  - `create_subscriber_through_http` now decodes the 201 response body and
    asserts `account_number` and `balance_cents`.
  - `create_subscriber_rejects_invalid_account_number` — `POST /subscribers`
    with a too-short account number returns 400.

### Added

- `GET /subscribers` — paginated list endpoint (first read-model/query step,
  Phase 2, Subscriber context, part 4):
  - `src/application/subscriber/query.rs` — `SubscriberListItem`
    (`id`, `account_number`, `status`, `balance_cents`) and `SubscriberPage`
    read models, deliberately separate from the `Subscriber` aggregate so a
    list screen doesn't load the full domain object.
  - `src/application/subscriber/repository.rs` — `SubscriberRepository::list()`
    returning `(Vec<SubscriberListItem>, u64)` instead of `Subscriber`.
  - `src/infrastructure/persistence/subscriber_repository.rs` — SQL query
    (`ORDER BY created_at DESC`, `LIMIT`/`OFFSET`) plus a `COUNT(*)` total,
    via a dedicated `SubscriberListRow`.
  - `src/application/subscriber/service.rs` — `SubscriberService::list()`
    with pagination guards: `page >= 1`, `1 <= page_size <= 100`.
  - `src/interfaces/http/dto.rs` — `SubscriberListItemResponse`,
    `SubscriberListResponse` (with `From<SubscriberListItem>`), and
    `SubscriberListQuery` (`page`, `page_size`, defaulting to 1 and 20).
  - `src/interfaces/http/subscriber.rs` — `list_subscribers` handler and
    route registration so `GET`/`POST /subscribers` share the resource URL.
- Integration tests (`tests/subscriber_integration.rs`):
  - `list_subscribers_returns_paginated_results` — HTTP list returns 200 with
    items and correct `page`/`page_size`/`total`.
  - `subscriber_pagination_returns_correct_page` — service-level pagination
    (3 rows, page 2 of size 2 yields 1 item, total 3).

### Changed

- `GET /subscribers` now supports `status` and `account_number` query
  filters (Phase 2, Subscriber context, part 4 / query object):
  - `src/application/subscriber/query.rs` — new `SubscriberListQuery` with
    `page`, `page_size`, `status: Option<SubscriberStatus>`, and
    `account_number: Option<String>`. The status filter is typed as the
    domain enum (`SubscriberStatus`), so arbitrary strings such as
    `"banana"` can never reach the application layer.
  - `src/application/subscriber/repository.rs` — `SubscriberRepository::list()`
    now takes `&SubscriberListQuery` instead of raw `offset`/`limit` arguments.
  - `src/application/subscriber/service.rs` — `SubscriberService::list()`
    accepts a `SubscriberListQuery` (pagination guards unchanged).
  - `src/interfaces/http/dto.rs` — the HTTP query DTO was renamed to
    `SubscriberListRequest` (to avoid colliding with the application query)
    and now deserializes `status` and `account_number`.
  - `src/interfaces/http/subscriber.rs` — `list_subscribers` converts the HTTP
    DTO into the application `SubscriberListQuery`; a new `parse_status()`
    helper maps query-string status text to `SubscriberStatus` and returns 400
    on an invalid value instead of silently accepting it.
  - `src/infrastructure/persistence/subscriber_repository.rs` — `list()`
    builds the page and `COUNT(*)` queries with `sqlx::QueryBuilder`,
    appending bound (parameterized) `status`/`account_number` filters — the
    SQL structure is dynamic but values are always bound, so there is no
    SQL-injection surface. Existing indexes (`idx_subscribers_status`, the
    UNIQUE constraint on `account_number`) already cover the new filters.
- Integration tests (`tests/subscriber_integration.rs`):
  - `list_subscribers_can_filter_by_status` — `?status=suspended` returns
    only the suspended subscriber.
  - `list_subscribers_can_filter_by_account_number` — exact-match filter
    returns the matching row.
  - `list_subscribers_rejects_invalid_status` — `?status=unknown` returns 400.
  - `subscriber_pagination_returns_correct_page` — updated to build a
    `SubscriberListQuery`.

### Added

- Optimistic concurrency control via a persistence `version` column (Phase 2,
  Subscriber context, part 5):
  - `migrations/20260924153511_add_subscriber_version.sql` —
    `ALTER TABLE subscribers ADD COLUMN version INTEGER NOT NULL DEFAULT 1`.
    Existing rows start at version `1`.
  - `src/domain/subscriber/entity.rs` — `Subscriber` now carries a
    `version: i64` field: `new()` sets it to `1`, `version()` exposes it
    read-only, and `increment_version()` advances it after a successful
    persist. `reconstitute()` accepts the version from the database.
  - `src/application/error.rs` — new `ApplicationError::ConcurrencyConflict`
    ("subscriber was modified by another request"), mapped to HTTP 409 in
    `src/interfaces/http/error.rs`.
- Integration test (`tests/subscriber_integration.rs`):
  - `stale_subscriber_update_is_rejected` — two copies of the same subscriber
    (both version 1); the first update succeeds (DB version → 2), then the
    stale copy's update fails because its `WHERE version = 1` no longer
    matches.

### Changed

- `subscribers` updates now use optimistic locking:
  - `src/infrastructure/persistence/subscriber_repository.rs` — `update()`
    becomes conditional: `SET ... version = version + 1 WHERE id = ? AND
    version = ?`. When `rows_affected() == 0` the repository distinguishes
    "subscriber does not exist" from "subscriber update conflict" with a
    follow-up existence check (`SELECT version ...`), bailing with a
    repository-specific message. `SubscriberRow`, the INSERT, and the
    `find_by_id` / `find_by_account_number` SELECTs now include `version`.
  - `src/application/subscriber/service.rs` — `suspend()` / `activate()` /
    `terminate()` call `subscriber.increment_version()` after a successful
    `update()`, keeping the in-memory aggregate's version in sync with the
    database (`domain 3` → `UPDATE 3 → 4` → `domain 4`); a failed update
    leaves the domain version untouched.
  - The concurrency mechanism is demonstrated but the repository still returns
    `anyhow` errors (mapped to `Infrastructure` → 500) for now; a typed
    repository-error design (`NotFound` / `Duplicate` / `ConcurrencyConflict` /
    `Database`) can replace this later.

### Added

- Domain events + first event-infrastructure boundary (Phase 2, Subscriber
  context, part 6):
  - `src/domain/events.rs` — `DomainEvent` enum recording facts about what
    already happened, named with past-tense verbs (`SubscriberSuspended`,
    `SubscriberActivated`, `SubscriberTerminated`), each carrying
    `subscriber_id` and `occurred_at` — distinct from commands
    (`SuspendSubscriber`, etc.). Exported from `src/domain/mod.rs`.
  - `src/domain/subscriber/entity.rs` — `Subscriber` now collects generated
    events in `events: Vec<DomainEvent>`:
    - `new()` and `reconstitute()` start with an empty event list, so loading
      a persisted subscriber never re-emits historical events.
    - `suspend()` / `activate()` / `terminate()` record the corresponding
      event only after a valid state transition
      (validate → change state → record event), so failed operations generate
      nothing.
    - `domain_events()` exposes the collection as an immutable
      `&[DomainEvent]`; `take_domain_events()` drains it with
      `std::mem::take`, so events can be dispatched exactly once.
  - `src/application/event_publisher.rs` — `EventPublisher` port
    (`async_trait`, `Send + Sync`) with a single `publish(DomainEvent)`
    method — the first event infrastructure boundary, so the application
    depends on an interface rather than a concrete broker (dependency
    inversion; the transport could later be SQLite, an outbox, Kafka,
    RabbitMQ, or a cloud bus). Exported from `src/application/mod.rs`.
  - `src/infrastructure/events/{mod,in_memory}.rs` —
    `InMemoryEventPublisher` holding an `Arc<Mutex<Vec<DomainEvent>>>` for
    now (intentionally simple), exported from `src/infrastructure/mod.rs`.
  - The publisher is deliberately **not** wired into `SubscriberService` yet:
    publishing outside the same transaction can leave
    (database update, event dispatch) inconsistent — the motivation for the
    transactional outbox pattern, which comes next.
- Unit tests (`src/domain/subscriber/entity.rs`):
  - `suspending_subscriber_generates_event` — one event of the right variant.
  - `invalid_suspend_does_not_generate_event` — a second, failed suspend
    leaves the event count unchanged.

### Added

- Transactional outbox pattern (Phase 2, Subscriber context, part 7):
  - `migrations/20260924155420_create_outbox_events.sql` — `outbox_events`
    table (`aggregate_type`/`aggregate_id`, `event_type`, `payload`,
    `occurred_at`/`created_at`, `published_at`, `attempts`, `last_error`) with
    an index on `(published_at, created_at)` for the future publisher worker.
    Applied to the local database via `cargo sqlx migrate run`.
  - `src/domain/events.rs` — `DomainEvent` now derives
    `Serialize + Deserialize`, plus metadata helpers so infrastructure never
    matches on event internals: `event_type()` (`subscriber.suspended` /
    `subscriber.activated` / `subscriber.terminated`), `aggregate_type()`,
    `aggregate_id()`, `occurred_at()`.
  - `src/domain/subscriber/entity.rs` — new `clear_domain_events()` (paired
    with the existing `domain_events()` slice accessor): events are read via
    `domain_events().to_vec()` and persisted, and only cleared *after* a
    successful save.
  - `src/application/subscriber/repository.rs` — `SubscriberRepository::update`
    replaced by `save(subscriber, events: &[DomainEvent])`, since one
    persistence operation now means "save the aggregate **and** its domain
    events".
  - `src/infrastructure/persistence/subscriber_repository.rs` — transactional
    `save()`: `pool.begin()` → optimistic-lock UPDATE (same `version` guard) →
    one INSERT per event into `outbox_events` (JSON payload) → `tx.commit()`.
    Any failure rolls back the whole transaction, so the system never ends up
    in "subscriber changed, event missing" — only "subscriber unchanged, event
    missing". A conflict (`rows_affected() == 0`) bails with
    `subscriber update conflict` (the previous existence-check branch was
    simplified away).
  - `src/application/subscriber/service.rs` — `suspend()` / `activate()` /
    `terminate()` now: mutate → `domain_events().to_vec()` → `repository.save()`
    → `clear_domain_events()` → `increment_version()`. `create()` still uses
    `repository.create()` unchanged (creation emits no event yet; a future
    `SubscriberCreated` would join the outbox the same way).
  - `EventPublisher` / `InMemoryEventPublisher` remain unwired: the intended
    flow is domain event → application service → `repository.save()` →
    [Subscriber row + Outbox row] in one transaction → future worker →
    `EventPublisher`, never a direct DB→publisher call inside a request.
- Integration test (`tests/subscriber_integration.rs`):
  - `suspend_persists_domain_event_to_outbox` — suspending a subscriber
    writes an `outbox_events` row with `event_type = subscriber.suspended`,
    `aggregate_type = subscriber`, and a payload containing
    `SubscriberSuspended` — proving domain event → JSON serialization →
    outbox persistence.
  - `stale_subscriber_update_is_rejected` — updated to the new
    `save(subscriber, events)` signature.

## [0.0.2] - 2026-09-19

### Added

- Web/API dependencies: `actix-web`, `tokio` (macros, rt-multi-thread), `serde` (derive), `serde_json`.
- Observability and error-handling dependencies: `anyhow`, `thiserror`, `tracing`, `tracing-subscriber` (fmt, env-filter).
- Utility and model dependencies: `uuid` (v4, serde), `chrono` (serde).
- Database dependency: `sqlx` 0.9 with `runtime-tokio`, `sqlite`, `macros`, `migrate`, `chrono`, `uuid`.
- Actix Web entry point (`main.rs`) replacing the hello-world stub.
- `GET /health` endpoint returning `{"status": "ok"}`.

## [0.0.1] - 2026-09-19

### Added

- Initial Rust project scaffold for the telco_si BSS/OSS platform.
- Target architecture:
  - Interfaces → Application → Domain layering with strict dependency rules.
  - Infrastructure implements domain/application ports (SQLx, SQLite, event bus, payments, object storage).
  - Bounded contexts: Subscriber, Inventory, Device, Usage, Rating, Roaming, Billing, Payment, Dunning, Document.

### Planned roadmap

- Phase 1 — Foundation: Cargo config, Actix Web, Tokio, SQLx + SQLite (WAL + foreign keys), config loader, entry point, health endpoints, error-handling conventions, logging/tracing. (in progress)
- Phase 2 — Subscriber context.
- Phase 3 — Inventory (MSISDN, SIM/IMSI).
- Phase 4 — Device (IMEI).
- Phase 5 — Usage / CDR.
- Phase 6 — Rating engine.
- Phase 7 — Roaming.
- Phase 8 — Billing.
- Phase 9 — Payments.
- Phase 10 — Dunning.
- Phase 11 — Documents / PDF.
- Phase 12 — Production architecture (events, outbox, observability, security, Docker, CI/CD).

The src/ directory is intentionally kept empty (only the hello-world `main.rs`) until
implementation steps are provided incrementally.
