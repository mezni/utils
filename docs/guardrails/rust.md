# Guardrail — Rust Clean Architecture

Applies to: `source/services/`, `source/crates/`

---

## Layer structure (mandatory for every service)

Every Actix-web service MUST follow this four-layer layout. No business logic in handlers. No database calls in domain types.

```
src/
  main.rs              # Bootstrap only: config, DB pool, Actix app factory
  config.rs            # Typed config via envy or config crate
  errors.rs            # Central AppError enum (see Error handling below)
  domain/              # Pure types and traits — zero infrastructure deps
    mod.rs
    station.rs         # e.g. Station, StationStatus, NewStation
  repository/          # Trait definitions + sqlx implementations
    mod.rs
    station_repo.rs    # trait StationRepository + struct PgStationRepository
  service/             # Business logic, orchestrates repositories
    mod.rs
    station_service.rs
  handlers/            # Actix extractors → service calls → HTTP responses
    mod.rs
    station_handler.rs
  middleware/          # Auth extraction, logging, request-id injection
    mod.rs
    auth.rs
```

Dependency direction is strict: `handlers → service → repository → domain`. Nothing flows upward.

---

## Error handling

Define one `AppError` enum per service. Every error variant must map to a specific HTTP status. No `unwrap()`, no `expect()`, no `panic!()` outside test code.

```rust
// errors.rs
use actix_web::HttpResponse;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Unauthorized")]
    Unauthorized,

    #[error("Forbidden")]
    Forbidden,

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Internal error")]
    Internal(#[from] anyhow::Error),
}

impl actix_web::ResponseError for AppError {
    fn error_response(&self) -> HttpResponse {
        match self {
            AppError::NotFound(msg)   => HttpResponse::NotFound().json(json!({ "error": msg })),
            AppError::Unauthorized    => HttpResponse::Unauthorized().finish(),
            AppError::Forbidden       => HttpResponse::Forbidden().finish(),
            AppError::Validation(msg) => HttpResponse::UnprocessableEntity().json(json!({ "error": msg })),
            AppError::Database(_)     => HttpResponse::InternalServerError().json(json!({ "error": "database error" })),
            AppError::Internal(_)     => HttpResponse::InternalServerError().json(json!({ "error": "internal error" })),
        }
    }
}
```

Rules:
- Use `thiserror` for error definitions. Never `Box<dyn Error>` as a return type in service or repository code.
- Use `anyhow` only in `main.rs` for startup errors.
- Every `?` propagation must have a matching `From` impl or `map_err`.
- Log the underlying error at the service layer before returning `AppError::Internal`. Never swallow.

---

## Repository pattern

```rust
// repository/station_repo.rs

#[async_trait]
pub trait StationRepository: Send + Sync {
    async fn find_by_id(&self, id: &str) -> Result<Station, AppError>;
    async fn find_nearby(&self, lat: f64, lng: f64, radius_m: i32) -> Result<Vec<StationSummary>, AppError>;
    async fn create(&self, input: NewStation) -> Result<Station, AppError>;
}

pub struct PgStationRepository {
    pool: PgPool,
}

#[async_trait]
impl StationRepository for PgStationRepository {
    async fn find_by_id(&self, id: &str) -> Result<Station, AppError> {
        sqlx::query_as!(
            Station,
            "SELECT * FROM inventory.stations WHERE id = $1 AND deleted_at IS NULL",
            id
        )
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("station {id}")))
    }
    // ...
}
```

Rules:
- Repository traits live in `repository/mod.rs` or individual files. Implementations are in the same file as the trait.
- Always use `sqlx::query!` or `sqlx::query_as!` macros. Never build SQL strings dynamically.
- `fetch_optional` + `.ok_or_else` is the pattern for single-row lookups. Never `fetch_one` directly (panics on missing row in some drivers).
- Inject pool via constructor, not thread-local or global state.

---

## Service layer

```rust
// service/station_service.rs

pub struct StationService<R: StationRepository> {
    repo: R,
    cache: Arc<dyn CacheClient>,
}

impl<R: StationRepository> StationService<R> {
    pub async fn create_station(&self, input: NewStation) -> Result<Station, AppError> {
        // 1. Validate domain rules
        input.validate()?;
        // 2. Persist (repository handles the transaction)
        let station = self.repo.create(input).await?;
        // 3. Bust cache synchronously
        self.cache.invalidate_nearby(station.latitude, station.longitude).await?;
        Ok(station)
    }
}
```

Rules:
- Services receive repository traits (not concrete types) via generic bounds or `Arc<dyn Trait>`.
- All multi-step operations that modify state must be orchestrated as a single unit — wrap in a DB transaction at the repository layer if multiple tables are touched.
- Services must not import `actix_web` types. They are pure business logic.
- Cache invalidation is always synchronous and always happens after a successful DB write, never before.

---

## Handler layer

```rust
// handlers/station_handler.rs

pub async fn create_station(
    auth: AuthenticatedUser,           // middleware extractor
    state: web::Data<AppState>,
    body: web::Json<CreateStationRequest>,
) -> Result<HttpResponse, AppError> {
    auth.require_role("role:partner")?;
    let station = state.station_service.create_station(body.into_inner().into()).await?;
    Ok(HttpResponse::Created().json(station))
}
```

Rules:
- Handlers are thin. One call to a service method, one HTTP response. No business logic.
- Role checks happen at the top of the handler, before any service call.
- Request bodies must be validated structs (use `validator` crate with `#[validate]` attributes on domain types).
- Response bodies must be explicitly typed structs — never return raw `serde_json::Value`.

---

## Shared crates

`crates/db-models` — shared sqlx model types used across services. No business logic.
`crates/validation` — shared validator implementations for domain types used in more than one service.

Rules:
- A type goes in `crates/db-models` only if two or more services need it.
- No Actix types in shared crates. Shared crates must compile without Actix.

---

## Code style rules

- `clippy::all` and `clippy::pedantic` are enabled. Zero warnings allowed in CI.
- `rustfmt` is applied on every file before commit.
- Visibility: default to private. Only `pub` what is needed by the next layer up.
- Lifetimes: avoid explicit lifetimes in handler/service signatures. If you need them, the design is wrong.
- Async: every async fn that touches IO must be cancellation-safe or explicitly documented as not.
- `Arc<Mutex<T>>` is a code smell at service level — redesign to use message passing or avoid shared mutable state.

---

## Self-check before submitting Rust code

- [ ] No `unwrap()` or `expect()` outside `#[cfg(test)]` blocks
- [ ] No raw SQL strings — only `sqlx::query!` macros
- [ ] Every new error variant has an HTTP mapping in `AppError`
- [ ] No `actix_web` imports in `domain/` or `service/`
- [ ] No business logic in handlers
- [ ] Clippy passes with zero warnings (`cargo clippy -- -D warnings`)
- [ ] `cargo fmt --check` passes
- [ ] Every public function has a doc comment
