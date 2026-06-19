# Research: Admin Service Core Operations

## Phase 0 Research Summary

This document consolidates research findings for the Admin Service implementation, addressing technical decisions, dependency best practices, and integration patterns.

## Technical Decisions

### 1. Actix-web Configuration for CRUD Endpoints

**Decision**: Use actix-web 4.x with HttpServer configuration, route handlers, and request/response models.

**Rationale**:
- Actix-web 4.x is the latest stable version (matches constitution requirement for Rust 1.88)
- Proven performance for web services with minimal overhead
- Excellent ecosystem with Actix websockets, routing, and middleware
- Easy integration with sqlx for compile-time type safety

**Alternatives Considered**:
- **Axum**: Popular async web framework but smaller ecosystem; less mature in Rust ecosystem
- **Rocket**: Easy to use but not async-native, different error handling paradigm
- **Warp**: Functional-style, less familiar to team; less community resources

**Implementation Pattern**:
```rust
// Route configuration pattern
cfg.route("/partner", web::post().to(create_partner))
    .route("/partner/:id", web::get().to(get_partner))
    .route("/partner/:id", web::put().to(update_partner));
```

---

### 2. Database Transaction Management for Multi-Entity Operations

**Decision**: Use sqlx `!query!` macros with explicit `sqlx::Transaction` for all multi-table writes.

**Rationale**:
- sqlx compile-time safety prevents SQL injection and type errors
- `sqlx::Transaction` provides ACID guarantees for multi-table operations
- Explicit transaction boundary ensures atomicity (all or nothing)
- Matches constitution requirement: "All multi-table data modifications within any microservice MUST be wrapped in a single database transaction"

**Alternatives Considered**:
- **Multiple queries without transaction**: Would allow partial updates, violating ACID
- **Database-level triggers for atomicity**: Would hide errors from application logic, making debugging harder
- **Try/except with rollback manual logic**: Too verbose; sqlx transactions handle this cleanly

**Implementation Pattern**:
```rust
let mut tx = pool.begin().await?;
sqlx::query!("INSERT INTO inventory.partners ...")
    .execute(&mut tx).await?;
sqlx::query!("INSERT INTO inventory.stations ...")
    .execute(&mut tx).await?;
tx.commit().await?;
```

---

### 3. Post-Commit Step Orchestration (MV Refresh, Redis Bust, Audit)

**Decision**: Execute all post-commit steps (materialized view refresh, Redis cache bust, audit log insert) in a service orchestration layer (`admin_orchestrator.rs`), NOT in the repository layer.

**Rationale**:
- Repository layer must be "audit-unaware" (per constitution: "Diff computed in service layer — repository layer MUST be audit-unaware")
- Orchestrator layer has full context for diff computation and can handle failures gracefully
- Separation of concerns: repositories only handle database operations, orchestrator handles business logic and cross-cutting concerns
- Failure policy: if MV refresh or Redis bust fails, log warning and proceed (do NOT roll back transaction)

**Alternatives Considered**:
- **Repository layer handles audit**: Would make repositories responsible for both data access and audit logic (violates single responsibility)
- **Async fire-and-forget**: Redis bust or MV refresh failures wouldn't be caught, potentially leaving stale data
- **All post-commit steps in main.rs**: Would mix business logic with framework setup

**Implementation Pattern**:
```rust
// Orchestrator pattern for post-commit steps
async fn create_partner(&self, pool: &PgPool, claims: &Claims, payload: CreatePartnerRequest) -> Result<Partner> {
    let mut tx = pool.begin().await?;

    // Repository layer handles raw database operations (audit-unaware)
    let partner = repository::create_partner(&mut tx, payload).await?;

    tx.commit().await?;

    // Orchestrator layer handles post-commit steps
    self.refresh_materialized_views(pool).await?;
    self.invalidate_redis_cache(pool, &partner.id).await?;
    self.log_audit_event(pool, &claims, "partner.created", &partner).await?;

    Ok(partner)
}
```

---

### 4. Redis Cache Invalidation Strategy

**Decision**: Synchronous cache bust in service orchestration layer after transaction commit, with failure tolerance (log warning, continue, set header).

**Rationale**:
- Synchronous ensures cache is invalidation before next request (no race conditions)
- Post-commit ensures database commit succeeds before cache operations
- Failure tolerance is critical: cache bust failures should NOT roll back successful database commits (per constitution)
- Set `X-Cache-Bust-Failed: true` header to notify client of cache inconsistency

**Alternatives Considered**:
- **Asynchronous cache bust via Redis Pub/Sub**: Would require event bus infrastructure (excluded in MVP-1 per constitution: "No asynchronous outbox patterns are deployed during the validation phase")
- **Cache bust before transaction commit**: Would risk leaving stale data if transaction fails
- **Ignore cache bust failures**: Could lead to user-facing inconsistencies

**Implementation Pattern**:
```rust
async fn invalidate_redis_cache(&self, pool: &PgPool, entity_id: &str, entity_type: &str) -> Result<()> {
    match self.redis_client.del(format!("stations:tile:1:1:1")).await {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::warn!(error = %e, "Redis cache bust failed, continuing without rollback");
            Ok(()) // Failure is tolerated
        }
    }
}
```

---

### 5. Idempotency Key Storage and Retrieval

**Decision**: Store idempotency keys in Redis with 24-hour TTL, check on every POST request before executing mutation.

**Rationale**:
- Redis is already available for cache invalidation, no new infrastructure needed
- 24-hour TTL matches typical API idempotency window
- Key format: UUID v4, namespaced with `idempotency:` prefix
- Stored responses are serialized (includes response body, status code, headers) for exact replay

**Alternatives Considered**:
- **In-memory HashMap**: Would lose idempotency after service restart (violates deduplication guarantee)
- **PostgreSQL table**: Would require transaction to check idempotency and execute mutation (unnecessary complexity)
- **Database-agnostic approach (e.g., file system)**: Not idiomatic for production applications

**Implementation Pattern**:
```rust
async fn handle_idempotency_key(&self, key: &str) -> Result<Option<Response>> {
    let key_str = format!("idempotency:{}", key);

    if let Some(stored) = self.redis_client.get(&key_str).await? {
        // Replay stored response
        tracing::info!("Idempotency key found, replaying response");
        return Ok(Some(serde_json::from_str::<Response>(&stored)?));
    }

    Ok(None) // Key not found, proceed with mutation
}

async fn store_idempotency_response(&self, key: &str, response: &Response) -> Result<()> {
    let key_str = format!("idempotency:{}", key);
    let ttl = 24 * 60 * 60; // 24 hours in seconds

    self.redis_client.set_ex(&key_str, &serde_json::to_string(response)?, ttl).await?;

    Ok(())
}
```

---

### 6. Entity Identifier Generation with NanoID Prefixes

**Decision**: Use `uuid` crate to generate UUID v5 values with deterministic hashing, format with OPR-/STA-/CHG- prefixes.

**Rationale**:
- UUID v5 provides deterministic uniqueness (same input → same ID)
- Prefixes clearly identify entity type for debugging and filtering
- NanoID-style format (OPR-123abc, STA-def456) is human-readable and URLsafe
- Constitution requirement: "Entity ID prefixes (NanoID): OPR- (partner), STA- (station), CHG- (charger)"

**Alternatives Considered**:
- **UUID v4 (random)**: Would require additional index lookup for type identification, no predictable format
- **Sequential integers**: Would expose data volume and create enumeration attacks
- **Custom alphanumeric codes**: Harder to implement securely, less standard

**Implementation Pattern**:
```rust
fn generate_entity_id(prefix: &str, input: &str) -> String {
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_OID, input.as_bytes());
    format!("{}{}", prefix, uuid)
}
// Example: generate_entity_id("OPR-", "partner-123") → "OPR-a1b2c3d4e5f6g7h8i9j0"
```

---

### 7. Scope Restriction Enforcement (Partner Isolation)

**Decision**: Enforce scope restrictions in service layer by querying partner_id from X-User-Roles header and validating entity ownership.

**Rationale**:
- Constitution requirement: "Scope enforcement: partner cannot mutate another partner's stations/chargers"
- All entity tables have foreign key to partner_id (parent entity)
- Service layer has access to both caller context (X-User-Id from Traefik) and target entity (partner_id from request)
- Enforcing at service layer is more maintainable than relying solely on database constraints

**Alternatives Considered**:
- **Database-level foreign key constraints**: Would prevent cross-partner mutations but wouldn't log or notify admins
- **Authorization-only at gateway**: Would leave gaps if Traefik misconfiguration occurs
- **Permission check in each repository method**: Would create duplication (violates DRY principle)

**Implementation Pattern**:
```rust
async fn validate_scope(&self, caller_partner_id: &str, target_partner_id: &str) -> Result<()> {
    if caller_partner_id != target_partner_id {
        return Err(AuthError::Forbidden(
            format!("Partner {} cannot mutate resources owned by partner {}",
                   caller_partner_id, target_partner_id)
        ));
    }
    Ok(())
}

// Usage in orchestrator:
let caller_partner_id = extract_partner_id_from_roles(&claims)?;
let partner = repository::get_partner(&mut tx, &partner_id).await?;
validate_scope(&caller_partner_id, &partner.partner_id)?;
```

---

### 8. Geographic Data with PostGIS

**Decision**: Use PostGIS GEOGRAPHY(Point, 4326) type for station location storage with SRID 4326 (WGS84).

**Rationale**:
- PostGIS is already used in the project (constitution requirement: "PostgreSQL 16 + PostGIS")
- GEOGRAPHY(Point, 4326) uses WGS84 standard (GPS coordinates), appropriate for charging station locations
- Indexing on location enables spatial queries (e.g., "find stations near X")
- sqlx macros support PostGIS types natively

**Alternatives Considered**:
- **GEOGRAPHY(Point, 3857)**: Uses meters, not standard for GPS coordinates
- **GEOGRAPHY(Point, 4269)**: Uses NAD83, less common for GPS
- **GEOMETRY(Point, 4326)**: Uses latitude/longitude in degrees, less precise for distance calculations

**Implementation Pattern**:
```sql
CREATE TABLE inventory.stations (
    id TEXT PRIMARY KEY CHECK (id ~ '^STA-.+'),
    partner_id TEXT NOT NULL REFERENCES inventory.partners(id),
    location GEOGRAPHY(Point, 4326) NOT NULL,
    -- other fields...
);
CREATE INDEX idx_stations_location ON inventory.stations USING GIST (location);
```

```rust
// Rust model
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Station {
    pub id: String,
    pub partner_id: String,
    pub location: Point<f64>, // sqlx::types::sqlx::postgres::PgPoint
    // other fields...
}
```

---

### 9. Audit Log with Before/After Snapshots

**Decision**: Store full JSONB snapshots of BEFORE and AFTER states in `analytics_db.audit_log` table, computed in orchestrator layer.

**Rationale**:
- Audit trail is critical for compliance and debugging
- Storing full snapshots (not just deltas) simplifies replay and debugging
- Orchestrator layer has full context to compute diffs before logging
- Audit failures should NOT rollback mutations (per constitution: "Audit log failure does not roll back mutation")

**Alternatives Considered**:
- **Only AFTER snapshots**: Would lose historical context for debugging
- **Only diffs in database triggers**: Would hide implementation details, make debugging harder
- **External audit service**: Would introduce new infrastructure not needed in MVP-1

**Implementation Pattern**:
```rust
async fn log_audit_event(&self, pool: &PgPool, claims: &Claims, action: &str, entity: &AuditEntity) -> Result<()> {
    let before_snapshot = match action {
        "partner.created" => None,
        _ => Some(serde_json::to_value(&entity.before_snapshot)?),
    };
    let after_snapshot = Some(serde_json::to_value(&entity.after_snapshot)?);

    sqlx::query!(
        "INSERT INTO analytics_db.audit_log (
            actor_id, action, target_type, target_id,
            before_snapshot, after_snapshot, payload, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
        ",
        claims.sub,
        action,
        entity.target_type,
        entity.target_id,
        before_snapshot,
        after_snapshot,
        serde_json::json!(entity.payload)
    ).execute(pool).await?;

    Ok(())
}
```

---

### 10. Error Handling and Response Contracts

**Decision**: Use `thiserror` for error types, define clear error contracts per endpoint, always return HTTP status codes matching the spec.

**Rationale**:
- thiserror provides idiomatic Rust error handling
- Clear error contracts ensure frontend can handle errors predictably
- All errors must be technology-agnostic (no stack traces in production responses)

**Alternatives Considered**:
- **Raw strings/panic**: Not production-ready, hides errors from users
- **Different error structures per endpoint**: Would make client-side error handling inconsistent
- **Detailed error messages with stack traces**: Security risk (could leak system internals)

**Implementation Pattern**:
```rust
#[derive(Error, Debug)]
pub enum AuthError {
    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Unauthorized")]
    Unauthorized,

    #[error("Forbidden")]
    Forbidden,

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Internal server error")]
    InternalError,
}

impl ResponseError for AuthError {
    fn error_response(&self) -> HttpResponse {
        let status_code = self.status_code();
        HttpResponse::build(status_code).json(json!({
            "error": self.error_code(),
            "details": self.details()
        }))
    }

    fn status_code(&self) -> StatusCode {
        match self {
            AuthError::ValidationError(_) => StatusCode::BAD_REQUEST,
            AuthError::Unauthorized => StatusCode::UNAUTHORIZED,
            AuthError::Forbidden => StatusCode::FORBIDDEN,
            AuthError::Conflict(_) => StatusCode::CONFLICT,
            AuthError::NotFound(_) => StatusCode::NOT_FOUND,
            AuthError::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_code(&self) -> String {
        match self {
            AuthError::ValidationError(_) => "validation_error".to_string(),
            AuthError::Unauthorized => "unauthorized".to_string(),
            AuthError::Forbidden => "forbidden".to_string(),
            AuthError::Conflict(_) => "constraint_violation".to_string(),
            AuthError::NotFound(_) => "not_found".to_string(),
            AuthError::InternalError => "internal_error".to_string(),
        }
    }
}
```

---

## Dependency Best Practices

### sqlx Compile-Time Safety

**Research**: sqlx provides compile-time type checking for database queries through the `!query!` macro family. The crate supports `sqlx::query!`, `sqlx::query_as!`, `sqlx::query_scalar!` for compile-time type validation of parameters and result sets.

**Best Practice**:
- Use `sqlx::query!` for `INSERT`, `UPDATE`, `DELETE` operations (type-checked parameters)
- Use `sqlx::query_as!` for `SELECT` operations with type-checked result rows
- Use `sqlx::query_scalar!` for single-value queries (type-checked result)
- Never use raw SQL strings (even with placeholders) — compile-time checking is essential for safety

**Implementation Example**:
```rust
// ✅ CORRECT: Compile-time type checking
let partner = sqlx::query_as!(
    Partner,
    r#"
    INSERT INTO inventory.partners (id, name, network_type, ...)
    VALUES ($1, $2, $3, ...)
    RETURNING *
    "#,
    id,
    name,
    network_type
).fetch_one(pool).await?;

// ❌ WRONG: Raw SQL string bypasses compile-time checking
let partner = sqlx::query("INSERT INTO partners ...")
    .bind(id)
    .bind(name)
    .fetch_one(pool).await?;
// Risk: parameter types not validated at compile time
```

---

### Redis Cache Invalidation Patterns

**Research**: Redis is used for GIS tile caching (keys: `stations:tile:{z}:{x}:{y}`) and idempotency key storage (keys: `idempotency:{uuid}`). Both are read-heavy, write-once patterns.

**Best Practice**:
- Use simple `DEL` for cache busting (no complex patterns needed)
- Use `EX` (expiration) parameter for idempotency keys (24-hour TTL)
- Treat cache bust failures as non-critical (log warning, set header, continue)
- Use connection pooling (provided by `redis` crate) for performance

**Implementation Example**:
```rust
// Cache busting pattern
async fn bust_tile_cache(&self, z: u8, x: u32, y: u32) -> Result<()> {
    let key = format!("stations:tile:{}:{}:{}", z, x, y);
    match self.redis_client.del(&key).await {
        Ok(_) => {
            tracing::debug!("Cache busted for tile {}", key);
            Ok(())
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to bust cache for tile {}", key);
            Ok(()) // Failure tolerated
        }
    }
}
```

---

### Error Handling in Async Rust

**Research**: Async Rust (Tokio) requires careful error handling to avoid blocking event loops. `?` operator with `Result<T, E>` is the standard pattern. For operations that shouldn't fail in production, use `expect()` only in test code (per constitution: "No unwrap() / expect() outside test code").

**Best Practice**:
- Use `?` operator to propagate errors up the call stack
- Define custom error types with `thiserror` for domain-specific errors
- Separate error handling into dedicated `error.rs` module
- Don't panic in production code — return errors instead

**Implementation Example**:
```rust
// ✅ CORRECT: Error propagation with ? operator
async fn create_partner(&self, pool: &PgPool, payload: CreatePartnerRequest) -> Result<Partner, AuthError> {
    let mut tx = pool.begin().await
        .map_err(|e| AuthError::InternalError)?; // Convert db errors to domain errors

    let partner = repository::create_partner(&mut tx, payload).await?;

    tx.commit().await
        .map_err(|e| AuthError::InternalError)?;

    Ok(partner)
}

// ❌ WRONG: Panic in production code
async fn create_partner(&self, pool: &PgPool, payload: CreatePartnerRequest) -> Partner {
    let mut tx = pool.begin().await.unwrap(); // PANIC in production
    repository::create_partner(&mut tx, payload).await.unwrap();
    tx.commit().await.unwrap();
    // ...
}
```

---

## Integration Patterns

### Service Layer Orchestration

**Research**: The orchestrator layer sits between routes and repositories, coordinating business logic and cross-cutting concerns (audit, cache bust, MV refresh). This pattern ensures repositories remain simple and audit-unaware.

**Best Practice**:
- Repository methods should be "dumb" — just database operations
- Orchestrator methods should handle business rules, validation, and post-commit steps
- Keep repositories thin: only CRUD operations, no business logic
- Orchestrator handles all failures gracefully: log warnings, set headers, continue

**Implementation Example**:
```rust
// Repository layer (dumb, database-only)
pub async fn create_partner(&mut tx: &mut Transaction<'_, Postgres>, payload: CreatePartnerRequest) -> Result<Partner, sqlx::Error> {
    sqlx::query_as!(
        Partner,
        r#"
        INSERT INTO inventory.partners (id, name, network_type, ...)
        VALUES ($1, $2, $3, ...)
        RETURNING *
        "#,
        payload.id,
        payload.name,
        payload.network_type
    ).fetch_one(&mut *tx).await
}

// Orchestrator layer (business logic + cross-cutting concerns)
pub async fn create_partner(&self, pool: &PgPool, claims: &Claims, payload: CreatePartnerRequest) -> Result<Partner, AuthError> {
    // Business validation
    if payload.name.is_empty() {
        return Err(AuthError::ValidationError("name is required".to_string()));
    }

    // Repository layer (audit-unaware)
    let mut tx = pool.begin().await
        .map_err(|e| AuthError::InternalError)?;
    let partner = repository::create_partner(&mut tx, payload).await
        .map_err(|e| AuthError::InternalError)?;
    tx.commit().await
        .map_err(|e| AuthError::InternalError)?;

    // Post-commit steps (orchestrator handles failures gracefully)
    self.refresh_materialized_views(pool).await
        .map_err(|e| {
            tracing::warn!("Failed to refresh materialized views: {}", e);
            AuthError::InternalError
        })?;
    self.invalidate_redis_cache(pool, &partner.id, "partner").await
        .map_err(|e| {
            tracing::warn!("Failed to bust Redis cache: {}", e);
            AuthError::InternalError
        })?;
    self.log_audit_event(pool, claims, "partner.created", &AuditEntity {
        target_type: "partner",
        target_id: &partner.id,
        before_snapshot: None,
        after_snapshot: partner.clone(),
        payload: json!({"name": partner.name}),
    }).await
        .map_err(|e| {
            tracing::error!("Failed to log audit event: {}", e);
            AuthError::InternalError
        })?;

    Ok(partner)
}
```

---

### Traefik Header Extraction for Context

**Research**: Admin Service must extract user context from Traefik headers (`X-User-Id`, `X-User-Roles`) and never from client body (constitution requirement: "X-User-Id / X-User-Roles trusted from Traefik only (never from client)").

**Best Practice**:
- Extract headers in middleware or route handler
- Validate that headers exist and are properly formatted
- Convert roles string to type-safe enum (e.g., `AdminRole` vs `PartnerRole`)
- Never trust headers without validation (could be manipulated by client)

**Implementation Example**:
```rust
// Middleware to extract and validate headers
pub fn extract_user_context(req: &HttpRequest) -> Result<UserContext, AuthError> {
    let user_id = req.headers()
        .get("X-User-Id")
        .and_then(|h| h.to_str().ok())
        .ok_or_else(|| AuthError::Unauthorized("X-User-Id header missing".to_string()))?;

    let roles_str = req.headers()
        .get("X-User-Roles")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .split(',')
        .map(|r| r.trim().to_string())
        .collect();

    Ok(UserContext {
        id: user_id.to_string(),
        roles,
    })
}

// Usage in route handler
pub async fn create_partner(
    req: web::Json<CreatePartnerRequest>,
    pool: web::Data<PgPool>,
) -> Result<Partner, AuthError> {
    let user_context = extract_user_context(&req)?;
    let orchestrator = AdminOrchestrator::new(pool);

    orchestrator.create_partner(&user_context, req.into_inner()).await
}
```

---

## Research Conclusion

All research questions have been answered. The technical decisions align with the project constitution and provide a solid foundation for implementation. The following artifacts are ready for Phase 1:
- `data-model.md`: Entity definitions, database schema, and validation rules
- `contracts/api-contracts.md`: HTTP endpoint contracts with request/response examples
- `contracts/error-contracts.md`: Error handling contracts and status codes
- `quickstart.md`: Developer onboarding guide for Admin Service

All constitution gates remain passed. Proceed to Phase 1 design.
