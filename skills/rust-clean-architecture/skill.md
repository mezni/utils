# Rust Clean Architecture Skill — BorneMap

## Purpose
Enforce strict domain-driven, layered architecture for all Rust services.

---

## Layer Rules

### 1. API Layer (handlers)
- **ONLY HTTP parsing + response mapping**
- **NO business logic**
- **NO DB queries directly**

### 2. Service Layer
- **business logic only**
- **orchestrates domain operations**
- **does NOT touch database**

### 3. Repository Layer
- **ONLY database access**
- **no HTTP knowledge**
- **isolates PostGIS queries**

---

## Required Pattern

**handler → service → repository → database**

```
API Handler
    ↓ (parse + validation)
    ↓ (map to domain model)
Service
    ↓ (business logic)
    ↓ (orchestrate)
Repository
    ↓ (DB operations)
    ↓ (PostGIS queries)
Database
```

---

## Forbidden Patterns

- ❌ logic inside handlers
- ❌ SQL inside controllers
- ❌ shared global state
- ❌ direct schema coupling across services
- ❌ business logic in repository layer

---

## Required Patterns

### 1. Error Handling

**Use Result<T, DomainError> everywhere:**
```rust
// handlers/mod.rs
pub async fn handle_get_stations(
    db: PgPool,
) -> Result<ApiResponse<Vec<Station>>, ApiError> {
    let stations = stations_service::get_all(db).await?;
    Ok(ApiResponse::new(stations))
}

// services/stations.rs
pub async fn get_all(db: PgPool) -> Result<Vec<Station>, DomainError> {
    let stations = stations_repo::find_all(db).await?;
    Ok(stations)
}

// repositories/stations.rs
pub async fn find_all(pool: PgPool) -> Result<Vec<Station>, DomainError> {
    let rows = sqlx::query_as::<_, Station>("SELECT * FROM stations")
        .fetch_all(&pool)
        .await?;
    Ok(rows)
}
```

**Never panic in production code:**
```rust
// ❌ WRONG
let station = stations_repo::find_one(id).await.unwrap();

// ✅ CORRECT
let station = stations_repo::find_one(id).await
    .map_err(|e| DomainError::StationNotFound(id))?;
```

**Map errors to API responses centrally:**
```rust
// handlers/stations.rs
pub async fn handle_get_stations(
    db: PgPool,
) -> Result<ApiResponse<Vec<Station>>, ApiError> {
    stations_service::get_all(db).await
        .map_err(|e| match e {
            DomainError::DatabaseError(_) => ApiError::InternalServerError,
            DomainError::StationNotFound(_) => ApiError::NotFound,
        })
}
```

---

### 2. PostGIS Rules

**All geo queries must be isolated in repository layer:**
```rust
// ❌ WRONG - SQL in service layer
pub async fn find_nearby(
    db: PgPool,
    lat: f64,
    lng: f64,
    radius: i32,
) -> Result<Vec<Station>, DomainError> {
    let stations = sqlx::query_as::<_, Station>(
        "SELECT * FROM stations
         WHERE ST_DWithin(
           ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
           ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography,
           $5
         )",
    )
    .bind(lat)
    .bind(lng)
    .bind(lat)
    .bind(lng)
    .bind(radius)
    .fetch_all(db)
    .await?;
    Ok(stations)
}

// ✅ CORRECT - SQL only in repository
// repositories/stations.rs
pub async fn find_nearby(
    pool: PgPool,
    lat: f64,
    lng: f64,
    radius: i32,
) -> Result<Vec<Station>, DomainError> {
    let rows = sqlx::query_as::<_, Station>(
        "SELECT * FROM stations
         WHERE ST_DWithin(
           ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
           ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography,
           $5
         )",
    )
    .bind(lat)
    .bind(lng)
    .bind(lat)
    .bind(lng)
    .bind(radius)
    .fetch_all(&pool)
    .await?;
    Ok(rows)
}

// services/stations.rs
pub async fn find_nearby(
    db: PgPool,
    lat: f64,
    lng: f64,
    radius: i32,
) -> Result<Vec<Station>, DomainError> {
    stations_repo::find_nearby(db, lat, lng, radius).await
}
```

---

## Layer Responsibilities

### API Layer (handlers)
**Responsibilities:**
- Parse HTTP request (JSON, query params)
- Validate request format
- Transform to domain model
- Call service layer
- Map service errors to API responses
- Return HTTP response

**Example:**
```rust
pub async fn handle_get_stations(
    Path(query_params): Path<GetStationsQuery>,
) -> Result<ApiResponse<Vec<Station>>, ApiError> {
    let limit = query_params.limit.unwrap_or(20);
    let offset = query_params.offset.unwrap_or(0);

    stations_service::get_all(db, limit, offset).await
        .map_err(|e| map_error(e))
}
```

### Service Layer
**Responsibilities:**
- Implement business logic
- Coordinate domain operations
- Apply business rules
- Validate domain model invariants
- Call repository layer
- Transform domain models

**Example:**
```rust
pub async fn get_all(
    db: PgPool,
    limit: u32,
    offset: u32,
) -> Result<Vec<Station>, DomainError> {
    // Business logic
    if limit > 100 {
        return Err(DomainError::InvalidParameter {
            field: "limit",
            reason: "Maximum 100 items",
        });
    }

    // Delegate to repository
    stations_repo::find_all(db, limit, offset).await
}
```

### Repository Layer
**Responsibilities:**
- Database access only
- Execute SQL queries
- Map database rows to domain models
- Handle connection pooling
- Implement PostGIS queries
- Return domain models

**Example:**
```rust
pub async fn find_all(
    pool: PgPool,
    limit: u32,
    offset: u32,
) -> Result<Vec<Station>, DomainError> {
    sqlx::query_as::<_, Station>(
        "SELECT * FROM stations LIMIT $1 OFFSET $2"
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(&pool)
    .await
    .map_err(|e| DomainError::DatabaseError(e.to_string()))
}
```

---

## Architecture Verification

**Before writing ANY code:**

1. **Identify layer:**
   - Is this API logic? → Handler
   - Is this business logic? → Service
   - Is this DB access? → Repository

2. **Check responsibilities:**
   - Handler: Parse, validate, map, call service
   - Service: Business rules, orchestrate, validate
   - Repository: DB access only, PostGIS, return models

3. **Verify separation:**
   - Does handler have business logic? ❌
   - Does service have SQL? ❌
   - Does repository know about HTTP? ❌

4. **Check error handling:**
   - Are errors Result<T, DomainError>? ✅
   - Are panics avoided? ✅
   - Are errors mapped to API responses? ✅

5. **Verify PostGIS isolation:**
   - Is all SQL in repository? ✅
   - Is PostGIS only in repository? ✅
   - Is service layer PostGIS-free? ✅

---

## Quick Reference

### Handler Template
```rust
pub async fn handle_XXX(
    Path(query): Path<QueryType>,
    // other parameters
) -> Result<ApiResponse<ResponseType>, ApiError> {
    // 1. Parse and validate
    let validated = validate_query(query)?;

    // 2. Call service
    let result = XXX_service::XXX(validated).await
        .map_err(|e| map_error(e))?;

    // 3. Map to response
    Ok(ApiResponse::new(result))
}
```

### Service Template
```rust
pub async fn XXX(
    db: PgPool,
    // other dependencies
) -> Result<ResponseType, DomainError> {
    // 1. Apply business rules
    validate_business_rules()?;

    // 2. Delegate to repository
    XXX_repo::XXX(db).await
}
```

### Repository Template
```rust
pub async fn XXX(
    pool: PgPool,
    // other parameters
) -> Result<Vec<Station>, DomainError> {
    sqlx::query_as::<_, Station>("SQL QUERY")
        .bind(/* parameters */)
        .fetch_all(&pool)
        .await
        .map_err(|e| DomainError::DatabaseError(e.to_string()))
}
```

---

## Final Check

**Before committing code:**

- [ ] Logic in handler? ❌ → Move to service
- [ ] SQL in service? ❌ → Move to repository
- [ ] Database access in service? ❌ → Move to repository
- [ ] Error not Result<T, DomainError>? ❌ → Add proper error type
- [ ] Panics in production? ❌ → Use Result, handle errors
- [ ] PostGIS in service? ❌ → Move to repository

---

*This skill enforces domain-driven, layered architecture for Rust backend services in BorneMap.*