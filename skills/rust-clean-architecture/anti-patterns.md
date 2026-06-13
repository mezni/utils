# Rust Clean Architecture — Anti-Patterns

## What to Avoid at All Costs

### 1. Fat Controllers (handlers)

**❌ WRONG — Logic in handlers:**
```rust
// handlers/stations.rs
pub async fn handle_get_stations(
    Path(params): Path<GetStationsQuery>,
    db: PgPool,
) -> Result<ApiResponse<Vec<Station>>, ApiError> {
    // ❌ Business logic here!
    let limit = params.limit.unwrap_or(20);

    if limit > 100 {
        return Err(ApiError::BadRequest("Limit too high".into()));
    }

    // ❌ SQL here!
    let rows = sqlx::query_as::<_, Station>(
        "SELECT * FROM stations LIMIT $1 OFFSET $2"
    )
    .bind(limit)
    .bind(params.offset.unwrap_or(0))
    .fetch_all(db)
    .await?;

    // ❌ SQL here!
    let stations = rows.into_iter().map(|row| {
        Station {
            id: row.id,
            name: row.name,
            // ❌ Business logic here!
            latitude: row.latitude,
            longitude: row.longitude,
            status: if row.latitude > 90 || row.latitude < -90 {
                "inactive".to_string()
            } else {
                "active".to_string()
            },
            power_kw: row.power_kw,
            connector_types: row.connector_types.unwrap_or_default(),
        }
    }).collect();

    Ok(ApiResponse::new(stations))
}
```

**✅ CORRECT — Clean separation:**
```rust
// handlers/stations.rs
pub async fn handle_get_stations(
    Path(params): Path<GetStationsQuery>,
) -> Result<ApiResponse<Vec<Station>>, ApiError> {
    stations_service::get_all(params).await
        .map_err(|e| map_error(e))
}

// services/stations.rs
pub async fn get_all(
    params: GetStationsQuery,
) -> Result<Vec<Station>, DomainError> {
    let validated = validate_params(&params)?;
    let stations = stations_repo::find_all(validated).await?;
    Ok(stations)
}

// repositories/stations.rs
pub async fn find_all(
    validated: GetStationsQuery,
) -> Result<Vec<Station>, DomainError> {
    let rows = sqlx::query_as::<_, StationRow>(
        "SELECT * FROM stations LIMIT $1 OFFSET $2"
    )
    .bind(validated.limit)
    .bind(validated.offset)
    .fetch_all(&pool)
    .await?;

    Ok(rows)
}
```

---

### 2. Mixed DB + Logic Layers

**❌ WRONG — Service knows about SQL:**
```rust
// services/stations.rs
pub async fn find_nearby(
    db: PgPool,
    lat: f64,
    lng: f64,
    radius: i32,
) -> Result<Vec<Station>, DomainError> {
    // ❌ SQL in service layer!
    let stations = sqlx::query_as::<_, Station>(
        "SELECT * FROM stations
         WHERE ST_DWithin(
           ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
           ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography,
           $5
         )
         ORDER BY ST_Distance(
           ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
           ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography
         ) ASC"
    )
    .bind(lat)
    .bind(lng)
    .bind(lat)
    .bind(lng)
    .bind(radius)
    .fetch_all(db)
    .await?;

    // ❌ Business logic in service that also has SQL!
    let stations = stations.into_iter().map(|mut s| {
        s.distance = calculate_distance(lat, lng, s.latitude, s.longitude);
        s
    }).collect();

    Ok(stations)
}
```

**✅ CORRECT — Pure business logic in service, SQL in repository:**
```rust
// services/stations.rs
pub async fn find_nearby(
    db: PgPool,
    lat: f64,
    lng: f64,
    radius: i32,
) -> Result<Vec<Station>, DomainError> {
    let stations = stations_repo::find_nearby(db, lat, lng, radius).await?;

    // ✅ Business logic only, no SQL!
    let stations = stations.into_iter().map(|mut s| {
        s.distance = calculate_distance(lat, lng, s.latitude, s.longitude);
        s
    }).collect();

    Ok(stations)
}

// repositories/stations.rs
pub async fn find_nearby(
    pool: PgPool,
    lat: f64,
    lng: f64,
    radius: i32,
) -> Result<Vec<Station>, DomainError> {
    // ✅ SQL only in repository!
    let rows = sqlx::query_as::<_, StationRow>(
        "SELECT * FROM stations
         WHERE ST_DWithin(
           ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
           ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography,
           $5
         )"
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
```

---

### 3. Ad-hoc SQL Queries in Handlers

**❌ WRONG — SQL everywhere:**
```rust
// handlers/stations.rs
pub async fn handle_get_station(
    Path(id): Path<String>,
) -> Result<ApiResponse<Station>, ApiError> {
    // ❌ Ad-hoc SQL in handler!
    let row = sqlx::query_as::<_, StationRow>(
        "SELECT * FROM stations WHERE id = $1"
    )
    .bind(&id)
    .fetch_one(db)
    .await?;

    // ❌ More ad-hoc SQL!
    let chargers = sqlx::query_as::<_, ChargerRow>(
        "SELECT * FROM chargers WHERE station_id = $1"
    )
    .bind(&row.id)
    .fetch_all(db)
    .await?;

    Ok(ApiResponse::new(Station {
        id: row.id,
        name: row.name,
        latitude: row.latitude,
        longitude: row.longitude,
        // ... more fields
    }))
}
```

**✅ CORRECT — Delegation to repository:**
```rust
// handlers/stations.rs
pub async fn handle_get_station(
    Path(id): Path<String>,
) -> Result<ApiResponse<Station>, ApiError> {
    stations_service::get_by_id(id).await
        .map_err(|e| map_error(e))
}

// services/stations.rs
pub async fn get_by_id(
    id: String,
) -> Result<Station, DomainError> {
    stations_repo::find_one(id).await
}

// repositories/stations.rs
pub async fn find_one(
    id: String,
) -> Result<Station, DomainError> {
    let station_row = sqlx::query_as::<_, StationRow>(
        "SELECT * FROM stations WHERE id = $1"
    )
    .bind(&id)
    .fetch_one(&pool)
    .await?;

    // ✅ Delegates to repository for chargers
    let chargers = chargers_repo::find_by_station_id(id.clone()).await?;

    Ok(Station {
        id: station_row.id,
        name: station_row.name,
        latitude: station_row.latitude,
        longitude: station_row.longitude,
        // ... more fields
        chargers,
    })
}
```

---

### 4. Ignoring Repository Abstraction

**❌ WRONG — Direct schema coupling:**
```rust
// services/stations.rs
pub async fn update_station(
    db: PgPool,
    id: String,
    name: String,
) -> Result<Station, DomainError> {
    // ❌ Direct schema coupling!
    let _ = sqlx::query(
        "UPDATE stations SET name = $1, updated_at = NOW() WHERE id = $2"
    )
    .bind(&name)
    .bind(&id)
    .execute(db)
    .await?;

    // ❌ Returns domain model, but schema coupling exists
    self.find_one(id).await
}
```

**✅ CORRECT — Pure abstraction:**
```rust
// services/stations.rs
pub async fn update_station(
    db: PgPool,
    id: String,
    name: String,
) -> Result<Station, DomainError> {
    stations_repo::update_name(id, name).await?;
    stations_repo::find_one(id).await
}

// repositories/stations.rs
pub async fn update_name(
    id: String,
    name: String,
) -> Result<(), DomainError> {
    sqlx::query(
        "UPDATE stations SET name = $1, updated_at = NOW() WHERE id = $2"
    )
    .bind(&name)
    .bind(&id)
    .execute(&pool)
    .await
    .map_err(|e| DomainError::DatabaseError(e.to_string()))?;

    Ok(())
}
```

---

### 5. Unwrapping Panics

**❌ WRONG — Production panics:**
```rust
// repositories/stations.rs
pub async fn find_one(
    id: String,
) -> Result<Station, DomainError> {
    let row = sqlx::query_as::<_, StationRow>(
        "SELECT * FROM stations WHERE id = $1"
    )
    .bind(&id)
    .fetch_one(&pool)
    .await
    .unwrap(); // ❌ Panics on error!
}
```

**✅ CORRECT — Proper error handling:**
```rust
// repositories/stations.rs
pub async fn find_one(
    id: String,
) -> Result<Station, DomainError> {
    sqlx::query_as::<_, StationRow>(
        "SELECT * FROM stations WHERE id = $1"
    )
    .bind(&id)
    .fetch_one(&pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => DomainError::StationNotFound(id.clone()),
        e => DomainError::DatabaseError(e.to_string()),
    })?
}
```

---

### 6. Global State

**❌ WRONG — Global state coupling:**
```rust
// services/stations.rs
static DB_POOL: Lazy<PgPool> = Lazy::new(|| {
    PgPool::connect("postgres://...").unwrap()
});

pub async fn get_all() -> Result<Vec<Station>, DomainError> {
    // ❌ Global state, hard to test
    let rows = sqlx::query_as::<_, StationRow>(
        "SELECT * FROM stations"
    )
    .fetch_all(&*DB_POOL)
    .await?;
    Ok(rows)
}
```

**✅ CORRECT — Dependency injection:**
```rust
// services/stations.rs
pub async fn get_all(
    db: PgPool,
) -> Result<Vec<Station>, DomainError> {
    // ✅ Dependency injection, easy to test
    let rows = sqlx::query_as::<_, StationRow>(
        "SELECT * FROM stations"
    )
    .fetch_all(&db)
    .await?;
    Ok(rows)
}
```

---

### 7. PostGIS Logic in Service Layer

**❌ WRONG — PostGIS in service:**
```rust
// services/stations.rs
pub async fn find_nearby(
    db: PgPool,
    lat: f64,
    lng: f64,
    radius: i32,
) -> Result<Vec<Station>, DomainError> {
    // ❌ PostGIS logic in service!
    let stations = sqlx::query_as::<_, Station>(
        "SELECT * FROM stations
         WHERE ST_DWithin(
           ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
           ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography,
           $5
         )
         ORDER BY ST_Distance(
           ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
           ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography
         ) ASC"
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
```

**✅ CORRECT — PostGIS only in repository:**
```rust
// services/stations.rs
pub async fn find_nearby(
    db: PgPool,
    lat: f64,
    lng: f64,
    radius: i32,
) -> Result<Vec<Station>, DomainError> {
    // ✅ Service only knows about domain logic
    stations_repo::find_nearby(db, lat, lng, radius).await
}

// repositories/stations.rs
pub async fn find_nearby(
    pool: PgPool,
    lat: f64,
    lng: f64,
    radius: i32,
) -> Result<Vec<Station>, DomainError> {
    // ✅ PostGIS logic isolated in repository
    let rows = sqlx::query_as::<_, StationRow>(
        "SELECT * FROM stations
         WHERE ST_DWithin(
           ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
           ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography,
           $5
         )"
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
```

---

## Prevention Checklist

Before committing code:

- [ ] Logic in handler? ❌ → Move to service
- [ ] SQL in service? ❌ → Move to repository
- [ ] Direct DB access in handler? ❌ → Move to repository
- [ ] Unwrap() in production? ❌ → Use proper error handling
- [ ] Global state? ❌ → Use dependency injection
- [ ] PostGIS in service? ❌ → Move to repository
- [ ] Mixed responsibilities? ❌ → Separate layers

---

*These anti-patterns prevent architecture decay and maintain clean, testable code.*