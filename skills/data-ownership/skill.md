# Data Ownership Skill — BorneMap

## Purpose
Prevent cross-service database corruption through strict data ownership rules.

---

## 🎯 Core Philosophy

**Each service owns its data. No service touches another service's data without a contract.**

---

## 🚫 The Problem

**Cross-service database corruption happens when:**
- Services access databases they don't own
- Services modify data they shouldn't
- Services read data they shouldn't
- No clear data boundaries

---

## 🔒 Core Rules

### 1. Each Service Owns Its Schema

**Explicit data ownership:**

```rust
// ❌ WRONG - Cross-service access
// admin-service accessing driver-service database
pub async fn handle_update_station(
    Path(id): Path<String>,
    UpdateStationRequest { name }: UpdateStationRequest,
) -> Result<ApiResponse<StationDto>, ApiError> {
    // ❌ Direct access to driver-service database!
    let row = sqlx::query_as::<_, StationRow>(
        "SELECT * FROM stations WHERE id = $1"
    )
    .bind(&id)
    .fetch_one(driver_service_db)
    .await?;

    // ❌ Modifying data outside its ownership
    sqlx::query(
        "UPDATE stations SET name = $1, updated_at = NOW() WHERE id = $2"
    )
    .bind(&name)
    .bind(&id)
    .execute(driver_service_db)
    .await?;

    Ok(ApiResponse::new(stations_service::get_one(id).await?))
}
```

```rust
// ✅ CORRECT - Each service owns its data
// admin-service owns its own database
pub async fn handle_update_station(
    Path(id): Path<String>,
    UpdateStationRequest { name }: UpdateStationRequest,
) -> Result<ApiResponse<StationDto>, ApiError> {
    // ✅ Only admin-service database
    let row = sqlx::query_as::<_, AdminStationRow>(
        "SELECT * FROM admin.stations WHERE id = $1"
    )
    .bind(&id)
    .fetch_one(admin_service_db)
    .await?;

    // ✅ Only admin-service modifications
    sqlx::query(
        "UPDATE admin.stations SET name = $1, updated_at = NOW() WHERE id = $2"
    )
    .bind(&name)
    .bind(&id)
    .execute(admin_service_db)
    .await?;

    // ✅ Returns data from its own database
    Ok(ApiResponse::new(admin_station_to_station(row)))
}
```

---

### 2. No Cross-Schema Writes

**Only owner can write to a schema:**

```rust
// ❌ WRONG - Cross-schema write
pub async fn handle_delete_station(
    Path(id): Path<String>,
) -> Result<ApiResponse<()>, ApiError> {
    // ❌ Deleting from inventory schema (admin-service owns admin schema)
    sqlx::query(
        "DELETE FROM inventory.stations WHERE id = $1"
    )
    .bind(&id)
    .execute(admin_service_db)
    .await?;

    // ❌ Write to wrong schema
    Ok(ApiResponse::new(()))
}

// ✅ CORRECT - Only own schema
pub async fn handle_delete_station(
    Path(id): Path<String>,
) -> Result<ApiResponse<()>, ApiError> {
    // ✅ Deleting from admin schema (admin-service owns it)
    sqlx::query(
        "DELETE FROM admin.stations WHERE id = $1"
    )
    .bind(&id)
    .execute(admin_service_db)
    .await?;

    // ✅ Only own schema modifications
    Ok(ApiResponse::new(()))
}
```

---

### 3. GIS Schema is Read-Only

**GIS is system ingestion only:**

```rust
// ❌ WRONG - GIS schema modification
pub async fn handle_import_station(
    ImportStationRequest { name, geom }: ImportStationRequest,
) -> Result<ApiResponse<()>, ApiError> {
    // ❌ Modifying GIS schema!
    sqlx::query(
        "INSERT INTO gis.station_seed (name, geom) VALUES ($1, $2)"
    )
    .bind(&name)
    .bind(&geom)
    .execute(gis_db)
    .await?;

    Ok(ApiResponse::new(()))
}

// ✅ CORRECT - GIS is read-only
pub async fn handle_import_station(
    ImportStationRequest { name, geom }: ImportStationRequest,
) -> Result<ApiResponse<()>, ApiError> {
    // ✅ GIS is system ingestion only
    // This operation is performed by ETL scripts, not runtime services
    // No runtime service touches GIS schema
    Err(ApiError::Forbidden("GIS schema is read-only".into()))
}
```

---

### 4. Analytics Schema is Append-Only

**Analytics is write-only:**

```rust
// ❌ WRONG - Analytics schema modification
pub async fn handle_update_analytics_event(
    Path(event_id): Path<String>,
    UpdateEventRequest { payload }: UpdateEventRequest,
) -> Result<ApiResponse<()>, ApiError> {
    // ❌ Modifying analytics events (append-only!)
    sqlx::query(
        "UPDATE analytics.events SET payload = $1 WHERE id = $2"
    )
    .bind(&payload)
    .bind(&event_id)
    .execute(analytics_db)
    .await?;

    Ok(ApiResponse::new(()))
}

// ❌ WRONG - Analytics deletion
pub async fn handle_delete_analytics_event(
    Path(event_id): Path<String>,
) -> Result<ApiResponse<()>, ApiError> {
    // ❌ Deleting from analytics!
    sqlx::query(
        "DELETE FROM analytics.events WHERE id = $1"
    )
    .bind(&event_id)
    .execute(analytics_db)
    .await?;

    Ok(ApiResponse::new(()))
}

// ✅ CORRECT - Analytics is append-only
pub async fn handle_track_event(
    TrackEventRequest { event }: TrackEventRequest,
) -> Result<ApiResponse<()>, ApiError> {
    // ✅ Only insert operations
    sqlx::query(
        "INSERT INTO analytics.events (type, payload) VALUES ($1, $2)"
    )
    .bind(&event.type)
    .bind(&event.payload)
    .execute(analytics_db)
    .await?;

    // ✅ Only insert, no update or delete
    Ok(ApiResponse::new(()))
}
```

---

### 5. No Shared DB Logic Between Services

**Services must be independent:**

```rust
// ❌ WRONG - Shared logic in repository
pub async fn get_station(
    id: String,
    db: PgPool,
) -> Result<StationDto, DomainError> {
    // ❌ Mixed logic from multiple services
    if id.starts_with("STA-") {
        // ❌ Driver-service logic
        return driver_service::get_station(id, db).await;
    } else if id.starts_with("USR-") {
        // ❌ Admin-service logic
        return admin_service::get_station(id, db).await;
    }
    // ❌ Ambiguous data ownership
}
```

```rust
// ✅ CORRECT - Each service owns its logic
pub async fn handle_get_station(
    Path(id): Path<String>,
) -> Result<ApiResponse<StationDto>, ApiError> {
    // ✅ Driver-service only
    let station = driver_service::get_station(id).await
        .map_err(|e| map_error(e))?;

    Ok(ApiResponse::new(station))
}

pub async fn handle_get_station(
    Path(id): Path<String>,
) -> Result<ApiResponse<StationDto>, ApiError> {
    // ✅ Admin-service only
    let station = admin_service::get_station(id).await
        .map_err(|e| map_error(e))?;

    Ok(ApiResponse::new(station))
}
```

---

## 📊 Data Ownership Matrix

| Schema | Owner | Read Access | Write Access | Services |
|--------|-------|-------------|--------------|----------|
| **inventory** | admin-service | driver-service | admin-service | driver-service (read), admin-service (read/write) |
| **gis** | Ingestion | driver-service, admin-service | ingestion scripts | driver-service (read-only), admin-service (read-only) |
| **users** | auth-service | driver-service (read), admin-service (read) | auth-service (write) | auth-service (write), driver-service (read), admin-service (read) |
| **analytics** | All services | None | All services (write-only) | All services (write-only) |
| **keycloak** | Keycloak internal | None | Keycloak internal | auth-service (gateway only) |

---

## 🔒 Ownership Rules

### strict rules

**1. Driver Service:**
- ✅ Owns: `inventory.station` (read)
- ✅ Owns: `inventory.charger` (read)
- ✅ Owns: `analytics.events` (write)
- ❌ Cannot modify: `gis` schema
- ❌ Cannot modify: `users` schema
- ❌ Cannot modify: `keycloak_db`

**2. Admin Service:**
- ✅ Owns: `admin.station` (read/write)
- ✅ Owns: `admin.charger` (read/write)
- ✅ Owns: `users` (read)
- ❌ Cannot modify: `inventory` schema (owned by driver-service)
- ❌ Cannot modify: `gis` schema
- ❌ Cannot modify: `keycloak_db`

**3. Auth Service:**
- ✅ Owns: `users` schema (write)
- ✅ Owns: `keycloak_db` (gateway only)
- ❌ Cannot modify: `inventory` schema
- ❌ Cannot modify: `gis` schema
- ❌ Cannot modify: `analytics` schema

**4. All Services:**
- ✅ Write to: `analytics` (write-only)
- ❌ Cannot modify: `gis` schema (read-only)
- ❌ Cannot modify: `keycloak_db` (internal)

---

## 🚫 Forbidden Patterns

### 1. Cross-Schema Writes

```rust
// ❌ WRONG
pub async fn handle_update_station(
    Path(id): Path<String>,
) -> Result<ApiResponse<()>, ApiError> {
    sqlx::query("UPDATE inventory.stations SET ...").execute(/*...*/);
    // ❌ Modifying inventory from admin-service
}
```

### 2. Cross-Schema Reads

```rust
// ❌ WRONG
pub async fn handle_get_station(
    Path(id): Path<String>,
) -> Result<ApiResponse<StationDto>, ApiError> {
    // ❌ Reading inventory from admin-service
    let row = sqlx::query_as::<_, StationRow>(
        "SELECT * FROM inventory.stations WHERE id = $1"
    )
    .bind(&id)
    .fetch_one(admin_service_db)
    .await?;
}
```

### 3. Analytics Modification

```rust
// ❌ WRONG
pub async fn handle_update_analytics(
    Path(id): Path<String>,
) -> Result<ApiResponse<()>, ApiError> {
    // ❌ Updating analytics events
    sqlx::query("UPDATE analytics.events SET ...").execute(/*...*/);
}
```

### 4. GIS Modification

```rust
// ❌ WRONG
pub async fn handle_import_station(
    ImportStationRequest { /*...*/ },
) -> Result<ApiResponse<()>, ApiError> {
    // ❌ Modifying GIS schema
    sqlx::query("INSERT INTO gis.station_seed (/*...*/)").execute(/*...*/);
}
```

---

## 🎯 Data Ownership Checklist

**Before ANY database operation:**

- [ ] Confirm service owns schema
- [ ] Verify read/write permissions
- [ ] Check if write operation is allowed
- [ ] Validate data ownership
- [ ] Verify no cross-service access
- [ ] Confirm no schema violations

---

## 📊 Data Ownership Validation

### Pre-Operation Check

```
1. Identify Schema
   ↓
2. Check Service Ownership
   - Does this service own the schema?
   - Or does another service own it?
   ↓
3. Verify Access Rights
   - Read access allowed?
   - Write access allowed?
   ↓
4. Validate Operation Type
   - Is this a read operation?
   - Or a write operation?
   - Is write operation allowed?
   ↓
5. Confirm No Cross-Service Access
   - Is this service touching another service's schema?
   - Is write operation allowed in this schema?
   ↓
6. Execute Operation
   - ✅ If all checks passed
   - ❌ If any check failed
```

---

## 🚦 Data Ownership Enforcement

### Cross-Schema Access Detected

**Block and require ADR:**

```markdown
# ADR-XXX: Cross-Schema Access Detection

## Context
LLM attempted to access [schema] from [service]

## Decision
❌ BLOCKED - Cross-schema access not allowed

## Rationale
- [Schema] owned by [service]
- [Service] cannot access [schema]
- Architecture requires clear boundaries

## Required Action
- Remove cross-schema access
- Implement proper API endpoints
- Update data ownership matrix
```

---

## 🔄 Data Ownership Matrix Update

**When adding new services or schemas:**

1. **Add to Data Ownership Matrix:**
   - New service → Owns which schemas
   - New schemas → Owned by which service
   - Access permissions

2. **Update Cross-Schema Rules:**
   - Define what cross-access is allowed
   - Define what cross-access is blocked
   - Define what cross-access requires ADR

3. **Update Validation Logic:**
   - Add cross-schema access checks
   - Block prohibited access
   - Require ADR for approved access

---

## 🧹 Data Ownership Compliance

### Current Compliance

**Services:**
- ✅ Driver-service owns inventory (read), analytics (write)
- ✅ Admin-service owns admin schemas
- ✅ Auth-service owns users (write)
- ✅ All services write only to analytics

**Schemas:**
- ✅ GIS schema is read-only
- ✅ Analytics schema is append-only
- ✅ Keycloak database is internal only

**Cross-Service Access:**
- ✅ Driver-service reads inventory
- ✅ Admin-service reads users
- ✅ No cross-schema writes
- ✅ No cross-schema unauthorized reads

---

*This skill prevents cross-service database corruption through strict data ownership.*