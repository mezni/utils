# Sprint 02 — Implementation Plan

**Status**: PLANNED
**Date**: 2026-06-24

---

## 1. Architecture Design

### System Context

```
 Client ──→ Traefik :80/:443 ──→ driver-service :3001
                                       │
                                       ├── GET /api/v1/health           (no DB)
                                       └── GET /api/v1/stations/nearby  (SQLx → PostgreSQL)
                                                                              │
                                                                              └── gis.find_nearby_stations()
```

### Service Impact Map

| Service | Port | Impact | Reason |
|---------|------|--------|--------|
| auth-service | 3000 | None | No changes |
| **driver-service** | **3001** | **New endpoints** | **This sprint's target** |
| admin-service | 3002 | None | No changes |

### Dependency Graph

```
Cargo.toml (axum + tokio + sqlx + serde)
    │
    ├── src/main.rs (server bootstrap)
    │
    ├── presentation/
    │   ├── routes.rs       (route registration)
    │   ├── health.rs       (GET /api/v1/health)
    │   ├── nearby.rs       (GET /api/v1/stations/nearby)
    │   └── dto.rs          (request/response types)
    │
    ├── application/
    │   └── get_nearby_stations.rs  (use-case orchestration)
    │
    ├── domain/
    │   ├── station.rs      (Station entity)
    │   └── errors.rs       (domain error types)
    │
    └── infrastructure/
        ├── db.rs           (database pool setup)
        └── repository.rs   (SQLx call to find_nearby_stations)
```

---

## 2. Clean Architecture Layer Mapping

### Domain Layer

```
station.rs:
  - Station struct { station_id, name, lat, lon, distance_km }
  - pure data, no deps

errors.rs:
  - NearbyError { InvalidLat, InvalidLon, InvalidRadius, InvalidLimit }
  - domain error enum, no DB/HTTP deps
```

### Application Layer

```
get_nearby_stations.rs:
  - NearbyQuery { lat, lon, radius: Option<i32>, limit: Option<i32> }
  - GetNearbyStationsUseCase { repo: impl StationRepository }
  - fn execute(query) -> Result<Vec<Station>, NearbyError>
  - orchestrates: validate → repo.find_nearby → return
```

### Infrastructure Layer

```
db.rs:
  - init_pool(database_url) -> PgPool
  - no business logic

repository.rs:
  - PgStationRepository { pool: PgPool }
  - fn find_nearby(&self, query) -> Result<Vec<Station>, SqlxError>
  - raw SQL: SELECT * FROM gis.find_nearby_stations($1, $2, $3, $4)
```

### Presentation Layer

```
routes.rs:
  - Router::new()
      .route("/api/v1/health", get(health_handler))
      .route("/api/v1/stations/nearby", get(nearby_handler))

health.rs:
  - GET /api/v1/health → 200 { status, service, version }

nearby.rs:
  - parse query params from URL
  - validate lat[-90,90], lon[-180,180], radius>0, limit[1,100]
  - call GetNearbyStationsUseCase
  - map result → JSON response
  - catch errors → 400/500 JSON error response

dto.rs:
  - NearbyRequest { lat, lon, radius: Option<i32>, limit: Option<i32> }
  - NearbyStationResponse { station_id, name, lat, lon, distance_km }
  - ErrorResponse { error: String }
```

---

## 3. API Contracts

### Health Check

```
GET /api/v1/health

Response 200:
{
  "status": "ok",
  "service": "driver-service",
  "version": "1.0.0"
}
```

### Nearby Stations

```
GET /api/v1/stations/nearby?lat=36.8&lon=10.1&radius=5000&limit=50

Response 200:
{
  "data": [
    {
      "station_id": "STA-abc123def456",
      "name": "Station Name",
      "lat": 36.8,
      "lon": 10.1,
      "distance_km": 1.23
    }
  ]
}

Response 400 (validation error):
{
  "error": "lat must be between -90 and 90"
}

Response 500 (internal error):
{
  "error": "internal server error"
}
```

### Validation Rules

| Param | Type | Bounds | Required |
|-------|------|--------|----------|
| `lat` | f64 | [-90.0, 90.0] | ✅ |
| `lon` | f64 | [-180.0, 180.0] | ✅ |
| `radius` | i32 | (0, ∞) | ❌ default 5000 |
| `limit` | i32 | [1, 100] | ❌ default 50 |

---

## 4. Testing Strategy

### Unit Tests

| Test | Scope |
|------|-------|
| Health response shape | presentation/health.rs |
| Parameter validation (all fields) | presentation/nearby.rs |
| Use-case: valid query → repository called | application/get_nearby_stations.rs |
| Use-case: invalid lat returns error | application/get_nearby_stations.rs |
| Domain: Station struct creation | domain/station.rs |

### Integration Tests

| Test | Scope |
|------|-------|
| DB function call returns stations | infrastructure/repository.rs |
| Empty result set → empty data array | e2e |
| Default radius/limit applied | e2e |

### Edge Cases

| Case | Expected |
|------|----------|
| Missing `lat` | 400 "missing required parameter: lat" |
| `lat=999` | 400 "lat must be between -90 and 90" |
| Zero results | 200 `{ "data": [] }` |
| `radius=0` | 400 "radius must be positive" |
| `limit=0` | 400 "limit must be between 1 and 100" |

---

## 5. Cargo Dependencies

```toml
[dependencies]
axum = "0.8"
tokio = { version = "1", features = ["full"] }
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tower-http = { version = "0.6", features = ["cors", "trace"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
thiserror = "2"
```

---

## 6. Risk Assessment

| Risk | Mitigation |
|------|------------|
| No PostgreSQL running | Service fails gracefully on startup |
| Missing `gis.find_nearby_stations()` function | Catch compile-time with SQLx prepare |
| Invalid coordinates from client | Strict validation before DB call |
| DB connection pool exhaustion | Configure pool size, add timeout |
| SQL injection via params | SQLx uses parameterized queries |
