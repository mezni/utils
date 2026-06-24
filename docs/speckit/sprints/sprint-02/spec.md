# Sprint 02 — Driver-Service API Bootstrap (Health + Nearby Stations)

**Status**: SPEC WRITTEN (Phase 0)
**Date**: 2026-06-24
**Constitution Version**: v1.15.2

---

## Scope Lock (NON-NEGOTIABLE)

| Domain | Included | Excluded |
|--------|----------|----------|
| Service | `driver-service` :3001 | Any new services |
| Endpoints | `GET /api/v1/health`, `GET /api/v1/stations/nearby` | Any additional endpoints |
| DB | Uses existing `find_nearby_stations` function | No schema changes |
| OSM Importer | ❌ None | No changes |
| Frontend | ❌ None | No apps changes |

---

## API Design

### 1. Health Check

```
GET /api/v1/health
```

**Response:**
```json
{
  "status": "ok",
  "service": "driver-service",
  "version": "1.0.0"
}
```

### 2. Nearby Stations

```
GET /api/v1/stations/nearby?lat=36.8&lon=10.1&radius=5000&limit=50
```

**Query Parameters:**

| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| `lat` | ✅ | — | Latitude (decimal degrees) |
| `lon` | ✅ | — | Longitude (decimal degrees) |
| `radius` | ❌ | 5000 | Search radius in meters |
| `limit` | ❌ | 50 | Max results |

**Response (200):**
```json
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
```

**Error (400):**
```json
{
  "error": "missing required parameter: lat"
}
```

---

## Architecture Compliance

| Constitution Rule | Check | Status |
|------------------|-------|--------|
| §2.1 No new services | Uses existing `driver-service` | ✅ |
| §3 Service topology | No topology change | ✅ |
| §4.1 Schema ownership | No DB writes to non-owned schemas | ✅ |
| §7 Rust Clean Architecture | domain/application/infrastructure/presentation | ✅ |
| §8 API Ownership | Nearby is driver-service owned (§8) | ✅ |
| §14 SQLx compile | Required in CI | ✅ |
| §19 KNOWN-003 | Nearby endpoint belongs to driver-service | ✅ |

---

## Business Rules

- Uses existing `gis.find_nearby_stations()` PostgreSQL function
- `radius` defaults to 5000 meters
- `limit` defaults to 50
- Results ordered by distance (handled by DB)
- Input validation: lat[-90,90], lon[-180,180], radius > 0, limit[1,100]
- No internal DB errors exposed to client

---

## Service Location

```
/source/services/driver-service/
├── Cargo.toml
├── src/
│   ├── main.rs
│   ├── domain/
│   │   ├── mod.rs
│   │   ├── station.rs
│   │   └── errors.rs
│   ├── application/
│   │   ├── mod.rs
│   │   └── get_nearby_stations.rs
│   ├── infrastructure/
│   │   ├── mod.rs
│   │   ├── db.rs
│   │   └── repository.rs
│   └── presentation/
│       ├── mod.rs
│       ├── routes.rs
│       ├── health.rs
│       ├── nearby.rs
│       └── dto.rs
```

---

## Dependencies

| Dependency | Purpose |
|-----------|---------|
| axum | HTTP framework |
| tokio | Async runtime |
| sqlx (postgres) | Database access |
| serde / serde_json | Serialization |
| tower-http | CORS, tracing middleware |
| tracing | Structured logging |

---

## Hard Stops

- New service introduced → HALT
- DB schema modified → HALT
- Business logic added outside `driver-service` → HALT
- SQL function altered → HALT
- Architecture layers violated → HALT
- Scope expansion → HALT
