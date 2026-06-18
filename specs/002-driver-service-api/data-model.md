# Data Model: Driver Service & Spatial API

## Overview

The driver-service reads from existing `inventory` schema tables via the Sprint 1.1 `gis.get_nearby_stations()` function. It does not define new database tables. This document covers the API response models and the configuration model.

---

## Response Models

### NearbyStation (returned by GET /api/v1/nearby)

| Field | Type | Description |
|-------|------|-------------|
| `station_id` | string | NanoID, `STA_` prefix |
| `station_name` | string | Display name |
| `latitude` | number (f64) | WGS84 latitude |
| `longitude` | number (f64) | WGS84 longitude |
| `distance_meters` | number (f64) | Geodesic distance from query point |
| `is_private` | boolean | True if home charger |
| `partner_name` | string \| null | Operator name (null if no partner) |

**Source**: Returned by `SELECT * FROM gis.get_nearby_stations(lng, lat, radius_meters)`

**Empty state**: `[]` (empty JSON array) when no stations match

**Error state**: HTTP 400 with `{"error": "description"}` on invalid input

---

### HealthStatus (returned by GET /health)

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | `"ok"` or `"degraded"` |

**Healthy**: `{"status": "ok"}` — HTTP 200, pool acquire succeeds within 500ms
**Degraded**: `{"status": "degraded"}` — HTTP 503, pool acquire fails or times out

---

### LogEntry (structured JSON log line)

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | string | ISO 8601 |
| `level` | string | `INFO`, `WARN`, `ERROR` |
| `method` | string | HTTP method |
| `path` | string | Request path |
| `status` | integer | HTTP status code |
| `duration_ms` | number | Request processing time |
| `trace_id` | string | Unique request identifier |

---

## Configuration Model

| Env Variable | Type | Default | Description |
|-------------|------|---------|-------------|
| `LISTEN_ADDR` | string | `0.0.0.0:3001` | Service bind address |
| `DATABASE_URL` | string | — | PostgreSQL connection string (required) |
| `DB_POOL_MIN` | integer | `1` | Minimum database connections |
| `DB_POOL_MAX` | integer | `10` | Maximum database connections |
| `CORS_ORIGINS` | string | `*` | Allowed CORS origins |
| `RUST_LOG` | string | `info` | Log level filter |

---

## State Transitions

The driver-service is stateless (read-only spatial queries). Its operational states:

```
STARTING → HEALTHY (pool acquired) ↔ DEGRADED (pool lost)
                  ↘ FAILED (startup timeout) → exit
```

- **STARTING**: Process launched, loading config, connecting to database
- **HEALTHY**: `/health` returns 200, requests served normally
- **DEGRADED**: `/health` returns 503, database unreachable; service still accepts requests and retries pool
- **FAILED**: Fatal startup error (invalid config, missing DATABASE_URL) — process exits
