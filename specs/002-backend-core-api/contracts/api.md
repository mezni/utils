# API Contract: MVP-1 Sprint 1 — Backend Core API

**Base URL**: `http://localhost:3000` (development) or `http://<container>:3000` (Docker)

**Path Prefix**: `/api/v1`

**Content-Type**: `application/json`

---

## `GET /api/v1/health`

Standard health check endpoint for Docker health checks and deployment orchestration.

### Response `200 OK`

```json
{
  "status": "ok",
  "database": "connected"
}
```

### Response `503`

Returned if database connectivity check fails.

```json
{
  "status": "error",
  "database": "disconnected"
}
```

---

## `GET /api/v1/stations`

Returns all stations from the database.

### Response `200 OK`

```json
[
  {
    "id": "STA-00001",
    "name": "STA-00001",
    "status": "active",
    "latitude": 36.7807266,
    "longitude": 10.1937043,
    "distance": 0.0
  },
  {
    "id": "STA-00002",
    "name": "STA-00002",
    "status": "active",
    "latitude": 35.3741719,
    "longitude": 7.2156482,
    "distance": 0.0
  }
]
```

### Response `503 Service Unavailable`

```json
{
  "error": {
    "code": "SERVICE_UNAVAILABLE",
    "message": "Database connection unavailable"
  }
}
```

---

## `GET /api/v1/stations/{id}`

Returns a single station by its unique identifier.

### Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `id` | string | Yes | Station ID (e.g., `STA-00001`) |

### Response `200 OK`

```json
{
  "id": "STA-00001",
  "name": "STA-00001",
  "status": "active",
  "latitude": 36.7807266,
  "longitude": 10.1937043,
  "distance": 0.0
}
```

### Response `404 Not Found`

```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "Station with id 'STA-99999' not found"
  }
}
```

### Response `400 Bad Request`

```json
{
  "error": {
    "code": "BAD_REQUEST",
    "message": "Invalid station ID format"
  }
}
```

---

## `GET /api/v1/stations/nearby`

Performs a PostGIS geospatial proximity search.

### Query Parameters

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `lat` | float | Yes | — | Latitude in decimal degrees (-90 to 90) |
| `lng` | float | Yes | — | Longitude in decimal degrees (-180 to 180) |
| `radius` | float | No | `5000` | Search radius in meters (must be > 0) |

### Response `200 OK` — stations found

```json
[
  {
    "id": "STA-00001",
    "name": "STA-00001",
    "status": "active",
    "latitude": 36.7807266,
    "longitude": 10.1937043,
    "distance": 1243.7
  }
]
```

`distance` is in meters from the query point (PostGIS `ST_Distance`).

### Response `200 OK` — empty result

```json
[]
```

### Response `400 Bad Request`

```json
{
  "error": {
    "code": "BAD_REQUEST",
    "message": "Parameter 'lat' is required"
  }
}
```

---

## Common Error Response Format

All error responses follow this shape:

```json
{
  "error": {
    "code": "ERROR_CODE",
    "message": "Human-readable description"
  }
}
```

| HTTP Status | Error Code | Meaning |
|---|---|---|
| 400 | `BAD_REQUEST` | Invalid parameters or request format |
| 404 | `NOT_FOUND` | Requested resource does not exist |
| 503 | `SERVICE_UNAVAILABLE` | Database or backend service unavailable |

---

## Rate Limiting

Not implemented in MVP-1. Connection pool (default 10-20 connections) provides implicit concurrency throttling.

## Authentication

None. Deferred to MVP-3.
