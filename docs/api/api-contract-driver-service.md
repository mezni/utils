# Driver Service API Contract

## Overview

This document defines the contract between the Driver Service and its clients (Driver application and external services).

## Endpoints

### 1. Health Check

**Path**: `GET /api/v1/health`

**Purpose**: Verify service and database connection status.

**Authentication**: None (Sprint 1.3)

**Request**: No parameters

**Response (200 OK)**:
```json
{
  "status": "ok",
  "service": "driver-service",
  "db": "ok"
}
```

**Response (500 Internal Server Error)**:
```json
{
  "error": "Database connection failed"
}
```

**Response (503 Service Unavailable)**:
```json
{
  "error": "Service not running"
}
```

**Success Criteria**:
- Returns 200 with correct JSON when database is available
- Returns 500 when database connection fails
- Returns 503 when service is not running

**Error Handling**:
- Database connection failure returns 500 Internal Server Error
- Service unavailability returns 503 Service Unavailable
- No specific error messages (future: add codes and user-facing messages)

---

### 2. Stations Nearby

**Path**: `GET /api/v1/stations/nearby`

**Purpose**: Find charging stations within a geographic radius.

**Authentication**: None (Sprint 1.3)

**Request Parameters**:

| Parameter | Type | Required | Default | Constraints |
|-----------|------|----------|----------|-------------|
| lat | f64 | Yes | - | -90 to 90 |
| lng | f64 | Yes | - | -180 to 180 |
| radius_km | f64 | Yes | - | 0.1 to 100 |

**Request Example**:
```
GET /api/v1/stations/nearby?lat=36.8188&lng=10.1657&radius_km=5
```

**Response (200 OK)**:
```json
{
  "stations": [
    {
      "id": "STN-1a2b",
      "name": "Tunis-Belvedere Station",
      "latitude": 36.864702,
      "longitude": 10.158423,
      "distance_km": 1.2
    },
    {
      "id": "STN-2c3d",
      "name": "Hammamet Station",
      "latitude": 36.846200,
      "longitude": 10.180000,
      "distance_km": 2.5
    }
  ]
}
```

**Response (400 Bad Request)**:
```json
{
  "error": "Invalid parameters: latitude must be between -90 and 90"
}
```

**Response (500 Internal Server Error)**:
```json
{
  "error": "Database query failed"
}
```

**Success Criteria**:
- Returns 200 with stations array when stations are within radius
- Returns empty array when no stations within radius
- Returns 400 for invalid parameters
- Returns 500 for database errors
- Stations sorted by distance ascending
- Distance calculated from query point to each station

**Error Handling**:
- Invalid parameters return 400 Bad Request
- Database errors return 500 Internal Server Error

---

## Data Types

### Station (Response Schema)

| Field | Type | Description |
|-------|------|-------------|
| id | String | Station NanoID (STN-...) |
| name | String | Station display name |
| latitude | f64 | Latitude in degrees |
| longitude | f64 | Longitude in degrees |
| distance_km | f64 | Distance from query point |

---

## Versioning

**Version**: v1

**Strategy**: Major version bump for breaking changes, minor for new endpoints/fields, patch for bug fixes

**Example**: v1 (current), v2 (if needed)

---

## Concurrency

- Requests are handled asynchronously via Actix-web
- No global state, each request is independent
- Database connections are pooled (PgPool from ev-db)

---

## Future Versions (Sprint 2.x)

**Authentication**: Add Bearer token header, 401 Unauthorized for invalid tokens

**Pagination**: Add `page` and `page_size` query parameters, response includes `page` and `total_pages`

**Filters**: Add optional query parameters for partner_id, status, connector_type

**Detailed Errors**: Add error code field (INVALID_PARAMETER, DATABASE_ERROR, etc.)

**Caching**: No caching in v1 (future: add Redis cache)

---

## Testing

**Unit Tests**: Test request validation (invalid lat/lng/radius values)

**Integration Tests**: Test endpoint behavior with seeded database (fixtures or using migrations)

**Performance Tests**: Verify <200ms response time for nearby query with 15 stations

**Test Files**:
- `tests/integration_test.rs` - Actix-web integration tests
- `tests/sql/test_stations_nearby.sql` - SQL for test fixtures and query tests