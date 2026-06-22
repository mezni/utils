# API Contracts: GIS Engine Foundation

**Feature**: 003 - GIS Engine Foundation
**Version**: 1.0.0
**Date**: 2026-06-22

## Overview

This document defines the API contracts for the GIS engine foundation, including spatial queries and station data retrieval endpoints.

## Versioning Strategy

- API version: v1.0.0
- Version in URL: `/api/v1/`
- Breaking changes: Will increment version number (e.g., v1.0.0 → v2.0.0)

## Base URL

**Driver Service**: `http://localhost:3001`

## Authentication

**Method**: JWT Bearer Token
**Header**: `Authorization: Bearer <JWT_TOKEN>`

All endpoints except `/health` require authentication.

---

## Endpoints

### GET /api/v1/driver/nearby

**Description**: Find charging stations within a specified radius of a point.

**Authentication**: Required (JWT)

**Parameters**:
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| lat | double | YES | - | Latitude of center point |
| lon | double | YES | - | Longitude of center point |
| radius | integer | YES | - | Radius in meters (min: 100, max: 100000) |
| limit | integer | NO | 20 | Number of results to return (max: 100) |

**Example Request**:
```http
GET /api/v1/driver/nearby?lat=40.7829&lon=-73.9654&radius=1000&limit=10 HTTP/1.1
Authorization: Bearer <JWT_TOKEN>
```

**Success Response** (200 OK):
```json
{
  "data": [
    {
      "id": "STA-abc123456789",
      "name": "Central Park Charging",
      "latitude": 40.7829,
      "longitude": -73.9654,
      "distance": 123.5,
      "amenity": "charging_station",
      "power": "50kW",
      "connector_types": ["Type 2", "CCS"],
      "is_available": true,
      "last_updated": "2026-06-22T10:30:00Z"
    }
  ],
  "query": {
    "lat": 40.7829,
    "lon": -73.9654,
    "radius": 1000,
    "limit": 10
  }
}
```

**Error Response** (400 Bad Request):
```json
{
  "error": "invalid_parameter",
  "message": "Radius must be between 100 and 100000 meters",
  "field": "radius"
}
```

**Error Response** (401 Unauthorized):
```json
{
  "error": "unauthorized",
  "message": "Missing or invalid Authorization header"
}
```

**Error Response** (422 Unprocessable Entity):
```json
{
  "error": "invalid_coordinates",
  "message": "Latitude must be between -90 and 90"
}
```

**Response Time**: < 500ms (without cache), < 50ms (with cache)

**Rate Limit**: 100 requests per minute (per user)

---

### GET /api/v1/driver/stations

**Description**: List all charging stations with pagination.

**Authentication**: Required (JWT)

**Parameters**:
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| page | integer | NO | 1 | Page number (1-indexed) |
| limit | integer | NO | 20 | Items per page (max: 100) |
| lat | double | YES | - | Latitude center point (for pagination) |
| lon | double | YES | - | Longitude center point (for pagination) |
| radius | integer | YES | - | Radius in meters (min: 100, max: 100000) |

**Example Request**:
```http
GET /api/v1/driver/stations?page=1&limit=20&lat=40.7829&lon=-73.9654&radius=5000 HTTP/1.1
Authorization: Bearer <JWT_TOKEN>
```

**Success Response** (200 OK):
```json
{
  "data": [
    {
      "id": "STA-abc123456789",
      "name": "Central Park Charging",
      "latitude": 40.7829,
      "longitude": -73.9654,
      "amenity": "charging_station",
      "power": "50kW",
      "connector_types": ["Type 2", "CCS"],
      "is_available": true
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "total": 100,
    "total_pages": 5,
    "has_next": true,
    "has_prev": false
  }
}
```

**Error Response** (400 Bad Request):
```json
{
  "error": "invalid_parameter",
  "message": "Page must be a positive integer"
}
```

**Response Time**: < 200ms (with pagination)

---

### GET /api/v1/driver/stations/{id}

**Description**: Get details for a specific charging station.

**Authentication**: Required (JWT)

**Parameters**:
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| id | string | YES | - | Station ID (e.g., "STA-abc123456789") |

**Example Request**:
```http
GET /api/v1/driver/stations/STA-abc123456789 HTTP/1.1
Authorization: Bearer <JWT_TOKEN>
```

**Success Response** (200 OK):
```json
{
  "id": "STA-abc123456789",
  "name": "Central Park Charging",
  "latitude": 40.7829,
  "longitude": -73.9654,
  "amenity": "charging_station",
  "power": "50kW",
  "connector_types": ["Type 2", "CCS"],
  "is_available": true,
  "operator": "Tesla",
  "address": {
    "street": "5th Avenue",
    "city": "New York",
    "country": "USA"
  },
  "last_updated": "2026-06-22T10:30:00Z",
  "osm_id": 123456789
}
```

**Error Response** (404 Not Found):
```json
{
  "error": "not_found",
  "message": "Station STA-abc123456789 not found"
}
```

**Response Time**: < 100ms

---

### POST /api/v1/gis/ingest

**Description**: Trigger OSM data ingestion (admin-only).

**Authentication**: Required (JWT with admin role)

**Request Body**: None (uses default configuration)

**Example Request**:
```http
POST /api/v1/gis/ingest HTTP/1.1
Authorization: Bearer <ADMIN_JWT_TOKEN>
```

**Success Response** (202 Accepted):
```json
{
  "status": "accepted",
  "message": "OSM ingestion started",
  "job_id": "osm-ingest-20260622-123456",
  "estimated_duration": "10 seconds"
}
```

**Error Response** (403 Forbidden):
```json
{
  "error": "forbidden",
  "message": "Admin role required for ingestion"
}
```

**Error Response** (503 Service Unavailable):
```json
{
  "error": "service_unavailable",
  "message": "Ingestion service is currently busy"
}
```

**Response Time**: Immediate (returns job ID, background processing)

**Rate Limit**: 5 requests per minute (per admin)

---

### GET /api/v1/gis/ingest/status/{job_id}

**Description**: Get ingestion job status (admin-only).

**Authentication**: Required (JWT with admin role)

**Parameters**:
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| job_id | string | YES | - | Job ID from POST /ingest |

**Example Request**:
```http
GET /api/v1/gis/ingest/status/osm-ingest-20260622-123456 HTTP/1.1
Authorization: Bearer <ADMIN_JWT_TOKEN>
```

**Success Response** (200 OK):
```json
{
  "job_id": "osm-ingest-20260622-123456",
  "status": "completed",
  "osm_id": "2001010710316625",
  "rows_processed": 150,
  "rows_success": 145,
  "rows_failed": 5,
  "duration_seconds": 8.5,
  "started_at": "2026-06-22T10:30:00Z",
  "completed_at": "2026-06-22T10:30:08Z"
}
```

**Status Values**:
- `pending`: Job queued
- `processing`: Job in progress
- `completed`: Job finished successfully
- `failed`: Job failed

**Response Time**: < 50ms

---

### GET /health

**Description**: Health check endpoint.

**Authentication**: Not required

**Example Request**:
```http
GET /health HTTP/1.1
```

**Success Response** (200 OK):
```json
{
  "status": "ok",
  "timestamp": "2026-06-22T10:30:00Z",
  "service": "driver-service",
  "version": "1.0.0",
  "gis_enabled": true
}
```

**Response Time**: < 10ms

---

## Error Response Schema

All error responses follow this schema:

```json
{
  "error": "error_code",
  "message": "Human-readable error message",
  "field": "optional_field_name"
}
```

**Common Error Codes**:
- `unauthorized`: Missing or invalid JWT token
- `forbidden`: Insufficient permissions
- `not_found`: Resource not found
- `invalid_parameter`: Invalid request parameter
- `service_unavailable`: Service temporarily unavailable
- `rate_limit_exceeded`: Too many requests

---

## Rate Limiting

**Default Limit**: 100 requests per minute (per user)
**Headers**:
- `X-RateLimit-Limit`: Rate limit (100)
- `X-RateLimit-Remaining`: Requests remaining
- `X-RateLimit-Reset`: Timestamp when limit resets

**Custom Limits**:
- Admin endpoints: 5 requests per minute
- Ingestion endpoints: 5 requests per minute

---

## CORS Configuration

**Allowed Origins**: `http://localhost:3000`, `http://localhost:3001`, `http://localhost:3002`, `file://*` (for testing)

**Allowed Methods**:
- GET
- POST
- OPTIONS

**Allowed Headers**:
- `Authorization`
- `Content-Type`
- `Accept`

---

## Data Formats

### Coordinate Format

**Standard**: WGS 84 (lat/lon)
**Precision**: Double precision (e.g., 40.7829, -73.9654)
**SRID**: 4326

### Timestamp Format

**Format**: ISO 8601 (UTC)
**Example**: `2026-06-22T10:30:00Z`

### ID Format

**Station ID**: nanoid(12) with "STA-" prefix
**Example**: `STA-abc123456789`

---

## Version History

| Version | Date | Description |
|---------|------|-------------|
| 1.0.0 | 2026-06-22 | Initial API contracts for GIS Engine Foundation |

---

## Testing

### Unit Tests

```rust
#[actix_web::test]
async fn test_nearby_query_success() {
    // Test successful query
    let req = test::TestRequest::get()
        .uri("/api/v1/driver/nearby?lat=40.7829&lon=-73.9654&radius=1000")
        .insert_header(("Authorization", "Bearer <TOKEN>"))
        .to_request();
    let resp = test::call_service(app, req).await;
    assert_eq!(resp.status().as_u16(), 200);
    let body = test::read_body(resp).await;
    let json: Response = serde_json::from_slice(&body).unwrap();
    assert!(!json.data.is_empty());
}
```

### Integration Tests

```bash
# Test nearby query
curl -H "Authorization: Bearer <JWT_TOKEN>" \
  "http://localhost:3001/api/v1/driver/nearby?lat=40.7829&lon=-73.9654&radius=1000"

# Test station detail query
curl -H "Authorization: Bearer <JWT_TOKEN>" \
  "http://localhost:3001/api/v1/driver/stations/STA-abc123456789"

# Test ingestion trigger
curl -X POST \
  -H "Authorization: Bearer <ADMIN_JWT_TOKEN>" \
  "http://localhost:3001/api/v1/gis/ingest"
```

---

## Implementation Notes

- All endpoints use SQLx compile-time verification
- Spatial queries use PostGIS functions
- Results are cached in Redis (cache key: `geo:radius:{lat}:{lon}:{radius}`)
- Pagination requires lat/lon/radius parameters (for spatial filtering)
- All endpoints log queries to analytics_db (ingestion events, query logs)
- Rate limiting implemented via actix-web middleware
- CORS configured for web and mobile clients
