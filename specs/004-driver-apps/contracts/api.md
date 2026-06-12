# API Contracts: Mobile & Web Driver Apps

**Feature**: MVP-1 Phase 4 - Mobile & Web Driver Apps
**Branch**: `004-driver-apps`
**Date**: 2026-06-12

## Overview

This document defines the external API contracts consumed by the driver apps. All endpoints are already implemented in Phase 2 and validated.

**Base URL**: `http://localhost:8080/api/v1` (local) or `https://api.bornemap.com/api/v1` (production)

**Authentication**: None (public API)

**Response Format**: JSON
**Content-Type**: `application/json`

---

## Endpoints

### 1. Get Station List

**Endpoint**: `GET /stations`

**Description**: Retrieve a paginated list of all charging stations.

**Query Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| page | integer | No (default: 1) | Page number (1-based) |
| per_page | integer | No (default: 20) | Items per page (1-100) |

**Example Request**:

```bash
curl -X GET "http://localhost:8080/api/v1/stations?page=1&per_page=20"
```

**Success Response**: `200 OK`

```json
{
  "data": [
    {
      "id": "STA-abc123",
      "name": "Tunis Central Station",
      "address": "123 Blvd de la Liberté, Tunis",
      "geometry": {
        "type": "Point",
        "coordinates": [10.1815, 36.8065]
      },
      "amenities": ["WiFi", "Parking", "Cafe"],
      "operating_hours": "24/7",
      "created_at": "2026-06-10T12:00:00Z",
      "updated_at": "2026-06-12T10:30:00Z"
    }
  ],
  "meta": {
    "page": 1,
    "per_page": 20,
    "total": 150,
    "total_pages": 8
  }
}
```

**Error Responses**:

| Status | Error Type | Description |
|--------|-----------|-------------|
| 400 | ValidationError | Invalid query parameters (e.g., negative page number) |
| 500 | InternalError | Server error, try again later |

**Frontend Usage**:
- Used for station list screen (pagination)
- Display in map markers when rendering visible area
- Support pull-to-refresh

**Caching**:
- React Query cache for 5 minutes
- Manual refresh via pull-to-refresh

---

### 2. Get Station Details

**Endpoint**: `GET /stations/{id}`

**Description**: Retrieve detailed information about a specific station, including chargers and images.

**Path Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| id | string | Yes | Station ID (e.g., "STA-abc123") |

**Example Request**:

```bash
curl -X GET "http://localhost:8080/api/v1/stations/STA-abc123"
```

**Success Response**: `200 OK`

```json
{
  "id": "STA-abc123",
  "name": "Tunis Central Station",
  "address": "123 Blvd de la Liberté, Tunis",
  "geometry": {
    "type": "Point",
    "coordinates": [10.1815, 36.8065]
  },
  "amenities": ["WiFi", "Parking", "Cafe"],
  "operating_hours": "24/7",
  "created_at": "2026-06-10T12:00:00Z",
  "updated_at": "2026-06-12T10:30:00Z",
  "chargers": [
    {
      "id": "CHR-xyz789",
      "station_id": "STA-abc123",
      "charger_type": "CCS",
      "connector_count": 2,
      "availability_status": "available",
      "power_kw": 50,
      "is_active": true,
      "created_at": "2026-06-10T12:00:00Z",
      "updated_at": "2026-06-12T10:30:00Z"
    }
  ],
  "images": [
    {
      "id": "IMG-123456",
      "station_id": "STA-abc123",
      "url": "https://cdn.bornemap.com/stations/STA-abc123/main.jpg",
      "caption": "Main entrance",
      "is_primary": true,
      "created_at": "2026-06-10T12:00:00Z"
    }
  ]
}
```

**Error Responses**:

| Status | Error Type | Description |
|--------|-----------|-------------|
| 404 | NotFound | Station not found |
| 500 | InternalError | Server error, try again later |

**Frontend Usage**:
- Used for station detail screen
- Load station images lazily (only when detail page is visible)
- Display charger information and availability status

**Caching**:
- React Query cache for 10 minutes (users often return to same station)

---

### 3. Get Nearby Stations

**Endpoint**: `GET /stations/nearby`

**Description**: Retrieve stations within a specified radius of a location. Uses PostGIS spatial query.

**Query Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| lat | float | Yes | Latitude coordinate (-90 to 90) |
| lng | float | Yes | Longitude coordinate (-180 to 180) |
| radius | integer | Yes | Search radius in kilometers (1-100) |

**Example Request**:

```bash
curl -X GET "http://localhost:8080/api/v1/stations/nearby?lat=36.8065&lng=10.1815&radius=10"
```

**Success Response**: `200 OK`

```json
{
  "data": [
    {
      "id": "STA-abc123",
      "name": "Tunis Central Station",
      "address": "123 Blvd de la Liberté, Tunis",
      "geometry": {
        "type": "Point",
        "coordinates": [10.1815, 36.8065]
      },
      "amenities": ["WiFi", "Parking", "Cafe"],
      "operating_hours": "24/7",
      "created_at": "2026-06-10T12:00:00Z",
      "updated_at": "2026-06-12T10:30:00Z"
    }
  ],
  "meta": {
    "count": 15,
    "radius_km": 10
  }
}
```

**Error Responses**:

| Status | Error Type | Description |
|--------|-----------|-------------|
| 400 | ValidationError | Invalid coordinates or radius |
| 404 | NotFound | No stations found within radius |
| 500 | InternalError | Server error, try again later |

**Frontend Usage**:
- Used for map screen (markers within search radius)
- Used for search by location
- Dynamic radius expansion (10km → 25km if <5 results)

**Performance**:
- Target: <100ms (p95)
- Use caching to avoid repeated queries

**Caching**:
- React Query cache for 2 minutes (users often zoom to same area)

---

### 4. Get Health Check

**Endpoint**: `GET /health`

**Description**: Check if the API is running and healthy. Used for monitoring.

**Example Request**:

```bash
curl -X GET "http://localhost:8080/api/v1/health"
```

**Success Response**: `200 OK`

```json
{
  "status": "healthy",
  "timestamp": "2026-06-12T14:30:00Z",
  "version": "1.0.0"
}
```

**Error Response**: `503 Service Unavailable` (if unhealthy)

**Frontend Usage**:
- Used for error detection (network down)
- Can show offline indicator if health check fails

---

## OSM Nominatim API Contract

**Endpoint**: `https://nominatim.openstreetmap.org/search`

**Description**: Geocoding API for converting text addresses to coordinates.

**Authentication**: None (but User-Agent required per ToS)

**Query Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| q | string | Yes | Search query (address, city, landmark) |
| format | string | No (default: json) | Output format |
| limit | integer | No (default: 1) | Maximum results (1-5) |
| addressdetails | integer | No (default: 0) | Include address details in results |

**User-Agent Header**:

```
User-Agent: BorneMap/1.0 (contact@bornemap.com)
```

**Example Request**:

```bash
curl -X GET "https://nominatim.openstreetmap.org/search?q=Tunis+Central&format=json&limit=1&addressdetails=1" \
  -H "User-Agent: BorneMap/1.0 (contact@bornemap.com)"
```

**Success Response**: `200 OK`

```json
[
  {
    "place_id": 123456,
    "licence": "Data © OpenStreetMap contributors",
    "osm_id": 123456,
    "osm_type": "way",
    "lat": "36.8065",
    "lon": "10.1815",
    "display_name": "123 Blvd de la Liberté, Tunis, Tunisia",
    "address": {
      "house_number": "123",
      "road": "Bld de la Liberté",
      "city": "Tunis",
      "state": "Tunisia",
      "country_code": "TN"
    }
  }
]
```

**Error Responses**:

| Status | Error Type | Description |
|--------|-----------|-------------|
| 429 | Too Many Requests | Rate limit exceeded |
| 500 | Server Error | OSM server error |

**Rate Limits**:
- 50 requests/minute per IP (free tier)
- User-Agent required for compliance

**Frontend Usage**:
- Used for station search by name/location
- Query debounced (300ms) to avoid excessive requests

**Caching**:
- Cache geocoding results for 5 minutes (common queries like "Tunis Central")
- Use memoization to avoid duplicate requests

**Retry Strategy**:
- Timeout: 10s
- Retries: 3 with exponential backoff (10s, 30s, 60s)
- User-friendly error message on rate limit exceeded

---

## Request/Response Patterns

### Logging Strategy

All API requests and responses are logged in structured JSON format:

```json
{
  "timestamp": "2026-06-12T14:30:00Z",
  "endpoint": "/stations/nearby",
  "method": "GET",
  "query_params": {
    "lat": "36.8065",
    "lng": "10.1815",
    "radius": "10"
  },
  "response_status": 200,
  "response_time_ms": 45,
  "user_agent": "BorneMap/1.0 (iOS 16.0, iPhone 13)"
}
```

**Logging Requirements**:
- Log all requests (method, endpoint, query params)
- Log all responses (status, response time)
- Log errors with stack traces
- Sanitize PII (addresses) before logging

---

### Error Handling

**Error Response Format**:

```json
{
  "error": {
    "type": "NotFound",
    "message": "Station STA-xyz789 not found",
    "timestamp": "2026-06-12T14:30:00Z"
  }
}
```

**Common Error Types**:

| Error Type | HTTP Status | Description |
|------------|-------------|-------------|
| ValidationError | 400 | Invalid input (bad request) |
| NotFound | 404 | Resource not found |
| Unauthorized | 401 | Authentication required (future) |
| Forbidden | 403 | Insufficient permissions (future) |
| RateLimitExceeded | 429 | Too many requests |
| InternalError | 500 | Server error |
| ServiceUnavailable | 503 | Service temporarily down |

---

## Performance Targets

### API Response Times

| Endpoint | Target (p95) | Target (p99) |
|----------|--------------|--------------|
| GET /stations | 200ms | 300ms |
| GET /stations/{id} | 150ms | 250ms |
| GET /stations/nearby | 100ms | 150ms |
| GET /health | <50ms | 100ms |

### Rate Limits

| Endpoint | Rate Limit | Burst Window |
|----------|------------|--------------|
| GET /stations | 100 req/min | 1 minute |
| GET /stations/{id} | 100 req/min | 1 minute |
| GET /stations/nearby | 60 req/min | 1 minute |
| OSM Nominatim | 50 req/min | 1 minute |

---

## Testing Contract Adherence

### Unit Tests

- Mock all API responses (success and error cases)
- Verify request parameters (lat, lng, radius, page, per_page)
- Verify response format (data, meta, error)
- Test pagination logic (page, per_page, total, total_pages)

### Integration Tests

- Test against live API (driver-service on :8080)
- Test against OSM Nominatim (live endpoint)
- Test error scenarios (network timeout, rate limits)
- Test pagination (first page, last page, empty results)

### Contract Tests (Future)

- Contract test stubs exist at:
  - `driver-service/tests/contract_health.rs`
  - `admin-service/tests/contract_health.rs`

**Note**: Full contract tests to be implemented in Phase 2 implementation (completed in Phase 2, to be integrated here in Phase 4).

---

## Security Considerations

**No Authentication Required (MVP)**:
- Public-access API (web app and mobile app)
- No authentication/authorization needed for discovery

**Security Headers**:
- CORS enabled for specific domains (future: frontend domains)
- No PII in API responses (addresses are public, but can be sanitized if needed)

**Input Validation**:
- All query parameters validated on server (lat, lng, radius, page, per_page)
- SQL injection prevented via prepared statements (Rust + sqlx)
- XSS prevented via content-type: application/json

**Rate Limiting**:
- Server-side rate limiting for API endpoints
- Client-side retry logic for OSM Nominatim (user-friendly)

---

## Future Enhancements

**Phase 3** (out of scope):
- Authentication via JWT tokens
- API key for server-to-server calls
- Rate limiting per user (when accounts exist)

**Phase 4+** (out of scope):
- GraphQL API (instead of REST)
- WebSockets for real-time updates
- Caching layer (Redis) to reduce database load

---

## Versioning

**Current Version**: `v1`

**Versioning Strategy**:
- URL-based versioning (`/api/v1/`)
- Breaking changes require new major version (v2)
- Backward compatibility maintained for minor versions

**Deprecation Policy**:
- Endpoints deprecated for 6 months before removal
- Deprecation notice returned in API response
- Examples:

```json
{
  "error": {
    "type": "Deprecated",
    "message": "Endpoint will be removed in v2",
    "timestamp": "2026-06-12T14:30:00Z"
  }
}
```
