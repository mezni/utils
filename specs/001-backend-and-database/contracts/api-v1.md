# API Contract: BorneMap v1

**Version**: 1.0  
**Service**: bornemap-service  
**MVP**: 1 (Sprint 1.1)  
**URL Prefix**: `/api/v1`  
**Status**: Active  
**Deprecation Date**: TBD (12 months after v2 release)  

---

## Overview

This contract documents the immutable v1 API for the BorneMap platform. All endpoints are served under `/api/v1/` prefix. Responses include no version identifier (version is implicit in URL path).

---

## Health Endpoint

### GET /api/v1/health

Returns service health status and database connectivity.

**Request**:
```
GET /api/v1/health
```

**Response** (200 OK):
```json
{
  "status": "ok",
  "service": "bornemap-service",
  "db": "ok"
}
```

**Response** (if DB unreachable, 200 OK):
```json
{
  "status": "ok",
  "service": "bornemap-service",
  "db": "error"
}
```

**Error Responses**: None (always 200)

---

## Partners Endpoints

### GET /api/v1/partners

List all partners.

**Request**:
```
GET /api/v1/partners
```

**Response** (200 OK):
```json
{
  "data": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "name": "Partner Name",
      "created_at": "2026-01-01T00:00:00Z"
    }
  ],
  "count": 1
}
```

---

### POST /api/v1/partners

Create a new partner.

**Request**:
```json
{
  "name": "New Partner"
}
```

**Response** (201 Created):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "New Partner",
  "created_at": "2026-01-01T00:00:00Z"
}
```

**Error Responses**:
- 422 Unprocessable Entity: Missing `name` field

---

### GET /api/v1/partners/{id}

Get a specific partner.

**Request**:
```
GET /api/v1/partners/550e8400-e29b-41d4-a716-446655440000
```

**Response** (200 OK):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Partner Name",
  "created_at": "2026-01-01T00:00:00Z"
}
```

**Error Responses**:
- 404 Not Found: Partner does not exist

---

### PUT /api/v1/partners/{id}

Update a partner.

**Request**:
```json
{
  "name": "Updated Name"
}
```

**Response** (200 OK):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Updated Name",
  "created_at": "2026-01-01T00:00:00Z"
}
```

**Error Responses**:
- 404 Not Found: Partner does not exist
- 422 Unprocessable Entity: Invalid request body

---

### DELETE /api/v1/partners/{id}

Delete a partner.

**Request**:
```
DELETE /api/v1/partners/550e8400-e29b-41d4-a716-446655440000
```

**Response** (204 No Content): No body

**Error Responses**:
- 404 Not Found: Partner does not exist
- 409 Conflict: Partner has associated stations (implementation decision: cascade or reject)

---

## Stations Endpoints

### GET /api/v1/stations

List all stations, optionally filtered by partner.

**Request**:
```
GET /api/v1/stations
GET /api/v1/stations?partner_id=550e8400-e29b-41d4-a716-446655440000
```

**Response** (200 OK):
```json
{
  "data": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440001",
      "partner_id": "550e8400-e29b-41d4-a716-446655440000",
      "name": "Station Name",
      "address": "123 Main St, Tunis",
      "latitude": 36.8065,
      "longitude": 10.1699,
      "charger_count": 4,
      "available_count": 2,
      "created_at": "2026-01-01T00:00:00Z",
      "updated_at": "2026-01-02T00:00:00Z"
    }
  ],
  "count": 1
}
```

---

### GET /api/v1/stations/nearby

Get nearby stations within a radius.

**Request**:
```
GET /api/v1/stations/nearby?lat=36.8065&lng=10.1699&radius_km=50
```

**Response** (200 OK):
```json
{
  "data": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440001",
      "partner_id": "550e8400-e29b-41d4-a716-446655440000",
      "name": "Station Name",
      "address": "123 Main St, Tunis",
      "latitude": 36.8065,
      "longitude": 10.1699,
      "charger_count": 4,
      "available_count": 2,
      "distance_m": 0,
      "created_at": "2026-01-01T00:00:00Z",
      "updated_at": "2026-01-02T00:00:00Z"
    }
  ],
  "count": 1
}
```

**Note**: `distance_m` calculated from provided coordinates; results ordered by distance ascending.

---

### POST /api/v1/stations

Create a new station.

**Request**:
```json
{
  "partner_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "New Station",
  "address": "456 Oak St, Sfax",
  "latitude": 34.7403,
  "longitude": 10.7603
}
```

**Response** (201 Created):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440002",
  "partner_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "New Station",
  "address": "456 Oak St, Sfax",
  "latitude": 34.7403,
  "longitude": 10.7603,
  "charger_count": 0,
  "available_count": 0,
  "created_at": "2026-01-03T00:00:00Z",
  "updated_at": "2026-01-03T00:00:00Z"
}
```

**Error Responses**:
- 422 Unprocessable Entity: Missing required fields or invalid lat/lng range
- 404 Not Found: Partner ID does not exist

---

### GET /api/v1/stations/{id}

Get a specific station with all chargers.

**Request**:
```
GET /api/v1/stations/550e8400-e29b-41d4-a716-446655440001
```

**Response** (200 OK):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440001",
  "partner_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Station Name",
  "address": "123 Main St, Tunis",
  "latitude": 36.8065,
  "longitude": 10.1699,
  "charger_count": 4,
  "available_count": 2,
  "created_at": "2026-01-01T00:00:00Z",
  "updated_at": "2026-01-02T00:00:00Z",
  "chargers": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440010",
      "station_id": "550e8400-e29b-41d4-a716-446655440001",
      "connector_type": "Type2",
      "power_kw": 22.0,
      "status": "available",
      "updated_at": "2026-01-02T12:00:00Z"
    },
    {
      "id": "550e8400-e29b-41d4-a716-446655440011",
      "station_id": "550e8400-e29b-41d4-a716-446655440001",
      "connector_type": "CCS",
      "power_kw": 50.0,
      "status": "in_use",
      "updated_at": "2026-01-02T11:00:00Z"
    }
  ]
}
```

---

### PUT /api/v1/stations/{id}

Update a station.

**Request**:
```json
{
  "name": "Updated Station Name",
  "address": "Updated Address"
}
```

**Response** (200 OK): Updated station object (same schema as POST response)

**Error Responses**:
- 404 Not Found: Station does not exist
- 422 Unprocessable Entity: Invalid request body

---

### DELETE /api/v1/stations/{id}

Delete a station (and cascade-delete chargers).

**Request**:
```
DELETE /api/v1/stations/550e8400-e29b-41d4-a716-446655440001
```

**Response** (204 No Content): No body

**Error Responses**:
- 404 Not Found: Station does not exist

---

## Chargers Endpoints

### GET /api/v1/chargers

List all chargers, optionally filtered by station.

**Request**:
```
GET /api/v1/chargers
GET /api/v1/chargers?station_id=550e8400-e29b-41d4-a716-446655440001
```

**Response** (200 OK):
```json
{
  "data": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440010",
      "station_id": "550e8400-e29b-41d4-a716-446655440001",
      "connector_type": "Type2",
      "power_kw": 22.0,
      "status": "available",
      "updated_at": "2026-01-02T12:00:00Z"
    }
  ],
  "count": 1
}
```

---

### POST /api/v1/chargers

Create a new charger.

**Request**:
```json
{
  "station_id": "550e8400-e29b-41d4-a716-446655440001",
  "connector_type": "Type2",
  "power_kw": 22.0
}
```

**Response** (201 Created):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440020",
  "station_id": "550e8400-e29b-41d4-a716-446655440001",
  "connector_type": "Type2",
  "power_kw": 22.0,
  "status": "available",
  "updated_at": "2026-01-03T00:00:00Z"
}
```

**Error Responses**:
- 422 Unprocessable Entity: Missing required fields
- 404 Not Found: Station ID does not exist

---

### GET /api/v1/chargers/{id}

Get a specific charger.

**Request**:
```
GET /api/v1/chargers/550e8400-e29b-41d4-a716-446655440010
```

**Response** (200 OK): Charger object (same schema as POST response)

---

### PUT /api/v1/chargers/{id}

Update a charger (primarily for status changes).

**Request**:
```json
{
  "status": "maintenance"
}
```

**Response** (200 OK): Updated charger object

**Allowed status values**: `available`, `in_use`, `maintenance`

**Error Responses**:
- 404 Not Found: Charger does not exist
- 422 Unprocessable Entity: Invalid status value

---

### DELETE /api/v1/chargers/{id}

Delete a charger.

**Request**:
```
DELETE /api/v1/chargers/550e8400-e29b-41d4-a716-446655440010
```

**Response** (204 No Content): No body

**Error Responses**:
- 404 Not Found: Charger does not exist

---

## Error Response Format

All error responses follow this format:

```json
{
  "detail": "Human-readable error message",
  "error_code": "ERROR_CODE" (optional)
}
```

**Common HTTP Status Codes**:
- `200 OK`: Successful GET/PUT
- `201 Created`: Successful POST
- `204 No Content`: Successful DELETE
- `400 Bad Request`: Malformed request (e.g., invalid version)
- `404 Not Found`: Resource or version not found
- `409 Conflict`: Business logic conflict (e.g., cascade constraint)
- `422 Unprocessable Entity`: Validation error (missing/invalid fields)
- `500 Internal Server Error`: Unexpected server error

---

## Contract Stability Guarantee

This v1 contract is **frozen**:
- No new required fields added to responses
- No existing fields removed from responses
- No endpoint URLs change
- Error codes remain consistent

Bug fixes to returned data are allowed (e.g., correcting charger count calculation).

Breaking changes require v2 contract.

---

## Next Version

v2 contract will be published in MVP-2 (estimated 6+ months). Until then, all clients use v1.

When v2 is released:
- This v1 contract remains supported for 12 months
- v1 endpoints marked deprecated in OpenAPI docs
- Migration guide published
- v2 may introduce breaking changes; v1 clients unaffected
