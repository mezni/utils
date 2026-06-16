# API Contracts — GIS Data & Nearby Discovery

**Feature**: GIS Data & Nearby Discovery — MVP-2 Sprint 2.0
**Last Updated**: 2026-06-16

## Overview

This document defines the API contracts for the GIS data layer, including the Nearby Discovery API and the Import API. These contracts specify request/response formats, error codes, and validation rules.

## Common Response Format

### Success Response

```json
{
  "data": { /* specific response data */ },
  "meta": {
    "request_id": "uuid",
    "timestamp": "2026-06-16T15:00:00Z"
  }
}
```

### Error Response

```json
{
  "error": {
    "code": "ERROR_CODE",
    "message": "Human-readable error message",
    "field": "optional_field_name"
  },
  "meta": {
    "request_id": "uuid",
    "timestamp": "2026-06-16T15:00:00Z"
  }
}
```

## Endpoint 1: GET /api/v1/nearby

**Purpose**: Retrieve nearby charging stations within specified radius

**Authentication**: Required (JWT token)

**Rate Limit**: 100 requests per minute per user

### Request

**Method**: `GET`

**Headers**:
```
Authorization: Bearer <jwt_token>
Content-Type: application/json
```

**Query Parameters**:

| Parameter | Type | Required | Default | Constraints | Example |
|-----------|------|----------|---------|-------------|---------|
| `lat` | float | Yes | — | -90 to 90 | 36.8 |
| `lon` | float | Yes | — | -180 to 180 | 10.18 |
| `radius_m` | integer | No | 5000 | 1–50000 | 5000 |
| `max_results` | integer | No | 50 | 1–100 | 10 |
| `visibility` | string | No | all | 'commercial', 'private_home', 'all' | commercial |

**Request Example**:
```http
GET /api/v1/nearby?lat=36.8&lon=10.18&radius_m=5000&max_results=10&visibility=commercial
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

### Response 200 (Success)

**Status**: `200 OK`

**Content-Type**: `application/json`

**Response**:
```json
{
  "data": {
    "stations": [
      {
        "id": "sta_abc123",
        "name": "Station Menzah",
        "location": {
          "lat": 36.84,
          "lon": 10.19
        },
        "address": "Rue des Jasmins, Menzah",
        "city": "Tunis",
        "distance_m": 1240,
        "visibility": "commercial",
        "status": "active",
        "chargers": [
          {
            "id": "chg_xyz789",
            "connector_type": "type2",
            "connector_count": 2,
            "power_kw": 22.0,
            "status": "available"
          }
        ]
      }
    ],
    "count": 1,
    "radius_m": 5000
  },
  "meta": {
    "request_id": "550e8400-e29b-41d4-a716-446655440000",
    "timestamp": "2026-06-16T15:00:00Z"
  }
}
```

### Response 400 (Bad Request) - GEO_001

**Status**: `400 Bad Request`

**Error Code**: `GEO_001`

**Description**: Invalid coordinates

**Response**:
```json
{
  "error": {
    "code": "GEO_001",
    "message": "Coordinates must be within valid geographic ranges (lat: -90 to 90, lon: -180 to 180)",
    "field": "coordinates"
  },
  "meta": {
    "request_id": "550e8400-e29b-41d4-a716-446655440001",
    "timestamp": "2026-06-16T15:00:00Z"
  }
}
```

### Response 400 (Bad Request) - GEO_002

**Status**: `400 Bad Request`

**Error Code**: `GEO_002`

**Description**: Radius exceeded maximum value

**Response**:
```json
{
  "error": {
    "code": "GEO_002",
    "message": "Radius must be between 1 and 50000 meters",
    "field": "radius_m"
  },
  "meta": {
    "request_id": "550e8400-e29b-41d4-a716-446655440002",
    "timestamp": "2026-06-16T15:00:00Z"
  }
}
```

### Response 400 (Bad Request) - GEO_003

**Status**: `400 Bad Request`

**Error Code**: `GEO_003`

**Description**: Max results exceeded

**Response**:
```json
{
  "error": {
    "code": "GEO_003",
    "message": "max_results must be between 1 and 100",
    "field": "max_results"
  },
  "meta": {
    "request_id": "550e8400-e29b-41d4-a716-446655440003",
    "timestamp": "2026-06-16T15:00:00Z"
  }
}
```

### Response 401 (Unauthorized) - AUTH_001

**Status**: `401 Unauthorized`

**Error Code**: `AUTH_001`

**Description**: Missing or invalid authorization header

**Response**:
```json
{
  "error": {
    "code": "AUTH_001",
    "message": "Missing or invalid authorization header. Please provide a valid JWT token.",
    "field": "authorization"
  },
  "meta": {
    "request_id": "550e8400-e29b-41d4-a716-446655440004",
    "timestamp": "2026-06-16T15:00:00Z"
  }
}
```

### Response 429 (Too Many Requests) - RATE_001

**Status**: `429 Too Many Requests`

**Error Code**: `RATE_001`

**Description**: Rate limit exceeded

**Response**:
```json
{
  "error": {
    "code": "RATE_001",
    "message": "Too many requests. Maximum 100 queries per minute.",
    "field": "rate_limit"
  },
  "meta": {
    "request_id": "550e8400-e29b-41d4-a716-446655440005",
    "timestamp": "2026-06-16T15:00:00Z"
  }
}
```

**Retry-After Header**:
```
Retry-After: 58
```

### Response 500 (Internal Server Error)

**Status**: `500 Internal Server Error`

**Error Code**: `INTERNAL_ERROR`

**Description**: Server-side error

**Response**:
```json
{
  "error": {
    "code": "INTERNAL_ERROR",
    "message": "An error occurred while processing your request. Please try again later.",
    "field": null
  },
  "meta": {
    "request_id": "550e8400-e29b-41d4-a716-446655440006",
    "timestamp": "2026-06-16T15:00:00Z"
  }
}
```

**Note**: Error messages do not expose implementation details (no stack traces, no database errors).

## Endpoint 2: POST /api/v1/import

**Purpose**: Trigger import of charging station data from OpenStreetMap

**Authentication**: Required (admin role)

**Rate Limit**: 1 request per 24 hours (admin operation)

### Request

**Method**: `POST`

**Headers**:
```
Content-Type: application/json
Authorization: Bearer <admin_jwt_token>
```

**Request Body**:
```json
{
  "bbox": {
    "min_lat": 30.0,
    "min_lon": 7.5,
    "max_lat": 37.5,
    "max_lon": 11.6
  }
}
```

**Body Schema**:

| Field | Type | Required | Constraints | Example |
|-------|------|----------|-------------|---------|
| `bbox` | object | Yes | Must contain min_lat, min_lon, max_lat, max_lon | See above |
| `bbox.min_lat` | float | Yes | -90 to 90 | 30.0 |
| `bbox.min_lon` | float | Yes | -180 to 180 | 7.5 |
| `bbox.max_lat` | float | Yes | min_lat ≤ max_lat | 37.5 |
| `bbox.max_lon` | float | Yes | min_lon ≤ max_lon | 11.6 |

**Request Example**:
```http
POST /api/v1/import
Content-Type: application/json
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
{
  "bbox": {
    "min_lat": 30.0,
    "min_lon": 7.5,
    "max_lat": 37.5,
    "max_lon": 11.6
  }
}
```

### Response 200 (Success)

**Status**: `200 OK`

**Content-Type**: `application/json`

**Response**:
```json
{
  "data": {
    "status": "success",
    "stations_imported": 1250,
    "stations_updated": 340,
    "stations_failed": 0,
    "start_time": "2026-06-16T15:00:00Z",
    "end_time": "2026-06-16T15:45:30Z",
    "duration_seconds": 2730
  },
  "meta": {
    "request_id": "550e8400-e29b-41d4-a716-446655440010",
    "timestamp": "2026-06-16T15:45:30Z"
  }
}
```

### Response 400 (Bad Request) - IMPORT_001

**Status**: `400 Bad Request`

**Error Code**: `IMPORT_001`

**Description**: Invalid bounding box parameters

**Response**:
```json
{
  "error": {
    "code": "IMPORT_001",
    "message": "Bounding box must be valid: min_lat ≤ max_lat and min_lon ≤ max_lon",
    "field": "bbox"
  },
  "meta": {
    "request_id": "550e8400-e29b-41d4-a716-446655440011",
    "timestamp": "2026-06-16T15:00:00Z"
  }
}
```

### Response 400 (Bad Request) - IMPORT_002

**Status**: `400 Bad Request`

**Error Code**: `IMPORT_002`

**Description**: Missing required fields

**Response**:
```json
{
  "error": {
    "code": "IMPORT_002",
    "message": "Missing required field: bbox",
    "field": "bbox"
  },
  "meta": {
    "request_id": "550e8400-e29b-41d4-a716-446655440012",
    "timestamp": "2026-06-16T15:00:00Z"
  }
}
```

### Response 401 (Unauthorized)

**Status**: `401 Unauthorized`

**Error Code**: `AUTH_001`

**Description**: Invalid or missing token

**Response**:
```json
{
  "error": {
    "code": "AUTH_001",
    "message": "Invalid authorization credentials",
    "field": "authorization"
  },
  "meta": {
    "request_id": "550e8400-e29b-41d4-a716-446655440013",
    "timestamp": "2026-06-16T15:00:00Z"
  }
}
```

### Response 403 (Forbidden)

**Status**: `403 Forbidden`

**Error Code**: `AUTH_003`

**Description**: User does not have admin role

**Response**:
```json
{
  "error": {
    "code": "AUTH_003",
    "message": "Admin role required to perform this operation",
    "field": "role"
  },
  "meta": {
    "request_id": "550e8400-e29b-41d4-a716-446655440014",
    "timestamp": "2026-06-16T15:00:00Z"
  }
}
```

### Response 500 (Internal Server Error)

**Status**: `500 Internal Server Error`

**Error Code**: `INTERNAL_ERROR`

**Description**: Server-side error

**Response**:
```json
{
  "error": {
    "code": "INTERNAL_ERROR",
    "message": "An error occurred while processing your request. Please try again later.",
    "field": null
  },
  "meta": {
    "request_id": "550e8400-e29b-41d4-a716-446655440015",
    "timestamp": "2026-06-16T15:00:00Z"
  }
}
```

## Error Codes Reference

| Code | HTTP | Category | Scenario | Description |
|------|------|----------|----------|-------------|
| `GEO_001` | 400 | Validation | Invalid coordinates | Lat/lon out of valid range |
| `GEO_002` | 400 | Validation | Radius exceeded | radius_m > 50000 |
| `GEO_003` | 400 | Validation | Max results exceeded | max_results > 100 |
| `AUTH_001` | 401 | Authentication | Missing/invalid token | No or invalid JWT |
| `AUTH_003` | 403 | Authorization | Insufficient permissions | User lacks admin role |
| `RATE_001` | 429 | Rate Limit | Query limit exceeded | > 100 queries/minute |
| `IMPORT_001` | 400 | Validation | Invalid bbox | Invalid bounding box |
| `IMPORT_002` | 400 | Validation | Missing fields | Required fields missing |
| `INTERNAL_ERROR` | 500 | Server | General error | Database/connection issues |

## Validation Rules Summary

### Coordinates
- `lat`: Must be -90 to 90
- `lon`: Must be -180 to 180
- `radius_m`: Must be 1–50000
- `max_results`: Must be 1–100
- `bbox.min_lat`, `bbox.min_lon`: Must be -90 to 90, -180 to 180
- `bbox.max_lat`, `bbox.max_lon`: Must be -90 to 90, -180 to 180
- `bbox.min_lat` ≤ `bbox.max_lat`
- `bbox.min_lon` ≤ `bbox.max_lon`

### Authentication
- Must include `Authorization: Bearer <token>` header
- Token must be valid and not expired
- Admin role required for import endpoint

### Rate Limiting
- Nearby API: 100 requests per minute per user
- Import API: 1 request per 24 hours
- Rate limit tracking by user ID from JWT

### Data Integrity
- Station ID must be unique
- Charger station_id must reference existing station
- Foreign key constraints enforced
- Soft deletes respected (deleted_at IS NULL)
- Only active stations returned by nearby API
