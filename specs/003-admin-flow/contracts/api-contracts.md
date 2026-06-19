# API Contracts: Admin Service Core Operations

## Overview

This document defines the HTTP interface contracts for the Admin Service, including all CRUD endpoints, idempotency support, and request/response formats.

**Base URL**: `http://localhost:3002/api/v1`

**Content-Type**: `application/json`

**Authentication**: Bearer token from Auth Service (validated at Traefik layer, audience: `admin-dashboard`)

---

## Endpoints

### 1. Partner Management

#### 1.1 Create Partner

**Endpoint**: `POST /api/v1/admin/partner`

**Description**: Creates a new partner entity.

**Idempotency**: Supported (Idempotency-Key header required)

**Request Body**:
```json
{
  "name": "Partner Alpha",
  "network_type": "COMPANY",
  "support_phone": "+216 71 123 456",
  "support_email": "contact@partner-alpha.tn"
}
```

**Response (201 Created)**:
```json
{
  "id": "OPR-a1b2c3d4e5f6g7h8i9j0",
  "name": "Partner Alpha",
  "network_type": "COMPANY",
  "support_phone": "+216 71 123 456",
  "support_email": "contact@partner-alpha.tn",
  "is_verified": false,
  "created_at": "2026-06-19T10:00:00Z",
  "updated_at": "2026-06-19T10:00:00Z"
}
```

**Response (400 Bad Request)**:
```json
{
  "error": "validation_error",
  "details": [
    {
      "field": "name",
      "message": "name is required"
    },
    {
      "field": "network_type",
      "message": "invalid network_type value"
    }
  ]
}
```

**Response (409 Conflict)**:
```json
{
  "error": "constraint_violation",
  "detail": "partner with name 'Partner Alpha' already exists"
}
```

**Response (403 Forbidden)**:
```json
{
  "error": "forbidden",
  "message": "Partner scope restriction: partner cannot mutate another partner's resources"
}
```

**Response (409 Conflict) - Duplicate Idempotency Key**:
```json
{
  "error": "duplicate_request"
}
```

**Headers**:
- `Idempotency-Replayed: true` (if key exists in Redis)
- `X-Cache-Bust-Failed: true` (if Redis bust failed)

---

#### 1.2 Get Partner

**Endpoint**: `GET /api/v1/admin/partner/:id`

**Description**: Retrieves a specific partner by ID.

**Request Parameters**:
- `id` (path): Partner ID (OPR- prefix)

**Response (200 OK)**:
```json
{
  "id": "OPR-a1b2c3d4e5f6g7h8i9j0",
  "name": "Partner Alpha",
  "network_type": "COMPANY",
  "support_phone": "+216 71 123 456",
  "support_email": "contact@partner-alpha.tn",
  "is_verified": false,
  "created_at": "2026-06-19T10:00:00Z",
  "updated_at": "2026-06-19T10:00:00Z"
}
```

**Response (404 Not Found)**:
```json
{
  "error": "not_found",
  "entity_type": "partner",
  "entity_id": "OPR-xxxxxxxxxxxxxxxxxxxxxx"
}
```

---

#### 1.3 Update Partner

**Endpoint**: `PUT /api/v1/admin/partner/:id`

**Description**: Updates an existing partner's details.

**Idempotency**: Supported (Idempotency-Key header required)

**Request Body**:
```json
{
  "name": "Partner Alpha Updated",
  "support_phone": "+216 71 999 999",
  "is_verified": true
}
```

**Note**: `name` and `network_type` cannot be updated (immutable after creation)

**Response (200 OK)**:
```json
{
  "id": "OPR-a1b2c3d4e5f6g7h8i9j0",
  "name": "Partner Alpha Updated",
  "network_type": "COMPANY",
  "support_phone": "+216 71 999 999",
  "support_email": "contact@partner-alpha.tn",
  "is_verified": true,
  "created_at": "2026-06-19T10:00:00Z",
  "updated_at": "2026-06-19T11:00:00Z"
}
```

**Response (400 Bad Request)**:
```json
{
  "error": "validation_error",
  "details": [
    {
      "field": "network_type",
      "message": "network_type cannot be updated after creation"
    }
  ]
}
```

**Response (403 Forbidden)**: Same as create (scope restriction)

**Response (404 Not Found)**: Same as get

**Headers**:
- `Idempotency-Replayed: true` (if key exists)
- `X-Cache-Bust-Failed: true` (if Redis bust failed)

---

#### 1.4 Delete Partner (Soft Delete)

**Endpoint**: `DELETE /api/v1/admin/partner/:id`

**Description**: Soft deletes a partner (sets deleted_at timestamp).

**Idempotency**: Supported (Idempotency-Key header required)

**Response (200 OK)**:
```json
{
  "message": "partner.deleted",
  "id": "OPR-a1b2c3d4e5f6g7h8i9j0"
}
```

**Response (410 Gone)**:
```json
{
  "error": "entity_deleted",
  "entity_type": "partner",
  "entity_id": "OPR-xxxxxxxxxxxxxxxxxxxxxx"
}
```

**Headers**:
- `Idempotency-Replayed: true` (if key exists)
- `X-Cache-Bust-Failed: true` (if Redis bust failed)

---

### 2. Station Management

#### 2.1 Create Station

**Endpoint**: `POST /api/v1/admin/station`

**Description**: Creates a new station for a partner.

**Idempotency**: Supported (Idempotency-Key header required)

**Request Body**:
```json
{
  "partner_id": "OPR-a1b2c3d4e5f6g7h8i9j0",
  "name": "Tunis Central Station",
  "address": "12 Rue de la Liberté, Tunis, Tunisia",
  "location": {
    "type": "Point",
    "coordinates": [10.1816, 36.8065]
  },
  "osm_id": 123456789
}
```

**Location Format**: GeoJSON Point (longitude, latitude)

**Response (201 Created)**:
```json
{
  "id": "STA-a1b2c3d4e5f6g7h8i9j0",
  "partner_id": "OPR-a1b2c3d4e5f6g7h8i9j0",
  "name": "Tunis Central Station",
  "address": "12 Rue de la Liberté, Tunis, Tunisia",
  "location": {
    "type": "Point",
    "coordinates": [10.1816, 36.8065]
  },
  "osm_id": 123456789,
  "created_at": "2026-06-19T10:00:00Z",
  "updated_at": "2026-06-19T10:00:00Z"
}
```

**Response (400 Bad Request)**:
```json
{
  "error": "validation_error",
  "details": [
    {
      "field": "name",
      "message": "name is required"
    },
    {
      "field": "location",
      "message": "invalid location format"
    }
  ]
}
```

**Response (403 Forbidden)**:
```json
{
  "error": "forbidden",
  "message": "Partner scope restriction: partner cannot mutate another partner's resources"
}
```

**Headers**:
- `Idempotency-Replayed: true` (if key exists)
- `X-Cache-Bust-Failed: true` (if Redis bust failed)

---

#### 2.2 Get Station

**Endpoint**: `GET /api/v1/admin/station/:id`

**Description**: Retrieves a specific station by ID.

**Request Parameters**:
- `id` (path): Station ID (STA- prefix)

**Response (200 OK)**:
```json
{
  "id": "STA-a1b2c3d4e5f6g7h8i9j0",
  "partner_id": "OPR-a1b2c3d4e5f6g7h8i9j0",
  "name": "Tunis Central Station",
  "address": "12 Rue de la Liberté, Tunis, Tunisia",
  "location": {
    "type": "Point",
    "coordinates": [10.1816, 36.8065]
  },
  "osm_id": 123456789,
  "created_at": "2026-06-19T10:00:00Z",
  "updated_at": "2026-06-19T10:00:00Z"
}
```

**Response (404 Not Found)**:
```json
{
  "error": "not_found",
  "entity_type": "station",
  "entity_id": "STA-xxxxxxxxxxxxxxxxxxxxxx"
}
```

---

#### 2.3 Update Station

**Endpoint**: `PUT /api/v1/admin/station/:id`

**Description**: Updates an existing station's details.

**Idempotency**: Supported (Idempotency-Key header required)

**Request Body**:
```json
{
  "name": "Tunis Central Station Updated",
  "address": "13 Rue de la Liberté, Tunis, Tunisia",
  "location": {
    "type": "Point",
    "coordinates": [10.1820, 36.8070]
  }
}
```

**Response (200 OK)**:
```json
{
  "id": "STA-a1b2c3d4e5f6g7h8i9j0",
  "partner_id": "OPR-a1b2c3d4e5f6g7h8i9j0",
  "name": "Tunis Central Station Updated",
  "address": "13 Rue de la Liberté, Tunis, Tunisia",
  "location": {
    "type": "Point",
    "coordinates": [10.1820, 36.8070]
  },
  "osm_id": 123456789,
  "created_at": "2026-06-19T10:00:00Z",
  "updated_at": "2026-06-19T11:00:00Z"
}
```

**Response (400 Bad Request)**: Same as create

**Response (403 Forbidden)**: Same as create (scope restriction)

**Response (404 Not Found)**: Same as get

**Headers**:
- `Idempotency-Replayed: true` (if key exists)
- `X-Cache-Bust-Failed: true` (if Redis bust failed)

---

#### 2.4 Delete Station (Soft Delete)

**Endpoint**: `DELETE /api/v1/admin/station/:id`

**Description**: Soft deletes a station (sets deleted_at timestamp).

**Idempotency**: Supported (Idempotency-Key header required)

**Response (200 OK)**:
```json
{
  "message": "station.deleted",
  "id": "STA-a1b2c3d4e5f6g7h8i9j0"
}
```

**Response (410 Gone)**:
```json
{
  "error": "entity_deleted",
  "entity_type": "station",
  "entity_id": "STA-xxxxxxxxxxxxxxxxxxxxxx"
}
```

**Headers**:
- `Idempotency-Replayed: true` (if key exists)
- `X-Cache-Bust-Failed: true` (if Redis bust failed)

---

### 3. Charger Management

#### 3.1 Create Charger

**Endpoint**: `POST /api/v1/admin/charger`

**Description**: Creates a new charger for a station.

**Idempotency**: Supported (Idempotency-Key header required)

**Request Body**:
```json
{
  "station_id": "STA-a1b2c3d4e5f6g7h8i9j0",
  "connector_type_id": 1,
  "status_id": 1,
  "current_type_id": 2,
  "power_kw": 50.0,
  "voltage": 480,
  "amperage": 100,
  "count_available": 1,
  "count_total": 2
}
```

**Reference IDs** (from configuration tables):
- `connector_type_id`: 1 (CCS1), 2 (CCS2), 3 (CHAdeMO), 4 (Type 2)
- `status_id`: 1 (Available), 2 (Occupied), 3 (Maintenance)
- `current_type_id`: 1 (AC), 2 (DC)

**Response (201 Created)**:
```json
{
  "id": "CHG-a1b2c3d4e5f6g7h8i9j0",
  "station_id": "STA-a1b2c3d4e5f6g7h8i9j0",
  "connector_type_id": 2,
  "status_id": 1,
  "current_type_id": 2,
  "power_kw": 50.0,
  "voltage": 480,
  "amperage": 100,
  "count_available": 1,
  "count_total": 2,
  "created_at": "2026-06-19T10:00:00Z",
  "updated_at": "2026-06-19T10:00:00Z"
}
```

**Response (400 Bad Request)**:
```json
{
  "error": "validation_error",
  "details": [
    {
      "field": "count_available",
      "message": "count_available cannot exceed count_total"
    },
    {
      "field": "count_total",
      "message": "count_total must be at least 1"
    }
  ]
}
```

**Response (403 Forbidden)**: Same as create (scope restriction)

**Headers**:
- `Idempotency-Replayed: true` (if key exists)
- `X-Cache-Bust-Failed: true` (if Redis bust failed)

---

#### 3.2 Get Charger

**Endpoint**: `GET /api/v1/admin/charger/:id`

**Description**: Retrieves a specific charger by ID.

**Request Parameters**:
- `id` (path): Charger ID (CHG- prefix)

**Response (200 OK)**:
```json
{
  "id": "CHG-a1b2c3d4e5f6g7h8i9j0",
  "station_id": "STA-a1b2c3d4e5f6g7h8i9j0",
  "connector_type_id": 2,
  "status_id": 1,
  "current_type_id": 2,
  "power_kw": 50.0,
  "voltage": 480,
  "amperage": 100,
  "count_available": 1,
  "count_total": 2,
  "created_at": "2026-06-19T10:00:00Z",
  "updated_at": "2026-06-19T10:00:00Z"
}
```

**Response (404 Not Found)**:
```json
{
  "error": "not_found",
  "entity_type": "charger",
  "entity_id": "CHG-xxxxxxxxxxxxxxxxxxxxxx"
}
```

---

#### 3.3 Update Charger

**Endpoint**: `PUT /api/v1/admin/charger/:id`

**Description**: Updates an existing charger's technical specifications.

**Idempotency**: Supported (Idempotency-Key header required)

**Request Body**:
```json
{
  "power_kw": 75.0,
  "voltage": 480,
  "amperage": 150,
  "status_id": 1,
  "count_available": 2,
  "count_total": 2
}
```

**Response (200 OK)**:
```json
{
  "id": "CHG-a1b2c3d4e5f6g7h8i9j0",
  "station_id": "STA-a1b2c3d4e5f6g7h8i9j0",
  "connector_type_id": 2,
  "status_id": 1,
  "current_type_id": 2,
  "power_kw": 75.0,
  "voltage": 480,
  "amperage": 150,
  "count_available": 2,
  "count_total": 2,
  "created_at": "2026-06-19T10:00:00Z",
  "updated_at": "2026-06-19T11:00:00Z"
}
```

**Response (400 Bad Request)**: Same as create

**Response (403 Forbidden)**: Same as create (scope restriction)

**Response (404 Not Found)**: Same as get

**Headers**:
- `Idempotency-Replayed: true` (if key exists)
- `X-Cache-Bust-Failed: true` (if Redis bust failed)

---

#### 3.4 Delete Charger (Soft Delete)

**Endpoint**: `DELETE /api/v1/admin/charger/:id`

**Description**: Soft deletes a charger (sets deleted_at timestamp).

**Idempotency**: Supported (Idempotency-Key header required)

**Response (200 OK)**:
```json
{
  "message": "charger.deleted",
  "id": "CHG-a1b2c3d4e5f6g7h8i9j0"
}
```

**Response (410 Gone)**:
```json
{
  "error": "entity_deleted",
  "entity_type": "charger",
  "entity_id": "CHG-xxxxxxxxxxxxxxxxxxxxxx"
}
```

**Headers**:
- `Idempotency-Replayed: true` (if key exists)
- `X-Cache-Bust-Failed: true` (if Redis bust failed)

---

## Idempotency Support

### Required Headers for POST Endpoints

All POST endpoints (create partner, create station, create charger, update partner, update station, update charger, delete partner, delete station, delete charger) MUST include:

**Idempotency-Key**: UUID v4 string

**Example**:
```
POST /api/v1/admin/partner
Idempotency-Key: a1b2c3d4-e5f6-7890-abcd-ef1234567890
Content-Type: application/json

{
  "name": "Partner Alpha",
  ...
}
```

### Response Headers for Idempotency

| Header | Value | Meaning |
|--------|-------|---------|
| `Idempotency-Replayed: true` | true | Request was a replay of a previous request with same idempotency key |
| `Idempotency-Replayed: true` | false (absent) | Request was new, mutation was executed |

---

## Error Handling Summary

| Error Type | Status Code | Error Code | Example Response |
|------------|-------------|------------|------------------|
| Validation error | 400 Bad Request | `validation_error` | 400 with details array |
| Unauthorized | 401 Unauthorized | `unauthorized` | `{ "error": "unauthorized" }` |
| Forbidden | 403 Forbidden | `forbidden` | `{ "error": "forbidden", "required_role": "role:admin" }` |
| Duplicate request (no key) | 409 Conflict | `duplicate_request` | `{ "error": "duplicate_request" }` |
| Constraint violation | 409 Conflict | `constraint_violation` | `{ "error": "constraint_violation", "detail": "..." }` |
| Entity not found | 404 Not Found | `not_found` | `{ "error": "not_found", "entity_type": "...", "entity_id": "..." }` |
| Entity deleted (soft delete) | 410 Gone | `entity_deleted` | `{ "error": "entity_deleted", "entity_type": "...", "entity_id": "..." }` |
| Internal error | 500 Internal Server Error | `internal_error` | `{ "error": "internal_error" }` |
| Redis bust failure | 200 OK (non-fatal) | — | Same response + `X-Cache-Bust-Failed: true` header |

---

## Cache Failure Policy

When Redis cache bust fails, the system does NOT rollback the database transaction. Instead:

1. Logs a structured warning (level: "warn", component: "redis_cache_bust", error: ...)
2. Sets response header: `X-Cache-Bust-Failed: true`
3. Returns successful response (200 OK) to client
4. Stale data corrects on next successful write or TTL expiry

**Example Response with Cache Bust Failure**:
```json
{
  "id": "OPR-a1b2c3d4e5f6g7h8i9j0",
  "name": "Partner Alpha",
  ...
}
```

Headers:
```
HTTP/1.1 200 OK
Content-Type: application/json
X-Cache-Bust-Failed: true
```

---

## Headers Extracted from Traefik

Admin Service reads the following headers from Traefik (never from client body):

| Header | Purpose | Example Value |
|--------|---------|---------------|
| `X-User-Id` | Actor user ID (USR- prefix) | `USR-123456789` |
| `X-User-Roles` | User roles (comma-separated) | `role:admin,role:partner` |

**Validation**:
- `X-User-Id` must exist and be non-empty
- `X-User-Roles` must be valid comma-separated string
- Headers are extracted in middleware or route handler (not in request body)

---

## Summary

This API contract defines:
- 9 endpoints: 3 for partners (create, get, update, delete), 3 for stations, 3 for chargers
- Idempotency support on all POST/PUT/DELETE endpoints
- Clear error contracts with status codes and error codes
- Cache bust failure policy (non-fatal, log warning, set header)
- Traefik header extraction for user context
- GeoJSON location format for stations
- Soft delete pattern (410 Gone for deleted entities)
