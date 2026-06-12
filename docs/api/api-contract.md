# BorneMap API Contract v1.0

**Version:** 1.0  
**Last Updated:** 2026-06-10  
**Status:** Active

---

## Overview

All endpoints are prefixed with `/api/v1/`.

### Response Format

**All responses:**
- Content-Type: `application/json`
- Timestamps: ISO 8601 UTC (e.g., `2026-06-10T14:30:00Z`)
- IDs: entity-prefixed nanoid (e.g., `STA-abc123`)

### Error Response

All error responses use this shape:

```json
{
  "error": {
    "code": "ERROR_CODE_SNAKE_CASE",
    "message": "Human-readable error message"
  }
}
```

### Pagination Response

List endpoints use this shape:

```json
{
  "data": [...],
  "meta": {
    "total": 142,
    "page": 1,
    "per_page": 20
  }
}
```

---

## Driver Service (:8080)

### GET /api/v1/stations

Returns a paginated list of all active (non-deleted) stations.

**Query Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `page` | integer | no | 1 | Page number (1-indexed) |
| `per_page` | integer | no | 20 | Results per page (max 100) |

**Response: 200 OK**

```json
{
  "data": [
    {
      "id": "STA-abc123",
      "name": "Station Lac 1",
      "address": "Avenue Habib Bourguiba, Tunis",
      "lat": 36.8189,
      "lng": 10.1658,
      "status": "available",
      "charger_count": 4,
      "available_chargers": 2,
      "partner_id": "PRT-xyz789"
    }
  ],
  "meta": {
    "total": 142,
    "page": 1,
    "per_page": 20
  }
}
```

**Station Status Enum:** `available` | `busy` | `offline` | `unknown`

---

### GET /api/v1/stations/nearby

Returns stations within a given radius, ordered by distance ascending. Core discovery endpoint.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `lat` | float | yes | Latitude (WGS 84, -90 to 90) |
| `lng` | float | yes | Longitude (WGS 84, -180 to 180) |
| `radius` | float | yes | Search radius in kilometers (> 0) |

**Response: 200 OK**

```json
{
  "data": [
    {
      "id": "STA-abc123",
      "name": "Station Lac 1",
      "address": "Avenue Habib Bourguiba, Tunis",
      "lat": 36.8189,
      "lng": 10.1658,
      "status": "available",
      "charger_count": 4,
      "available_chargers": 2,
      "distance_km": 0.43,
      "partner_id": "PRT-xyz789"
    }
  ],
  "meta": {
    "total": 8,
    "center": {
      "lat": 36.8065,
      "lng": 10.1815
    },
    "radius_km": 5.0
  }
}
```

**Error: 400 Bad Request** — missing or invalid parameters

```json
{
  "error": {
    "code": "INVALID_COORDINATES",
    "message": "lat, lng, and radius are required and must be valid numbers"
  }
}
```

---

### GET /api/v1/stations/{id}

Returns full station detail including all chargers and opening hours.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | string | Station ID (e.g., `STA-abc123`) |

**Response: 200 OK**

```json
{
  "id": "STA-abc123",
  "name": "Station Lac 1",
  "address": "Avenue Habib Bourguiba, Tunis",
  "lat": 36.8189,
  "lng": 10.1658,
  "status": "available",
  "opening_hours": "24/7",
  "partner_id": "PRT-xyz789",
  "partner_name": "TotalEnergies TN",
  "chargers": [
    {
      "id": "CHR-def456",
      "type": "CCS2",
      "power_kw": 50,
      "status": "available",
      "price_per_kwh": 0.45
    },
    {
      "id": "CHR-def457",
      "type": "Type2",
      "power_kw": 22,
      "status": "busy",
      "price_per_kwh": 0.35
    }
  ],
  "created_at": "2026-01-15T09:00:00Z",
  "updated_at": "2026-06-10T14:30:00Z"
}
```

**Charger Type Enum:** `CCS2` | `CHAdeMO` | `Type2` | `GBT` | `Type1`

**Charger Status Enum:** `available` | `busy` | `faulted` | `offline`

**Error: 404 Not Found**

```json
{
  "error": {
    "code": "STATION_NOT_FOUND",
    "message": "Station STA-abc123 does not exist"
  }
}
```

---

## Admin Service (:8081)

All admin endpoints require authentication via JWT in the `Authorization` header:
```
Authorization: Bearer <jwt_token>
```

JWT must contain role `partner` or `admin`.

### GET /api/v1/stations

Returns stations for the authenticated user's scope.

**Behavior by Role:**
- `partner` — returns only stations owned by their partner (WHERE `partner_id = JWT.partner_id`)
- `admin` — returns all stations

**Query Parameters:**

| Parameter | Type | Required | Default |
|-----------|------|----------|---------|
| `page` | integer | no | 1 |
| `per_page` | integer | no | 20 |

**Response: 200 OK**

Same shape as driver-service GET /api/v1/stations.

**Error: 401 Unauthorized** — missing or invalid JWT

```json
{
  "error": {
    "code": "UNAUTHORIZED",
    "message": "Missing or invalid authorization token"
  }
}
```

---

### POST /api/v1/stations

Creates a new station owned by the authenticated partner.

**Authorization:** Requires role `partner` or `admin`

**Request Body:**

```json
{
  "name": "Station Ariana Centre",
  "address": "Avenue de la République, Ariana",
  "lat": 36.8625,
  "lng": 10.1956,
  "opening_hours": "06:00-23:00",
  "chargers": [
    {
      "type": "CCS2",
      "power_kw": 50,
      "price_per_kwh": 0.45
    },
    {
      "type": "Type2",
      "power_kw": 22,
      "price_per_kwh": 0.35
    }
  ]
}
```

**Field Requirements:**

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `name` | string | yes | 1-255 chars |
| `address` | string | yes | 1-255 chars |
| `lat` | float | yes | -90 to 90 |
| `lng` | float | yes | -180 to 180 |
| `opening_hours` | string | yes | Human-readable (e.g., "24/7", "06:00-23:00") |
| `chargers` | array | yes | At least 1 charger |
| `chargers[].type` | enum | yes | CCS2 \| CHAdeMO \| Type2 \| GBT \| Type1 |
| `chargers[].power_kw` | float | yes | > 0 |
| `chargers[].price_per_kwh` | float | yes | >= 0 |

**Response: 201 Created**

```json
{
  "id": "STA-newxyz",
  "name": "Station Ariana Centre",
  "address": "Avenue de la République, Ariana",
  "lat": 36.8625,
  "lng": 10.1956,
  "status": "offline",
  "opening_hours": "06:00-23:00",
  "partner_id": "PRT-xyz789",
  "chargers": [
    {
      "id": "CHR-newxyz",
      "type": "CCS2",
      "power_kw": 50,
      "status": "offline",
      "price_per_kwh": 0.45
    },
    {
      "id": "CHR-newxyza",
      "type": "Type2",
      "power_kw": 22,
      "status": "offline",
      "price_per_kwh": 0.35
    }
  ],
  "created_at": "2026-06-10T14:30:00Z",
  "updated_at": "2026-06-10T14:30:00Z"
}
```

**New stations always have `status: "offline"`.**  
**New chargers always have `status: "offline"`.**

**Error: 400 Bad Request** — validation failure

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "name is required; chargers must contain at least 1 item"
  }
}
```

---

### PUT /api/v1/stations/{id}

Partially updates an existing station. Only provided fields are changed.

**Authorization:** Requires role `partner` or `admin`. Partners can only update their own stations.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | string | Station ID |

**Request Body (all fields optional):**

```json
{
  "name": "Station Ariana Centre v2",
  "address": "Rue updated",
  "opening_hours": "24/7",
  "status": "available"
}
```

**Updatable Fields:**

| Field | Type | Notes |
|-------|------|-------|
| `name` | string | 1-255 chars |
| `address` | string | 1-255 chars |
| `opening_hours` | string | Human-readable |
| `status` | enum | available \| busy \| offline \| unknown |

**Response: 200 OK**

Returns full updated station object (same as GET /api/v1/stations/{id}).

**Error: 403 Forbidden** — partner lacks permission

```json
{
  "error": {
    "code": "FORBIDDEN",
    "message": "You do not have permission to update this station"
  }
}
```

**Error: 404 Not Found** — station does not exist

```json
{
  "error": {
    "code": "STATION_NOT_FOUND",
    "message": "Station STA-abc123 does not exist"
  }
}
```

---

### DELETE /api/v1/stations/{id}

Soft-deletes a station. Sets `deleted_at` timestamp; record remains in database.

**Authorization:** Requires role `partner` or `admin`. Partners can only delete their own stations.

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | string | Station ID |

**Response: 204 No Content**

No response body.

**Error: 403 Forbidden** — partner lacks permission

```json
{
  "error": {
    "code": "FORBIDDEN",
    "message": "You do not have permission to delete this station"
  }
}
```

**Error: 404 Not Found**

```json
{
  "error": {
    "code": "STATION_NOT_FOUND",
    "message": "Station STA-abc123 does not exist"
  }
}
```

---

### GET /api/v1/partners

Admin only. Returns paginated list of all partners.

**Authorization:** Requires role `admin`

**Query Parameters:**

| Parameter | Type | Required | Default |
|-----------|------|----------|---------|
| `page` | integer | no | 1 |
| `per_page` | integer | no | 20 |

**Response: 200 OK**

```json
{
  "data": [
    {
      "id": "PRT-xyz789",
      "name": "TotalEnergies TN",
      "contact_email": "ops@totalenergies.tn",
      "station_count": 12,
      "created_at": "2026-01-01T00:00:00Z"
    }
  ],
  "meta": {
    "total": 5,
    "page": 1,
    "per_page": 20
  }
}
```

**Error: 403 Forbidden** — non-admin attempting access

```json
{
  "error": {
    "code": "FORBIDDEN",
    "message": "Only admins can access this endpoint"
  }
}
```

---

### POST /api/v1/partners

Admin only. Creates a new partner. Partners cannot self-register.

**Authorization:** Requires role `admin`

**Request Body:**

```json
{
  "name": "TotalEnergies TN",
  "contact_email": "ops@totalenergies.tn"
}
```

**Field Requirements:**

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `name` | string | yes | 1-255 chars |
| `contact_email` | string | yes | Valid email |

**Response: 201 Created**

```json
{
  "id": "PRT-xyz789",
  "name": "TotalEnergies TN",
  "contact_email": "ops@totalenergies.tn",
  "station_count": 0,
  "created_at": "2026-06-10T14:30:00Z"
}
```

**Error: 400 Bad Request**

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "name and contact_email are required"
  }
}
```

---

### PUT /api/v1/partners/{id}

Admin only. Partially updates a partner.

**Authorization:** Requires role `admin`

**Path Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `id` | string | Partner ID |

**Request Body (all fields optional):**

```json
{
  "name": "TotalEnergies Tunisia",
  "contact_email": "newops@totalenergies.tn"
}
```

**Response: 200 OK**

Returns full updated partner object.

**Error: 404 Not Found**

```json
{
  "error": {
    "code": "PARTNER_NOT_FOUND",
    "message": "Partner PRT-abc123 does not exist"
  }
}
```

---

### POST /api/v1/events

Ingests a single clickstream event. Fire-and-forget (no guarantee of processing).

**Authorization:** Optional (can be from dashboard or anonymous)

**Request Body:**

```json
{
  "event_type": "station_viewed",
  "session_id": "sess-abc123",
  "user_id": "USR-abc123",
  "payload": {
    "station_id": "STA-abc123",
    "source": "map_marker"
  },
  "occurred_at": "2026-06-10T14:30:00Z"
}
```

**Field Requirements:**

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `event_type` | enum | yes | See event types below |
| `session_id` | string | yes | Client session ID |
| `user_id` | string | no | User ID if authenticated |
| `payload` | object | yes | Event-specific data (flexible) |
| `occurred_at` | string (ISO 8601) | yes | Event timestamp (UTC) |

**Event Types:**
- `station_viewed` — user opened station detail
- `station_searched` — user initiated search
- `nearby_searched` — user triggered geolocation search
- `charger_detail_viewed` — user opened charger info
- `map_panned` — user panned map
- `map_zoomed` — user zoomed map
- `session_started` — new session created

**Response: 202 Accepted**

Event queued for processing. Fire-and-forget.

```json
{
  "accepted": true
}
```

**Error: 400 Bad Request** — invalid event_type

```json
{
  "error": {
    "code": "INVALID_EVENT_TYPE",
    "message": "event_type must be one of: station_viewed, station_searched, ..."
  }
}
```

---

### POST /api/v1/events/batch

Ingests multiple events in one call. Max 100 events per batch.

**Authorization:** Optional

**Request Body:**

```json
{
  "events": [
    {
      "event_type": "station_viewed",
      "session_id": "sess-abc123",
      "user_id": "USR-abc123",
      "payload": { "station_id": "STA-abc123", "source": "map_marker" },
      "occurred_at": "2026-06-10T14:30:00Z"
    },
    {
      "event_type": "map_panned",
      "session_id": "sess-abc123",
      "user_id": null,
      "payload": { "lat": 36.81, "lng": 10.16, "zoom": 13 },
      "occurred_at": "2026-06-10T14:30:05Z"
    }
  ]
}
```

**Constraints:**

- Max 100 events per batch
- Each event follows same validation rules as single event endpoint

**Response: 202 Accepted**

```json
{
  "accepted": 2,
  "rejected": 0
}
```

If some events are invalid, they are rejected but others are accepted.

**Error: 400 Bad Request** — batch exceeds 100 events

```json
{
  "error": {
    "code": "BATCH_TOO_LARGE",
    "message": "Maximum 100 events per batch; received 150"
  }
}
```

---

## HTTP Status Codes

| Code | Meaning |
|------|---------|
| 200 | OK (successful GET, PUT) |
| 201 | Created (successful POST) |
| 202 | Accepted (async/fire-and-forget) |
| 204 | No Content (successful DELETE) |
| 400 | Bad Request (validation error) |
| 401 | Unauthorized (missing/invalid JWT) |
| 403 | Forbidden (RBAC violation) |
| 404 | Not Found (resource missing) |
| 500 | Internal Server Error |

---

## Rate Limiting (Future)

Reserved for MVP-2+. Currently no rate limits on any endpoint.

---

## Versioning

This contract is v1.0 and is stable for MVP-1 and MVP-2.

Breaking changes (v1.1+) will follow semantic versioning and require:
1. ADR documenting the breaking change
2. Changelog entry
3. Migration guide for clients
