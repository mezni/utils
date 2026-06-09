# Contracts: Driver Service API

**Base URL**: `http://localhost:8080/api`

**Content-Type**: `application/json`

**Error Response Shape**:
```json
{
  "error": {
    "code": "not_found",
    "message": "Station not found"
  }
}
```

Error codes: `not_found`, `bad_request`, `internal_error`, `db_error`.

---

## GET /api/health

Health check endpoint.

**Response 200**:
```json
{
  "status": "ok",
  "version": "0.1.0"
}
```

---

## GET /api/stations/nearby

Find stations within a radius of a coordinate point.

**Query Parameters**:

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `lat` | `f64` | Yes | — | Center point latitude (-90 to 90) |
| `lng` | `f64` | Yes | — | Center point longitude (-180 to 180) |
| `radius` | `f64` | No | `10000` | Search radius in meters (max 500000) |
| `limit` | `u32` | No | `20` | Max results (max 100) |
| `offset` | `u32` | No | `0` | Pagination offset |

**Response 200**:
```json
[
  {
    "id": "STN001",
    "name": "Tunis Centre Urbain",
    "address": "Avenue Habib Bourguiba, Tunis",
    "latitude": 36.8008,
    "longitude": 10.1815,
    "availability_status": "available",
    "distance_meters": 1234.5
  }
]
```

**Response 400**: Invalid lat/lng (out of range), radius exceeds max.

---

## GET /api/stations/markers

Get stations within a bounding box (for map viewport).

**Query Parameters**:

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `south` | `f64` | Yes | — | South latitude of bbox |
| `west` | `f64` | Yes | — | West longitude of bbox |
| `north` | `f64` | Yes | — | North latitude of bbox |
| `east` | `f64` | Yes | — | East longitude of bbox |

**Response 200**:
```json
[
  {
    "id": "STN001",
    "name": "Tunis Centre Urbain",
    "latitude": 36.8008,
    "longitude": 10.1815,
    "availability_status": "available"
  }
]
```

---

## GET /api/stations/search

Search stations by name/address with optional connector type filter.

**Query Parameters**:

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `q` | `string` | Yes | — | Search query (min 2 chars) |
| `connector_type` | `string` | No | — | Filter by connector type: `type2`, `type3`, `ccs`, `chademo` |
| `limit` | `u32` | No | `20` | Max results (max 100) |
| `offset` | `u32` | No | `0` | Pagination offset |

**Response 200**:
```json
[
  {
    "id": "STN001",
    "name": "Tunis Centre Urbain",
    "address": "Avenue Habib Bourguiba, Tunis",
    "latitude": 36.8008,
    "longitude": 10.1815,
    "availability_status": "available"
  }
]
```

**Response 400**: Query too short (< 2 chars), invalid connector_type.

---

## GET /api/stations/{id}

Get station detail with full charger list.

**Path Parameters**:

| Param | Type | Description |
|-------|------|-------------|
| `id` | `string` | Station ID (e.g., `STN001`) |

**Response 200**:
```json
{
  "id": "STN001",
  "name": "Tunis Centre Urbain",
  "address": "Avenue Habib Bourguiba, Tunis",
  "latitude": 36.8008,
  "longitude": 10.1815,
  "chargers": [
    {
      "id": "CHG001",
      "connector_type": "type2",
      "power_kw": 22.0,
      "status": "available"
    },
    {
      "id": "CHG002",
      "connector_type": "ccs",
      "power_kw": 150.0,
      "status": "available"
    }
  ]
}
```

**Response 404**: Station not found or not visible (partner flags).

---

## GET /api/stations/{id}/reviews

Placeholder endpoint for future reviews feature.

**Path Parameters**:

| Param | Type | Description |
|-------|------|-------------|
| `id` | `string` | Station ID |

**Response 200**:
```json
{
  "station_id": "STN001",
  "message": "Reviews are coming soon"
}
```

**Response 404**: Station not found.
