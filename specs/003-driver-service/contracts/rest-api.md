# REST API Contract: Driver Service

**Base URL**: `http://localhost:8080`  
**API Prefix**: `/api/v1`  
**Content Type**: `application/json`  

---

## `GET /api/v1/stations`

List all stations (lightweight, for map markers).

### Query Parameters

None.

### Response 200

```json
{
  "data": [
    {
      "id": "STA-abc123def456",
      "name": "Station Alpha",
      "address": "Tunis Centre",
      "latitude": 36.8065,
      "longitude": 10.1815
    }
  ],
  "error": null,
  "meta": {
    "count": 3
  }
}
```

### Response Fields

| Field | Type | Always | Description |
|-------|------|--------|-------------|
| data | Station[] | yes | Array of stations (max 100) |
| meta.count | number | yes | Number of stations in this response |

---

## `GET /api/v1/stations/nearby`

Find stations within a geographic radius, ordered by distance.

### Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| lat | number | yes | Center latitude (-90 to 90) |
| lng | number | yes | Center longitude (-180 to 180) |
| radius_m | number | yes | Search radius in meters (> 0) |

### Response 200

```json
{
  "data": [
    {
      "id": "STA-abc123def456",
      "name": "Station Alpha",
      "address": "Tunis Centre",
      "latitude": 36.8065,
      "longitude": 10.1815
    }
  ],
  "error": null,
  "meta": {
    "count": 1
  }
}
```

### Response 422 (Validation Error)

```json
{
  "data": null,
  "error": {
    "code": "validation_error",
    "message": "Invalid input parameters",
    "details": [
      { "field": "lat", "message": "Must be between -90 and 90" },
      { "field": "radius_m", "message": "Must be greater than 0" }
    ]
  },
  "meta": null
}
```

---

## `GET /api/v1/stations/{id}`

Get station details including chargers and partner.

### Path Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| id | string | yes | Station identifier |

### Response 200

```json
{
  "data": {
    "id": "STA-abc123def456",
    "name": "Station Alpha",
    "address": "Tunis Centre",
    "latitude": 36.8065,
    "longitude": 10.1815,
    "chargers": [
      {
        "id": "CHG-abc123def456",
        "connector_type": "CCS2",
        "power_kw": 150.0,
        "status": "available"
      }
    ],
    "partner": {
      "id": "PRT-abc123def456",
      "name": "Test Partner",
      "type": "business"
    }
  },
  "error": null,
  "meta": null
}
```

### Response 404

```json
{
  "data": null,
  "error": {
    "code": "not_found",
    "message": "Station 'nonexistent' not found"
  },
  "meta": null
}
```

---

## `GET /api/v1/health`

Service health and readiness check.

### Response 200 (Healthy)

```json
{
  "data": {
    "status": "ok",
    "database": "connected"
  },
  "error": null,
  "meta": null
}
```

### Response 503 (Unhealthy)

```json
{
  "data": null,
  "error": {
    "code": "service_unavailable",
    "message": "Database connection lost"
  },
  "meta": null
}
```

---

## Common Error Responses

### 500 Internal Error

```json
{
  "data": null,
  "error": {
    "code": "internal_error",
    "message": "An unexpected error occurred"
  },
  "meta": null
}
```
