# API Specification

**Phase**: 1 — Foundation
**Related Tasks**: TASK-25 through TASK-42
**Base Path**: `/api/v1`
**Last Updated**: 2026-06-07

---

## Conventions

- All endpoints are served under the `/api/v1` prefix
- Request and response bodies are JSON
- Errors follow a consistent format:
  ```json
  { "error": "not_found" }
  { "error": "bad_request", "message": "..." }
  { "error": "internal_error" }
  { "error": "unauthorized" }
  { "error": "forbidden" }
  ```
- IDs use NanoID prefixes (PRT-..., STN-..., CHG-..., etc.)
- Timestamps are ISO 8601 (UTC)

---

## Driver Service

**Service Port**: 8080 (internal)
**Base URL**: `/api/v1`

### Health Check

```
GET /api/v1/health
```

**Response 200**:
```json
{
  "status": "ok",
  "service": "driver-service",
  "db": "ok"
}
```

**Response 503**:
```json
{
  "status": "degraded",
  "service": "driver-service",
  "db": "unreachable"
}
```

### Nearby Stations

```
GET /api/v1/stations/nearby
```

**Query Parameters**:
| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| lat | float | ✓ | — | Latitude (WGS84, -90 to 90) |
| lng | float | ✓ | — | Longitude (WGS84, -180 to 180) |
| radius_km | float | | 10 | Search radius in kilometers |
| limit | integer | | 20 | Maximum results |

**Response 200**:
```json
{
  "stations": [
    {
      "id": "STN-t001",
      "name": "Tunis Centre",
      "address": "Avenue Habib Bourguiba, Tunis",
      "latitude": 36.8188,
      "longitude": 10.1657,
      "distance_m": 1250.4,
      "charger_count": 3,
      "available_count": 2
    }
  ],
  "count": 1
}
```

**Errors**:
- 400: `lat` or `lng` out of range

---

## Admin Service

**Service Port**: 8081 (internal)
**Base URL**: `/api/v1`

### Health Check

```
GET /api/v1/health
```

Same response format as Driver Service.

### Partners

#### Create Partner

```
POST /api/v1/partners
```

**Request Body**:
```json
{
  "name": "TotalEnergies Tunisie"
}
```

**Response 201**:
```json
{
  "id": "PRT-alpha001",
  "name": "TotalEnergies Tunisie",
  "created_at": "2026-06-07T12:00:00Z"
}
```

#### List Partners

```
GET /api/v1/partners
```

**Response 200**:
```json
{
  "partners": [
    {
      "id": "PRT-alpha001",
      "name": "TotalEnergies Tunisie",
      "created_at": "2026-06-07T12:00:00Z"
    }
  ],
  "count": 1
}
```

#### Get Partner

```
GET /api/v1/partners/{id}
```

**Response 200**: Single partner object
**Response 404**: `{ "error": "not_found" }`

#### Update Partner

```
PUT /api/v1/partners/{id}
```

**Request Body**:
```json
{
  "name": "New Partner Name"
}
```

**Response 200**: Updated partner object
**Response 404**: `{ "error": "not_found" }`

#### Delete Partner

```
DELETE /api/v1/partners/{id}
```

**Response 204**: No content
**Response 404**: `{ "error": "not_found" }`

**Note**: Only succeeds if partner has no associated stations.

### Stations

#### Create Station

```
POST /api/v1/stations
```

**Request Body**:
```json
{
  "partner_id": "PRT-alpha001",
  "name": "Tunis Centre",
  "address": "Avenue Habib Bourguiba, Tunis",
  "latitude": 36.8188,
  "longitude": 10.1657
}
```

**Response 201**:
```json
{
  "id": "STN-t001",
  "partner_id": "PRT-alpha001",
  "name": "Tunis Centre",
  "address": "Avenue Habib Bourguiba, Tunis",
  "latitude": 36.8188,
  "longitude": 10.1657,
  "created_at": "2026-06-07T12:00:00Z",
  "updated_at": "2026-06-07T12:00:00Z"
}
```

#### List Stations

```
GET /api/v1/stations
```

**Query Parameters**:
| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| partner_id | string | | — | Filter by partner |
| limit | integer | | 50 | Max results |
| offset | integer | | 0 | Pagination offset |

**Response 200**:
```json
{
  "stations": [...],
  "count": 15
}
```

#### Get Station

```
GET /api/v1/stations/{id}
```

**Response 200**: Single station object
**Response 404**: `{ "error": "not_found" }`

#### Update Station

```
PUT /api/v1/stations/{id}
```

**Request Body** (all fields optional):
```json
{
  "name": "Updated Name",
  "address": "New Address",
  "latitude": 36.8200,
  "longitude": 10.1700
}
```

**Response 200**: Updated station object
**Response 404**: `{ "error": "not_found" }`

#### Delete Station

```
DELETE /api/v1/stations/{id}
```

**Response 204**: No content
**Response 404**: `{ "error": "not_found" }`

**Note**: Only succeeds if station has no associated chargers.

### Chargers

#### Create Charger

```
POST /api/v1/chargers
```

**Request Body**:
```json
{
  "station_id": "STN-t001",
  "connector_type": "ccs",
  "power_kw": 150.0,
  "status": "available"
}
```

**Response 201**:
```json
{
  "id": "CHG-001",
  "station_id": "STN-t001",
  "connector_type": "ccs",
  "power_kw": 150.0,
  "status": "available",
  "updated_at": "2026-06-07T12:00:00Z"
}
```

#### List Chargers

```
GET /api/v1/chargers
```

**Query Parameters**:
| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| station_id | string | | — | Filter by station |

**Response 200**:
```json
{
  "chargers": [...],
  "count": 24
}
```

#### Get Charger

```
GET /api/v1/chargers/{id}
```

**Response 200**: Single charger object
**Response 404**: `{ "error": "not_found" }`

#### Update Charger

```
PUT /api/v1/chargers/{id}
```

**Request Body**:
```json
{
  "status": "maintenance",
  "power_kw": 50.0
}
```

**Response 200**: Updated charger object
**Response 404**: `{ "error": "not_found" }`

#### Delete Charger

```
DELETE /api/v1/chargers/{id}
```

**Response 204**: No content
**Response 404**: `{ "error": "not_found" }`

---

## Error Reference

| HTTP Status | Error Code | Description |
|---|---|---|
| 400 | `bad_request` | Invalid input, out-of-range parameters |
| 404 | `not_found` | Entity not found |
| 500 | `internal_error` | Database error or unexpected failure |

All errors return JSON:
```json
{ "error": "not_found" }
{ "error": "bad_request", "message": "lat must be between -90 and 90" }
```
