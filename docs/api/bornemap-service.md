# BorneMap Service API

MVP-1 FastAPI service endpoints and contracts.

**Base URL**: `http://localhost:8000`

**Prefix**: All endpoints under `/api`

**Status**: In progress (Sprint 1.1)

---

## Health

### GET /api/health

Service health check with database connectivity.

**Response** (200):
```json
{
  "status": "ok",
  "service": "bornemap-service",
  "db": "ok"
}
```

**Response** (503 if database unavailable):
```json
{
  "status": "error",
  "service": "bornemap-service",
  "db": "error"
}
```

---

## Partners

### GET /api/partners

List all partners.

**Query Parameters**: None

**Response** (200):
```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "SolaRent Tunisia",
    "created_at": "2026-01-15T10:30:00Z"
  },
  {
    "id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
    "name": "TunisEnergie",
    "created_at": "2026-01-15T10:35:00Z"
  }
]
```

---

### GET /api/partners/:id

Get partner detail.

**Path Parameters**:
- `id` (UUID): Partner ID

**Response** (200):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "SolaRent Tunisia",
  "created_at": "2026-01-15T10:30:00Z"
}
```

**Response** (404):
```json
{
  "detail": "Partner not found"
}
```

---

### POST /api/partners

Create a new partner.

**Request Body**:
```json
{
  "name": "NewPartner Inc"
}
```

**Response** (201):
```json
{
  "id": "7cb64810-9dad-11d1-80b4-00c04fd430c8",
  "name": "NewPartner Inc",
  "created_at": "2026-01-15T11:00:00Z"
}
```

**Response** (422 if name missing):
```json
{
  "detail": [
    {
      "loc": ["body", "name"],
      "msg": "field required",
      "type": "value_error.missing"
    }
  ]
}
```

---

### PUT /api/partners/:id

Update partner name.

**Path Parameters**:
- `id` (UUID): Partner ID

**Request Body**:
```json
{
  "name": "UpdatedPartner Name"
}
```

**Response** (200):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "UpdatedPartner Name",
  "created_at": "2026-01-15T10:30:00Z"
}
```

**Response** (404):
```json
{
  "detail": "Partner not found"
}
```

---

### DELETE /api/partners/:id

Delete partner.

**Path Parameters**:
- `id` (UUID): Partner ID

**Response** (204): No content

**Response** (404):
```json
{
  "detail": "Partner not found"
}
```

**Note**: Deleting a partner with associated stations cascades or returns error (to be decided).

---

## Stations

### GET /api/stations

List all stations.

**Query Parameters**:
- `partner_id` (UUID, optional): Filter by partner

**Response** (200):
```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440001",
    "partner_id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Tunis Central Hub",
    "address": "Avenue de la Liberté, Tunis",
    "latitude": 36.8065,
    "longitude": 10.1815,
    "charger_count": 4,
    "available_count": 2,
    "created_at": "2026-01-15T10:30:00Z",
    "updated_at": "2026-01-15T10:30:00Z"
  }
]
```

---

### GET /api/stations/:id

Get station detail with all chargers.

**Path Parameters**:
- `id` (UUID): Station ID

**Response** (200):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440001",
  "partner_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Tunis Central Hub",
  "address": "Avenue de la Liberté, Tunis",
  "latitude": 36.8065,
  "longitude": 10.1815,
  "chargers": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440010",
      "connector_type": "Type2",
      "power_kw": 22,
      "status": "available",
      "updated_at": "2026-01-15T10:30:00Z"
    },
    {
      "id": "550e8400-e29b-41d4-a716-446655440011",
      "connector_type": "CCS",
      "power_kw": 50,
      "status": "in_use",
      "updated_at": "2026-01-15T10:35:00Z"
    }
  ],
  "created_at": "2026-01-15T10:30:00Z",
  "updated_at": "2026-01-15T10:30:00Z"
}
```

**Response** (404):
```json
{
  "detail": "Station not found"
}
```

---

### GET /api/stations/nearby

Find nearby stations using Euclidean distance.

**Query Parameters**:
- `lat` (float, required): Latitude (-90 to 90)
- `lng` (float, required): Longitude (-180 to 180)
- `radius_km` (float, required): Search radius in kilometers

**Response** (200):
```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440001",
    "partner_id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Tunis Central Hub",
    "address": "Avenue de la Liberté, Tunis",
    "latitude": 36.8065,
    "longitude": 10.1815,
    "charger_count": 4,
    "available_count": 2,
    "distance_m": 250,
    "created_at": "2026-01-15T10:30:00Z",
    "updated_at": "2026-01-15T10:30:00Z"
  }
]
```

**Sorted by**: `distance_m` ascending

---

### POST /api/stations

Create a new station.

**Request Body**:
```json
{
  "partner_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "New Station",
  "address": "Street Name, City",
  "latitude": 36.8065,
  "longitude": 10.1815
}
```

**Response** (201):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440002",
  "partner_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "New Station",
  "address": "Street Name, City",
  "latitude": 36.8065,
  "longitude": 10.1815,
  "created_at": "2026-01-15T11:00:00Z",
  "updated_at": "2026-01-15T11:00:00Z"
}
```

**Response** (422 if validation fails):
```json
{
  "detail": [
    {
      "loc": ["body", "latitude"],
      "msg": "ensure this value is less than or equal to 90",
      "type": "value_error.number.not_le"
    }
  ]
}
```

---

### PUT /api/stations/:id

Update station fields.

**Path Parameters**:
- `id` (UUID): Station ID

**Request Body** (all fields optional):
```json
{
  "name": "Updated Station Name",
  "address": "New Address",
  "latitude": 36.7800,
  "longitude": 10.2000
}
```

**Response** (200): Updated station object

**Response** (404): Station not found

---

### DELETE /api/stations/:id

Delete station (cascades to chargers).

**Path Parameters**:
- `id` (UUID): Station ID

**Response** (204): No content

**Response** (404): Station not found

---

## Chargers

### GET /api/chargers

List all chargers.

**Query Parameters**:
- `station_id` (UUID, optional): Filter by station

**Response** (200):
```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440010",
    "station_id": "550e8400-e29b-41d4-a716-446655440001",
    "connector_type": "Type2",
    "power_kw": 22,
    "status": "available",
    "updated_at": "2026-01-15T10:30:00Z"
  }
]
```

---

### GET /api/chargers/:id

Get charger detail.

**Path Parameters**:
- `id` (UUID): Charger ID

**Response** (200):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440010",
  "station_id": "550e8400-e29b-41d4-a716-446655440001",
  "connector_type": "Type2",
  "power_kw": 22,
  "status": "available",
  "updated_at": "2026-01-15T10:30:00Z"
}
```

**Response** (404): Charger not found

---

### POST /api/chargers

Create a new charger.

**Request Body**:
```json
{
  "station_id": "550e8400-e29b-41d4-a716-446655440001",
  "connector_type": "Type2",
  "power_kw": 22
}
```

**Response** (201):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440012",
  "station_id": "550e8400-e29b-41d4-a716-446655440001",
  "connector_type": "Type2",
  "power_kw": 22,
  "status": "available",
  "updated_at": "2026-01-15T11:00:00Z"
}
```

**Response** (422): Validation error

---

### PUT /api/chargers/:id

Update charger (primarily status updates).

**Path Parameters**:
- `id` (UUID): Charger ID

**Request Body** (all fields optional):
```json
{
  "status": "maintenance",
  "connector_type": "CCS",
  "power_kw": 50
}
```

**Response** (200): Updated charger object

**Response** (404): Charger not found

---

### DELETE /api/chargers/:id

Delete charger.

**Path Parameters**:
- `id` (UUID): Charger ID

**Response** (204): No content

**Response** (404): Charger not found

---

## Error Responses

All error responses follow this format:

```json
{
  "detail": "Human-readable error message"
}
```

### HTTP Status Codes

| Code | Meaning |
|------|---------|
| 200 | OK — Request succeeded |
| 201 | Created — Resource created successfully |
| 204 | No Content — Request succeeded, no response body (DELETE) |
| 400 | Bad Request — Malformed request |
| 404 | Not Found — Resource not found |
| 422 | Unprocessable Entity — Validation error |
| 500 | Internal Server Error — Server error |
| 503 | Service Unavailable — Database unreachable |

---

## Notes

- All timestamps are ISO 8601 UTC format.
- All IDs are UUIDs (v4) in MVP-1.
- Latitude: -90 to 90. Longitude: -180 to 180.
- Charger status: `available`, `in_use`, `maintenance`.
- No pagination or filtering beyond listed query parameters in MVP-1.

---

**Last Updated**: Sprint 1.1 (in progress)
