# API Contracts: Admin Service

**Date**: 2026-06-09 | **Branch**: `010-admin-service` | **Base URL**: `http://localhost:8081/api`

All endpoints are under `/api` prefix. Request/response bodies are JSON (`application/json`). All write operations use the `X-Partner-Id` header for audit fields (optional — defaults to `"admin"`).

## Common Error Response

```json
{
  "error": {
    "code": "error_code",
    "message": "Human-readable description"
  }
}
```

| Code | HTTP Status | Meaning |
|------|-------------|---------|
| `not_found` | 404 | Resource not found |
| `validation_error` | 400 | Invalid input data |
| `bad_request` | 400 | Malformed request |
| `conflict` | 409 | FK violation or duplicate |
| `internal_error` | 500 | Unexpected server error |
| `db_error` | 500 | Database operation failed |

---

## Health

### `GET /api/health`

**Response 200**:
```json
{
  "status": "ok",
  "version": "0.1.0"
}
```

---

## Partners

### `POST /api/partners` — Create partner

**Request**:
```json
{
  "name": "New Partner",
  "type": "business",
  "is_verified": false,
  "is_live": false,
  "is_active": true
}
```

**Response 201**:
```json
{
  "id": "PRT004",
  "name": "New Partner",
  "type": "business",
  "is_verified": false,
  "is_live": false,
  "is_active": true,
  "created_at": "2026-06-09T12:00:00Z",
  "created_by": "admin",
  "updated_at": "2026-06-09T12:00:00Z",
  "updated_by": "admin"
}
```

**Errors**: 400 if name empty, type invalid; 409 if duplicate (theoretical — IDs are NanoID)

---

### `GET /api/partners` — List partners

**Query params**: `page` (default 1), `page_size` (default 20, max 100)

**Response 200**:
```json
{
  "data": [PartnerResponse, ...],
  "total": 3,
  "page": 1,
  "page_size": 20,
  "total_pages": 1
}
```

---

### `GET /api/partners/{id}` — Get partner

**Response 200**: `PartnerResponse`

**Errors**: 404 if not found

---

### `PUT /api/partners/{id}` — Update partner (partial)

**Request**: Any subset of Partner fields
```json
{
  "name": "Updated Name",
  "is_verified": true
}
```

**Response 200**: `PartnerResponse` (full updated entity)

**Errors**: 400 if invalid field values; 404 if not found

---

### `DELETE /api/partners/{id}` — Delete partner

**Response 200**: `{"deleted": true}`

**Note**: Performs soft delete (`is_active = false`). CASCADE FK ensures child records remain but are invisible to driver queries.

**Errors**: 404 if not found

---

## Stations

### `POST /api/stations` — Create station

**Request**:
```json
{
  "partner_id": "PRT001",
  "name": "New Station",
  "address": "123 Main St",
  "latitude": 36.8065,
  "longitude": 10.1815
}
```

**Response 201**: `StationResponse`

**Errors**: 400 if lat/lng out of range, name empty; 404 if partner_id not found

---

### `GET /api/stations` — List stations

**Query params**: `partner_id` (optional filter), `page` (default 1), `page_size` (default 20, max 100)

**Response 200**:
```json
{
  "data": [StationResponse, ...],
  "total": 15,
  "page": 1,
  "page_size": 20,
  "total_pages": 1
}
```

---

### `GET /api/stations/{id}` — Get station

**Response 200**: `StationResponse`

**Errors**: 404 if not found

---

### `PUT /api/stations/{id}` — Update station (partial)

**Request**: Any subset of Station fields (except partner_id)

**Response 200**: `StationResponse`

**Errors**: 400 if invalid field values; 404 if not found

---

### `DELETE /api/stations/{id}` — Delete station

**Response 200**: `{"deleted": true}`

**Note**: Hard delete. CASCADE removes chargers and availability records.

**Errors**: 404 if not found

---

## Chargers

### `POST /api/chargers` — Create charger

**Request**:
```json
{
  "station_id": "STN001",
  "connector_type": "ccs",
  "power_kw": 150.0,
  "status": "offline"
}
```

**Response 201**: `ChargerResponse`

**Errors**: 400 if invalid connector_type, power_kw <= 0, invalid status; 404 if station_id not found

---

### `GET /api/chargers` — List chargers

**Query params**: `station_id` (optional filter), `page` (default 1), `page_size` (default 20, max 100)

**Response 200**:
```json
{
  "data": [ChargerResponse, ...],
  "total": 24,
  "page": 1,
  "page_size": 20,
  "total_pages": 2
}
```

---

### `GET /api/chargers/{id}` — Get charger

**Response 200**: `ChargerResponse`

**Errors**: 404 if not found

---

### `PUT /api/chargers/{id}` — Update charger (partial)

**Request**: Any subset of Charger fields

**Response 200**: `ChargerResponse`

**Errors**: 400 if invalid field values; 404 if not found

---

### `DELETE /api/chargers/{id}` — Delete charger

**Response 200**: `{"deleted": true}`

**Errors**: 404 if not found

---

## Station Availability

### `POST /api/stations/{id}/availability` — Update station availability

**Request**:
```json
{
  "status": "available"
}
```

**Response 201**:
```json
{
  "id": "SA016",
  "station_id": "STN001",
  "status": "available",
  "updated_by": "admin",
  "updated_at": "2026-06-09T12:00:00Z"
}
```

**Note**: Each call creates a new record (append-only). The driver service reads the latest record.

**Errors**: 400 if invalid status; 404 if station not found
