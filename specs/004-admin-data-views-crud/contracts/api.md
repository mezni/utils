# API Contracts: Admin Data Views & CRUD

## Base URL

All endpoints mounted under `/api/v1/`. Requests include `Authorization: Bearer <JWT>` header.

---

## Partners

### GET /api/v1/partners

List all partner profiles.

**Response**:
```json
{
  "data": [
    {
      "id": "PRT-abc123def456",
      "user_id": "USR-xyz789uvw012",
      "display_name": "Tunisie Charge",
      "classification": "Business",
      "tax_id": "1234567K",
      "contact_phone": "+21650123456",
      "logo_url": "https://cdn.example.com/logo.png",
      "created_at": "2026-05-25T10:00:00Z"
    }
  ],
  "total": 5
}
```

### POST /api/v1/partners

Create partner profile (may be bundled with user creation).

**Request**:
```json
{
  "email": "partner@example.com",
  "password": "securepassword",
  "name": "Ahmed Ben Salem",
  "display_name": "EcoCharge Tunisie",
  "classification": "Business",
  "tax_id": "89101112K",
  "contact_phone": "+21650123457"
}
```

**Response**: `201 Created` with created partner object.

### PATCH /api/v1/partners/:id

Update partner profile fields. Partial updates supported.

**Request**:
```json
{
  "display_name": "EcoCharge Tunisie Updated",
  "contact_phone": "+21650987654"
}
```

**Response**: `200 OK` with updated partner object.

### DELETE /api/v1/partners/:id

Soft-delete partner profile. Sets `deleted_at`.

**Response**: `204 No Content`

---

## Stations

### GET /api/v1/stations

List all stations. Supports `?limit=` and `?offset=` for pagination.

**Response**:
```json
{
  "data": [
    {
      "id": "STN-m4k2n9p1q5v8",
      "name": "Station Tunis Centre",
      "address": "15 Avenue Habib Bourguiba",
      "city": "Tunis",
      "latitude": 36.8065,
      "longitude": 10.1815,
      "owner_id": "PRT-abc123def456",
      "owner_name": "Tunisie Charge",
      "is_operational": true,
      "is_test": false,
      "created_at": "2026-05-25T10:00:00Z"
    }
  ],
  "total": 100
}
```

### POST /api/v1/stations

Create a new station.

**Request**:
```json
{
  "name": "Station Sousse Plage",
  "address": "Route de la Corniche",
  "city": "Sousse",
  "latitude": 35.8256,
  "longitude": 10.6412,
  "owner_id": "PRT-abc123def456",
  "is_operational": true
}
```

**Response**: `201 Created` with created station object.

### PATCH /api/v1/stations/:id

Update station fields. Partial updates supported.

**Response**: `200 OK` with updated station object.

### DELETE /api/v1/stations/:id

Soft-delete station. Sets `deleted_at`.

**Response**: `204 No Content`

---

## Chargers

### GET /api/v1/chargers

List all chargers across all stations. Supports `?station_id=` filter.

**Response**:
```json
{
  "data": [
    {
      "id": "CHG-a7d3f9g2h1j4",
      "station_id": "STN-m4k2n9p1q5v8",
      "station_name": "Station Tunis Centre",
      "connector_type_id": "CNT-x1y2z3a4b5c6",
      "connector_type_name": "Type 2",
      "power_kw": 22.0,
      "current_type": "AC",
      "status": "available"
    }
  ],
  "total": 300
}
```

### GET /api/v1/stations/:id/chargers

List chargers for a specific station.

**Response**: Same shape as above, filtered to station.

### POST /api/v1/stations/:id/chargers

Create a charger under a station.

**Request**:
```json
{
  "connector_type_id": "CNT-x1y2z3a4b5c6",
  "power_kw": 22.0,
  "current_type": "AC",
  "status": "available"
}
```

**Response**: `201 Created` with created charger object.

### PATCH /api/v1/chargers/:id

Update charger status or details.

**Request**:
```json
{
  "status": "occupied"
}
```

**Response**: `200 OK` with updated charger object.

### DELETE /api/v1/chargers/:id

Hard-delete charger. Permanently removes from database.

**Response**: `204 No Content`

---

## Connector Types

### GET /api/v1/connector-types

List all connector types.

**Response**:
```json
{
  "data": [
    {
      "id": "CNT-x1y2z3a4b5c6",
      "name": "Type 2",
      "description": "IEC 62196 Type 2, 7.4kW single-phase",
      "created_at": "2026-05-25T10:00:00Z"
    }
  ],
  "total": 5
}
```

### POST /api/v1/connector-types

Create a new connector type.

**Request**:
```json
{
  "name": "CHAdeMO",
  "description": "Japanese DC fast charging standard, up to 62.5kW"
}
```

**Response**: `201 Created` with created connector type object.

### PATCH /api/v1/connector-types/:id

Update connector type name or description.

**Response**: `200 OK` with updated connector type object.

### DELETE /api/v1/connector-types/:id

Soft-delete connector type. Blocked with `409 Conflict` if referenced by any charger.

**Success Response**: `204 No Content`

**Conflict Response**: `409 Conflict`
```json
{
  "error": "Cannot delete connector type: referenced by 12 charger(s)"
}
```

---

## Error Response Format

All endpoints return errors in uniform format:

```json
{
  "error": "Human-readable error message",
  "code": "ERROR_CODE",
  "details": {}
}
```

**Common HTTP Status Codes**:
- `400 Bad Request` — validation failure
- `401 Unauthorized` — missing/invalid JWT
- `403 Forbidden` — insufficient permissions
- `404 Not Found` — resource does not exist
- `409 Conflict` — delete blocked by referential integrity
- `422 Unprocessable Entity` — semantic validation error
- `500 Internal Server Error` — unexpected server error
