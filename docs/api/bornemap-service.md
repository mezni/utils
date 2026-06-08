# BorneMap API v1 Documentation

**Service**: bornemap-service  
**Version**: 1.0  
**API Prefix**: `/api/v1`  
**Status**: Active  
**Base URL**: `http://localhost:8000/api/v1` (local) | `https://api.bornemap.tn/api/v1` (production)

---

## API Versioning

All BorneMap API endpoints are versioned through the URL path prefix. This document describes **v1**, the first active version released in Sprint 1.1.

### Version Support Policy

- **Active Version**: v1 (current)
- **Support Window**: 12 months from v2 release
- **Deprecation Notice Period**: 30 days before version sunset

### URL-Based Versioning

All endpoints must include the version in the URL path:

- ✅ Correct: `/api/v1/stations`
- ❌ Incorrect: `/api/stations` (returns 404)
- ❌ Incorrect: `/api/v999/stations` (returns 404)

### No Version Field in Responses

Responses do not include a version field. The version is implicit in the URL path.

---

## Endpoints

### Health Endpoint

#### GET /api/v1/health

Returns service health status and database connectivity.

**Request**:
```http
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

**Response** (Database Error, 200 OK):
```json
{
  "status": "ok",
  "service": "bornemap-service",
  "db": "error"
}
```

**Error Responses**: None (always returns 200)

---

### Partners Endpoints

#### GET /api/v1/partners

List all partners (EV charging station operators).

**Request**:
```http
GET /api/v1/partners
```

**Response** (200 OK):
```json
{
  "data": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "name": "TuniCharge",
      "created_at": "2026-01-15T10:30:00Z"
    }
  ],
  "count": 1
}
```

**Error Responses**: None

---

#### POST /api/v1/partners

Create a new partner.

**Request**:
```json
{
  "name": "New Charging Company"
}
```

**Response** (201 Created):
```json
{
  "id": "660e8400-e29b-41d4-a716-446655440000",
  "name": "New Charging Company",
  "created_at": "2026-06-08T14:30:00Z"
}
```

**Error Responses**:
- 422 Unprocessable Entity: `name` field missing or invalid

---

#### GET /api/v1/partners/{id}

Get a single partner by ID.

**Request**:
```http
GET /api/v1/partners/550e8400-e29b-41d4-a716-446655440000
```

**Response** (200 OK):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "TuniCharge",
  "created_at": "2026-01-15T10:30:00Z"
}
```

**Error Responses**:
- 404 Not Found: Partner does not exist

---

#### PUT /api/v1/partners/{id}

Update a partner.

**Request**:
```json
{
  "name": "Updated Partner Name"
}
```

**Response** (200 OK):
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Updated Partner Name",
  "created_at": "2026-01-15T10:30:00Z"
}
```

**Error Responses**:
- 404 Not Found: Partner does not exist
- 422 Unprocessable Entity: Invalid request body

---

#### DELETE /api/v1/partners/{id}

Delete a partner.

**Request**:
```http
DELETE /api/v1/partners/550e8400-e29b-41d4-a716-446655440000
```

**Response** (204 No Content):
```
(empty body)
```

**Error Responses**:
- 404 Not Found: Partner does not exist

---

### Stations Endpoints

#### GET /api/v1/stations

List all stations with optional partner filter.

**Request**:
```http
GET /api/v1/stations
GET /api/v1/stations?partner_id=550e8400-e29b-41d4-a716-446655440000
```

**Query Parameters**:
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `partner_id` | UUID | No | Filter by partner ID |

**Response** (200 OK):
```json
{
  "data": [
    {
      "id": "770e8400-e29b-41d4-a716-446655440000",
      "partner_id": "550e8400-e29b-41d4-a716-446655440000",
      "name": "Tunis Central Station",
      "address": "123 Avenue Bourguiba, Tunis",
      "latitude": 36.8065,
      "longitude": 10.1815,
      "charger_count": 4,
      "available_count": 3,
      "created_at": "2026-01-15T10:30:00Z",
      "updated_at": "2026-06-08T14:30:00Z"
    }
  ],
  "count": 1
}
```

**Error Responses**: None

---

#### GET /api/v1/stations/nearby

Find stations near coordinates, ordered by Euclidean distance.

**Request**:
```http
GET /api/v1/stations/nearby?lat=36.8065&lng=10.1815&radius_km=50
```

**Query Parameters**:
| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `lat` | float | Yes | — | Latitude (-90 to 90) |
| `lng` | float | Yes | — | Longitude (-180 to 180) |
| `radius_km` | float | No | 50 | Search radius in kilometers |

**Response** (200 OK):
```json
{
  "data": [
    {
      "id": "770e8400-e29b-41d4-a716-446655440000",
      "partner_id": "550e8400-e29b-41d4-a716-446655440000",
      "name": "Tunis Central Station",
      "address": "123 Avenue Bourguiba, Tunis",
      "latitude": 36.8065,
      "longitude": 10.1815,
      "charger_count": 4,
      "available_count": 3,
      "created_at": "2026-01-15T10:30:00Z",
      "updated_at": "2026-06-08T14:30:00Z"
    }
  ],
  "count": 1
}
```

**Distance Calculation**: Euclidean distance (simplified), returns empty list if no stations within radius.

**Error Responses**:
- 422 Unprocessable Entity: Invalid latitude (-90 to 90) or longitude (-180 to 180)

---

#### POST /api/v1/stations

Create a new station.

**Request**:
```json
{
  "partner_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "New Station",
  "address": "456 Rue de la Paix, Sfax",
  "latitude": 34.7406,
  "longitude": 10.7603
}
```

**Response** (201 Created):
```json
{
  "id": "880e8400-e29b-41d4-a716-446655440000",
  "partner_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "New Station",
  "address": "456 Rue de la Paix, Sfax",
  "latitude": 34.7406,
  "longitude": 10.7603,
  "charger_count": 0,
  "available_count": 0,
  "created_at": "2026-06-08T14:30:00Z",
  "updated_at": "2026-06-08T14:30:00Z"
}
```

**Error Responses**:
- 422 Unprocessable Entity: Invalid latitude/longitude or missing required fields

---

#### GET /api/v1/stations/{id}

Get a single station with all chargers.

**Request**:
```http
GET /api/v1/stations/770e8400-e29b-41d4-a716-446655440000
```

**Response** (200 OK):
```json
{
  "id": "770e8400-e29b-41d4-a716-446655440000",
  "partner_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Tunis Central Station",
  "address": "123 Avenue Bourguiba, Tunis",
  "latitude": 36.8065,
  "longitude": 10.1815,
  "chargers": [
    {
      "id": "990e8400-e29b-41d4-a716-446655440000",
      "connector_type": "Type2",
      "power_kw": 22.0,
      "status": "available"
    }
  ],
  "charger_count": 1,
  "available_count": 1,
  "created_at": "2026-01-15T10:30:00Z",
  "updated_at": "2026-06-08T14:30:00Z"
}
```

**Error Responses**:
- 404 Not Found: Station does not exist

---

#### PUT /api/v1/stations/{id}

Update a station.

**Request**:
```json
{
  "partner_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Updated Station Name",
  "address": "Updated Address",
  "latitude": 36.8065,
  "longitude": 10.1815
}
```

**Response** (200 OK):
```json
{
  "id": "770e8400-e29b-41d4-a716-446655440000",
  "partner_id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Updated Station Name",
  "address": "Updated Address",
  "latitude": 36.8065,
  "longitude": 10.1815,
  "charger_count": 1,
  "available_count": 1,
  "created_at": "2026-01-15T10:30:00Z",
  "updated_at": "2026-06-08T15:00:00Z"
}
```

**Error Responses**:
- 404 Not Found: Station does not exist
- 422 Unprocessable Entity: Invalid latitude/longitude

---

#### DELETE /api/v1/stations/{id}

Delete a station.

**Request**:
```http
DELETE /api/v1/stations/770e8400-e29b-41d4-a716-446655440000
```

**Response** (204 No Content):
```
(empty body)
```

**Error Responses**:
- 404 Not Found: Station does not exist

---

### Chargers Endpoints

#### GET /api/v1/chargers

List all chargers with optional station filter.

**Request**:
```http
GET /api/v1/chargers
GET /api/v1/chargers?station_id=770e8400-e29b-41d4-a716-446655440000
```

**Query Parameters**:
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `station_id` | UUID | No | Filter by station ID |

**Response** (200 OK):
```json
{
  "data": [
    {
      "id": "990e8400-e29b-41d4-a716-446655440000",
      "station_id": "770e8400-e29b-41d4-a716-446655440000",
      "connector_type": "Type2",
      "power_kw": 22.0,
      "status": "available",
      "created_at": "2026-01-15T10:30:00Z",
      "updated_at": "2026-06-08T14:30:00Z"
    }
  ],
  "count": 1
}
```

**Error Responses**: None

---

#### POST /api/v1/chargers

Create a new charger.

**Request**:
```json
{
  "station_id": "770e8400-e29b-41d4-a716-446655440000",
  "connector_type": "CCS",
  "power_kw": 50.0
}
```

**Response** (201 Created):
```json
{
  "id": "aa0e8400-e29b-41d4-a716-446655440000",
  "station_id": "770e8400-e29b-41d4-a716-446655440000",
  "connector_type": "CCS",
  "power_kw": 50.0,
  "status": "available",
  "created_at": "2026-06-08T14:30:00Z",
  "updated_at": "2026-06-08T14:30:00Z"
}
```

**Error Responses**:
- 422 Unprocessable Entity: Missing required fields or invalid power_kw

---

#### GET /api/v1/chargers/{id}

Get a single charger.

**Request**:
```http
GET /api/v1/chargers/990e8400-e29b-41d4-a716-446655440000
```

**Response** (200 OK):
```json
{
  "id": "990e8400-e29b-41d4-a716-446655440000",
  "station_id": "770e8400-e29b-41d4-a716-446655440000",
  "connector_type": "Type2",
  "power_kw": 22.0,
  "status": "available",
  "created_at": "2026-01-15T10:30:00Z",
  "updated_at": "2026-06-08T14:30:00Z"
}
```

**Error Responses**:
- 404 Not Found: Charger does not exist

---

#### PUT /api/v1/chargers/{id}

Update a charger (primary use case: updating status).

**Request**:
```json
{
  "connector_type": "Type2",
  "power_kw": 22.0
}
```

**Response** (200 OK):
```json
{
  "id": "990e8400-e29b-41d4-a716-446655440000",
  "station_id": "770e8400-e29b-41d4-a716-446655440000",
  "connector_type": "Type2",
  "power_kw": 22.0,
  "status": "available",
  "created_at": "2026-01-15T10:30:00Z",
  "updated_at": "2026-06-08T15:00:00Z"
}
```

**Error Responses**:
- 404 Not Found: Charger does not exist
- 422 Unprocessable Entity: Invalid request body

---

#### DELETE /api/v1/chargers/{id}

Delete a charger.

**Request**:
```http
DELETE /api/v1/chargers/990e8400-e29b-41d4-a716-446655440000
```

**Response** (204 No Content):
```
(empty body)
```

**Error Responses**:
- 404 Not Found: Charger does not exist

---

## Error Responses

All error responses follow this format:

```json
{
  "detail": "Error message describing what went wrong"
}
```

### Common HTTP Status Codes

| Status | Meaning | Example |
|--------|---------|---------|
| 200 | OK | Successful GET, PUT request |
| 201 | Created | Successful POST request |
| 204 | No Content | Successful DELETE request |
| 404 | Not Found | Resource does not exist |
| 422 | Unprocessable Entity | Invalid request data (validation error) |
| 500 | Internal Server Error | Unexpected server error |

---

## Request/Response Formats

### Request Headers

All requests should include:

```http
Content-Type: application/json
```

### Response Headers

All responses include:

```http
Content-Type: application/json
```

---

## Pagination

List endpoints return a paginated response format:

```json
{
  "data": [/* array of items */],
  "count": 10
}
```

**Note**: MVP-1 returns all items (no limit/offset parameters). Pagination will be added in MVP-2.

---

## Field Types & Validation

### UUIDs

All IDs are UUID v4 strings:
```
550e8400-e29b-41d4-a716-446655440000
```

### Coordinates (Latitude/Longitude)

- **Latitude**: Float, range -90 to 90
- **Longitude**: Float, range -180 to 180

### Charger Status

Valid values:
- `available`: Charger is available for use
- `in_use`: Charger is currently in use
- `maintenance`: Charger is under maintenance

### Connector Types

Valid values include (not exhaustive):
- `Type2` — IEC 62196 Type 2 (European standard)
- `CCS` — Combined Charging System
- `CHAdeMO` — Japanese fast charging standard
- `Tesla` — Tesla proprietary connector

---

## API Documentation

### Interactive Documentation

- **Swagger UI**: `http://localhost:8000/api/docs`
- **ReDoc**: `http://localhost:8000/api/redoc`
- **OpenAPI Spec**: `http://localhost:8000/api/openapi.json`

---

## Deprecation Policy

### Notification

When a version is scheduled for deprecation:

1. **6 months before sunset**: Announcement on `/api/docs`
2. **1 month before sunset**: Deprecation header added to responses
3. **Sunset date**: Version endpoint returns 404

### Migration to v2

See `docs/guides/api-migration-v1-to-v2.md` (available when v2 released in MVP-2).

---

## Rate Limiting

Not implemented in MVP-1. Coming in MVP-2.

---

## Changelog

### v1.0.0 (Sprint 1.1 - 2026-06-08)

**Endpoints** (16 total):
- 1 Health endpoint
- 5 Partners endpoints
- 7 Stations endpoints (includes nearby search)
- 5 Chargers endpoints

**Features**:
- URL-based versioning at `/api/v1`
- JSON request/response format
- UUID identifiers
- Coordinate validation (latitude -90 to 90, longitude -180 to 180)
- Station charger counts (total & available)
- Nearby stations search (Euclidean distance)

---

## Support

For questions or issues:
- Review this documentation at `docs/api/bornemap-service.md`
- Check architectural decisions at `docs/adr/ADR-018-api-versioning.md`
- View API contracts at `specs/001-backend-and-database/contracts/api-v1.md`
- See feature specification at `specs/001-backend-and-database/spec.md`
