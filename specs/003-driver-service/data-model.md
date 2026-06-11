# Data Model: Driver Service

**Purpose**: API-level data model for the Driver Service. Domain entities (Station, Charger, Partner) are defined in `borne-data` — this document covers the API response DTOs.

---

## Station List Response

Represents a lightweight station for map markers. Returned by `GET /api/v1/stations` and `GET /api/v1/stations/nearby`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | string | yes | Station identifier (nanoid prefix format) |
| name | string | yes | Station display name |
| address | string or null | no | Street address |
| latitude | number | yes | WGS84 latitude (-90 to 90) |
| longitude | number | yes | WGS84 longitude (-180 to 180) |

**Validation**:
- latitude: -90 to 90
- longitude: -180 to 180
- name: non-empty string, max 255 characters

---

## Station Detail Response

Detailed station view with chargers and partner. Returned by `GET /api/v1/stations/{id}`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | string | yes | Station identifier |
| name | string | yes | Station display name |
| address | string or null | no | Street address |
| latitude | number | yes | WGS84 latitude |
| longitude | number | yes | WGS84 longitude |
| chargers | Charger[] | yes | List of charging units (may be empty) |
| partner | Partner | yes | Operating partner |

### Charger

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | string | yes | Charger identifier |
| connector_type | string | yes | Connector standard (e.g. CCS2, Type2, CHAdeMO) |
| power_kw | number | yes | Power output in kilowatts |
| status | string | yes | Current status (available, occupied, offline) |

### Partner

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | string | yes | Partner identifier |
| name | string | yes | Partner display name |
| type | string | yes | Partner type (business, personal) |

---

## Nearby Query Parameters

Input for `GET /api/v1/stations/nearby`.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| lat | number | yes | — | Center latitude (-90 to 90) |
| lng | number | yes | — | Center longitude (-180 to 180) |
| radius_m | number | yes | — | Search radius in meters (must be > 0) |

**Validation**:
- lat: -90 to 90, required
- lng: -180 to 180, required
- radius_m: > 0, required
- Max 100 results returned (no pagination until MVP-5)

---

## Health Response

Returned by `GET /api/v1/health`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| status | string | yes | `"ok"` if service healthy, `"error"` otherwise |
| database | string | yes | `"connected"` or `"disconnected"` |

---

## Error Response

Returned on any error (4xx, 5xx).

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| error | object | yes | Error envelope |
| error.code | string | yes | Machine-readable error key (e.g. "not_found", "validation_error") |
| error.message | string | yes | Human-readable error description |
| error.details | object[] | no | Field-level validation errors |

### Validation Error Detail

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| field | string | yes | Name of the invalid parameter |
| message | string | yes | Description of the validation failure |

---

## Error Code Reference

| Code | HTTP Status | Description |
|------|-------------|-------------|
| not_found | 404 | Requested resource (station, partner) does not exist |
| validation_error | 422 | Invalid input parameters |
| internal_error | 500 | Unexpected server error |
| service_unavailable | 503 | Database connection lost or service not ready |

---

## JSON Envelope

Every response follows this envelope:

```json
{
  "data": { ... },
  "error": null,
  "meta": {
    "count": 3
  }
}
```

Or on error:

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
