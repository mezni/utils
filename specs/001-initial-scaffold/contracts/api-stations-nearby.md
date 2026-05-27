# API Contract: GET /api/v1/stations/nearby

## Request

**Method**: GET
**Path**: `/api/v1/stations/nearby`
**Query Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| lat | f64 | YES | Center latitude for nearby search (WGS 84) |
| lng | f64 | YES | Center longitude for nearby search (WGS 84) |

**Headers**:
- `Accept: application/json`

**Example**:
```
GET /api/v1/stations/nearby?lat=36.8065&lng=10.1815
```

## Response

**Status**: 200 OK
**Content-Type**: application/json
**Body**: JSON array of StationHub objects

### StationHub Schema

```json
{
  "id": "stn-e3b0c442",
  "name": "LES BERGES DU LAC 2 HUB",
  "provider_id": "prv-k9x2m47a",
  "provider_name": "TotalEnergies Tunisia",
  "latitude": 36.8324,
  "longitude": 10.2321,
  "status": "Available",
  "chargers": [
    {
      "id": "chg-7b2a19f4",
      "plug_type": "CCS2",
      "power_output": 120,
      "status": "Available"
    }
  ],
  "updated_at": "2026-05-27T13:00:00Z"
}
```

### Validation Rules

- `id`: MUST match `^[a-z]{3}-[a-f0-9]{8}$`
- `latitude`: MUST be between -90 and 90
- `longitude`: MUST be between -180 and 180
- `status`: MUST be `"Available"` or `"Occupied"`
- `updated_at`: MUST be ISO 8601 UTC format
- `chargers` array: MUST NOT be empty
- Each charger `id`: MUST match `^chg-[a-f0-9]{8}$`
- Each charger `power_output`: MUST be positive integer (kW)

## Error Responses

| Status | Condition |
|--------|-----------|
| 400 Bad Request | Missing or invalid `lat`/`lng` parameters |
| 500 Internal Server Error | Server-side failure |

**Error body**:
```json
{
  "error": "descriptive error message"
}
```
