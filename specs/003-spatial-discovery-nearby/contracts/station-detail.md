# API Contract: Station Detail (Mobile)

**Base Path**: `/api/v1/stations`

## Get Station Detail

`GET /api/v1/stations/{id}`

Returns full station information. Already implemented in Phase 1 — verified
here for mobile app compatibility.

### Success Response (200 OK)

```json
{
  "id": "STN-seed00000001",
  "owner_id": "USR-seedprt00001",
  "name": "Station A Tunis Centre",
  "address": "1 Rue de la Liberté",
  "city": "Tunis",
  "longitude": 10.1815,
  "latitude": 36.8065,
  "is_operational": true,
  "is_test": true,
  "created_at": "2026-05-26T10:00:00.123456Z",
  "updated_at": "2026-05-26T10:00:00.123456Z"
}
```

### Error Responses

- `404 Not Found`: Station does not exist or is soft-deleted.

## List Chargers for Station

`GET /api/v1/stations/{station_id}/chargers`

Returns all chargers for a specific station. Already implemented in Phase 1.

### Query Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `cursor` | string | null | Pagination cursor |
| `limit` | integer | 50 | Page size (max 100) |

### Success Response (200 OK)

```json
{
  "data": [
    {
      "id": "CHG-seed00000001",
      "station_id": "STN-seed00000001",
      "connector_type_id": "CNT-seed00000001",
      "power_kw": 22.0,
      "current_type": "AC",
      "status": "available",
      "created_at": "2026-05-26T10:00:00.123456Z",
      "updated_at": "2026-05-26T10:00:00.123456Z"
    }
  ],
  "pagination": {
    "next_cursor": "eyJjcmVhdGVkX2F0IjoiMjAyNi0wNS0yNlQxMDowMDowMC4xMjM0NTZaIiwiaWQiOiJDSM4t...",
    "has_more": true
  }
}
```

### Error Responses

- `404 Not Found`: Station does not exist or is soft-deleted.
