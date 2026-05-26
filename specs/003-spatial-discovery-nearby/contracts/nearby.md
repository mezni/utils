# API Contract: Nearby Stations Discovery

**Base Path**: `/api/v1/stations`

## Discover Nearby Stations

`GET /api/v1/stations/nearby`

Returns charging stations within a given radius, ordered by ascending distance
from the query point. Public endpoint — no authentication required.

### Query Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `longitude` | `f64` | Yes | — | User's longitude (WGS 84, SRID 4326). Range: -180 to 180. |
| `latitude` | `f64` | Yes | — | User's latitude (WGS 84, SRID 4326). Range: -90 to 90. |
| `radius_meters` | `f64` | No | `20000.0` | Search radius in meters. Must be > 0. |
| `include_test` | `bool` | No | `false` | Include sandbox/test records. |

### Success Response (200 OK)

```json
[
  {
    "station_id": "STN-seed00000001",
    "station_name": "Station A Tunis Centre",
    "address": "1 Rue de la Liberté",
    "city": "Tunis",
    "longitude": 10.1815,
    "latitude": 36.8065,
    "distance_meters": 123.45,
    "available_chargers_count": 2,
    "is_test": true
  },
  {
    "station_id": "STN-seed00000025",
    "station_name": "Station B Tunis Nord",
    "address": "25 Avenue des Nations Unies",
    "city": "Tunis",
    "longitude": 10.1900,
    "latitude": 36.8200,
    "distance_meters": 1840.67,
    "available_chargers_count": 0,
    "is_test": true
  }
]
```

Response is a flat JSON array. No pagination wrapper — the result set is
capped at 50 and non-paginated by design.

### Error Responses

- `422 Unprocessable Entity`:
  ```json
  {
    "type": "https://httpstatuses.org/422",
    "title": "Validation Error",
    "status": 422,
    "detail": "longitude: -180 to 180, got 200.0",
    "errors": {
      "longitude": "Value out of range: -180 to 180"
    }
  }
  ```
  Triggered by: missing/invalid longitude/latitude, out-of-range coordinates,
  negative or zero radius.

- `400 Bad Request`: Invalid query parameter types (e.g., non-numeric longitude).

### Behavior Notes

- Results are ordered by ascending `distance_meters` (nearest first).
- Maximum 50 results returned regardless of how many stations match.
- When `include_test` is false (default), stations with `is_test = true` are
  excluded at the SQL level.
- Stations with zero available chargers still appear (`available_chargers_count: 0`).
- Soft-deleted stations (deleted_at IS NOT NULL) are always excluded.
- The endpoint is intentionally unauthenticated — mobile discovery requires no
  login.
- No partner-scoping — all non-test, non-deleted stations are visible.
