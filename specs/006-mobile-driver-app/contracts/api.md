# API Contracts: Mobile Driver App — Map Discovery

## Base URL

Mobile app connects to the production backend API. The `/api/v1/stations/nearby` endpoint is publicly accessible (no JWT required).

---

## Nearby Stations

### GET /api/v1/stations/nearby

Find stations within a radius of the given coordinates. Public endpoint — no auth required.

**Query Parameters**:

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `latitude` | number | required | Center latitude for search |
| `longitude` | number | required | Center longitude for search |
| `radius_meters` | number | 20000 | Search radius in meters |
| `include_test` | boolean | false | Must be false or absent; test records always excluded |

**Response**: `200 OK`
```json
{
  "data": [
    {
      "id": "STN-k4m2n9p1q5v8",
      "name": "Ariana Supercharge",
      "address": "15 Rue des Entrepreneurs",
      "city": "Ariana",
      "latitude": 36.8665,
      "longitude": 10.1647,
      "available_chargers": 3,
      "distance_meters": 1250.0,
      "is_operational": true,
      "is_test": false
    }
  ],
  "total": 1
}
```

**Empty Response**: `200 OK` with empty data array when no stations found within radius.

---

## Station Chargers

### GET /api/v1/stations/{id}/chargers

Fetch chargers for a specific station. Called when the driver opens the bottom sheet.

**Response**: `200 OK`
```json
{
  "data": [
    {
      "id": "CHG-m9n2p1q5v8k4",
      "station_id": "STN-k4m2n9p1q5v8",
      "connector_type_id": "CNT-abc123def456",
      "power_kw": 22.0,
      "current_type": "AC",
      "status": "available"
    }
  ],
  "total": 3
}
```

## Error Responses

### Backend Unreachable

The app should handle network errors gracefully and display a retry message.

### Rate Limiting

No rate limiting is enforced in MVP0.
