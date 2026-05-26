# API Contracts: Admin Portal Phase 3

These are the backend API endpoints consumed by the admin portal for Phase 3 functionality.

## Stations

### GET /api/v1/stations

List all stations for map markers and metric count.

**Authentication**: Required (JWT - admin role)

**Query Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | integer | 50 | Max records per page |
| `offset` | integer | 0 | Pagination offset |
| `include_test` | boolean | false | Include sandbox/test records |

**Response** (200):

```json
{
  "data": [
    {
      "id": "STN-4f7d2a8b9c02",
      "name": "Station Name",
      "city": "Tunis",
      "latitude": 36.8065,
      "longitude": 10.1815,
      "is_operational": true,
      "partner_id": "PRT-..."
    }
  ],
  "total": 100,
  "limit": 50,
  "offset": 0
}
```

**Fields consumed by Phase 3**:

| Field | Consumer | Purpose |
|-------|----------|---------|
| `id` | BaseMap (StationMarker), MetricChip | Marker identification, total count |
| `name` | BaseMap (popup) | Display in marker popup |
| `city` | BaseMap (popup) | Display in marker popup |
| `latitude`, `longitude` | BaseMap | Marker position on map |
| `total` | MetricChip | Display in "Total Stations" chip |

### GET /api/v1/stations/:id

Station detail (not directly consumed in Phase 3, but used for "View Details" link target).

**Response** (200):

```json
{
  "id": "STN-4f7d2a8b9c02",
  "name": "Station Name",
  "address": "123 Rue Example",
  "city": "Tunis",
  "latitude": 36.8065,
  "longitude": 10.1815,
  "is_operational": true,
  "partner_id": "PRT-..."
}
```

## Chargers

### GET /api/v1/chargers

List all chargers for total count metric.

**Authentication**: Required (JWT - admin role)

**Query Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `include_test` | boolean | false | Include sandbox/test records |

**Response** (200):

```json
{
  "data": [
    {
      "id": "CHG-...",
      "station_id": "STN-...",
      "connector_type_id": "CNT-...",
      "power_kw": 22.0,
      "current_type": "AC",
      "status": "available"
    }
  ],
  "total": 300
}
```

**Consumed by**: MetricChip — total count for "Total Chargers" chip.

## Partners

### GET /api/v1/partners

List all partner profiles for total count metric.

**Authentication**: Required (JWT - admin role)

**Query Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `include_test` | boolean | false | Include sandbox/test records |

**Response** (200):

```json
{
  "data": [
    {
      "id": "PRT-...",
      "display_name": "Partner Name"
    }
  ],
  "total": 5
}
```

**Consumed by**: MetricChip — total count for "Total Partners" chip.

## Error Responses

All endpoints return standard error responses:

```json
{
  "type": "https://bornemap.tn/problems/...",
  "title": "Error Title",
  "status": 400,
  "detail": "Human-readable error description"
}
```

| Status | Meaning | Phase 3 Handling |
|--------|---------|-----------------|
| 401 Unauthorized | Missing/invalid JWT | Redirect to login |
| 403 Forbidden | Insufficient role | Show "Access Denied" |
| 500 Internal Error | Server error | Skeleton → error state on chips and map |
