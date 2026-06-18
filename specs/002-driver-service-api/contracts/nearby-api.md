# Contract: GET /api/v1/nearby

## Purpose

Returns charging stations within a given radius of a geographic coordinate, sorted by geodesic distance ascending.

## Request

`GET /api/v1/nearby?lat={lat}&lng={lng}&radius={radius_meters}`

### Query Parameters

| Parameter | Type | Required | Constraints | Description |
|-----------|------|----------|-------------|-------------|
| `lat` | number | yes | -90 to 90 | WGS84 latitude |
| `lng` | number | yes | -180 to 180 | WGS84 longitude |
| `radius` | number | yes | 1 to 200000 | Search radius in meters (max 200 km) |

## Response

### 200 OK

```json
{
  "stations": [
    {
      "station_id": "STA_001",
      "station_name": "Tunis Centre",
      "latitude": 36.8005,
      "longitude": 10.181,
      "distance_meters": 7229.0,
      "is_private": false,
      "partner_name": "BorneMap Tunisia"
    }
  ]
}
```

### 200 OK — Empty Result

```json
{
  "stations": []
}
```

### 400 Bad Request

```json
{
  "error": "Latitude must be between -90 and 90"
}
```

## Backend Function

Calls `gis.get_nearby_stations(lng, lat, radius_meters)` — defined in Sprint 1.1.

## Errors

| HTTP Status | Condition |
|-------------|-----------|
| 400 | Invalid lat/lng/radius bounds |
| 503 | Database connection pool exhausted |

## JSON Response Fields

| Field | Type | Always Present | Description |
|-------|------|----------------|-------------|
| `station_id` | string | yes | NanoID (STA_ prefix) |
| `station_name` | string | yes | Display name |
| `latitude` | number | yes | WGS84 latitude |
| `longitude` | number | yes | WGS84 longitude |
| `distance_meters` | number | yes | Geodesic distance from query point |
| `is_private` | boolean | yes | Home charger flag |
| `partner_name` | string \| null | yes | Operator name or null |
