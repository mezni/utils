# Driver Service API Contract

> Contract between frontend apps (Driver Web, Driver Mobile) and the Driver Service backend.

## Base URL

- **Development**: `http://localhost:3001/api/v1`
- **Driver Web Proxy**: `/api/v1` (Vite proxy → `http://localhost:3001`)
- **Driver Mobile**: `http://<host>:3001/api/v1` (configurable via env)

## Endpoints

### Health Check

```
GET /api/v1/health
```

**Response 200**:
```json
{
  "status": "ok",
  "service": "driver-service",
  "db": "ok"
}
```

### Stations Nearby

```
GET /api/v1/stations/nearby?lat={lat}&lng={lng}&radius_km={radius}
```

**Parameters**:
| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| lat | float | yes | — | Center latitude (WGS84) |
| lng | float | yes | — | Center longitude (WGS84) |
| radius_km | float | no | 50 | Search radius in kilometers |

**Response 200**:
```json
{
  "stations": [
    {
      "id": "STN-abc123",
      "name": "Station Name",
      "latitude": 36.8188,
      "longitude": 10.1657,
      "address": " Tunis",
      "available_chargers": 3,
      "total_chargers": 5
    }
  ]
}
```

**Response 400** (invalid parameters):
```json
{
  "error": "Invalid parameters",
  "details": "lat must be between -90 and 90"
}
```

**Response 500** (server error):
```json
{
  "error": "Internal server error"
}
```

## Usage Notes

- Driver Mobile must use the full URL (including host) since it runs on-device
- Driver Web uses Vite proxy — just `/api/v1/stations/nearby?...`
- No authentication required (public endpoint per Constitution Principle VI)
