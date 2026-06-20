# Driver API — Interface Contracts

Base URL: `/api/v1/driver`

## GET /health

### Response 200
```json
{
  "status": "ok",
  "database": "connected",
  "timestamp": "2026-06-20T12:00:00Z"
}
```

## GET /nearby

### Query Parameters
| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| lat | float | yes | — | Latitude (WGS84) |
| lon | float | yes | — | Longitude (WGS84) |
| radius | integer | no | 10000 | Search radius in meters (max 50000) |

### Response 200
```json
{
  "stations": [
    {
      "id": "STA-abc123def456",
      "name": "Station Name",
      "distance_m": 450,
      "max_power_kw": 150.0,
      "power_tier": "ultra_fast",
      "status": "active",
      "available_connectors": 2,
      "total_connectors": 4,
      "location": {
        "lat": 36.8065,
        "lon": 10.1815
      }
    }
  ]
}
```

### Response 200 (empty)
```json
{
  "stations": []
}
```

## GET /stations/:id

### Path Parameters
| Param | Type | Description |
|-------|------|-------------|
| id | string | Station ID (STA- prefix) |

### Response 200
```json
{
  "id": "STA-abc123def456",
  "name": "Station Name",
  "status": "active",
  "partner_name": "Operator Name",
  "location": {
    "lat": 36.8065,
    "lon": 10.1815
  },
  "address": "123 Main St, Tunis",
  "max_power_kw": 150.0,
  "power_tier": "ultra_fast",
  "chargers": [
    {
      "id": "CHR-xyz789uvw012",
      "vendor": "ABB",
      "model": "Terra 184",
      "max_power_kw": 150.0,
      "status": "active",
      "connectors": [
        {
          "id": "CON-345rst678abc",
          "type": "CCS",
          "current_type": "DC",
          "max_power_kw": 150.0,
          "status": "available"
        },
        {
          "id": "CON-901def234ghi",
          "type": "CHAdeMO",
          "current_type": "DC",
          "max_power_kw": 100.0,
          "status": "in_use"
        }
      ]
    }
  ]
}
```

### Response 404
```json
{
  "error": "station_not_found",
  "message": "No station found with id STA-abc123def456"
}
```

## PostgreSQL Function: find_nearby_stations

### Signature
```sql
CREATE OR REPLACE FUNCTION find_nearby_stations(
    p_lat DOUBLE PRECISION,
    p_lon DOUBLE PRECISION,
    p_radius_m INTEGER DEFAULT 10000,
    p_limit INTEGER DEFAULT 50
)
RETURNS TABLE(
    station_id VARCHAR(15),
    name VARCHAR(255),
    distance_m DOUBLE PRECISION,
    max_power_kw DECIMAL(6,2),
    power_tier VARCHAR(20),
    status VARCHAR(20),
    available_connectors INTEGER,
    total_connectors INTEGER
)
```

### Query Pattern
```sql
SELECT
    station_id,
    station_name AS name,
    ST_Distance(location, ST_MakePoint(p_lon, p_lat)::geography) AS distance_m,
    max_power_kw,
    power_tier,
    status,
    available_connectors,
    total_connectors
FROM mv_stations_geo
WHERE ST_DWithin(location, ST_MakePoint(p_lon, p_lat)::geography, p_radius_m)
ORDER BY distance_m
LIMIT p_limit;
```
