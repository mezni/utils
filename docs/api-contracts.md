# API Contracts — BorneMap MVP-1

**Base URL**: `/api/v1`

**Port**: `driver-service:3001`

## Endpoints

### Health Check

```
GET /api/v1/health
```

**Response** `200 OK`:
```json
{
    "status": "ok"
}
```

---

### Nearby Stations Discovery

```
GET /api/v1/stations/nearby?longitude={lon}&latitude={lat}&radius={meters}
```

**Query Parameters**:
| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `longitude` | f64 | Yes | — | Driver longitude (WGS84) |
| `latitude` | f64 | Yes | — | Driver latitude (WGS84) |
| `radius` | f64 | No | 5000 | Search radius in meters |

**Constraints**:
- `longitude` MUST be within `[7.0, 12.0]` (Tunisia bounds)
- `latitude` MUST be within `[30.0, 38.0]` (Tunisia bounds)
- `radius` MUST be > 0

**Response** `200 OK`:
```json
{
    "stations": [
        {
            "station_id": "STA-001",
            "station_name": "Tunis Central Charging Hub",
            "station_address": "Avenue Habib Bourguiba, Tunis",
            "distance_meters": 234.5,
            "latitude": 36.8065,
            "longitude": 10.1815,
            "available_chargers": [
                {
                    "charger_id": "CHR-001",
                    "code": "CHARGER-A",
                    "plug_type": "ccs2",
                    "max_power_kw": 150,
                    "status": "ONLINE"
                },
                {
                    "charger_id": "CHR-002",
                    "code": "CHARGER-B",
                    "plug_type": "type2",
                    "max_power_kw": 22,
                    "status": "AVAILABLE"
                }
            ]
        }
    ]
}
```

**Response** `400 Bad Request` (out of bounds):
```json
{
    "error": "Coordinates outside Tunisia operational bounds"
}
```

**Response** `400 Bad Request` (invalid radius):
```json
{
    "error": "search_radius_meters must be positive"
}
```

**Response** `422 Unprocessable Entity`:
```json
{
    "error": "Missing required parameter: latitude"
}
```

**Response** `503 Service Unavailable`:
```json
{
    "error": "Database connection failed"
}
```

---

## Data Model (API-facing)

### StationDto
| Field | Type | Description | Source |
|-------|------|-------------|--------|
| `station_id` | string | Unique station identifier (STA- prefix) | `gis.osm_stations.id` |
| `station_name` | string | Display name of the station | `gis.osm_stations.name` |
| `station_address` | string/null | Physical address | `gis.osm_stations.address` |
| `distance_meters` | f64 | Distance from query point (meters) | `ST_Distance(...)` |
| `latitude` | f64 | Station latitude (WGS84) | `ST_Y(coordinates)` |
| `longitude` | f64 | Station longitude (WGS84) | `ST_X(coordinates)` |
| `available_chargers` | ChargerDto[] | Array of chargers at this station | `jsonb_agg(...)` |

### ChargerDto
| Field | Type | Description | Source |
|-------|------|-------------|--------|
| `charger_id` | string | Unique charger identifier (CHR- prefix) | `inventory.chargers.id` |
| `code` | string | Hardware identifier code at station | `inventory.chargers.identifier_code` |
| `plug_type` | string | Connector standard (ccs2, type2, chademo) | `inventory.chargers.plug_type_code` |
| `max_power_kw` | int | Maximum power output in kilowatts | `inventory.chargers.max_power_kw` |
| `status` | string | Operational status (ONLINE, CHARGING, FAULTED, OFFLINE) | `inventory.chargers.status` |
