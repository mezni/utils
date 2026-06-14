# Data Model: MVP-1 Sprint 1 — Backend Core API

## Entity: Station

**Source**: `platform_db.inventory.station` (PostGIS table, seeded by Sprint 0 OSM import)

| Field | Type | Required | Validation | Notes |
|---|---|---|---|---|
| `id` | `VARCHAR(20)` | Yes | PK, format `STA-NNNNN` | Primary key, read-only |
| `name` | `VARCHAR(255)` | No | — | Empty string if NULL in DB |
| `status` | `VARCHAR(20)` | Yes | `CHECK IN ('active', 'maintenance', 'inactive')` | Default `'active'`; nearby search filters `status = 'active'` |
| `latitude` | `NUMERIC(10,8)` | No | -90 to 90 | May be NULL; filtered out in results |
| `longitude` | `NUMERIC(11,8)` | No | -180 to 180 | May be NULL; filtered out in results |
| `location` | `GEOGRAPHY(POINT, 4326)` | Yes | Valid WGS 84 point | PostGIS geography type; source of truth for geospatial queries |
| `created_at` | `TIMESTAMP` | No | Auto-set on insert | Returned by DB but excluded from API response |

## API Response Shape

```json
{
  "id": "STA-00001",
  "name": "STA-00001",
  "status": "active",
  "latitude": 36.7807266,
  "longitude": 10.1937043,
  "distance": 0.0
}
```

- `distance` = 0 for list/detail endpoints; computed in meters for nearby endpoint
- NULL `name` → empty string in response
- NULL `latitude`/`longitude` stations are excluded from results

## Validation Rules

### Parameter Validation (FR-007)

| Parameter | Rule |
|---|---|
| `lat` | Required for nearby; range -90 to 90 |
| `lng` | Required for nearby; range -180 to 180 |
| `radius` | Optional for nearby; must be > 0; default 5000m |

### Query Filtering

| Endpoint | Filter |
|---|---|
| `GET /stations` | None — returns all stations regardless of status |
| `GET /stations/{id}` | None — returns station by ID regardless of status |
| `GET /stations/nearby` | `WHERE status = 'active'` only |
| All endpoints | `WHERE latitude IS NOT NULL AND longitude IS NOT NULL` |

## PostGIS Queries

### Nearby Search (FR-004)

```sql
SELECT
  id, name, status, latitude, longitude,
  ST_Distance(location, ST_SetSRID(ST_MakePoint($lng, $lat), 4326)::geography) AS distance
FROM inventory.station
WHERE
  status = 'active'
  AND ST_DWithin(location, ST_SetSRID(ST_MakePoint($lng, $lat), 4326)::geography, $radius)
ORDER BY distance ASC
```

### Health Check

```sql
SELECT 1
```
