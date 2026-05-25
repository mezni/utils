# BorneMap — Technical Production Implementations

## 1. Actix-web Route Configuration

File: `sources/backend/src/domain/infrastructure/mod.rs`

### Nearby Stations Endpoint

**Route**: `GET /api/v1/stations/nearby`

#### Query Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `longitude` | `f64` | Yes | — | User's longitude (SRID 4326) |
| `latitude` | `f64` | Yes | — | User's latitude (SRID 4326) |
| `radius_meters` | `f64` | No | `20000.0` (20km) | Search radius in meters |
| `include_test` | `bool` | No | `false` | Whether to include test records |

#### Response

```json
[
  {
    "station_id": "STN-abc123def456",
    "station_name": "Station Name",
    "address": "Route de Tunis KM 5",
    "city": "Tunis",
    "longitude": 10.1815,
    "latitude": 36.8065,
    "distance_meters": 1234.56,
    "available_chargers_count": 2,
    "is_test": false
  }
]
```

#### Isolation Behavior

The `include_test` parameter defaults to `false`, ensuring the **mobile app never sees test records** unless explicitly requested. Admin tools may pass `include_test=true` when the sandbox workspace selector is active.

## 2. High-Performance Discovery Repository

File: `sources/backend/src/domain/infrastructure/repository.rs`

### Function Signature

```rust
pub async fn find_nearby_stations_bounded(
    pool: &PgPool,
    user_lng: f64,
    user_lat: f64,
    radius_meters: f64,
    include_test: bool,
) -> Result<Vec<NearbyStationResult>, sqlx::Error>
```

### Query Logic

```sql
SELECT
    s.id as station_id,
    s.name as station_name,
    s.address,
    s.city,
    ST_X(s.coordinates::geometry) as longitude,
    ST_Y(s.coordinates::geometry) as latitude,
    ST_Distance(s.coordinates, ST_MakePoint($1, $2)::geography) as distance_meters,
    COUNT(c.id) FILTER (WHERE c.status = 'available') as available_chargers_count,
    s.is_test
FROM stations s
LEFT JOIN chargers c ON c.station_id = s.id
WHERE s.deleted_at IS NULL
  AND ST_DWithin(s.coordinates, ST_MakePoint($1, $2)::geography, $3)
  AND ($4 = TRUE OR s.is_test = FALSE)
GROUP BY s.id
ORDER BY distance_meters ASC
LIMIT 50
```

### Performance Characteristics

| Aspect | Implementation |
|--------|---------------|
| Spatial index | GIST index on `coordinates` column |
| Soft-delete filter | Partial index `idx_stations_active_lookup` |
| Distance calculation | `ST_DWithin` for radius bounding (uses index) |
| Result count | Hard-capped at 50 via `LIMIT` |
| Available charger count | `COUNT(...) FILTER (WHERE c.status = 'available')` — single-pass aggregation |

### Isolation Boundary

The clause `AND ($4 = TRUE OR s.is_test = FALSE)` ensures:

- When `include_test = false` (default): Only non-test stations are returned
- When `include_test = true`: All stations matching the spatial query are returned
- This filter is applied at the **SQL level**, not the application level, preventing any potential leak

### SLO Target

This query must execute within **≤ 200ms** under concurrent production workloads against the seeded dataset of 100 stations.
