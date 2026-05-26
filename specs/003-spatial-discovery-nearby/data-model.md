# Data Model Addendum: Spatial Discovery — Nearby API

**Phase**: 1 (Design & Contracts)

## New Types

### NearbyStationResult (Read-Only Projection)

This is NOT a persisted entity. It is a query result struct returned by the
nearby discovery endpoint.

| Field | Type | Source | Description |
|-------|------|--------|-------------|
| `station_id` | `String` | `stations.id` | STN- prefixed semantic ID |
| `station_name` | `String` | `stations.name` | Station display name |
| `address` | `String` | `stations.address` | Street address |
| `city` | `String` | `stations.city` | City name |
| `longitude` | `f64` | `ST_X(stations.coordinates::geometry)` | Longitude (4326) |
| `latitude` | `f64` | `ST_Y(stations.coordinates::geometry)` | Latitude (4326) |
| `distance_meters` | `f64` | `ST_Distance(stations.coordinates, $point)` | Geodesic distance from query point |
| `available_chargers_count` | `i64` | `COUNT(*) FILTER (WHERE chargers.status = 'available')` | Count of available chargers |
| `is_test` | `bool` | `stations.is_test` | Sandbox flag |

### Serialization

```rust
struct NearbyStationResult {
    station_id: String,
    station_name: String,
    address: String,
    city: String,
    longitude: f64,
    latitude: f64,
    distance_meters: f64,
    available_chargers_count: i64,
    is_test: bool,
}
```

Derives `Serialize`, `Deserialize`, `sqlx::FromRow`. Returned as a flat JSON
array (no `{data: [...]}` wrapper).

## Existing Entities (Unchanged)

No changes to any existing table, column, index, or enum. The nearby query uses:
- `stations` table (unchanged, Phase 1 schema)
- `chargers` table (unchanged, Phase 1 schema)
- GIST index on `stations.coordinates` (created in Phase 1 migration
  `20260526000005_create_stations.up.sql`)

## Query Shape

```
stations ──LEFT JOIN── chargers (ON station_id)
    │                        │
    │                   FILTER WHERE status = 'available'
    │                        │
    └── GROUP BY stations.id ──┘
         ORDER BY ST_Distance(...)
         LIMIT 50
```

The `LEFT JOIN` ensures stations with zero available chargers still appear in
results (with `available_chargers_count = 0`).

## Index Utilization

| Index | Type | Phase Created | Used For |
|-------|------|---------------|----------|
| `idx_stations_coordinates` | GIST | Phase 1 | `ST_DWithin` bounding filter |
| `idx_stations_active_lookup` | Partial B-tree (deleted_at IS NULL) | Phase 1 | Soft-delete filter |
| `idx_chargers_station_status` | B-tree (station_id, status) | Phase 1 | Aggregation join + FILTER |
