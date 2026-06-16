# GIS Specification

## OSM Importer

### Purpose
Import Tunisian charging station data from OpenStreetMap into the `gis` schema as reference data. This is a one-shot or scheduled batch process (MVP-2 scope).

### Container
Runs as a Docker Compose service (`osm-importer`). Written as a Rust binary or Python script that:
1. Downloads OSM data for Tunisia (from Geofabrik or Overpass API)
2. Filters for `amenity=charging_station` nodes/ways
3. Transforms tags to `gis.osm_stations` columns
4. Upserts into `gis.osm_stations` (matched on `osm_id`)

### OSM Tag Mapping

| OSM Tag | `gis.osm_stations` Column | Notes |
|---------|---------------------------|-------|
| `id` (node/way) | `osm_id` | |
| `name` | `name` | |
| `amenity=charging_station` | (implicit) | Filter condition |
| `addr:*` | `address` | Concatenated from addr:street, addr:housenumber, etc. |
| `addr:city` | `city` | |
| `operator` | `operator` | |
| `capacity` | `capacity` | Number of charging spots |
| `socket:*` | (raw_tags) | Charger connector details stored in JSONB |
| (all other tags) | `raw_tags` | Full tag dump as JSONB |

### Geometry
- Node: use `ST_SetSRID(ST_MakePoint(lon, lat), 4326)` converted to geography
- Way: use centroid of way geometry

### Importer Flow
1. Download Tunisia OSM extract (`.osm.pbf`)
2. Parse with `osmpbf` or `osmium` tool
3. Filter `amenity=charging_station`
4. For each match: upsert into `gis.osm_stations`
5. Also import city boundaries (`boundary=administrative`, `admin_level=8`) into `gis.osm_cities`
6. Also import major roads (`highway=motorway/trunk/primary/secondary`) into `gis.osm_roads`

---

## Nearby SQL Function

### Signature

```sql
CREATE OR REPLACE FUNCTION gis.nearby(
    lat double precision,
    lon double precision,
    radius_m integer DEFAULT 5000,
    max_results integer DEFAULT 50
)
RETURNS TABLE(
    id varchar(32),
    name varchar(255),
    latitude double precision,
    longitude double precision,
    address text,
    city varchar(100),
    status station_status,
    visibility station_visibility,
    distance_m double precision,
    has_24h_access boolean
)
LANGUAGE sql STABLE
AS $$
    SELECT
        s.id,
        s.name,
        ST_Y(s.location::geometry) as latitude,
        ST_X(s.location::geometry) as longitude,
        s.address,
        s.city,
        s.status,
        s.visibility,
        ST_Distance(s.location, ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography) as distance_m,
        s.has_24h_access
    FROM inventory.station s
    WHERE s.deleted_at IS NULL
      AND s.status = 'active'
      AND ST_DWithin(
          s.location,
          ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography,
          radius_m
      )
    ORDER BY distance_m
    LIMIT max_results;
$$;
```

### Input Parameters

| Param | Type | Default | Range | Description |
|-------|------|---------|-------|-------------|
| `lat` | `double precision` | required | -90 to 90 | User's latitude |
| `lon` | `double precision` | required | -180 to 180 | User's longitude |
| `radius_m` | `integer` | 5000 | 100 to 50000 | Search radius in meters |
| `max_results` | `integer` | 50 | 1 to 200 | Max results to return |

### Output Columns

| Column | Type | Description |
|--------|------|-------------|
| `id` | `varchar(32)` | Station ID (STA_...) |
| `name` | `varchar(255)` | Station name |
| `latitude` | `double precision` | Decoupled from geography for JSON serialization |
| `longitude` | `double precision` | Decoupled from geography for JSON serialization |
| `address` | `text` | Street address |
| `city` | `varchar(100)` | City |
| `status` | `station_status` | Always 'active' (filtered) |
| `visibility` | `station_visibility` | 'commercial' or 'private_home' |
| `distance_m` | `double precision` | Distance from query point in meters |
| `has_24h_access` | `boolean` | 24h access flag |

### Edge Cases

| Scenario | Behavior |
|----------|----------|
| No stations in radius | Returns empty result set (not an error) |
| Radius exceeds max (50000) | Application-layer rejects with `GEO_002` |
| Invalid coordinates (lat > 90) | Application-layer rejects with `GEO_001` |
| Station at exact query point | Returns with `distance_m = 0` |
| Overlapping station locations | Returns both, ordered by distance (tiebreaker: name) |
| Station with no chargers | Still returned (chargers in separate query) |

### Performance

- GIST index on `inventory.station.location` covers `ST_DWithin`
- Index on `status` filters only active stations before spatial computation
- Index on `deleted_at IS NULL` expression index if performance warrants
- Expected: < 5ms query time for 50km radius with 10k stations

---

## GIS Service — Standalone Spatial API

### Role

The GIS Service is a dedicated read-optimized spatial API. It is **not** a proxy or library embedded in Driver Service — it runs as an independent service (`:3003`) with its own Redis cache layer.

### Service Relationship

```
Driver Service  ──HTTP──> GIS Service :3003  (spatial reads)
Admin Service   ──HTTP──> GIS Service :3003  (spatial reads)
GIS Service     ──Redis──> :6379             (cache)
GIS Service     ──SQL──> platform_db (gis + inventory schemas)
```

Driver Service and Admin Service call GIS Service internally for spatial queries. They do **not** query PostGIS directly for spatial reads.

### Endpoints

#### `GET /api/v1/nearby`

Spatial search for active stations. Public endpoint (no auth).

**Query params**: `lat` (double, required), `lon` (double, required), `radius_m` (int, optional, default 5000), `max_results` (int, optional, default 50).

**Caching**: Result cached in Redis with key `nearby:{lat:.2f}:{lon:.2f}:{radius_m}`, TTL 120 seconds. Cache is invalidated on station write via cache-bust endpoint.

**Response 200**: Station array with id, name, location, address, distance_m, visibility, status, chargers (see `docs/spec/api-contracts.md`).

**Error codes**: `GEO_001`, `GEO_002`, `GEO_003` (cache unavailable).

#### `GET /api/v1/stations/{id}`

Station detail with chargers. Public endpoint.

**Caching**: Cached per-station with TTL 300 seconds.

**Error codes**: `STA_001`.

#### `POST /api/v1/internal/cache/invalidate`

Cache-bust endpoint called by Driver/Admin Service after station/charger writes. Internal — Docker network only, no public exposure.

**Request**:
```json
{
  "station_ids": ["STA_xxxxx", "STA_yyyyy"],
  "reason": "station_update"
}
```

**Response 200**: `{ "invalidated": true, "keys_affected": 12 }`

### Redis Cache Strategy

| Cache Key Pattern | Value | TTL | Invalidated By |
|-------------------|-------|-----|----------------|
| `nearby:{lat}:{lon}:{radius}` | Serialized nearby result array | 120s | Cache-bust endpoint |
| `station:{id}` | Serialized station + chargers | 300s | Cache-bust endpoint |
| `nearby:hot:{lat_grid}:{lon_grid}` | Pre-computed hot zone results | 600s | Time-based expiry |

**Cache-aside pattern**:
1. GIS Service receives request
2. Check Redis for matching key
3. If hit: return cached result
4. If miss: query `gis.nearby()` SQL function, serialize result, store in Redis with TTL, return

### Performance Targets

| Operation | Target | Under Load |
|-----------|--------|------------|
| Nearby query (cache hit) | < 5ms | < 10ms |
| Nearby query (cache miss) | < 50ms | < 200ms |
| Station detail (cache hit) | < 3ms | < 5ms |
| Station detail (cache miss) | < 20ms | < 50ms |
| Cache invalidation | < 100ms | < 500ms |
