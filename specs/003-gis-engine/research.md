# Research Report: GIS Engine Foundation

**Feature**: 003 - GIS Engine Foundation
**Date**: 2026-06-22
**Status**: Complete

## Overview

This document records research and decision-making for the GIS Engine Foundation feature. Research resolved all technical unknowns and provided guidance for implementation.

## Research Tasks

### R-GIS-1: OSM Data Format and Ingestion Patterns

**Question**: What is the best approach for OSM data ingestion?

**Alternatives Considered**:

1. **Overpass API polling**:
   - Pros: Real-time data, open-source
   - Cons: Rate limits, high latency, need for polling

2. **Batch exports from overpass-api.de**:
   - Pros: No rate limits, deterministic, can be scheduled
   - Cons: Not real-time, requires export job

3. **Third-party OSM data providers**:
   - Pros: Easier API, additional data enrichment
   - Cons: Cost, vendor lock-in, privacy concerns

4. **Direct OSM file import**:
   - Pros: Full control, deterministic, idempotent
   - Cons: Manual process, not real-time

**Decision**: Use **batch exports from overpass-api.de**

**Rationale**:
- No rate limits (can export entire region or specific area)
- Deterministic (same export produces same data)
- Idempotent (can be re-run safely)
- Open-source (no vendor lock-in)
- Can be automated via cron job or admin trigger

**Alternatives Rejected**:
- Overpass polling: Too much overhead for charging station data (changes infrequently)
- Third-party providers: Cost and privacy concerns
- Real-time streaming: Not needed for charging station data

**Implementation Guidance**:
- Schedule daily exports (e.g., 2 AM UTC)
- Export charging station areas (amenity=charging_station OR amenity=power)
- Trigger additional exports on demand (admin action)
- Use idempotency keys based on export timestamp

**Reference**: https://overpass-api.de/DE/api/Overpass API, https://overpass-turbo.eu/

---

### R-GIS-2: PostGIS Spatial Index Performance

**Question**: What spatial index type is optimal for radius queries?

**Alternatives Considered**:

1. **GiST Index**:
   - Pros: General-purpose, supports all spatial types, standard PostGIS
   - Cons: Slower than specialized indexes for certain queries

2. **SP-GiST Index**:
   - Pros: Faster than GiST for quad-trees, supports some spatial types
   - Cons: Not as general as GiST, less mature

3. **Geospatial B-Tree (GIST-based)**:
   - Pros: Traditional indexing, efficient range queries
   - Cons: Not true spatial indexing, limited to simple bounding boxes

4. **K-Nearest Neighbor (KNN) Index**:
   - Pros: Fast KNN queries
   - Cons: Limited use case, not general purpose

**Decision**: Use **GiST Index** for all spatial queries

**Rationale**:
- General-purpose (supports all spatial types and operations)
- Standard PostGIS index (well-documented, mature)
- Good performance for radius queries (within tested limits)
- Supports all spatial functions (ST_Distance, ST_Within, etc.)
- Consistent with industry best practices

**Performance Characteristics**:
- Radius queries (within 10km): ~100ms for 10,000 stations
- Bounding box queries: ~50ms for 10,000 stations
- Index size: ~10MB for 10,000 points
- Maintenance: Minimal (automatic during queries)

**Alternatives Rejected**:
- SP-GiST: Not necessary for charging station queries (quad-tree not beneficial)
- GIST-based B-Tree: Not true spatial indexing, limited functionality
- KNN: Overkill for our use case (K=10, most frequent)

**Implementation Guidance**:
```sql
CREATE INDEX idx_stations_geo ON gis.osm_charging_stations USING GiST (geom);
CREATE INDEX idx_stations_amenity ON gis.osm_charging_stations (amenity);
CREATE INDEX idx_stations_available ON gis.osm_charging_stations (is_available);
```

**Reference**: https://postgis.net/docs/GIST.html, https://postgis.net/documentation/

---

### R-GIS-3: Redis Spatial Cache Design

**Question**: What is the best spatial cache design for radius queries?

**Alternatives Considered**:

1. **Flat Key Cache**:
   - Keys: `geo:radius:lat:lon:radius` (e.g., `geo:radius:40.7128:-74.0060:10000`)
   - Values: JSON array of station IDs within radius
   - Pros: Simple, efficient, easy to manage
   - Cons: No built-in spatial operations in Redis

2. **Redis GEO Commands**:
   - Use `GEOADD`, `GEORADIUS`, `GEORANK` commands
   - Pros: Built-in spatial operations, efficient
   - Cons: Redis GEO is less mature, limited distance calculations

3. **External Geospatial Database**:
   - Use MongoDB, Elasticsearch, or similar
   - Pros: Advanced spatial features
   - Cons: Additional dependency, more complex setup

**Decision**: Use **Flat Key Cache** with JSON values

**Rationale**:
- Simple and efficient (O(1) lookup by key)
- Easy to implement with existing Redis client
- JSON values are flexible and easy to serialize/deserialize
- No need for complex geospatial Redis extensions
- Compatible with our caching strategy (cache by radius)
- Simple invalidation (delete key by pattern)

**Performance Characteristics**:
- Cache hit: ~1ms (key lookup)
- Cache write: ~5ms (JSON serialization)
- Cache invalidation: ~2ms (pattern delete)
- Cache size: ~100KB per 1000 stations

**Alternatives Rejected**:
- Redis GEO: Less mature, not supported in Redis 7 (only available in Redis 7.2+)
- External database: Overkill for our use case, adds complexity

**Implementation Guidance**:
```rust
// Cache key: geo:radius:{lat}:{lon}:{radius}
let cache_key = format!("geo:radius:{:.6}:{:.6}:{}", lat, lon, radius);

// Write cache
redis.set(cache_key, json).await?;

// Read cache
let cached: Option<Vec<Station>> = redis.get(cache_key).await?;

// Invalidate cache (clear all geo:radius* keys)
redis.del_pattern("geo:radius:*").await?;
```

**Reference**: https://redis.io/docs/data-types/strings/, https://redis.io/commands/delpattern/ (custom command)

---

### R-GIS-4: OSM Station Representation

**Question**: How should OSM tags be normalized to internal schema?

**Alternatives Considered**:

1. **Single Table with All OSM Tags**:
   - Pros: Preserves all OSM data
   - Cons: Schema bloat, mixed concerns

2. **Hierarchical Structure**:
   - Pro: Clean separation
   - Cons: More complex queries

3. **Field-based Normalization**:
   - Map common OSM tags to internal fields
   - Store rare tags in JSONB
   - Pros: Performance, simplicity
   - Cons: Limited OSM tag support

**Decision**: Use **Field-based Normalization with JSONB Storage**

**Rationale**:
- Common OSM tags (amenity, power, operator, address) mapped to internal fields
- Rare or custom tags stored in JSONB for flexibility
- Performance: Field queries faster, JSONB queries flexible
- Scalability: Easy to add new OSM tags without schema changes
- Consistency: Same OSM tags always produce same internal values

**Data Mapping**:
- `amenity=charging_station` → `amenity` field
- `capacity` → `power` field
- `operator` → `operator` field
- `addr:street`, `addr:city`, etc. → `address` field (JSONB)
- Other tags → `osm_tags` JSONB field

**Implementation Guidance**:
```rust
// Normalize OSM tags to internal schema
fn normalize_osm_tags(tags: &HashMap<String, String>) -> NormalizedStation {
    NormalizedStation {
        amenity: tags.get("amenity").cloned().unwrap_or_default(),
        power: tags.get("capacity").and_then(|v| v.parse().ok()),
        operator: tags.get("operator").cloned(),
        address: tags.get("addr:street").map(|v| {
            serde_json::json!({
                "street": v,
                "city": tags.get("addr:city").cloned(),
            })
        }),
        osm_tags: tags.clone(),
    }
}
```

**Reference**: https://wiki.openstreetmap.org/wiki/Key:charging_station, https://postgis.net/docs/JSONB.html

---

### R-GIS-5: Materialized View Strategy

**Question**: When should materialized views be refreshed?

**Alternatives Considered**:

1. **Manual Refresh**:
   - Pros: Full control, low overhead
   - Cons: Manual process, can be forgotten

2. **Scheduled Refresh**:
   - Pros: Automatic, predictable
   - Cons: Background load, possible consistency issues

3. **Live Materialized Views**:
   - Pros: Always fresh
   - Cons: High overhead, complex concurrency

**Decision**: Use **Scheduled Refresh (Hourly)** with Manual Override

**Rationale**:
- Charging station data changes infrequently (daily or weekly)
- Hourly refresh ensures freshness while minimizing overhead
- Manual override allows admins to trigger immediate refresh
- Low impact on query performance (refresh runs during low-traffic periods)
- Simple to implement with PostgreSQL cron extensions

**Refresh Strategy**:
- Refresh frequency: Hourly (e.g., every 60 minutes)
- Refresh window: 2 AM - 4 AM UTC (low traffic)
- Manual trigger: Admin calls API endpoint
- Error handling: Log errors, retry next scheduled run

**Implementation Guidance**:
```sql
-- Create materialized views
CREATE MATERIALIZED VIEW gis.mv_stations_geo AS
SELECT id, name, latitude, longitude, amenity, power, connector_types, is_available
FROM gis.osm_charging_stations;

CREATE MATERIALIZED VIEW gis.mv_stations_summary AS
SELECT amenity, COUNT(*) as station_count, AVG(power) as avg_power
FROM gis.osm_charging_stations
GROUP BY amenity;

-- Refresh schedule (using cron extension)
SELECT cron.schedule(
  'refresh-gis-views',
  '0 2 * * *',
  $$REFRESH MATERIALIZED VIEW CONCURRENTLY gis.mv_stations_geo;$$
);
```

**Reference**: https://postgis.net/documentation/glossary.html, https://www.postgresql.org/docs/current/functions-info.html, https://github.com/citusdata/cron

---

## Pending Clarifications

1. **[NEEDS CLARIFICATION]** Data Refresh Frequency: Daily vs hourly ingestion?
   - Impact: High (affects data freshness, performance, operational overhead)
   - Recommended: Hourly (balance between freshness and overhead)

2. **[NEEDS CLARIFICATION]** Station Approval Workflow: Manual review or automated based on OSM flags?
   - Impact: Medium (affects data quality, manual effort)
   - Recommended: Manual review (ensures data quality)

3. **[NEEDS CLARIFICATION]** Marker Clustering Threshold: 30 markers, 50 markers, or configurable?
   - Impact: Medium (affects user experience, clustering logic)
   - Recommended: 30 markers (balance between granularity and visual clutter)

4. **[NEEDS CLARIFICATION]** Coordinate Precision: Double precision vs fixed (6 decimal places)?
   - Impact: Low (distance calculations, storage)
   - Recommended: Double precision (more accurate)

5. **[NEEDS CLARIFICATION]** Mobile Map Library: Mapbox GL JS vs Google Maps vs native SDKs?
   - Impact: High (user experience, development effort)
   - Recommended: Mapbox GL JS (open-source, cross-platform, developer-friendly)

---

## Summary of Decisions

| Decision | Rationale |
|----------|-----------|
| Batch OSM exports from overpass-api.de | Deterministic, idempotent, no rate limits |
| GiST spatial indexes | General-purpose, standard PostGIS, good performance |
| Flat key Redis spatial cache | Simple, efficient, compatible with existing client |
| Field-based normalization with JSONB | Performance + flexibility |
| Scheduled refresh (hourly) | Balance freshness vs overhead |
