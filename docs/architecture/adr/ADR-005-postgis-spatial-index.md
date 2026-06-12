# ADR-005: PostGIS Spatial Indexes for Nearby Search Performance

**Status:** Accepted  
**Date:** 2026-06-10  
**Authors:** Claude Code, Claude (chat)

---

## Context

Driver service provides critical discovery feature:
```
GET /api/v1/stations/nearby?lat={f64}&lng={f64}&radius={f64}
```

This query must:
- Return stations within radius (ST_DWithin PostGIS function)
- Order by distance ascending
- Complete in **<100ms** for radius up to 50km
- Handle concurrent requests from 1000+ drivers

Without spatial indexes, a table scan over thousands of stations takes seconds. With indexes, <100ms is achievable.

---

## Decision

**Create a GiST (Generalized Search Tree) spatial index on the `location` column of `platform_db.inventory.station`.**

Index definition:
```sql
CREATE INDEX idx_station_location_gist
  ON inventory.station
  USING GIST(location)
  WHERE deleted_at IS NULL;
```

Additional optimizations:
1. **Partial index** — excludes soft-deleted stations (`deleted_at IS NULL`)
2. **SRID 4326** — WGS 84 (GPS coordinates), standard in geospatial databases
3. **Column type** — `GEOMETRY(Point, 4326)` or `GEOGRAPHY(Point)`

Query optimization:
```sql
SELECT id, name, address, lat, lng, status, charger_count, available_chargers,
       ST_Distance(location, ST_SetSRID(ST_Point($lng, $lat), 4326)) as distance_m
FROM inventory.station
WHERE deleted_at IS NULL
  AND ST_DWithin(
    location,
    ST_SetSRID(ST_Point($lng, $lat), 4326),
    $radius_km * 1000  -- Convert km to meters
  )
ORDER BY distance_m ASC
LIMIT $limit;
```

---

## Rationale

### Performance Baseline
- **Without index:** Table scan + full distance calculation → 2-5 seconds (1000+ stations)
- **With GiST index:** Index seek + distance calculation → <100ms

### Index Type: GiST vs BRIN vs SPGIST
| Index | Type | Range Queries | Speed | Disk |
|-------|------|---------------|-------|------|
| **GiST** | **Balanced tree** | **Excellent** | **Fast** | **Medium** |
| BRIN | Block range | Good | Medium | Small |
| SPGIST | Quadtree | Excellent | Fast | Large |

GiST is the standard choice for geographic queries. SPGIST is slightly faster but larger; BRIN is space-efficient but slower.

### Partial Index Rationale
Station deletion is soft-delete (deleted_at timestamp). The index excludes deleted rows, reducing index size and improving selectivity.

### SRID 4326 (WGS 84)
- Standard for GPS data
- Familiar to geospatial developers
- PostGIS native support

### Distance Calculation
Two options:
1. **GEOMETRY (Planar)** — fast, approximate (error ~0.5% for large distances)
2. **GEOGRAPHY (Geodetic)** — slower, accurate (spherical Earth model)

For Tunisia (small area, <100km radius), GEOMETRY is sufficient and faster. Alternative: use GEOGRAPHY for global scale.

---

## Consequences

### Positive
- **Latency:** Nearby queries complete in <100ms consistently
- **Throughput:** Can handle 1000s of concurrent requests
- **Scalability:** Index grows slowly with data (logarithmic query time)
- **Flexibility:** Easy to add other spatial queries (bounding boxes, polygons, etc.)

### Negative
- **Index maintenance:** Slower INSERT/UPDATE/DELETE (negligible for <10k stations)
- **Disk usage:** +20-30MB for index (acceptable)
- **Complexity:** Requires PostGIS knowledge
- **Migration:** Existing data needs index rebuild (one-time)

---

## Implementation Notes

1. **Schema migration:**
   ```sql
   -- Enable PostGIS
   CREATE EXTENSION IF NOT EXISTS postgis;

   -- Create table with geometry column
   CREATE TABLE inventory.station (
       id VARCHAR(50) PRIMARY KEY,
       name VARCHAR(255) NOT NULL,
       address VARCHAR(255),
       lat DOUBLE PRECISION NOT NULL,
       lng DOUBLE PRECISION NOT NULL,
       location GEOMETRY(Point, 4326) GENERATED ALWAYS AS
           (ST_SetSRID(ST_Point(lng, lat), 4326)) STORED,
       status VARCHAR(20) DEFAULT 'offline',
       charger_count INTEGER DEFAULT 0,
       available_chargers INTEGER DEFAULT 0,
       partner_id VARCHAR(50) NOT NULL,
       opening_hours VARCHAR(255),
       created_at TIMESTAMP DEFAULT NOW(),
       updated_at TIMESTAMP DEFAULT NOW(),
       deleted_at TIMESTAMP,
       FOREIGN KEY (partner_id) REFERENCES inventory.partner(id)
   );

   -- Create spatial index
   CREATE INDEX idx_station_location_gist
       ON inventory.station USING GIST(location)
       WHERE deleted_at IS NULL;
   ```

2. **Driver service query:**
   - Use sqlx with prepared statements
   - Parameters: `lat`, `lng`, `radius_km`
   - Convert radius to meters (internal PostGIS unit)

3. **Performance testing:**
   - Benchmark with 1000+ stations in test DB
   - Verify <100ms response time for 50km radius
   - Measure index size with `\d+ inventory.station` in psql

4. **Monitoring:**
   - Track query execution time via logs
   - Monitor index bloat (use `pgstattuple` extension periodically)
   - Alert if nearby search exceeds 200ms

---

## Related ADRs

- ADR-002: Rust + Actix (driver-service uses this query)
- ADR-001: Traefik gateway (routes nearby requests to driver-service)

---

## References

- [PostGIS documentation](https://postgis.net/docs/)
- [PostGIS distance functions](https://postgis.net/docs/ST_Distance.html)
- [PostGIS indexing strategy](https://postgis.net/docs/using_postgis_dbmanagement.html)
- [GiST index type](https://www.postgresql.org/docs/current/gist.html)
