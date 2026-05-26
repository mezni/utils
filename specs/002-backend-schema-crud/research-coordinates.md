# Research: PostGIS Coordinate Storage & Query Patterns

**Feature**: `002-backend-schema-crud`
**Date**: 2026-05-26
**Scope**: Station coordinate storage, validation, indexing, and retrieval with PostGIS on PostgreSQL 16+

---

## Decision: Schema Definition & Access Patterns

### Column Definition

```sql
coordinates GEOGRAPHY(Point, 4326) NOT NULL
```

- `GEOGRAPHY` (not `GEOMETRY`) — calculates on a sphere, so `ST_DWithin` returns true meter distances without manual degree-to-meter conversion
- `Point` — single coordinate per station, no need for LineString or Polygon
- `SRID 4326` — WGS 84, the GPS standard; mandated by constitution §2 and `docs/05-identity-security-geospatial.md`

### Insert Pattern

```sql
INSERT INTO stations (..., coordinates, ...)
VALUES (..., ST_SetSRID(ST_MakePoint($1, $2), 4326), ...)
-- $1 = longitude (f64), $2 = latitude (f64)
```

- `ST_MakePoint(lng, lat)` — **longitude first**, matching PostGIS convention and constitution mandate
- `ST_SetSRID(..., 4326)` — explicitly tags the SRID; required for `GEOGRAPHY` column insertion
- Alternative shorthand `ST_MakePoint($1, $2)::geography` also works but `ST_SetSRID` is more explicit and catches SRID mismatches at insert time

### Select Pattern

```sql
SELECT
    ST_X(coordinates::geometry) AS longitude,
    ST_Y(coordinates::geometry) AS latitude
FROM stations
```

- `ST_X` / `ST_Y` operate on `GEOMETRY`, so `::geometry` cast is needed from `GEOGRAPHY`
- `ST_X` = longitude, `ST_Y` = latitude — consistent with the insert order

### Spatial Index

```sql
CREATE INDEX idx_stations_spatial_coordinates ON stations USING GIST (coordinates)
```

- GIST is the only index type that supports spatial operators on `GEOGRAPHY`
- This index serves both exact-match lookups and `ST_DWithin` radius queries (Phase 2)
- Index name matches `docs/07-database-schema.md` line 98

### Nearby Query (Phase 2 — validated for future compatibility)

```sql
SELECT s.id, s.name,
    ST_X(s.coordinates::geometry) AS longitude,
    ST_Y(s.coordinates::geometry) AS latitude,
    ST_Distance(s.coordinates, ST_MakePoint($1, $2)::geography) AS distance_meters
FROM stations s
WHERE s.deleted_at IS NULL
  AND ST_DWithin(s.coordinates, ST_MakePoint($1, $2)::geography, $3)
ORDER BY distance_meters ASC
LIMIT 50
```

- `ST_DWithin` on `GEOGRAPHY` uses the GIST index and returns meters natively
- Constitution SLO: ≤200ms — GIST index + LIMIT 50 achieves this on 100-station seed data
- `ST_Distance` on `GEOGRAPHY` returns meters without manual conversion

### Validation (Application Layer)

```rust
fn validate_coordinates(lng: f64, lat: f64) -> Result<(), String> {
    if !(-180.0..=180.0).contains(&lng) {
        return Err(format!("longitude must be between -180 and 180, got {lng}"));
    }
    if !(-90.0..=90.0).contains(&lat) {
        return Err(format!("latitude must be between -90 and 90, got {lat}"));
    }
    Ok(())
}
```

- Application-layer validation gives structured, localized error messages (FR-015)
- Database-level `CHECK` as a defense-in-depth backstop:

```sql
ALTER TABLE stations ADD CONSTRAINT chk_coordinates_valid
    CHECK (
        ST_X(coordinates::geometry) BETWEEN -180 AND 180
        AND ST_Y(coordinates::geometry) BETWEEN -90 AND 90
    );
```

- The CHECK is technically redundant for SRID 4326 `GEOGRAPHY(Point)` (PostGIS rejects out-of-range values on insert), but it makes the constraint visible in schema introspection tools

---

## Rationale

### Why GEOGRAPHY over GEOMETRY

| Aspect | GEOGRAPHY | GEOMETRY |
|--------|-----------|----------|
| Distance unit | Meters (native) | Degrees (must convert) |
| `ST_DWithin` radius | Meters directly | Requires `ST_Transform` or manual conversion |
| Accuracy | Geodesic (sphere) | Cartesian (flat plane) — inaccurate at distance |
| Index support | GIST | GIST |
| Performance | ~5-10% slower than GEOMETRY for simple ops | Faster for flat-plane calculations |
| Constitution compliance | **Required** (§2: `GEOGRAPHY(Point, 4326)`) | Non-compliant |

`GEOGRAPHY` is mandated by the constitution and is the correct choice for real-world distance queries. The ~5-10% overhead is negligible at 100 stations and well within the 200ms SLO.

### Why a Single GEOGRAPHY Column over Separate lat/lng Columns

| Aspect | Single `coordinates` column | Separate `latitude` + `longitude` |
|--------|----------------------------|-----------------------------------|
| Spatial index | GIST (native) | Not possible — B-tree only |
| `ST_DWithin` | Index-accelerated | Requires `ST_MakePoint` on-the-fly (no index) |
| Data integrity | PostGIS validates SRID | No built-in spatial validation |
| API output | `ST_X`/`ST_Y` extraction | Direct column reads |
| Future queries | KNN, bounding box, etc. | Must reconstruct point each time |

Separate columns would make Phase 2 nearby search a full table scan. The single `GEOGRAPHY` column with a GIST index is the only viable approach for spatial queries.

### Why `ST_SetSRID(ST_MakePoint(...), 4326)` over `ST_MakePoint(...)::geography`

Both produce identical results. `ST_SetSRID` is preferred because:
1. It makes the SRID explicit in the query text — self-documenting
2. If the column SRID ever changes, the mismatch is caught immediately rather than silently coercing
3. SQLx compile-time verification can validate the function signature

### Why Application-Layer Validation as Primary

- Structured error responses with field-level detail (which coordinate is invalid, what the value was)
- Consistent with the RFC 7807 error envelope (R8)
- Testable in unit tests without a database
- The database CHECK constraint serves as a safety net, not the primary validation path

---

## Alternatives Considered

### A1: Separate `latitude FLOAT` + `longitude FLOAT` Columns

**Rejected.** No spatial index support. `ST_DWithin` would require `ST_MakePoint` on every row at query time — full table scan. Violates constitution §2 which mandates `GEOGRAPHY(Point, 4326)`.

### A2: GEOMETRY(Point, 4326) instead of GEOGRAPHY

**Rejected.** `GEOMETRY` operates on a flat Cartesian plane. Distance calculations return degrees, not meters, requiring manual conversion (`degrees * 111139` approximation). This approximation is only accurate at the equator and diverges significantly at higher latitudes. Constitution §2 explicitly requires `GEOGRAPHY`. For Tunisia (latitude ~34°N), the degree-to-meter conversion error with GEOMETRY would be ~15-20%.

### A3: Store as JSONB `{"lat": 36.8, "lng": 10.1}`

**Rejected.** No spatial indexing, no PostGIS function support, no data integrity. Would require extracting and reconstructing points for every spatial query.

### A4: Database CHECK as Sole Validation

**Rejected as primary.** CHECK constraints produce generic PostgreSQL error messages (e.g., `new row for relation "stations" violates check constraint "chk_coordinates_valid"`). The application cannot easily extract which field failed or provide a user-friendly message. Used as defense-in-depth only.

### A5: PostGIS `ST_MakePoint` with Implicit Geography Cast

```sql
ST_MakePoint($1, $2)::geography
```

**Viable but not preferred.** Works identically to `ST_SetSRID(ST_MakePoint(...), 4326)` but is less explicit about the SRID. If a future migration changes the column's SRID, the implicit cast would silently succeed with wrong data. `ST_SetSRID` catches this at insert time.

---

## Key Integration Notes

### Rust Struct Representation

```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct Station {
    pub id: String,
    pub owner_id: String,
    pub name: String,
    pub address: String,
    pub city: String,
    pub longitude: f64,
    pub latitude: f64,
    pub is_operational: bool,
    pub is_test: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}
```

- `longitude` and `latitude` are separate `f64` fields in the Rust model
- The `GEOGRAPHY` column is never exposed to Rust directly — SQLx does not have a native `geography` type decoder
- Insert: Rust `f64` pair → `ST_SetSRID(ST_MakePoint($1, $2), 4326)` in SQL
- Select: `ST_X(coordinates::geometry) AS longitude, ST_Y(coordinates::geometry) AS latitude` → Rust `f64` fields via `sqlx::FromRow`

### API Request/Response Shape

```json
{
    "longitude": 10.1815,
    "latitude": 36.8065
}
```

- Longitude first in API transport (matching constitution §2 and `docs/05-identity-security-geospatial.md`)
- `f64` in Rust serializes to JSON number with full precision
- Coordinate ordering (lng, lat) is consistent across: API, SQL, and internal model

### SQLx Type Mapping

| Direction | Rust Type | SQL Expression | SQL Type |
|-----------|-----------|----------------|----------|
| Insert (parameter) | `f64` | `$1` in `ST_MakePoint($1, $2)` | `double precision` |
| Select (result) | `f64` | `ST_X(coordinates::geometry)` | `double precision` |
| Select (result) | `f64` | `ST_Y(coordinates::geometry)` | `double precision` |

SQLx maps `f64` ↔ `double precision` natively. No custom type registration needed.

### SQLx Compile-Time Verification

With `sqlx::query!` / `sqlx::query_as!`, the macro connects to the database at compile time and verifies:
- `ST_MakePoint` accepts two `double precision` arguments
- `ST_SetSRID` accepts the result and an `integer` SRID
- `ST_X` / `ST_Y` return `double precision`

This catches SQL errors before runtime. Requires `SQLX_OFFLINE=true` + `sqlx-data.json` in CI, or a live PostGIS database.

### Migration SQL

```sql
-- In 20260525000000_init.up.sql
CREATE EXTENSION IF NOT EXISTS postgis;

-- stations table
CREATE TABLE stations (
    id VARCHAR(64) PRIMARY KEY,
    owner_id VARCHAR(64) NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    name VARCHAR(255) NOT NULL,
    address TEXT NOT NULL,
    city VARCHAR(100) NOT NULL,
    coordinates GEOGRAPHY(Point, 4326) NOT NULL,
    is_operational BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ DEFAULT NULL,
    is_test BOOLEAN NOT NULL DEFAULT FALSE
);

-- Spatial index
CREATE INDEX idx_stations_spatial_coordinates ON stations USING GIST (coordinates);

-- Defense-in-depth coordinate range constraint
ALTER TABLE stations ADD CONSTRAINT chk_coordinates_valid
    CHECK (
        ST_X(coordinates::geometry) BETWEEN -180 AND 180
        AND ST_Y(coordinates::geometry) BETWEEN -90 AND 90
    );
```

### Floating-Point Precision

- `f64` (64-bit IEEE 754) provides ~15 significant decimal digits
- WGS 84 coordinates at 6 decimal places give ~0.11m precision — more than sufficient
- Round-trip fidelity (insert → select) is guaranteed for 6+ decimal places
- SC-007 requires "same longitude and latitude values submitted (within floating-point tolerance)" — `f64` satisfies this

### Phase 2 Compatibility

The GIST index on `coordinates` directly supports:
- `ST_DWithin(geography, geography, meters)` — radius search with index acceleration
- `ST_Distance(geography, geography)` — exact geodesic distance in meters
- `<->` KNN distance operator — nearest-neighbor ordering with index
- `ST_Expand` + bounding box — pre-filter optimization for large datasets

No schema changes needed between Phase 1 (CRUD) and Phase 2 (nearby search). The index and column type are already correct.
