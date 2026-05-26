# Research: Spatial Discovery — Nearby API & SLO Validation

**Phase**: 0 (Outline & Research)

## Decisions

### Decision 1: Spatial Query Pattern — `ST_DWithin` + `ST_Distance` with GIST Index

- **Decision**: Use `ST_DWithin(s.coordinates, ST_MakePoint($lng, $lat)::geography, $radius)` for the bounding filter and `ST_Distance(s.coordinates, ST_MakePoint($lng, $lat)::geography)` for distance calculation. Both operate on `GEOGRAPHY` types for meter-based results.
- **Rationale**: `ST_DWithin` is the industry-standard PostGIS spatial bounding function. It leverages the GIST index (created in Phase 1 migration) for index-assisted filtering — the spatial equivalent of a B-tree range scan. `ST_Distance` then computes exact geodesic distances only for rows that pass the `ST_DWithin` filter. The `GEOGRAPHY` type ensures results are in meters (not degrees), which is required for the 20km default radius and distance display to drivers.
- **Alternatives considered**: `ST_Distance` without `ST_DWithin` (full table scan — ST_Distance is not indexable), `<->` KNN operator (optimized for ORDER BY + LIMIT without explicit radius, but does not enforce a hard boundary), `ST_Intersects` with a buffer polygon (more complex, no benefit over `ST_DWithin`)

### Decision 2: Result Shape — Read-Only Projection, No New Table

- **Decision**: The nearby endpoint returns a `NearbyStationResult` struct that is a read-only query projection, not a persisted entity. No new database table or migration is needed.
- **Rationale**: Nearby results are computed on the fly by joining `stations` and `chargers` with aggregation. There is no business need to cache or persist these results — each request is unique to the user's location. The `LEFT JOIN` + `COUNT(DISTINCT ...) FILTER (WHERE ...)` pattern produces correct results in a single query round-trip.
- **Alternatives considered**: Materialized view (stale data, requires refresh, overkill for 50-row result), Redis cache (premature optimization — database can handle 50-row spatial queries under 200ms), separate `nearby_results` table (no business case to persist ephemeral computed data)

### Decision 3: Benchmark Tool — `oha` (Rust HTTP Load Tester)

- **Decision**: Use `oha` for SLO benchmarking. Install via `cargo install oha`.
- **Rationale**: `oha` is a Rust-native HTTP benchmarking tool with built-in latency percentiles (p50, p75, p90, p95, p99), concurrent request support, and JSON output. No external runtime dependency (unlike `wrk` which requires `luajit` or compilation). Single binary, simple CLI: `oha -n 1000 -c 10 http://...`.
- **Alternatives considered**: `wrk` (industry standard but requires C compilation, luajit dependency, no JSON output), `hey` (Go-based, OK but not Rust-native), custom Rust benchmark using `tokio` + `reqwest` (more flexible but requires writing/maintaining benchmark code)

### Decision 4: No Authentication on Nearby Endpoint

- **Decision**: The nearby endpoint is public (unauthenticated). No JWT validation or auth middleware is applied.
- **Rationale**: The mobile driver app may show the map before the user logs in. Requiring authentication before nearby discovery degrades UX and adds friction to the primary use case. `is_test` isolation is enforced at the SQL level regardless of authentication status.
- **Alternatives considered**: Anonymous JWT tokens (extra complexity, no security benefit since the endpoint is read-only), API key header (added deployment burden for mobile app distribution)

### Decision 5: No Partner Scoping on Nearby Endpoint

- **Decision**: The nearby endpoint returns all non-test stations regardless of owner. Partner scoping is not applied.
- **Rationale**: Nearby discovery is a driver-facing feature — drivers should see all available public charging stations, not just those belonging to a specific partner. Partner-scoped filtering is reserved for management endpoints (station CRUD in Phase 1). The Constitution's multi-tenancy principle applies to management, not discovery.
- **Alternatives considered**: Partner-specific nearby endpoint (unnecessary — drivers don't care about partner boundaries), admin-only nearby with partner filter (not useful for the mobile use case)

### Decision 6: `LIMIT 50` as Hard Cap

- **Decision**: The nearby query uses `LIMIT 50` with no cursor pagination. Hard cap.
- **Rationale**: Mobile map screens have finite viewport area — showing more than 50 stations causes visual clutter and degrades render performance on mobile devices. The 20km default radius + LIMIT 50 keeps the result set small and fast. Unlike management list endpoints, there is no "next page" for nearby discovery — the user moves and re-queries.
- **Alternatives considered**: Cursor-based pagination (adds complexity, not needed for mobile viewport — users pan/zoom to trigger new queries), `LIMIT 100` (too many for mobile map markers), no limit (unbounded query could return hundreds of results, increasing latency and mobile rendering cost)

### Decision 7: `available_chargers_count` via `COUNT(*) FILTER (WHERE ...)`

- **Decision**: Use PostgreSQL's `COUNT(*) FILTER (WHERE c.status = 'available')` to compute available charger count in a single query pass, rather than a subquery or separate query.
- **Rationale**: 1 query vs N+1. PostgreSQL's `FILTER` clause is a SQL standard extension (implemented since PG 9.4) that adds a condition to an aggregate without a `CASE` expression. It's more readable and slightly faster than `COUNT(CASE WHEN ...)` and avoids a correlated subquery or lateral join. The `LEFT JOIN` ensures stations with zero chargers still appear (with `available_chargers_count = 0`).
- **Alternatives considered**: Correlated subquery `SELECT (SELECT COUNT(*) FROM chargers WHERE station_id = s.id AND status = 'available')` (N+1 risk if not optimized, harder to read), separate query after main result (two round trips), `CASE WHEN ... THEN 1 END` with `COUNT` (equivalent but more verbose than `FILTER`)
