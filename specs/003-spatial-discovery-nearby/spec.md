# Feature Specification: Spatial Discovery — Nearby API & SLO Validation

**Feature Branch**: `007-spatial-discovery-nearby`

**Created**: 2026-05-26

**Status**: Draft

**Input**: User description: "Phase 2 from docs/plan_mvp0.md — Spatial Discovery: Nearby API & SLO Validation"

## User Scenarios & Testing

### User Story 1 - Driver Discovers Nearby Stations (Priority: P1)

An EV driver opens the mobile app and sees charging stations near their current
location, ordered by distance, with available charger counts — without test
stations ever appearing.

**Why this priority**: This is the core value proposition of the platform.
Without nearby discovery, the mobile driver app has no purpose. Every other
feature supports this primary use case.

**Independent Test**: Can be tested by issuing a GET request to
`/api/v1/stations/nearby?longitude=10.1815&latitude=36.8065` and verifying
returned stations are ordered by distance, within the default 20km radius,
capped at 50 results, and no test stations appear.

**Acceptance Scenarios**:

1. **Given** the seed data with 100 test stations near Tunis (10.18, 36.81),
   **When** a driver queries nearby with coordinates near the seed stations,
   **Then** stations are returned ordered by ascending distance, each with
   distance in meters and available charger count.
2. **Given** test stations exist in the database, **When** a driver queries
   nearby without `include_test=true`, **Then** no test station appears in the
   results.
3. **Given** 100+ stations match the radius, **When** a driver queries nearby,
   **Then** at most 50 stations are returned.

---

### User Story 2 - Platform Verifies Spatial SLO (Priority: P2)

A platform engineer runs a benchmark to confirm the nearby endpoint meets the
≤200ms SLO at p95 under concurrent load.

**Why this priority**: The constitution requires spatial queries ≤200ms. This
must be verified before the mobile app (Phase 6) depends on it. Failure here
blocks Phase 6 and requires query optimization before proceeding.

**Independent Test**: Can be tested by running a benchmark script (wrk, oha, or
custom Rust bench) against the nearby endpoint with concurrency 10 and 1000
requests, then checking that p95 latency ≤ 200ms.

**Acceptance Scenarios**:

1. **Given** the server is running with seed data loaded, **When** the
   benchmark runs 1000 requests at concurrency 10, **Then** p95 latency is
   ≤ 200ms.
2. **Given** the benchmark fails the SLO, **When** the query is analyzed with
   `EXPLAIN ANALYZE`, **Then** the GIST index is being used and any missing
   index is added before retry.

---

### User Story 3 - Mobile App Shows Station Detail (Priority: P3)

A driver taps a station marker on the map and sees full station details
including its chargers, connector types, and statuses.

**Why this priority**: Station detail is the follow-up interaction after
discovery. It's lower priority than the nearby endpoint itself because the
mobile app can show basic info from the nearby results and add detail later.

**Independent Test**: Can be tested by fetching a known station's detail at
`/api/v1/stations/{id}` and its chargers at
`/api/v1/stations/{id}/chargers`, verifying all fields are returned.

**Acceptance Scenarios**:

1. **Given** a station exists with chargers, **When** a driver fetches
   `/api/v1/stations/{id}`, **Then** the response includes all station fields
   (name, address, city, coordinates, operational status).
2. **Given** a station has chargers, **When** a driver fetches
   `/api/v1/stations/{id}/chargers`, **Then** each charger includes connector
   type details, power, current type, and status.

---

### Edge Cases

- What happens when coordinates are outside valid ranges? The endpoint rejects
  with 422 validation error.
- What happens when the search radius is negative or zero? The endpoint rejects
  with 422.
- What happens when no stations are within the search radius? An empty array
  is returned.
- What happens when all stations within radius are test stations and
  `include_test` is false? An empty array is returned.
- What happens when the database has no stations at all? An empty array.
- What happens when the station detail endpoint is called with a soft-deleted
  station ID? 404 Not Found.
- What happens when the station has no chargers? The chargers list returns an
  empty array.

## Requirements

### Functional Requirements

- **FR-001**: System MUST provide a `GET /api/v1/stations/nearby` endpoint
  accepting `longitude`, `latitude`, optional `radius_meters` (default 20000.0),
  and optional `include_test` (default false) query parameters.
- **FR-002**: The nearby endpoint MUST return stations ordered by ascending
  geodesic distance from the query point.
- **FR-003**: The nearby endpoint MUST cap results at 50 stations maximum.
- **FR-004**: Each result MUST include `station_id`, `station_name`, `address`,
  `city`, `longitude`, `latitude`, `distance_meters`, `available_chargers_count`,
  and `is_test`.
- **FR-005**: The nearby endpoint MUST use `ST_DWithin` with the GIST spatial
  index for bounding, and MUST apply `is_test` isolation at the SQL level.
- **FR-006**: The system MUST exclude test records by default when `include_test`
  is not explicitly set to true.
- **FR-007**: The system MUST validate longitude (-180 to 180) and latitude
  (-90 to 90) on input, returning 422 for invalid values.
- **FR-008**: The system MUST provide a `GET /api/v1/stations/{id}` endpoint
  returning full station detail.
- **FR-009**: The system MUST provide a `GET /api/v1/stations/{id}/chargers`
  endpoint returning chargers for a specific station (already implemented in
  Phase 1, verified here).
- **FR-010**: The nearby endpoint MUST complete within ≤200ms at p95 under
  concurrent load (1000 requests, concurrency 10).

### Key Entities

- **NearbyStationResult**: A read-only projection combining station fields with
  computed distance and available charger count. Not a persisted entity — derived
  from a spatial query joining `stations` + `chargers`.
- **Station** (existing): Extended with spatial lookup support via GIST index.
- **Charger** (existing): Used for `available_chargers_count` aggregation.

## Success Criteria

### Measurable Outcomes

- **SC-001**: Nearby query returns results in ≤200ms p95 under 1000 requests at
  concurrency 10 against the seeded 100-station / 300-charger dataset.
- **SC-002**: Test stations never appear in nearby results when `include_test`
  is false — zero leaks across 1000 queries.
- **SC-003**: Nearby results are always ordered by ascending distance — verified
  by checking `distance_meters` monotonicity across 1000 responses.
- **SC-004**: Station detail endpoint returns all required fields for any
  non-deleted station — verified across all 100 seed stations.
- **SC-005**: The SLO benchmark is reproducible — running it twice produces
  consistent pass/fail results.

## Clarifications

### Session 2026-05-26

- Q: Should the nearby endpoint require authentication? → A: No — the mobile
  driver app may not have a logged-in user during discovery. The endpoint is
  public (unauthenticated) but respects `is_test` isolation.
- Q: Should the nearby endpoint support partner-scoped filtering? → A: No —
  nearby discovery is for drivers seeing all public stations. Partner scoping
  applies to management endpoints only.
- Q: What benchmark tool should be used? → A: `oha` (Rust HTTP benchmark) for
  simplicity, or `wrk`. The spec is tool-agnostic — the SLO is the requirement.
- Q: How should 0 available chargers be rendered? → A: `available_chargers_count`
  should be 0 (not null) — the `COUNT(...) FILTER (WHERE ...)` returns 0 for
  stations with no available chargers.
- Q: Are distance values returned in meters? → A: Yes. `ST_Distance` on
  `GEOGRAPHY` returns meters natively.

## Assumptions

- The GIST index on `stations.coordinates` already exists from Phase 1
  migration.
- The `stations` table is populated with seed data (100 stations near Tunis,
  ~36.8°N, 10.2°E).
- The mobile app sends the user's device GPS coordinates as query parameters.
- The benchmark runs on the same machine or a machine with comparable network
  latency to the database (local Docker Compose stack).
- The `available_chargers_count` includes only chargers with status
  `available`, not idle/occupied/faulted/offline units.
- Station detail and charger list endpoints already exist from Phase 1 — this
  phase only verifies they meet mobile app requirements.
