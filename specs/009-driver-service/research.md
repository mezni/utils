# Research: Driver Service

**Phase**: Phase 0 — Technology & pattern research for Sprint 2.3

**Date**: 2026-06-09

## Technology Decisions

### Web Framework

- **Decision**: Actix-web 4
- **Rationale**: Already specified in project constitution. Actix-web is the fastest Rust web framework, well-suited for I/O-bound API services. Mature ecosystem with middleware support and strong typing.
- **Alternatives considered**: Axum (rejected — not in constitution, team experience with Actix-web), Warp (rejected — overly complex type system for simple REST), Rocket (rejected — no async support in older versions).

### Database Access

- **Decision**: sqlx 0.8 with compile-time checked queries and runtime migration support
- **Rationale**: Already established in ev-db crate from Sprint 2.1. sqlx embeds migrations in the binary, ensuring schema version sync. Compile-time checking catches SQL errors before deployment.
- **Alternatives considered**: Diesel (rejected — ORM overhead, not async by default), sea-orm (rejected — additional dependency, not in project stack).

### Async Runtime

- **Decision**: tokio (multi-threaded)
- **Rationale**: Actix-web builds on tokio. Already a workspace dependency. Multi-threaded runtime handles concurrent requests efficiently.
- **Alternatives considered**: async-std (rejected — not Actix-web compatible), smol (rejected — smaller ecosystem).

### Configuration

- **Decision**: Environment variables parsed manually with `std::env::var`
- **Rationale**: Simple, zero-dependency. The service needs only DATABASE_URL, PORT, and HOST. No complex config files required at this scale. A config struct with defaults reduces boilerplate.
- **Alternatives considered**: dotenv (rejected — adds dependency for trivial parsing), config-rs (rejected — overkill for 3 env vars), clap (rejected — for CLI tools, not services).

### Error Handling

- **Decision**: Custom `AppError` enum implementing `actix_web::ResponseError`
- **Rationale**: Actix-web's `ResponseError` trait returns appropriate HTTP status codes and JSON error bodies. An enum with variants for NotFound, BadRequest, InternalError, and DbError provides exhaustive error handling without external crates.
- **Alternatives considered**: anyhow (rejected — for application-level error handling, not API responses), thiserror (rejected — kept for internal error types, but API errors go through AppError).

### Spatial Queries

- **Decision**: Raw SQL with PostGIS `ST_DWithin` and bounding box operators, executed via sqlx
- **Rationale**: The spatial index (`idx_station_location`) is already created by migration 0003. Raw SQL gives full control over query plans. sqlx compile-time checking validates SQL against the live database.
- **Key queries identified**:
  - **Nearby**: `ST_DWithin(location::geography, $1::geography, $2)` with `ORDER BY ST_Distance(location::geography, $1::geography)`
  - **Bbox**: `ST_MakeEnvelope($1, $2, $3, $4, 4326) && location` (bbox overlap operator)
  - **Search**: `(name ILIKE $1 OR address ILIKE $1)` with optional `connector_type IN` subquery join
  - **Detail**: `SELECT ... FROM station WHERE id = $1` + `SELECT ... FROM charger WHERE station_id = $1`

### Integration Testing

- **Decision**: `sqlx::test` with a dedicated test database
- **Rationale**: `sqlx::test` resets the database between tests, ensuring isolation. The test database can use the same schema migrations as production. Tests run against real PostgreSQL, catching SQL errors that mocks would miss.
- **Test database setup**: `borne_map_test` database, migrations applied, seeds loaded per test module.

### Partner Visibility Filter

- **Decision**: Applied via JOIN in every station query: `JOIN "ev-platform".partner p ON s.partner_id = p.id WHERE p.is_verified = true AND p.is_live = true AND p.is_active = true`
- **Rationale**: Single source of truth. No caching layer needed at this scale. The JOIN over partner (3 rows in dev, unlikely to exceed 10k in production) adds negligible cost compared to the spatial index scan.
- **Edge case**: Partner flags change → subsequent queries reflect immediately. No stale cache issue.

### Dockerfile

- **Decision**: Multi-stage build with `rust:1.85-slim-bookworm` builder and `debian:bookworm-slim` runtime
- **Rationale**: Standard Rust multi-stage pattern. Builder compiles the entire workspace; runtime copies only the binary. Final image size ~50MB.
- **Note**: Docker Compose comes in Sprint 2.5. This sprint produces the Dockerfile only.

## Query Details

### Nearby Stations (ST_DWithin)

```sql
SELECT s.id, s.name, s.address, s.latitude, s.longitude,
       sa.status AS availability_status,
       ST_Distance(s.location::geography, $1::geography) AS distance_meters
FROM "ev-platform".station s
LEFT JOIN LATERAL (
    SELECT status FROM "ev-platform".station_availability
    WHERE station_id = s.id
    ORDER BY updated_at DESC
    LIMIT 1
) sa ON true
JOIN "ev-platform".partner p ON s.partner_id = p.id
WHERE p.is_verified = true AND p.is_live = true AND p.is_active = true
  AND ST_DWithin(s.location, $1::geography, $2)
ORDER BY distance_meters
LIMIT $3
OFFSET $4;
```

### Bbox Markers

```sql
SELECT s.id, s.name, s.latitude, s.longitude,
       sa.status AS availability_status
FROM "ev-platform".station s
LEFT JOIN LATERAL (
    SELECT status FROM "ev-platform".station_availability
    WHERE station_id = s.id
    ORDER BY updated_at DESC
    LIMIT 1
) sa ON true
JOIN "ev-platform".partner p ON s.partner_id = p.id
WHERE p.is_verified = true AND p.is_live = true AND p.is_active = true
  AND ST_MakeEnvelope($1, $2, $3, $4, 4326) && s.location;
```

### Search Stations

```sql
SELECT DISTINCT s.id, s.name, s.address, s.latitude, s.longitude
FROM "ev-platform".station s
JOIN "ev-platform".partner p ON s.partner_id = p.id
LEFT JOIN "ev-platform".charger c ON c.station_id = s.id
WHERE p.is_verified = true AND p.is_live = true AND p.is_active = true
  AND (s.name ILIKE $1 OR s.address ILIKE $1)
  AND ($2::text IS NULL OR c.connector_type = $2)
ORDER BY s.name
LIMIT $3
OFFSET $4;
```

### Station Detail

```sql
-- Station query
SELECT s.id, s.name, s.address, s.latitude, s.longitude
FROM "ev-platform".station s
JOIN "ev-platform".partner p ON s.partner_id = p.id
WHERE p.is_verified = true AND p.is_live = true AND p.is_active = true
  AND s.id = $1;

-- Chargers query
SELECT c.id, c.connector_type, c.power_kw, c.status
FROM "ev-platform".charger c
WHERE c.station_id = $1
ORDER BY c.id;
```

### Station Reviews (Stub)

```sql
-- No query needed. Returns placeholder JSON response.
```

## Best Practices

- **Route registration**: Actix-web `ServiceConfig` with `configure()` pattern for modular route setup
- **AppState**: Shared `PgPool` wrapped in `web::Data<AppState>` for thread-safe access
- **Error responses**: Consistent JSON error shape `{"error": {"code": "...", "message": "..."}}`
- **Pagination**: `LIMIT`/`OFFSET` for nearby and search endpoints (via ev-db Paginated<T> if needed)
- **Logging**: Structured logging via `log`/`env_logger` crate — request ID, duration, status code
- **Graceful shutdown**: Actix-web `Signal` handler for SIGTERM/SIGINT
