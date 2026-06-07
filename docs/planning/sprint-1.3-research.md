# Sprint 1.3 - Driver Service Research

## Overview

All technology decisions for this sprint are pre-determined by the existing project architecture (ADR-001 through ADR-015) and constitution. The service must use Rust + Actix-web and connect to the existing PostgreSQL + PostGIS database.

## Technology Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Runtime | Rust 1.70+ | Existing stack per ADR-005, memory safety, async support for high concurrency |
| Web framework | Actix-web 4.0 | Async web framework per ADR-005, excellent performance, comprehensive middleware ecosystem |
| Database connection | ev-db shared crate | Reusing Sprint 1.1 (PgPool factory, pagination structs) to maintain consistency |
| Serialization | serde + serde_json | Standard Rust libraries for JSON API responses |
| Database driver | PostgreSQL native driver (ev-db) | Reusing existing `ev-db` crate, minimal overhead |
| Error handling | Custom Result types with Serde-compatible errors | Clean API error responses (health: ok/error, nearby: array or error object) |
| Configuration | Environment variables only | Consistent with Sprint 1.2, no external config files, simple deployment |
| Testing framework | actix-web-test + sqlx-test | Standard testing libraries for Actix-web and PostgreSQL tests |
| Containerization | Multi-stage Dockerfile | Compilation speed, smaller images, production-ready output |
| Running migrations | sqlx::migrate::runner() | Rust native migration runner, simpler than shell script, type-safe migration files |
| Service startup | Migration application in main.rs | Auto-apply migrations on startup, ensuring database is ready |

## Alternatives Considered

| Alternative | Rejected Because |
|-------------|-----------------|
| HTTP server framework: axum instead of actix-web | Already using actix-web per ADR-005 (Sprint 1.1, Sprint 1.2), ecosystem consistency |
| sqlx async in main thread | Would block startup, poor performance under load, inconsistent with Actix-web async model |
| External migration runner (Flyway) | Requires separate binary, adds complexity, migrations are simple (2 endpoints, no schema changes in this sprint) |
| Connection pooling approach | ev-db already implements connection pooling, no decision needed |
| CORS middleware | Not needed for driver service (future backend API), can add later if exposed to web frontend |
| Authentication (OAuth2/JWT) | Not in scope for Sprint 1.3 (Sprint 2.x will add Keycloak) |
| Caching layer (Redis) | Overkill for initial version, database query is fast (<200ms), caching can be added later |
| Pagination | Not in scope for Sprint 1.3, data set small (15 stations), can add in future sprints |
| API versioning (v1 vs v2) | Single version now, version API to avoid drift, can add in future |

## API Contract Design

### Health Endpoint Contract

**Request:**
```
GET /api/v1/health
Headers: (none)
```

**Response (200 OK):**
```json
{
  "status": "ok",
  "service": "driver-service",
  "db": "ok"
}
```

**Response (500 Internal Server Error):**
```json
{
  "error": "Database connection failed"
}
```

**Response (503 Service Unavailable):**
```json
{
  "error": "Service not running"
}
```

### Nearby Endpoint Contract

**Request:**
```
GET /api/v1/stations/nearby?lat={lat}&lng={lng}&radius_km={radius}
Headers: (none)
```

**Parameters:**
- `lat` (required, numeric, -90 to 90)
- `lng` (required, numeric, -180 to 180)
- `radius_km` (required, numeric, 0.1 to 100)

**Response (200 OK):**
```json
{
  "stations": [
    {
      "id": "STN-1a2b",
      "name": "Tunis-Belvedere Station",
      "latitude": 36.864702,
      "longitude": 10.158423,
      "distance_km": 1.2
    }
  ]
}
```

**Response (400 Bad Request):**
```json
{
  "error": "Invalid parameters: latitude must be between -90 and 90"
}
```

**Response (500 Internal Server Error):**
```json
{
  "error": "Database query failed"
}
```

## Security Considerations

- **No authentication for now**: API endpoints open in Sprint 1.3 (Sprint 2.x will add Keycloak)
- **Input validation**: Validate lat/lng/radius range before query
- **SQL injection**: Use parameterized queries via ev-db crate
- **Database credentials**: Loaded from environment variable (POSTGRES_URL)
- **Error messages**: No sensitive information in error responses

## Performance Considerations

- **Connection pooling**: ev-db uses PgPool for connection reuse
- **Spatial query optimization**: Using `ST_DWithin(gis.station_locations.geom, point, radius*1000)` with GiST index (from Sprint 1.2)
- **Query optimization**: Only select necessary columns (id, name, lat, lng, distance calculation)
- **Response size**: Small (15 stations max, ~1KB per station)
- **Async handling**: Actix-web async model for concurrent requests
- **Database connection**: Pool size 5-10 connections for development, 20-50 for production

## Future Enhancements

| Feature | Future Sprint | Impact on Data Model |
|---------|---------------|---------------------|
| Pagination | Sprint 2.x | Add `page` and `page_size` parameters |
| Filters (partner_id, status) | Sprint 2.x | Add filter fields to request |
| Station details (charger count) | Sprint 2.x | Add `charger_count: i32` to `StationResponse` |
| Authentication | Sprint 2.x | Add `Authorization: Bearer <token>` header, return 401 Unauthorized |
| Detailed errors | Sprint 2.x | Add `code` field (e.g., "INVALID_PARAMETER", "DATABASE_ERROR") |
| Rate limiting | Sprint 2.x | No schema impact, header-based |
| Filtering by date range | Sprint 2.x | Add `start_date`, `end_date` parameters |