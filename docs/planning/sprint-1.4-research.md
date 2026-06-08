# Research: Admin Service

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
| Error handling | Custom Result types with Serde-compatible errors | Clean API error responses (health: ok/error, CRUD: entity not found, validation errors) |
| Configuration | Environment variables only | Consistent with Sprint 1.2 and Sprint 1.3, no external config files, simple deployment |
| Testing framework | actix-web-test + sqlx-test | Standard testing libraries for Actix-web and PostgreSQL tests |
| Containerization | Multi-stage Dockerfile | Compilation speed, smaller images, production-ready output |
| Running migrations | sqlx::migrate::runner() | Rust native migration runner, simpler than shell script, type-safe migration files |
| Service startup | Migration application in main.rs | Auto-apply migrations on startup, ensuring database is ready |

## CRUD Endpoint Design

### RESTful Pattern

**GET /api/v1/partners** - Retrieve all partners (paginated)
**GET /api/v1/partners/:id** - Retrieve single partner by ID
**POST /api/v1/partners** - Create new partner
**PUT /api/v1/partners/:id** - Update existing partner
**DELETE /api/v1/partners/:id** - Delete partner

Same pattern for stations and chargers, with idempotent `GET` (avoid `GET /api/v1/partners/new`).

### Data Validation

- **Partner fields**: name, email, phone, address (required)
- **Station fields**: partner_id (FK), name, latitude, longitude, address (required)
- **Charger fields**: station_id (FK), connector_type, power_kw, status (required)
- **ConnectorType enum**: Type 2, CCS, CHAdeMO, GB/T, Tesla Supercharger
- **ChargerStatus enum**: available, unavailable, fault, maintenance
- **ID format validation**: Use existing NanoID generators (ev-core)

### Error Handling

**404 Not Found**: Entity not found (partner with given ID, station with given ID, charger with given ID)

**400 Bad Request**: Invalid data (validation errors, missing required fields, invalid FK references)

**409 Conflict**: Duplicate resource (partner with same email)

**500 Internal Server Error**: Database errors, FK constraint violations

**Validation patterns**:
- Use Rust's validator crate for data validation
- Use sqlx's type-safe query builder with bind parameters
- Return clear error messages for each validation failure

## Alternatives Considered

| Alternative | Rejected Because |
|-------------|-----------------|
| HTTP server framework: axum instead of actix-web | Already using actix-web per ADR-005 (Sprint 1.1, Sprint 1.3, Sprint 1.2), ecosystem consistency |
| sqlx async in main thread | Would block startup, poor performance under load, inconsistent with Actix-web async model |
| External migration runner (Flyway) | Requires separate binary, adds complexity, migrations are simple (15 endpoints, no schema changes in this sprint) |
| Connection pooling approach | ev-db already implements connection pooling, no decision needed |
| CORS middleware | Not needed for admin service (future backend API), can add later if exposed to web frontend |
| Authentication (OAuth2/JWT) | Not in scope for Sprint 1.4 (Sprint 2.x will add Keycloak) |
| Caching layer (Redis) | Overkill for initial version, database CRUD is fast (<200ms), caching can be added later |
| Pagination for all endpoints | Not in scope for Sprint 1.4 (data set small: 3 partners, 15 stations, 24 chargers), can add in future sprints |
| API versioning (v1 vs v2) | Single version now, version API to avoid drift, can add in future |
| GraphQL instead of REST | REST is simpler for CRUD operations, GraphQL overkill for 15 endpoints |

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
  "service": "admin-service",
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

### CRUD Endpoints Contract

**Partner CRUD**:

**GET /api/v1/partners** (paginated):
```json
{
  "partners": [
    {
      "id": "PRT-001",
      "name": "Tunis Power",
      "email": "contact@tunispower.tn",
      "phone": "+216 71 123 456",
      "address": "Tunis, Tunisia"
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total_pages": 1,
    "total_items": 3
  }
}
```

**GET /api/v1/partners/:id**:
```json
{
  "id": "PRT-001",
  "name": "Tunis Power",
  "email": "contact@tunispower.tn",
  "phone": "+216 71 123 456",
  "address": "Tunis, Tunisia"
}
```

**POST /api/v1/partners**:
```json
{
  "name": "Carsharing Tunis",
  "email": "support@carsharing.tn",
  "phone": "+216 71 789 012",
  "address": "Tunis, Tunisia"
}
```

Response (201 Created):
```json
{
  "id": "PRT-002",
  "name": "Carsharing Tunis",
  "email": "support@carsharing.tn",
  "phone": "+216 71 789 012",
  "address": "Tunis, Tunisia"
}
```

**PUT /api/v1/partners/:id**:
```json
{
  "name": "Tunis Power Updated",
  "email": "new@email.tn",
  "phone": "+216 71 555 555",
  "address": "Updated Address, Tunis"
}
```

Response (200 OK):
```json
{
  "id": "PRT-001",
  "name": "Tunis Power Updated",
  "email": "new@email.tn",
  "phone": "+216 71 555 555",
  "address": "Updated Address, Tunis"
}
```

**DELETE /api/v1/partners/:id**:
- Response (204 No Content)

Same pattern applies to stations and chargers with appropriate field mappings.

### Station Locations Handling

When creating/updating stations, the service must:
1. Validate partner_id exists (FK check)
2. Create/Update `inventory.station` record
3. Optionally trigger spatial location creation (could be done via trigger or inline)
4. Return appropriate responses

## Security Considerations

- **No authentication for now**: API endpoints open in Sprint 1.4 (Sprint 2.x will add Keycloak)
- **Input validation**: Validate all CRUD inputs before database operations
- **SQL injection**: Use parameterized queries via ev-db crate
- **Database credentials**: Loaded from environment variable (POSTGRES_URL)
- **Error messages**: No sensitive information in error responses
- **FK constraints**: Leverage PostgreSQL foreign key constraints for data integrity
- **ID uniqueness**: Use ev-core NanoID generators for unique IDs

## Performance Considerations

- **Connection pooling**: ev-db uses PgPool for connection reuse
- **Query optimization**: Only select necessary columns for read operations
- **Response size**: Small (3 partners, 15 stations, 24 chargers max)
- **Async handling**: Actix-web async model for concurrent requests
- **Database connection**: Pool size 5-10 connections for development, 20-50 for production
- **Pagination**: Optional for all CRUD endpoints (future enhancement)

## CRUD Patterns

**Read operations**:
- Use `ev-db::OffsetParams` for pagination (if implemented)
- Filter by FK (e.g., stations by partner_id)
- Sort by appropriate fields (stations by name or partner_id)

**Write operations**:
- Use transactions for multi-step operations (e.g., create station + charger together)
- Validate all inputs before insert/update
- Handle FK violations gracefully (return 400 with clear message)
- Handle duplicate constraints gracefully (return 409 with clear message)

**Delete operations**:
- Cascade deletes not allowed (constitution rule: single source of truth)
- Should prevent deletion if foreign keys exist (FK constraint)
- Return 404 if entity not found

## Future Enhancements

| Feature | Future Sprint | Impact on Data Model |
|---------|---------------|---------------------|
| Authentication (OAuth2/JWT) | Sprint 2.x | Add Authorization header, 401 Unauthorized |
| Pagination | Sprint 1.5+ | Add `page` and `page_size` parameters |
| Filters (partner_id, status) | Sprint 2.x | Add filter fields to requests |
| Station location auto-creation | Sprint 1.5+ | Automatic GIS sync when station created |
| Detailed error messages with codes | Sprint 2.x | Add `code` field (e.g., "VALIDATION_ERROR", "DUPLICATE_ENTITY") |
| Rate limiting | Sprint 2.x | No schema impact, header-based |
| Bulk operations (create multiple) | Sprint 2.x | Add POST /api/v1/partners/batch endpoint |
| Audit logging | Sprint 2.x | Add created_at, updated_at, updated_by fields |