# Research: Admin Service

**Date**: 2026-06-09 | **Branch**: `010-admin-service` | **Spec**: [spec.md](./spec.md)

## Architecture Decisions

### Web Framework

- **Decision**: Actix-web 4
- **Rationale**: Existing Driver Service uses Actix-web 4. Consistent framework within workspace. Proven with sqlx + tokio.
- **Alternatives considered**: Axum (routing model differs from existing patterns; not worth introducing second framework)

### Database Access

- **Decision**: sqlx 0.8 with non-compile-time `query_as::<_, T>()`
- **Rationale**: Matches Driver Service. Avoids needing live DB during build (compile-time `query_as!` requires `DATABASE_URL` at build time).
- **Alternatives considered**: sqlx compile-time queries (require DB at build time — rejected for CI simplicity), diesel (heavier ORM, different pattern)

### Async Runtime

- **Decision**: tokio (multi-threaded, via `#[actix_web::main]`)
- **Rationale**: Actix-web 4 depends on tokio. Matches Driver Service. `rt-multi-thread` feature already in workspace dependencies.

### Configuration

- **Decision**: Environment variables with `std::env::var`
- **Rationale**: Matches Driver Service. Zero external dependencies for config parsing.
- **Variables**: `DATABASE_URL` (required), `HOST` (default `0.0.0.0`), `PORT` (default `8081`), `RUST_LOG` (default `info`)
- **Alternatives considered**: dotenv (adds dependency for marginal benefit), config-rs (overkill for 4 vars)

### Error Handling

- **Decision**: Custom `AppError` enum implementing `actix_web::ResponseError`
- **Rationale**: Identical pattern to Driver Service. Returns JSON `{"error": {"code": "...", "message": "..."}}`.
- **New error codes needed**: `validation_error` (400 for invalid input — distinct from `bad_request` for malformed requests), `conflict` (409 for FK/unique violations)

### Dev Scope Testing (X-Partner-Id)

- **Decision**: Optional `X-Partner-Id` header extracted via `actix_web::HttpRequest::headers()`
- **Rationale**: Simple header extraction. No auth middleware needed in MVP-2. When present, scopes operations to that partner (e.g., list only that partner's stations).
- **Used for**: `created_by`/`updated_by` audit fields, filtering list endpoints
- **MVP-3**: Will be replaced by Keycloak JWT token extractor

### Port Allocation

- **Decision**: Port 8081
- **Rationale**: Driver Service already uses 8080. Clear separation. Stated in spec assumptions.

### Dockerfile Pattern

- **Decision**: Multi-stage build matching Driver Service
- **Rationale**: Consistent deployment. Same base images, same workspace copy strategy.
- **Exposes**: Port 8081

## Database Integration

### Migration Strategy

- **Decision**: Use existing migrations (0001–0004). Admin Service does not modify schema.
- **Migration on startup**: `sqlx::migrate!("../../database/migrations")` — path relative to binary. Same as Driver Service approach but needs adjustment for workspace layout.

### Seeds

- **Decision**: Reuse existing seeds (001–004). No new seeds needed.
- **Note**: `PRT002` (EcoCharge, not live) and `PRT003` (Ahmed Ben Salem, not verified) provide ready test cases for scope filtering

### Key SQL Patterns

| Operation | Pattern | Notes |
|-----------|---------|-------|
| Create partner | `INSERT INTO "ev-platform".partner (id, name, type, ...) VALUES ($1, $2, $3, ...) RETURNING *` | ID generated via `ev_core::generate_id("PRT", ...)` |
| Update partner | `UPDATE "ev-platform".partner SET name = COALESCE($2, name), ... WHERE id = $1 RETURNING *` | Partial update — only set fields that are provided |
| List stations | `SELECT * FROM "ev-platform".station WHERE partner_id = $1 [optional scope]` | X-Partner-Id filters list |
| Create availability | `INSERT INTO "ev-platform".station_availability (station_id, status, ...) VALUES ($1, $2, ...) RETURNING *` | Append-only |
| Soft delete | `UPDATE "ev-platform".partner SET is_active = false, updated_by = $2 ...` for partners | Hard delete for stations/chargers |

## Integration Testing

### Test Strategy

- **Decision**: Integration tests against live PostgreSQL (same as Driver Service)
- **Approach**: `sqlx::test` with test database. Each test creates its own data via API calls, verifies responses, cleans up.
- **Level**: Integration tests covering:
  - Partner CRUD lifecycle
  - Station CRUD lifecycle (with partner FK)
  - Charger CRUD lifecycle (with station FK)
  - Availability append-only behavior
  - X-Partner-Id scope filtering
  - Error cases (missing FK, invalid enum values, not found)

### Test Database Setup

- Same as Driver Service: dedicated `borne_map_test` database
- `sqlx::migrate!` run before tests
- Seeds not loaded for tests (tests create their own data)
