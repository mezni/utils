# Research: Driver Service

**Phase**: Phase 0 — Tech decisions and unknowns resolution for Sprint 1.2

---

## Framework Choice: Actix-web vs Axum

**Decision**: Actix-web 4

**Rationale**: The project constitution at `docs/constitution.md` §5.4 specifies Actix-web as the backend framework. This is a non-negotiable architecture decision. Actix-web offers:
- Mature async HTTP server with built-in actor model
- Strong compile-time routing and middleware pipeline
- Excellent performance benchmarks
- Large ecosystem with well-documented patterns for REST APIs

**Alternatives considered**: Axum (growing ecosystem, tower-based middleware) — not evaluated as constitution requires Actix-web.

---

## API Response Envelope Design

**Decision**: Consistent JSON envelope with `data`, `error`, and `meta` top-level keys

**Rationale**: 
- `data`: Contains the response payload (station list, station detail, health status)
- `error`: Present only on error responses, contains `code` (string key) and `message` (human-readable)
- `meta`: Optional metadata (result count, request_id for tracing)
- Enables frontend to use a single parsing strategy across all endpoints
- The `code` field uses human-readable kebab-case strings (e.g. "not_found", "validation_error", "internal_error")

**Alternatives considered**:
- HTTP status only (no body envelope) — rejected because it prevents consistent field-level error reporting
- JSON:API spec — too heavy for MVP-1; consider for MVP-5
- GraphQL-style — far too heavy for this scope

---

## Route Conflict Resolution: `/stations/nearby` vs `/stations/{id}`

**Decision**: Actix-web resource scoping with explicit route registration

**Rationale**: 
- Register `/stations/nearby` before the parameterized `/stations/{id}` in the router
- Actix-web matches routes in registration order, so `/stations/nearby` catches before `{id}` can match "nearby" as a station ID
- This eliminates the need for a custom path prefix or query-parameter hack (e.g. `?id=...`)

**Alternatives considered**:
- `/stations/nearby?lat=...&lng=...` — chosen approach (query parameters for nearby coordinates)
- `/stations/near/{lat}/{lng}/{radius}` — path-based, rejected for consistency with REST conventions
- Single `/stations` endpoint with query parameter filtering — rejected because nearby is a fundamentally different operation from list

---

## Validation Strategy

**Decision**: Server-side parameter validation at handler layer using Actix-web's `Query<T>` with serde deserialization + custom validate methods

**Rationale**:
- Actix-web's `Query<T>` automatically deserializes query parameters into typed structs
- Add a `validate()` method to each DTO struct that returns field-level errors
- Since MVP-1 has no auth, validation is the primary defense against malformed input
- Covers: lat range [-90, 90], lng range [-180, 180], radius_m > 0

**Alternatives considered**:
- `validator` crate — could add later, but custom validation is simpler and more explicit for MVP-1
- Middleware-level validation — overkill for 3 simple endpoints

---

## Dependency on `borne-data`

**Decision**: Direct workspace dependency via `Cargo.toml`

**Rationale**:
- `borne-data` (Sprint 1.1) provides `list_all`, `find_nearby`, `find_by_id` query functions with proper `DataLayerError` handling
- Driver service wraps these in Actix-web handlers, adding HTTP response mapping (DataLayerError → HTTP status + JSON error body)
- No need for a separate repository layer — the `borne-data` functions ARE the repository

**Implications**:
- Driver service must depend on `source/services/libs/borne-data` as a path dependency
- No additional DB connection management needed — `borne-data`'s `create_pool()` handles that
- Database schema changes require updating `borne-data` models AND driver service DTOs

---

## Connection Pool Sharing

**Decision**: Create pool once at startup, share via Actix-web application state

**Rationale**:
- `borne-data::create_pool()` returns a `PgPool` configured with connection limits
- Store as `web::Data<PgPool>` in Actix-web app state
- All handlers extract the pool from state — no per-request pool creation

---

## Health Check Design

**Decision**: `GET /api/v1/health` returns HTTP 200 with `{"status": "ok", "database": "connected"}` when DB pool is responsive; HTTP 503 with `{"status": "error", "database": "disconnected"}` when DB is unreachable

**Rationale**:
- Simple connectivity check: execute `SELECT 1` against the pool
- If the pool can acquire a connection, DB is healthy
- If not, report 503 so orchestration (Docker healthcheck, K8s) can restart the service
- FR-011 ensures all requests are logged including health checks

---

## Logging & Tracing

**Decision**: `tracing` crate (per constitution) with JSON-formatted logs emitted to stdout

**Rationale**:
- Actix-web `Logger` middleware captures method, path, status, duration
- Custom tracing middleware adds request_id for correlation
- FR-011: log all incoming requests
- Docker-friendly: logs to stdout in JSON format for consumption by log aggregators
