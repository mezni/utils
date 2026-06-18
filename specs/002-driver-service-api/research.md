# Research: Driver Service & Spatial API

## Technical Decisions

### 1. Web Framework: Actix-web

**Decision**: Actix-web 4.x

**Rationale**: Specified in the constitution (Tech Stack constraint). Actix-web provides the fastest HTTP throughput in the Rust ecosystem, built-in actors, and a mature middleware ecosystem (CORS, logging, compression).

**Alternatives considered**: Axum (Tokio-native, growing ecosystem but not constitution-mandated), Rocket (convenience-focused but less performant).

---

### 2. Database Driver: sqlx with compile-time checks

**Decision**: `sqlx 0.8+` with `postgres`, `runtime-tokio`, `tls-native-tls` features

**Rationale**: Constitutional mandate for compile-time verified queries (`query_as!`, `query!` macros). The `PGPOOL` option for offline mode enables compile-time checking without a live database during builds.

**Alternatives considered**: `tokio-postgres` (runtime-only checks, no compile-time safety), `diesel` (ORM overhead, no async-native support).

---

### 3. Logging: tracing + tracing-subscriber

**Decision**: `tracing` crate with `tracing-subscriber` configured as a JSON logger to stdout

**Rationale**: Clarity Q1 resolved to structured JSON logging with method, path, status, duration, trace ID. `tracing` is the de facto standard for async Rust observability, integrates natively with tokio, and `tracing-subscriber` provides a `json` layer out of the box.

**Key configuration**: `tracing-subscriber::fmt().json().with_target(false).init()`

---

### 4. Connection Pool: sqlx::PgPool

**Decision**: Use `sqlx::PgPool::builder()` with `max_connections` configurable via env var

**Rationale**: sqlx's built-in pool is production-grade, supports health checks via `acquire_timeout`, and integrates seamlessly with compile-time query macros. Pool size defaults to `min: 1, max: 10`.

**Health check pattern**:
```rust
// On /health: test pool acquire with a short timeout
pool.acquire_timeout(Duration::from_millis(500)).await.is_ok()
```

---

### 5. Configuration: environment variables via `config` + `dotenvy`

**Decision**: Use the `config` crate with a layered approach — defaults, then `.env` file, then environment variables

**Rationale**: 12-factor app methodology. `.env` for local dev, env vars in Docker Compose for deployment.

**Required variables**:
| Variable | Default | Description |
|----------|---------|-------------|
| `LISTEN_ADDR` | `0.0.0.0:3001` | Bind address |
| `DATABASE_URL` | — | PostgreSQL connection string |
| `DB_POOL_MIN` | `1` | Min pool connections |
| `DB_POOL_MAX` | `10` | Max pool connections |
| `CORS_ORIGINS` | `*` | Allowed CORS origins |
| `RUST_LOG` | `info` | Log level filter |

---

### 6. Traefik Routing

**Decision**: Add a Traefik router rule for `/api/v1/*` → driver-service:3001, with a health check on `/health`

**Rationale**: Sprint 1.2 covers routing only (TLS deferred to MVP-6 per spec assumptions). The existing Docker Compose mesh gets a `traefik` service with file-based dynamic config.

**Router config**:
```yaml
routers:
  driver-api:
    rule: "PathPrefix(`/api/v1/`)"
    service: driver-service
    middlewares:
      - strip-api-prefix
services:
  driver-service:
    loadBalancer:
      servers:
        - url: "http://driver-service:3001"
healthCheck:
  path: "/health"
  interval: "10s"
  timeout: "3s"
```
