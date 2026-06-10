# Research: Docker Compose and CI/CD

**Date**: 2026-06-09 | **Branch**: `011-docker-compose-ci` | **Spec**: [spec.md](./spec.md)

## Architecture Decisions

### Docker Compose Version

- **Decision**: Compose v3.8+ (supported by `docker compose` CLI in all modern Docker versions)
- **Rationale**: Supports `healthcheck`, `depends_on` with `condition: service_healthy`, and named networks. No v2-specific features needed.
- **Alternatives considered**: v2 (fewer features), v3.9+ (no benefits for this use case)

### Network Strategy

- **Decision**: Single named `borne-network` bridge network for all services
- **Rationale**: Simple, flat network. Services communicate by container name. No need for overlay or multiple networks.
- **Service names**: `postgres`, `driver-service`, `admin-service`, `dashboard`, `driver-web`, `driver-mobile`

### Health Check Pattern

| Service | Health Check | Interval | Retries |
|---------|-------------|----------|---------|
| PostgreSQL | `pg_isready -U postgres` | 10s | 5 |
| Driver Service | `curl -f http://localhost:8080/api/health` | 15s | 3 |
| Admin Service | `curl -f http://localhost:8081/api/health` | 15s | 3 |

- **Rationale**: PostgreSQL health check uses `pg_isready` (native, zero dependencies). Rust services use `curl` via `/api/health` endpoint.
- **Note**: `curl` must be installed in the service Docker images (add to Dockerfile runtime stage if not present)

### Database Migration Strategy

- **Decision**: Run `sqlx::migrate!()` as Rust code at service startup (compile-time macro embedded in binary).
- **Rationale**: Already planned per spec FR-004. Driver Service and Admin Service both call `sqlx::migrate!("../../database/migrations")` on startup. Compile-time embedding means migrations are in the binary — no separate migration step.
- **Migration path**: `./database/migrations/` relative to workspace root. Must be accessible at build time via the Dockerfile COPY.

### Frontend API Configuration

- **Decision**: Environment variable `API_BASE_URL` with default fallback `http://localhost:8080`
- **Rationale**: Each frontend app (Dashboard, Driver Web, Driver Mobile) already reads API base URL from `.env` or runtime env vars. Docker Compose sets `API_BASE_URL=http://driver-service:8080` for backend calls.
- **Note**: Frontend apps run in development mode (`pnpm dev`) — no static build required for local Docker Compose

### GitHub Actions Workflow Design

- **Decision**: 2 path-scoped workflows (not 6 as originally described)
- **Rationale**: Spec says "six workflows" but there are only 2 Rust services. Each workflow triggers on paths for its service, runs `cargo build`, `cargo test`, `cargo clippy -- -D warnings`, and optionally builds Docker image.

| Workflow | Path Trigger | Steps |
|----------|-------------|-------|
| `driver-service.yml` | `source/services/driver-service/**`, `source/crates/**` | cargo build, test, clippy |
| `admin-service.yml` | `source/services/admin-service/**`, `source/crates/**` | cargo build, test, clippy |

**Note**: Both workflows trigger on `source/crates/**` changes since shared crates affect both services.

### CI Docker Build Strategy

- **Decision**: Build Docker image in CI and run `docker compose up` for integration tests
- **Rationale**: Ensures Dockerfile works end-to-end. Can verify health checks pass during CI.
- **Alternative considered**: Unit tests only in CI, Docker build skipped — rejected for lack of integration coverage

## Existing Patterns

From `source/services/driver-service/Dockerfile`:
- Multi-stage: `rust:1.85-slim-bookworm` builder → `debian:bookworm-slim` runtime
- Runtime deps: `ca-certificates`, `libssl3`, `libpq5`
- Port: 8080
- Command: `/driver-service`

From `source/services/admin-service/Dockerfile`:
- Same base images as driver-service
- Port: 8081
- Command: `/admin-service`

## Environment Variables Summary

| Variable | Default | Used By |
|----------|---------|---------|
| `DATABASE_URL` | — | driver-service, admin-service |
| `HOST` | `0.0.0.0` | driver-service, admin-service |
| `PORT` | 8080/8081 | driver-service, admin-service |
| `RUST_LOG` | `info` | driver-service, admin-service |
| `API_BASE_URL` | `http://localhost:8080` | dashboard, driver-web, driver-mobile |
