# Docker Compose Contract: Backend Services

## Overview

Two new services added to `infra/docker-compose.yml`: `driver-service` and `admin-service`.
Existing DB services (platform-db, analytics-db) unchanged.

## New Services

### driver-service

```yaml
driver-service:
  build:
    context: .
    dockerfile: infra/docker/driver-service.Dockerfile
    args:
      SERVICE_NAME: driver-service
      APP_PORT: 8080
  container_name: bornemap-driver
  ports:
    - "8080:8080"
  environment:
    RUST_LOG: ${RUST_LOG:-info}
    PLATFORM_DB_URL: postgresql://borneadmin:${PLATFORM_DB_PASSWORD:-borne_dev_2026}@platform-db:5432/platform_db
    PORT: 8080
  depends_on:
    platform-db:
      condition: service_healthy
  networks:
    - bornemap
  restart: unless-stopped
```

### admin-service

```yaml
admin-service:
  build:
    context: .
    dockerfile: infra/docker/admin-service.Dockerfile
    args:
      SERVICE_NAME: admin-service
      APP_PORT: 8081
  container_name: bornemap-admin
  ports:
    - "8081:8081"
  environment:
    RUST_LOG: ${RUST_LOG:-info}
    PLATFORM_DB_URL: postgresql://borneadmin:${PLATFORM_DB_PASSWORD:-borne_dev_2026}@platform-db:5432/platform_db
    ANALYTICS_DB_URL: postgresql://borneadmin:${ANALYTICS_DB_PASSWORD:-borne_dev_2026}@analytics-db:5432/analytics_db
    PORT: 8081
  depends_on:
    platform-db:
      condition: service_healthy
    analytics-db:
      condition: service_healthy
  networks:
    - bornemap
  restart: unless-stopped
```

## Dockerfiles

Each service has a multi-stage Dockerfile in `infra/docker/`:

- `infra/docker/driver-service.Dockerfile`
- `infra/docker/admin-service.Dockerfile`

Both use the same template with different `SERVICE_NAME` and `APP_PORT` build args.

### Dockerfile Stages

1. **chef**: rust:1.80-alpine3.20 + musl-dev, pkg-config, openssl-dev, protoc, clang, lld + cargo-chef
2. **planner**: Generate dependency recipe with `cargo chef prepare`
3. **builder**: Compile with `cargo chef cook` + `cargo build --release`, SQLX_OFFLINE=true
4. **runtime**: alpine:3.20 + ca-certificates, tzdata, libgcc. Non-root `app` user.

### .dockerignore

```gitignore
.git/
.github/
docs/
specs/
scripts/
source/front/
*.md
target/
.DS_Store
```

## Build Optimizations

- **cargo-chef**: Dependency caching — only rebuilds when Cargo.toml changes
- **SQLX_OFFLINE=true**: No database needed during Docker build
- **lld linker**: Faster linking via `RUSTFLAGS="-C link-arg=-fuse-ld=lld"`
- **Release profile**: `lto = "fat"`, `codegen-units = 1`, `strip = true`, `opt-level = 3`

## Networking

All services on the `bornemap` bridge network. DB services are reachable by container name:
- `platform-db:5432`
- `analytics-db:5433`
