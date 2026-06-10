# Docker Compose Service Contracts

**Date**: 2026-06-09 | **Branch**: `011-docker-compose-ci` | **File**: `docker-compose.yml`

## Service Definitions

### postgres

| Property | Value |
|----------|-------|
| Image | `postgis/postgis:17-3.5` |
| Container name | `borne-postgres` |
| Port | `5432:5432` |
| Health check | `pg_isready -U postgres` (interval 10s, retries 5) |
| Volumes | `pgdata:/var/lib/postgresql/data` |
| Env vars | `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` |

### driver-service

| Property | Value |
|----------|-------|
| Build | `source/services/driver-service/` |
| Container name | `borne-driver-service` |
| Port | `8080:8080` |
| Health check | `curl -f http://localhost:8080/api/health` (interval 15s, retries 3) |
| Depends on | `postgres` (condition: service_healthy) |
| Env vars | `DATABASE_URL=postgres://postgres:postgres@postgres:5432/borne_map`, `PORT=8080`, `RUST_LOG=info` |

### admin-service

| Property | Value |
|----------|-------|
| Build | `source/services/admin-service/` |
| Container name | `borne-admin-service` |
| Port | `8081:8081` |
| Health check | `curl -f http://localhost:8081/api/health` (interval 15s, retries 3) |
| Depends on | `postgres` (condition: service_healthy) |
| Env vars | `DATABASE_URL=postgres://postgres:postgres@postgres:5432/borne_map`, `PORT=8081`, `RUST_LOG=info` |

### dashboard (optional for dev)

| Property | Value |
|----------|-------|
| Build | `source/apps/dashboard/` |
| Container name | `borne-dashboard` |
| Port | `5173:5173` |
| Depends on | `driver-service`, `admin-service` |
| Env vars | `API_BASE_URL=http://driver-service:8080` |

### driver-web (optional for dev)

| Property | Value |
|----------|-------|
| Build | `source/apps/driver-web/` |
| Container name | `borne-driver-web` |
| Port | `5174:5174` |
| Depends on | `driver-service` |
| Env vars | `API_BASE_URL=http://driver-service:8080` |

### driver-mobile (optional for dev)

| Property | Value |
|----------|-------|
| Build | `source/apps/driver-mobile/` |
| Container name | `borne-driver-mobile` |
| Port | `8081:8081` |
| Depends on | `driver-service` |
| Env vars | `API_BASE_URL=http://driver-service:8080` |

## Networks

- **Name**: `borne-network` (bridge driver)
- **All services** join this network for inter-service communication

## Volumes

- **Name**: `pgdata` (persistent PostgreSQL data across container restarts)

## Startup Order

```text
postgres (healthy) ─┬─> driver-service (healthy) ─┬─> dashboard
                    │                              ├─> driver-web
                    └─> admin-service (healthy)     └─> driver-mobile
```
