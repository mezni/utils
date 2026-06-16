# Environment Variables

---

## Auth Service

| Variable | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `HOST` | string | `0.0.0.0` | No | Bind address |
| `PORT` | integer | `3000` | No | Service port |
| `LOG_LEVEL` | string | `info` | No | Log level (trace/debug/info/warn/error) |
| `DATABASE_URL` | string | — | Yes | Postgres connection string for `platform_db` |
| `KEYCLOAK_URL` | string | — | Yes | Keycloak server base URL |
| `KEYCLOAK_ADMIN_USER` | string | — | Yes | Keycloak admin username |
| `KEYCLOAK_ADMIN_PASSWORD` | string | — | Yes | Keycloak admin password |
| `KEYCLOAK_DRIVERS_REALM` | string | `bornemap-drivers` | No | Drivers realm name |
| `KEYCLOAK_STAFF_REALM` | string | `bornemap-staff` | No | Staff realm name |

## Driver Service

| Variable | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `HOST` | string | `0.0.0.0` | No | Bind address |
| `PORT` | integer | `3001` | No | Service port |
| `LOG_LEVEL` | string | `info` | No | Log level |
| `DATABASE_URL` | string | — | Yes | Postgres connection string for `platform_db` |
| `KEYCLOAK_URL` | string | — | Yes | Keycloak server base URL (for JWKS validation) |
| `GIS_SERVICE_URL` | string | `http://gis-service:3003` | No | GIS Service URL for spatial reads |

## Admin Service

| Variable | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `HOST` | string | `0.0.0.0` | No | Bind address |
| `PORT` | integer | `3002` | No | Service port |
| `LOG_LEVEL` | string | `info` | No | Log level |
| `DATABASE_URL` | string | — | Yes | Postgres connection string for `platform_db` |
| `KEYCLOAK_URL` | string | — | Yes | Keycloak server base URL (for JWKS validation) |
| `GIS_SERVICE_URL` | string | `http://gis-service:3003` | No | GIS Service URL for spatial reads |

## GIS Service

| Variable | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `HOST` | string | `0.0.0.0` | No | Bind address |
| `PORT` | integer | `3003` | No | Service port |
| `LOG_LEVEL` | string | `info` | No | Log level |
| `DATABASE_URL` | string | — | Yes | Postgres connection string for `platform_db` |
| `REDIS_URL` | string | `redis://redis:6379` | No | Redis connection string |
| `CACHE_TTL_NEARBY_SEC` | integer | `120` | No | TTL for nearby query cache |
| `CACHE_TTL_STATION_SEC` | integer | `300` | No | TTL for station detail cache |
| `CACHE_SECRET` | string | — | Yes | Shared secret for internal cache-bust endpoint |

## Shared Crate Configuration

Set via service-level env vars (no dedicated config file):

| Variable | Used By | Description |
|----------|---------|-------------|
| `DATABASE_MAX_CONNECTIONS` | All services | DB pool size (default: 10) |
| `DATABASE_CONNECT_TIMEOUT` | All services | Connection timeout in seconds (default: 10) |

## OSM Importer

| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| `DATABASE_URL` | — | Yes | Postgres connection string for `platform_db` |
| `OSM_EXTRACT_URL` | — | Yes | URL to Tunisia OSM .pbf extract |
| `IMPORT_BATCH_SIZE` | `1000` | No | Batch insert size |

## Databases (docker-compose)

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_USER` | `bornemap` | DB superuser |
| `POSTGRES_PASSWORD` | `bornemap_dev` | DB password (dev only) |
| `PLATFORM_DB` | `platform_db` | Main database name |
| `KEYCLOAK_DB` | `keycloak_db` | Keycloak database name |
| `ANALYTICS_DB` | `analytics_db` | Analytics database name |

## Keycloak (docker-compose)

| Variable | Default | Description |
|----------|---------|-------------|
| `KEYCLOAK_ADMIN` | `admin` | Keycloak admin console user |
| `KEYCLOAK_ADMIN_PASSWORD` | `admin123` | Keycloak admin password (dev only) |
| `KC_DB_URL` | — | Keycloak DB connection string |
| `KC_HOSTNAME` | `localhost` | Keycloak hostname |

---

## `.env.example` Template

```env
# Database
PLATFORM_DB_USER=bornemap
PLATFORM_DB_PASSWORD=bornemap_dev
PLATFORM_DB_NAME=platform_db
PLATFORM_DB_PORT=5432

KEYCLOAK_DB_USER=bornemap
KEYCLOAK_DB_PASSWORD=bornemap_dev
KEYCLOAK_DB_NAME=keycloak_db
KEYCLOAK_DB_PORT=5433

ANALYTICS_DB_USER=bornemap
ANALYTICS_DB_PASSWORD=bornemap_dev
ANALYTICS_DB_NAME=analytics_db
ANALYTICS_DB_PORT=5434

# Cache
REDIS_PORT=6379
CACHE_SECRET=dev-cache-secret

# Services
AUTH_SERVICE_PORT=3000
DRIVER_SERVICE_PORT=3001
ADMIN_SERVICE_PORT=3002
GIS_SERVICE_PORT=3003

# Logging
LOG_LEVEL=info
```
