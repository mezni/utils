# Docker Compose Map

## Container Overview

| Service | Image | Port(s) | Depends On | MVP |
|---------|-------|---------|------------|-----|
| `platform_db` | `postgis/postgis:15-3.4` | `5432` | — | MVP-1 |
| `keycloak_db` | `postgres:16-alpine` | `5433` | — | MVP-1 |
| `analytics_db` | `postgres:16-alpine` | `5434` | — | MVP-1 |
| `keycloak` | `quay.io/keycloak/keycloak:25.0` | `8080` | `keycloak_db` | MVP-3 |
| `auth-service` | Built from `services/auth-service` | `3000` | `platform_db` (keycloak added in MVP-3) | MVP-1 |
| `driver-service` | Built from `services/driver-service` | `3001` | `platform_db` | MVP-1 |
| `admin-service` | Built from `services/admin-service` | `3002` | `platform_db` | MVP-1 |
| `gis-service` | Built from `services/gis-service` | `3003` | `platform_db`, `redis` | MVP-2 |
| `redis` | `redis:7-alpine` | `6379` | — | MVP-2 |
| `osm-importer` | Built from `infra/osm-importer` | — | `platform_db` | MVP-2 |
| `traefik` | `traefik:v3.1` | `443`, `80` | various | MVP-6 |

---

## MVP-1 docker-compose.yml

```yaml
version: "3.9"

services:
  platform_db:
    image: postgis/postgis:15-3.4
    environment:
      POSTGRES_USER: bornemap
      POSTGRES_PASSWORD: bornemap_dev
      POSTGRES_DB: platform_db
    ports:
      - "5432:5432"
    volumes:
      - platform_db_data:/var/lib/postgresql/data
      - ../infra/db/init-platform-db.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U bornemap -d platform_db"]
      interval: 5s
      timeout: 5s
      retries: 5

  keycloak_db:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: bornemap
      POSTGRES_PASSWORD: bornemap_dev
      POSTGRES_DB: keycloak_db
    ports:
      - "5433:5432"
    volumes:
      - keycloak_db_data:/var/lib/postgresql/data

  analytics_db:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: bornemap
      POSTGRES_PASSWORD: bornemap_dev
      POSTGRES_DB: analytics_db
    ports:
      - "5434:5432"
    volumes:
      - analytics_db_data:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

  auth-service:
    build: ../services/auth-service
    ports:
      - "3000:3000"
    environment:
      HOST: 0.0.0.0
      PORT: 3000
      DATABASE_URL: postgres://bornemap:bornemap_dev@platform_db:5432/platform_db
      LOG_LEVEL: info
    depends_on:
      platform_db:
        condition: service_healthy

  driver-service:
    build: ../services/driver-service
    ports:
      - "3001:3001"
    environment:
      HOST: 0.0.0.0
      PORT: 3001
      DATABASE_URL: postgres://bornemap:bornemap_dev@platform_db:5432/platform_db
      GIS_SERVICE_URL: http://gis-service:3003
      LOG_LEVEL: info
    depends_on:
      platform_db:
        condition: service_healthy

  admin-service:
    build: ../services/admin-service
    ports:
      - "3002:3002"
    environment:
      HOST: 0.0.0.0
      PORT: 3002
      DATABASE_URL: postgres://bornemap:bornemap_dev@platform_db:5432/platform_db
      GIS_SERVICE_URL: http://gis-service:3003
      LOG_LEVEL: info
    depends_on:
      platform_db:
        condition: service_healthy

  gis-service:
    build: ../services/gis-service
    ports:
      - "3003:3003"
    environment:
      HOST: 0.0.0.0
      PORT: 3003
      DATABASE_URL: postgres://bornemap:bornemap_dev@platform_db:5432/platform_db
      REDIS_URL: redis://redis:6379
      CACHE_SECRET: dev-cache-secret
      LOG_LEVEL: info
    depends_on:
      platform_db:
        condition: service_healthy
      redis:
        condition: service_healthy

volumes:
  platform_db_data:
  keycloak_db_data:
  analytics_db_data:
  redis_data:
```

## Volumes

| Volume Name | Purpose | Persist |
|-------------|---------|---------|
| `platform_db_data` | PostGIS data directory | Yes |
| `keycloak_db_data` | Keycloak Postgres data | Yes |
| `analytics_db_data` | Analytics Postgres data | Yes |
| `redis_data` | Redis persistence (RDB/AOF) | Yes |

## Network

All containers share a default bridge network `bornemap-network` (created automatically by Docker Compose as the project network). Service discovery via container name.

```
platform_db:5432  <── auth-service:3000
                    <── driver-service:3001
                    <── admin-service:3002
                    <── gis-service:3003
                    <── osm-importer (MVP-2)

keycloak_db:5432  <── keycloak:8080 (MVP-3)

keycloak:8080     <── auth-service:3000 (MVP-3)

redis:6379        <── gis-service:3003
```

## Startup Order

1. `platform_db`, `keycloak_db`, `analytics_db` (parallel — no interdependency)
2. `redis` (MVP-2+)
3. `auth-service`, `driver-service`, `admin-service` (parallel — depend on platform_db; auth-service adds keycloak dep in MVP-3)
4. `gis-service` (MVP-2 — depends on platform_db + redis)
5. `osm-importer` (MVP-2 — one-shot, depends on platform_db)
6. `keycloak` (MVP-3 — depends on keycloak_db)
7. `traefik` (MVP-6 — depends on all services)

## Build Contexts

All services have their Dockerfile in the service directory. Expected locations:

```
infra/docker-compose.yml
services/auth-service/Dockerfile
services/driver-service/Dockerfile
services/admin-service/Dockerfile
services/gis-service/Dockerfile
infra/osm-importer/Dockerfile (MVP-2)
```

Docker Compose file lives at `infra/docker-compose.yml` with `build` contexts pointing to `../services/<name>` (equivalently `./services/<name>` from project root).
