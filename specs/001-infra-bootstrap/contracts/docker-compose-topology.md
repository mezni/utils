# Docker Compose Topology Contract

**File**: `source/infra/docker-compose.yml`

## Services

| Service Name | Image | Container Name | Internal Port | Host Port |
|-------------|-------|---------------|---------------|-----------|
| `postgres` | `postgis/postgis:16-3.4` | `bornemap-postgres` | 5432 | 5432 |
| `redis` | `redis:7-alpine` | `bornemap-redis` | 6379 | 6379 |
| `keycloak` | `quay.io/keycloak/keycloak:25.0` | `bornemap-keycloak` | 8080 | 8080 |
| `traefik` | `traefik:v3.1` | `bornemap-traefik` | 80 | 80 |
| `auth-service` | Dockerfile stub | `bornemap-auth-stub` | 3000 | — |
| `admin-service` | Dockerfile stub | `bornemap-admin-stub` | 3002 | — |
| `driver-service` | Dockerfile stub | `bornemap-driver-stub` | 3001 | — |

## Networks

| Network | Driver |
|---------|--------|
| `bornemap-net` | bridge |

All services attach to `bornemap-net`. Container DNS names are the service names.

## Volumes

| Volume | Driver |
|--------|--------|
| `pgdata` | local |
| `keycloak_data` | local |
| `redis_data` | local |

## Dependency Order

```
postgres (healthy) → keycloak (depends_on: postgres)
all others: no startup dependency (health checked independently)
```

## Environment Variables Contract

See `.env.example` for the full list. Key variables:

| Variable | Used By | Example |
|----------|---------|---------|
| `POSTGRES_PASSWORD` | postgres | `devpassword` |
| `KEYCLOAK_ADMIN` | keycloak | `admin` |
| `KEYCLOAK_ADMIN_PASSWORD` | keycloak | `admin123` |
| `KC_DB_URL` | keycloak | `jdbc:postgresql://postgres:5432/keycloak_db` |
| `KC_DB_USERNAME` | keycloak | `keycloak` |
| `KC_DB_PASSWORD` | keycloak | `keycloakdev` |
