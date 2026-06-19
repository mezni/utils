# Research: Infrastructure Bootstrap

**Phase 0 output** — Technology decisions and rationale for Sprint 0.

## Technology Choices

### Docker Compose as orchestration layer

- **Decision**: Docker Compose v2
- **Rationale**: Lightest-weight option for single-developer local environment. No Kubernetes complexity. Constitution mandates Monolith-first (I.4). All 4 containers are well-known Docker images with Compose-native health checks.
- **Alternatives considered**: Docker Compose v1 (deprecated), Kubernetes/minikube (too heavy for dev), Podman Compose (less ecosystem support)

### Postgres image

- **Decision**: `postgis/postgis:16-3.4`
- **Rationale**: Official PostGIS-bundled image, Postgres 16 is latest stable with PostGIS 3.4. Single image eliminates need to install PostGIS extension separately.
- **Alternatives considered**: `postgres:16` + manual `CREATE EXTENSION postgis` (reliable but more init script code)

### Redis image

- **Decision**: `redis:7-alpine`
- **Rationale**: Official Alpine-based image, minimal footprint (~30MB). Redis 7 is the current stable line. No configuration needed for MVP.
- **Alternatives considered**: `redis:7` (larger image, no benefit for dev)

### Keycloak image

- **Decision**: `quay.io/keycloak/keycloak:25.0`
- **Rationale**: Official Keycloak distribution. Version 25 is the current stable. Uses Quay registry per Keycloak's distribution policy.
- **Alternatives considered**: `jboss/keycloak` (deprecated legacy image)

### Traefik image

- **Decision**: `traefik:v3.1`
- **Rationale**: Official Traefik v3 image. v3 is current stable. File-based dynamic configuration is simplest for MVP needs.
- **Alternatives considered**: Traefik v2 (older, fewer features), nginx (would need custom config for service discovery)

### Authentication approach

- **Decision**: Keycloak realm export file (`bornemap-realm.json`)
- **Rationale**: Declarative, version-controllable. No runtime API calls needed for initial setup. Constitution mandates Auth Service as sole Keycloak proxy (I.5).
- **Alternatives considered**: Keycloak admin API at startup (fragile, requires network ordering)

### Database bootstrap

- **Decision**: SQL init scripts mounted into Postgres `docker-entrypoint-initdb.d/`
- **Rationale**: Postgres runs init scripts in alphabetical order on first startup. No migration tool needed for schema creation.
- **Alternatives considered**: Flyway/sqlx migrations (overkill for schema bootstrap), manual `psql` (not automated)

### Credential management

- **Decision**: `.env` file with `.env.example`
- **Rationale**: Constitution mandates no credentials in git (I.5). Standard practice for Docker Compose projects.
- **Alternatives considered**: Hardcoded (violates constitution), secret management tools (overkill for dev env)

### Stub containers

- **Decision**: Minimal HTTP stubs (Python or BusyBox httpd)
- **Rationale**: Lightweight verification that Traefik routes correctly. Discarded immediately when real services arrive in Sprints 1–2.
- **Alternatives considered**: Traefik mirror middleware (doesn't verify container routing), Caddy (overkill for a stub), none (routing untestable until Sprint 1)

## Key Docker Compose V2 Settings

| Setting | Value | Rationale |
|---------|-------|-----------|
| `name` | `bornemap` | Consistent project namespace |
| `services.*.networks` | `bornemap-net` | Single shared network per clarification |
| `services.keycloak.depends_on` | `postgres` | Keycloak needs its DB backend |
| `services.keycloak.healthcheck` | `curl --fail http://localhost:8080/health || exit 1` | Keycloak exposes health endpoint |
| `services.postgres.healthcheck` | `pg_isready -U postgres` | Standard Postgres health check |
| `services.redis.healthcheck` | `redis-cli ping \| grep -q PONG` | Standard Redis health check |
| `services.traefik.healthcheck` | TCP port 80 check | Simple connectivity check |
| `volumes.pgdata` | Named volume | Data persistence across restarts |
| `volumes.keycloak_data` | Named volume | Keycloak DB persistence |
| `volumes.redis_data` | Named volume | Redis persistence |

## Port Mapping

| Service | Container Port | Host Port | Purpose |
|---------|---------------|-----------|---------|
| Postgres | 5432 | 5432 | Direct DB access for development |
| Redis | 6379 | 6379 | Direct cache access for development |
| Keycloak | 8080 | 8080 | Admin console + token endpoint |
| Traefik | 80 | 80 | API gateway entry point |
| Traefik dashboard | 8080 (internal) | Not exposed | Debug only, accessed via port 80 |

## Health Check Sequence

```
start → Postgres healthy → Keycloak healthy → Redis healthy → Traefik healthy → Stubs healthy
```

Dependency chain in Compose: `keycloak.depends_on: postgres`, all others independent.
