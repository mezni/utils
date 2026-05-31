# Research: Infrastructure Foundation (MVP Runtime Core)

## PostGIS Initialization

**Decision**: Use official postgis/postgis:16-3.4 image with init SQL
**Rationale**: The postgis image extends the official PostgreSQL image and
supports `/docker-entrypoint-initdb.d/` for running SQL on first boot. An
init SQL script will run `CREATE EXTENSION IF NOT EXISTS postgis` and
`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`.
**Alternatives considered**: Building a custom Docker image — unnecessary,
the official image supports init scripts natively.

## Keycloak PostgreSQL Backing

**Decision**: Share the same PostgreSQL instance (bornemap DB), separate
schema managed by Keycloak
**Rationale**: Keycloak manages its own schema when pointed at a database.
Using the same Postgres instance simplifies the stack. Keycloak's
`KC_DB_URL` / `KC_DB_USERNAME` / `KC_DB_PASSWORD` env vars point to the
same Postgres service.
**Alternatives considered**: Dedicated Postgres instance for Keycloak —
adds complexity without benefit for Phase 1.

## Keycloak Realm & Roles

**Decision**: Use a JSON realm export mounted at `/opt/keycloak/data/import/`
for automatic import on first boot
**Rationale**: Keycloak supports auto-import of realm JSON files. The file
will define three roles: `registered_driver`, `partner`, `admin`.
**Alternatives considered**: Manual role creation via admin console —
not reproducible; Keycloak REST API setup — adds startup dependency.

## Traefik Routing

**Decision**: Use Docker provider with container labels for automatic route
discovery
**Rationale**: Traefik's Docker provider reads labels from running containers
to configure routing, eliminating manual route config files. Services declare
their own routes via Docker Compose labels.
**Alternatives considered**: Static file-based configuration — more manual,
less discoverable.

## MongoDB First-Boot Database

**Decision**: Use `MONGO_INITDB_DATABASE=clickstream` env var to create the
database on first boot
**Rationale**: Official MongoDB image supports `MONGO_INITDB_DATABASE` and
`/docker-entrypoint-initdb.d/` scripts for first-boot initialization.
**Alternatives considered**: Manual creation after startup — adds setup step.

## RabbitMQ Management

**Decision**: Enable management plugin (included in `rabbitmq:4-management`
image), expose port 15672
**Rationale**: The management image includes the UI plugin by default — no
additional configuration needed.
**Alternatives considered**: Standard image + manual plugin enable — more
complex, no benefit.

## Container Restart Policy

**Decision**: `unless-stopped` for all services
**Rationale**: Standard Docker Compose restart policy for infrastructure
services — containers restart on crash but stay stopped if explicitly
stopped by the user.
**Alternatives considered**: `always` — less control; `on-failure` — doesn't
cover Docker daemon restarts.

## Health Check Patterns

| Service | Check Command | Interval | Retries |
|---------|--------------|----------|---------|
| PostgreSQL | `pg_isready -U bornemap` | 10s | 5 |
| MongoDB | `mongosh --eval "db.runCommand({ping:1})"` | 10s | 5 |
| RabbitMQ | `rabbitmq-diagnostics check_running` | 10s | 5 |
| Keycloak | `curl -f http://localhost:8080/health` | 15s | 10 |
| Traefik | `wget --spider http://localhost:8080/api/http/routers` | 10s | 5 |

## Docker Compose File Organization

**Decision**: Single `docker-compose.yml` file with all 5 services plus
Traefik, organized into sections
**Rationale**: Single file is simplest and most transparent for Phase 1.
Can be split in later phases if the stack grows.
**Alternatives considered**: Multiple compose files (`docker-compose.yml` +
`docker-compose.override.yml`) — unnecessary complexity for Phase 1.
