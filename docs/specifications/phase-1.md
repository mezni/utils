# Phase 1: Infrastructure Foundation

**Status**: Complete

## Scope

Establish the runtime infrastructure layer — databases, message broker,
identity provider, and reverse proxy. No business entities are introduced.

## Deliverables

- Docker Compose stack with 4 core services
- PostgreSQL + PostGIS with PostGIS and uuid-ossp extensions
- Analytics schema (JSONB) for clickstream storage
- RabbitMQ message broker with management UI
- Keycloak identity provider backed by PostgreSQL
- Traefik reverse proxy with automatic Docker service discovery
- Environment-based configuration via `.env`

## Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| postgis | postgis/postgis:16-3.4 | 5432 | Primary database + GIS |
| rabbitmq | rabbitmq:4-management | 5672, 15672 | Message broker |
| keycloak | keycloak/keycloak:latest | 8080 | Identity & access |
| traefik | traefik:latest | 80 | API gateway |

## Functional Requirements

- **FR-001**: Single `docker compose up` boots all services
- **FR-002**: Traefik entrypoint on port 80 with Docker provider
- **FR-003**: Traefik routes `/auth/*` to Keycloak
- **FR-004**: PostgreSQL has PostGIS and uuid-ossp extensions
- **FR-005**: PostgreSQL data persists to named Docker volume
- **FR-006**: PostgreSQL has `analytics` schema with JSONB tables
- **FR-007**: RabbitMQ management UI on port 15672
- **FR-008**: Keycloak backed by PostgreSQL with 3 roles
- **FR-009**: Single `bornemap-net` Docker network
- **FR-010**: No database ports exposed externally
- **FR-013**: `.env.example` with all configurable variables
- **FR-014**: No hardcoded secrets in compose files
- **FR-015**: Portable between local and CI environments

## Success Criteria

- `docker compose up` produces a fully functional stack
- All 4 services pass health checks within 60 seconds
- Traefik routes to Keycloak admin console at `/auth`
- PostgreSQL accessible from any container on `bornemap-net`
- Full restart preserves data across container restarts
