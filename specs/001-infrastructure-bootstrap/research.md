# Research: Infrastructure Bootstrap

## PostgreSQL 16 + PostGIS 3.4

**Decision**: Use `postgis/postgis:16-3.4` Docker image

**Rationale**: Official PostGIS image bundles PostgreSQL 16 with PostGIS 3.4 pre-installed. Eliminates manual extension management and version compatibility issues.

**Alternatives considered**:
- Separate postgres + postgresql-contrib: Requires manual PostGIS installation
- TimescaleDB + PostGIS: Over-engineered for MVP-1

## Docker Compose Network Topology

**Decision**: Single internal Docker network (`borne-net`) with named containers

**Rationale**: MVP-1 services (driver-service, admin-service, clickstream-service) run outside Docker during development. Only databases and Keycloak are containerized. The internal network isolates data layer from host exposure.

**Port mapping strategy**:
- Host port 5432 → container 5432 (platform_db)
- Host port 5433 → container 5432 (analytics_db)
- Host port 8083 → container 8080 (Keycloak)

## Healthcheck Endpoint Pattern

**Decision**: `GET /health` returning `{"status": "ok"}` with 200 status

**Rationale**: Standard pattern used across Actix-web services. Future MVP iterations can extend with database connectivity checks.

## Seed Data Strategy

**Decision**: SQL migration script executed on first database startup

**Rationale**: Docker Compose init scripts run once on volume creation. Repeatable and version-controlled. Avoids manual seeding.
