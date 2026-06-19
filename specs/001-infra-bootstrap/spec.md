# Feature Specification: Infrastructure Bootstrap

**Feature Branch**: `001-infra-bootstrap`

**Created**: 2026-06-18

**Status**: Draft

**Input**: User description: "docs/specs/mvp-1-admin-flow.md Sprint 0 — Docker Compose base stack (Postgres 16+PostGIS, Redis, Keycloak, Traefik), Keycloak realm bootstrap, DB schema bootstrap, Traefik routing"

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Developer starts the full backend stack with one command (Priority: P1)

A developer needs to run the entire BorneMap backend infrastructure locally to begin developing or testing services. They run a single command and all core dependencies (database, cache, identity provider, gateway) start up in the correct order with working defaults.

**Why this priority**: Without a working local environment, no service can be developed or tested. This is the foundation for all subsequent work.

**Independent Test**: Can be fully tested by running the start command on a clean machine and verifying all containers reach healthy state within a reasonable time window.

**Acceptance Scenarios**:

1. **Given** no infrastructure is running, **When** the developer executes the start command, **Then** all containers (Postgres+PostGIS, Redis, Keycloak, Traefik) reach healthy state within 120 seconds.
2. **Given** all containers are healthy, **When** the developer executes the stop command, **Then** all containers shut down cleanly and no orphan processes remain.
3. **Given** the stack is running, **When** the developer runs the start command again, **Then** existing containers are reused without data loss.

---

### User Story 2 — Developer verifies all services are reachable (Priority: P1)

A developer needs to confirm that each infrastructure component is accessible from the host machine and that services can communicate with each other.

**Why this priority**: Unreachable services waste debugging time and block all feature development.

**Independent Test**: Can be tested independently by running connectivity checks against each component after the stack starts.

**Acceptance Scenarios**:

1. **Given** the stack is running, **When** a developer connects to Postgres on port 5432, **Then** the connection succeeds and the `platform_db`, `keycloak_db`, and `analytics_db` databases exist.
2. **Given** the stack is running, **When** a developer opens the Keycloak admin console in a browser, **Then** the login page loads and the `bornemap` realm is visible after authentication.
3. **Given** the stack is running, **When** a developer sends a request to Redis on port 6379, **Then** the connection succeeds and `PING` responds with `PONG`.
4. **Given** the stack is running, **When** a developer connects as `auth_service_role`, **Then** only the `users` schema is accessible and `inventory`/`gis` schemas return permission denied.
5. **Given** the stack is running, **When** a developer connects as `admin_service_role`, **Then** the `inventory` schema is accessible and writeable.
6. **Given** the stack is running, **When** a developer sends a request to Traefik on port 80, **Then** Traefik responds with a routing response (not connection refused).

---

### User Story 3 — Developer verifies Traefik routes to correct backend services (Priority: P2)

A developer needs to confirm that the API gateway correctly routes requests to the appropriate backend services by path prefix. This must work before authentication middleware is enabled.

**Why this priority**: Incorrect routing causes requests to reach the wrong service or fail silently, making all subsequent development untrustworthy.

**Independent Test**: Can be tested independently by sending HTTP requests to each route and checking the destination.

**Acceptance Scenarios**:

1. **Given** the stack is running with mock/placeholder services, **When** a request is sent to `/api/v1/auth/login`, **Then** it reaches the auth-service container (or returns a recognizable placeholder response).
2. **Given** the stack is running, **When** a request is sent to `/api/v1/admin/partner`, **Then** it reaches the admin-service container.
3. **Given** the stack is running, **When** a request is sent to `/api/v1/driver/stations`, **Then** it reaches the driver-service container.
4. **Given** the stack is running, **When** a request is sent to an undefined path, **Then** Traefik returns a 404 response consistently.

---

### Edge Cases

- What happens when ports 5432, 6379, 8080, or 80 are already in use on the host?
- How does the system handle container startup failures (e.g., Postgres takes too long and Keycloak crashes)?
- What happens when the developer runs the start command on a machine without Docker installed?
- How does the system handle partial restarts (only some containers need rebuilding)?

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Developers MUST be able to start all infrastructure services with a single command.
- **FR-002**: All services MUST start in the correct dependency order (database before services that depend on it).
- **FR-003**: Postgres MUST support both PostGIS spatial queries and serve as the backend for `platform_db`, `keycloak_db`, and `analytics_db` databases.
- **FR-004**: Keycloak MUST be pre-configured with the `bornemap` realm, including three clients (`mobile-driver-app`, `web-driver-app`, `admin-dashboard`) and three roles (`role:driver`, `role:partner`, `role:admin`).
- **FR-005**: Database schemas (`gis`, `inventory`, `users`) and initial tables MUST be created automatically on first startup.
- **FR-006**: Each backend service (auth, admin, driver) MUST have a dedicated PostgreSQL database role with schema-scoped privileges.
- **FR-007**: Traefik MUST route requests by path prefix to the correct service container (`/api/v1/auth/*` → auth-service, `/api/v1/admin/*` → admin-service, `/api/v1/driver/*` → driver-service).
- **FR-008:** all configuration data (realm exports, database schemas, Traefik config) MUST be version-controlled in the repository.
- **FR-009**: All infrastructure configuration MUST be reproducible — a fresh checkout followed by the start command produces an identical environment.
- **FR-010**: The Keycloak admin console MUST be accessible at a documented URL for manual realm management during development, using credentials from a `.env` file with documented defaults in `.env.example`.

### Key Entities *(include if feature involves data)*

- **Docker Compose topology**: The full service graph (Postgres, Redis, Keycloak, Traefik) with networks, volumes, dependencies, and health checks.
- **Postgres databases**: Three logically separate databases (`platform_db`, `keycloak_db`, `analytics_db`) within a single Postgres instance.
- **Database schemas**: Namespaces within `platform_db` — `gis` (spatial reference), `inventory` (operational entities), `users` (auth profiles).
- **Database roles**: `auth_service_role`, `admin_service_role`, `driver_service_role`, `admin_analytics_role` — each scoped to specific schemas.
- **Keycloak realm export**: The declarative `bornemap-realm.json` containing client definitions, roles, and protocol mappers.
- **Traefik routing config**: Dynamic routing rules mapping URL path prefixes to Docker service backends.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A developer with Docker installed can go from `git clone` to a fully running stack in under 5 minutes (excluding image download time).
- **SC-002**: All four infrastructure containers (Postgres+PostGIS, Redis, Keycloak, Traefik) report healthy status within 120 seconds of the start command.
- **SC-003**: All three PostgreSQL databases are queryable and their schemas match the documented DDL exactly.
- **SC-004**: Keycloak realm, all three clients, and all three roles are present and correctly configured after initial startup without manual intervention.
- **SC-005**: Each Traefik route reaches the correct backend service (verified by distinct placeholder responses or log inspection).
- **SC-006**: The full infrastructure stack can be torn down and recreated without leaving residual state that affects the next run.
- **SC-007**: A developer can verify the health of all components in under 30 seconds using documented commands or a provided health check.

## Clarifications

### Session 2026-06-18

- Q: How are Keycloak admin credentials managed? → A: `.env` file with documented defaults in `.env.example` (standard dev pattern).
- Q: Should Sprint 0 set up segmented Docker networks? → A: Single shared Docker network. Network segmentation deferred to Sprint 3.
- Q: How to verify routing when backend services don't exist yet? → A: Lightweight stub HTTP containers returning fixed responses for each route.

## Assumptions

- The developer has Docker and Docker Compose v2 installed on their machine.
- Target host runs Linux (or macOS/Windows with Docker Desktop). No bare-metal provisioning scripts.
- No TLS termination is configured at this stage — Traefik routes HTTP only.
- No authentication middleware is enabled on Traefik routes (Sprint 3 covers this).
- Infrastructure runs on standard ports (5432, 6379, 8080, 80). Port conflicts are handled by the developer.
- Keycloak is configured via a static realm export file, not through the admin API at startup.
- The database bootstrap runs via migration scripts (DDL files), not through an ORM or migration framework.
- Monitoring, logging aggregation, and alerting are out of scope for this sprint.
- Keycloak admin credentials (`KEYCLOAK_ADMIN`, `KEYCLOAK_ADMIN_PASSWORD`) are provided via `.env` file with documented defaults in a checked-in `.env.example`.
- All services share a single Docker network. Network segmentation for Keycloak isolation is deferred to Sprint 3.
- Lightweight stub HTTP containers are included in Docker Compose for auth-service, admin-service, and driver-service to enable routing verification in Sprint 0.
