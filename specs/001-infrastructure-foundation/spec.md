# Feature Specification: Infrastructure Foundation (MVP Runtime Core)

**Feature Branch**: `001-infrastructure-foundation`

**Created**: 2026-05-30

**Status**: Draft

**Input**: Phase 1 — Infrastructure Foundation Specification (MVP Runtime Core)

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Boot Full Infrastructure Stack (Priority: P1)

As a developer, I want to run a single command (`docker compose up`) and
have all infrastructure services boot correctly so that I can begin
developing backend services against a live environment.

**Why this priority**: Every subsequent feature depends on a working runtime
stack. Without this, no backend service can be developed or tested.

**Independent Test**: Run `docker compose up` on a clean checkout. After
startup, verify all service containers are running and healthy via
`docker compose ps`.

**Acceptance Scenarios**:

1. **Given** a clean clone of the repository, **When** I run `docker compose up`,
   **Then** all 5 core services (Traefik, PostgreSQL+PostGIS, MongoDB,
   RabbitMQ, Keycloak) start without errors.
2. **Given** the stack is running, **When** I run `docker compose ps`,
   **Then** all services show status "Up" and pass their health checks.
3. **Given** the stack is running, **When** I stop and restart all containers
   (`docker compose down && docker compose up`), **Then** all services
   recover without manual intervention.

---

### User Story 2 - Verify Service Connectivity (Priority: P2)

As a developer, I want to verify that each infrastructure service is
reachable and functional from within the Docker network so that I can
confidently develop against them.

**Why this priority**: Having services running is insufficient — they must
also be logically accessible and responsive for development to proceed.

**Independent Test**: From a temporary container on the same network,
connect to each service's endpoint and verify a response.

**Acceptance Scenarios**:

1. **Given** the stack is running, **When** I curl Traefik's entrypoint on
   port 80, **Then** I receive an HTTP response (non-connection-refused).
2. **Given** the stack is running, **When** I connect to PostgreSQL on port
   5432 from a container on `bornemap-net`, **Then** the database
   `bornemap` is accessible with user `bornemap`.
3. **Given** the stack is running, **When** I access RabbitMQ management UI
   at port 15672, **Then** the login page loads successfully.
4. **Given** the stack is running, **When** I access Keycloak at `/auth`,
   **Then** the admin console is reachable.

---

### User Story 3 - Configure Stack via Environment (Priority: P3)

As an operator, I want to configure all service parameters through
environment variables in a `.env` file so that the stack is portable
between local and CI environments.

**Why this priority**: Portability ensures the same configuration approach
works across developer machines and CI runners without code changes.

**Independent Test**: Modify a `.env` value (e.g., PostgreSQL password),
restart the stack, and verify the new value takes effect.

**Acceptance Scenarios**:

1. **Given** a `.env.example` file exists, **When** I copy it to `.env` and
   modify a configuration value, **Then** the corresponding service uses
   the new value after restart.
2. **Given** no `.env` file exists, **When** I run `docker compose up`,
   **Then** the stack fails with a clear error about missing environment
   variables (no silent defaults).

---

### Edge Cases

- What happens when a service fails to start (e.g., port conflict)?
  The stack should report the failure via container exit code and logs.
- How does the system handle volume persistence after a restart?
  Volumes must retain data across container restarts.
- What happens when Keycloak cannot reach PostgreSQL on first boot?
  Keycloak must retry the connection rather than fail permanently.
- How does the stack behave when Docker is low on disk space?
  Containers should fail with clear Docker layer errors, not silent data
  corruption.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST boot all infrastructure services via a single
  `docker compose up` command with zero manual setup.
- **FR-002**: Traefik MUST expose an HTTP entrypoint on port 80 with Docker
  provider enabled for automatic service discovery.
- **FR-003**: Traefik MUST route requests under `/auth/*` to the Keycloak
  service.
- **FR-004**: PostgreSQL MUST have PostGIS and uuid-ossp extensions enabled.
- **FR-005**: PostgreSQL MUST persist data to a named Docker volume across
  container restarts.
- **FR-006**: MongoDB MUST have a `clickstream` database created on first
  boot and persist data to a named Docker volume.
- **FR-007**: RabbitMQ MUST have management UI enabled on port 15672 and
  persist queue data to a named Docker volume.
- **FR-008**: Keycloak MUST be backed by PostgreSQL and pre-configure three
  roles: `registered_driver`, `partner`, `admin`.
- **FR-009**: All services MUST run on a single Docker network named
  `bornemap-net`.
- **FR-010**: No database ports (PostgreSQL, MongoDB) MUST be exposed
  externally in the production profile.
- **FR-011**: All services MUST expose a `GET /health` endpoint returning
  `{ "status": "ok" }`.
- **FR-012**: All services MUST output structured JSON logs containing:
  timestamp, service name, log level, message.
- **FR-013**: A `.env.example` file MUST exist at the repository root
  defining all configurable environment variables with default values.
- **FR-014**: No credentials or secrets MUST be hardcoded in Docker Compose
  files — all configuration MUST be injectable via environment variables.
- **FR-015**: Services MUST be portable between local machines and CI
  runners — no dependency on host-specific state or binaries.

### Key Entities

This phase introduces no business entities. It establishes the runtime
infrastructure layer only — databases, message broker, identity provider,
and reverse proxy.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A developer can clone the repo and run `docker compose up` to
  obtain a fully functional identity + database + messaging + proxy stack
  with zero manual configuration steps.
- **SC-002**: All 5 core services pass health checks within 60 seconds of
  `docker compose up`.
- **SC-003**: Traefik successfully routes to Keycloak admin console at
  `/auth`.
- **SC-004**: PostgreSQL with PostGIS is accessible from any container on
  the `bornemap-net` network.
- **SC-005**: Full stack restart (`docker compose down && docker compose up`)
  completes without manual intervention and all volumes persist correctly.
- **SC-006**: All services produce structured JSON logs observable via
  `docker compose logs`.
- **SC-007**: The stack boots identically on a developer machine and a CI
  runner with no host-specific configuration.

## Assumptions

- Docker Engine and Docker Compose plugin (v2+) are pre-installed on the
  developer machine and CI runner.
- No external DNS or certificate setup is required for local development
  (Traefik operates on plain HTTP, port 80).
- Keycloak realm auto-import is optional in Phase 1 — roles can be
  preconfigured manually or via a JSON realm export.
- MongoDB runs without authentication in Phase 1 (dev mode). Authentication
  will be added in a later phase.
- The host machine has sufficient resources (minimum 4 GB RAM, 10 GB disk)
  to run all 5 containers simultaneously.
- Business services (Driver/Admin Service, GIS Worker, Clickstream Service)
  and frontend applications are out of scope and will be added in later
  phases.
- CI/CD pipeline setup (GitHub Actions, GHCR push, SSH deploy) is out of
  scope for this phase — only Docker Compose compatibility is required.
- All services use official Docker images where available
  (postgis/postgis, mongo, rabbitmq, quay.io/keycloak/keycloak,
  traefik).
