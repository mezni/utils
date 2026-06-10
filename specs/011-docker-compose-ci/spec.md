# Feature Specification: Docker Compose and CI/CD

**Feature Branch**: `011-docker-compose-ci`

**Created**: 2026-06-09

**Status**: Draft

**Input**: User description: "Docker Compose with health checks and depends_on. Six GitHub Actions path-scoped workflows. Both services run sqlx::migrate! on startup. Frontend apps update API base URLs to point to Rust services."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Deploy Both Services Together (Priority: P1)

A developer or operations team starts all services (PostgreSQL, PostGIS, Driver Service, Admin Service, Frontend apps) with a single command. All services are fully initialized and operational before the system is ready for use.

**Why this priority**: Docker Compose is the foundation for local development, testing, and production deployment. This must work before any user can interact with the application.

**Independent Test**: Run `docker-compose up` and verify:
1. PostgreSQL and PostGIS containers start successfully
2. Driver Service and Admin Service start (no startup failures)
3. All containers show "healthy" status
4. Health endpoints return 200 (`/api/health`)
5. Services are accessible on their respective ports (8080, 8081)

**Acceptance Scenarios**:

1. **Given** PostgreSQL is running, **When** running `docker-compose up`, **Then** all services start and health endpoints are accessible
2. **Given** services are running, **When** querying `/api/health`, **Then** each service responds with `{"status":"ok","version":"..."}` at their respective ports
3. **Given** a service container, **When** checking container health, **Then** the health check passes within 30 seconds

---

### User Story 2 - Service Dependency Management (Priority: P1)

Services start in the correct order so that the database is ready before services try to connect. If a service fails to start, dependent services wait or fail gracefully.

**Why this priority**: Prevents race conditions where services try to connect to the database before it's ready, leading to startup failures. Critical for reliability.

**Independent Test**:
1. Start all services with `docker-compose up`
2. Wait 60 seconds for full startup
3. Verify database migrations ran successfully on both services
4. Stop the database and verify services don't crash
5. Restart database and verify services reconnect

**Acceptance Scenarios**:

1. **Given** PostgreSQL container is running, **When** starting services, **Then** Driver Service and Admin Service start after database health check passes
2. **Given** Driver Service and Admin Service are running, **When** querying their APIs, **Then** requests succeed with database connection established
3. **Given** PostgreSQL container is stopped, **When** querying API endpoints, **Then** services return 503 Service Unavailable or appropriate error

---

### User Story 3 - CI/CD Pipelines for Automated Testing (Priority: P2)

Pull requests trigger automated CI pipelines that build, test, and validate the code before merging. Changes to any Rust service trigger separate workflows.

**Why this priority**: Ensures code quality before merging, catches integration issues early, provides confidence in releases.

**Independent Test**:
1. Create a pull request with a simple fix
2. Verify CI pipeline runs automatically
3. Check that tests pass and linting succeeds
4. Verify no regression in other services

**Acceptance Scenarios**:

1. **Given** a pull request is created, **When** the workflow triggers, **Then** all six workflows run in sequence
2. **Given** a workflow runs, **When** checking the logs, **Then** compilation succeeds, tests pass, and Docker images are built successfully
3. **Given** tests fail, **When** reviewing the CI results, **Then** the specific test failure is clearly visible in the log output

---

### User Story 4 - Frontend API Configuration (Priority: P2)

Frontend applications (Dashboard, Driver Web, Driver Mobile) are configured to communicate with the correct backend services using environment variables.

**Why this priority**: Ensures frontend can reach the right backend endpoints during development and deployment. Critical for proper communication.

**Independent Test**:
1. Update `API_BASE_URL` environment variable to point to Docker Compose services
2. Verify frontend can successfully call backend APIs
3. Test with different API base URLs (localhost, container network)

**Acceptance Scenarios**:

1. **Given** `API_BASE_URL` points to Driver Service, **When** frontend requests data, **Then** requests go to `http://driver-service:8080/api`
2. **Given** `API_BASE_URL` points to Admin Service, **When** frontend requests data, **Then** requests go to `http://admin-service:8081/api`
3. **Given** `API_BASE_URL` is not set, **When** frontend attempts API calls, **Then** it uses a default fallback URL or shows appropriate error

---

### Edge Cases

- What happens if one service crashes during startup?
- How does the system handle PostgreSQL restart while services are running?
- What happens if Docker Compose fails to pull the latest image?
- How does the system handle concurrent API requests during startup?

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST provide a single `docker-compose.yml` that orchestrates PostgreSQL, PostGIS, Driver Service, Admin Service, and frontend apps
- **FR-002**: Docker Compose MUST include health checks for all services (PostgreSQL via pg_isready, services via `/api/health`)
- **FR-003**: Services MUST use `depends_on` with `condition: service_healthy` to ensure correct startup order
- **FR-004**: Both Driver Service and Admin Service MUST run `sqlx::migrate!` on startup to apply database migrations
- **FR-005**: Frontend apps MUST use environment variable `API_BASE_URL` to configure backend endpoints (default: `http://localhost:8080`)
- **FR-006**: Docker Compose MUST expose services on container network (e.g., `driver-service:8080`, `admin-service:8081`)
- **FR-007**: GitHub Actions MUST have 6 path-scoped workflows, one for each Rust service (driver-service, admin-service)
- **FR-008**: Each GitHub Actions workflow MUST build the Rust service, run `cargo test`, and build a Docker image
- **FR-009**: Each GitHub Actions workflow MUST trigger on pull requests and pushes to the service's branch only
- **FR-010**: CI pipelines MUST validate that `cargo clippy -- -D warnings` passes

### Key Entities

- **Docker Compose Service**: Containerized service (PostgreSQL, Driver Service, Admin Service, Frontend apps) with health checks and dependencies
- **GitHub Actions Workflow**: Automated CI pipeline for a specific service with path-based triggers
- **Environment Variable**: Configuration value (e.g., `DATABASE_URL`, `API_BASE_URL`, `HOST`, `PORT`) used by services

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Running `docker-compose up` successfully starts all 5 services within 60 seconds
- **SC-002**: Health endpoints return 200 within 30 seconds of `docker-compose up` completion
- **SC-003**: All 6 GitHub Actions workflows run successfully on each pull request to their respective branches
- **SC-004**: Tests pass and clippy warnings are zero in all CI pipelines
- **SC-005**: Frontend applications successfully call backend APIs using Docker Compose service names
- **SC-006**: Services recover gracefully when PostgreSQL restarts (reconnect and continue serving)

## Assumptions

- Docker is installed and available on development and CI environments
- PostgreSQL 17 + PostGIS 3.5 image is available on Docker Hub
- GitHub Actions runner has sufficient resources to build and test all services
- API base URL defaults to `http://localhost:8080` for Driver Service
- No secrets or sensitive data is hardcoded in Docker Compose or CI configurations
- Frontend apps can be rebuilt locally by running `pnpm install && pnpm build` after `docker-compose up`
- Database migrations are idempotent and safe to run multiple times
- Health check timeout is 30 seconds (reasonable for development environments)
