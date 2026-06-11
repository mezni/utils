# Feature Specification: Infrastructure Bootstrap

**Feature Branch**: `001-infrastructure-bootstrap`

**Created**: 2026-06-10

**Status**: Draft

**Input**: User description: "sprint 0"

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Developer can run the full system locally (Priority: P1)

As a developer joining the project, I want to run a single command and have the entire backend infrastructure (databases, services) start up so that I can begin development without manual setup.

**Why this priority**: This is the foundational requirement — no other development can proceed until the system is runnable locally.

**Independent Test**: A new developer can clone the repo, run the startup command, and verify all services respond correctly within 5 minutes of first attempt.

**Acceptance Scenarios**:

1. **Given** a fresh clone of the repository, **When** the developer runs the startup command, **Then** all required infrastructure starts without errors
2. **Given** the infrastructure is running, **When** a developer accesses the database at the expected port, **Then** they can connect and run queries
3. **Given** the infrastructure is running, **When** a developer stops it with the shutdown command, **Then** all containers stop cleanly with no orphan processes

---

### User Story 2 — Developer can verify system health (Priority: P1)

As a developer, I want each service to expose a health endpoint so that I can verify the system is operational before starting feature work.

**Why this priority**: Without health verification, developers cannot distinguish between a startup failure and a code bug.

**Independent Test**: Start the system, call each service's health endpoint, and confirm all return a success response.

**Acceptance Scenarios**:

1. **Given** the infrastructure is running, **When** a developer calls the health endpoint, **Then** it returns a 200 status code within 2 seconds
2. **Given** a service is not running, **When** a developer calls its health endpoint, **Then** it returns an error status clearly indicating unavailability

---

### User Story 3 — Developer can run spatial queries (Priority: P1)

As a developer, I want the primary database to support spatial queries so that I can implement location-based features.

**Why this priority**: The entire discovery experience depends on spatial queries — this must work before any discovery feature can be built.

**Independent Test**: Connect to the database, run a simple spatial query, and confirm it returns correct results.

**Acceptance Scenarios**:

1. **Given** the database is initialized, **When** a developer runs a spatial function (distance calculation), **Then** it returns a correct numeric result
2. **Given** the database is initialized, **When** a developer checks for the spatial extension, **Then** it is confirmed enabled

---

### Edge Cases

- What happens when the developer's machine does not have Docker installed? System should detect and provide a clear error message with installation instructions.
- How does the system handle port conflicts with existing services? Should detect occupied ports and fail with a clear message identifying the conflict.
- How does startup behave when an unclean shutdown left stale containers? Should clean up or provide a reset command.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST start all required infrastructure components with a single command
- **FR-002**: System MUST provide a way to verify each component's health status
- **FR-003**: System MUST support spatial database queries out of the box
- **FR-004**: System MUST clean up all resources when shut down
- **FR-005**: System MUST detect and report missing prerequisites (Docker, ports) with actionable error messages
- **FR-006**: System MUST start in under 60 seconds on a development machine with a warm Docker cache
- **FR-007**: System MUST NOT require manual configuration beyond a single environment file
- **FR-008**: System MUST persist data between restarts so that seed data survives shutdown

### Key Entities *(include if feature involves data)*

N/A — this feature establishes infrastructure, not data entities.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A new developer can go from repo clone to running system in under 10 minutes following documented setup
- **SC-002**: All infrastructure components start within 60 seconds of running the startup command
- **SC-003**: All health endpoints respond with success within 2 seconds of system startup
- **SC-004**: Spatial database queries execute successfully on first attempt after startup
- **SC-005**: System shutdown completes within 10 seconds with zero orphaned processes

## Assumptions

- Developers have Docker and Docker Compose installed on their machine
- Developers have Rust toolchain installed for running services outside containers
- The development environment is macOS or Linux (Windows via WSL2)
- Ports 5432, 5433, 8083 are available on the development machine
- No cloud services or external APIs are required for local development
- The monorepo structure (/source, /infra, /docs) will be created as part of this sprint
