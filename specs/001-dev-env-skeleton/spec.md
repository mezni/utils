# Feature Specification: Dev Environment + CI/CD + Runnable Skeleton

**Feature Branch**: `001-dev-env-skeleton`

**Created**: 2026-05-27

**Status**: Draft

**Input**: User description: "Phase 1 Spec - Dev Environment + CI/CD + Runnable System Skeleton"

## Clarifications

### Session 2026-05-27

- Q: What should the mobile app display when it cannot reach the backend? → A: "Connection Error" with retry prompt
- Q: How should missing runtime dependencies be surfaced to the developer? → A: Integrated startup check with clear error
- Q: What logging format should the backend use for Phase 1? → A: Structured JSON logging

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Developer environment bootstrap (Priority: P1)

A developer can clone the repository, set up the environment, and verify the
backend service is running within minutes. This covers the full local development
loop: clone, install dependencies, start services, and confirm they respond.

**Why this priority**: Without a working dev environment, no other development
can proceed. This is the foundational capability that unblocks the entire team.

**Independent Test**: A developer on a fresh machine can follow the README to
clone, run `cargo run -p core-service`, and see the health endpoint respond at
`/api/v1/health/live`.

**Acceptance Scenarios**:

1. **Given** a clean clone of the repository on a developer machine,
   **When** the developer runs the backend start command,
   **Then** the service starts on the expected port and responds to health checks.
2. **Given** the backend service is running,
   **When** a GET request is made to the liveness endpoint,
   **Then** a JSON response with status "alive" and service name is returned.
3. **Given** the backend service is running,
   **When** a GET request is made to the readiness endpoint,
   **Then** a JSON response with status "ready" and service name is returned.

---

### User Story 2 - Mobile-backend connectivity (Priority: P2)

A developer can launch both the backend and mobile frontend and observe
end-to-end connectivity. The mobile app displays the backend health status,
proving the integration surface works.

**Why this priority**: Validating that the mobile runtime can reach the backend
is the core integration checkpoint for the full stack.

**Independent Test**: Launch backend, launch Expo Go app, observe that the
app displays "Core Service: alive" without any manual configuration changes.

**Acceptance Scenarios**:

1. **Given** the backend service is running,
   **When** the mobile app is started with Expo Go,
   **Then** it fetches the backend health endpoint and displays the status.
2. **Given** the backend is unreachable or returns an error,
   **When** the mobile app attempts the health check,
   **Then** it displays a "Connection Error" message with a retry prompt.

---

### User Story 3 - Automated quality gates (Priority: P3)

Every pull request automatically runs linting, tests, and builds. This ensures
code quality is enforced before changes are merged.

**Why this priority**: Automated quality gates prevent regressions and maintain
codebase health as the team grows.

**Independent Test**: Open a PR, observe that CI pipelines (lint, test, build)
run automatically and report status back to the PR.

**Acceptance Scenarios**:

1. **Given** a pull request is opened,
   **When** CI triggers,
   **Then** linting passes for both backend and frontend code.
2. **Given** a pull request is opened,
   **When** CI triggers,
   **Then** all tests pass.
3. **Given** a pull request is opened,
   **When** CI triggers,
   **Then** the Docker build for core-service succeeds.

---

### Edge Cases

- Developer machine lacks required runtime (Rust, Node.js, pnpm): the backend
  startup command should validate prerequisites inline and report a clear error
  message naming the missing dependency.
- Port conflict when starting the backend: service should fail with a clear
  error message indicating the port is already in use.
- Mobile app attempts to connect while backend is starting: app should show a
  "Connection Error" message with a retry prompt rather than crashing.
- CI pipeline fails due to infrastructure issue (e.g., Docker not available):
  failures should distinguish between code issues and environment issues.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Developers MUST be able to start the backend service with a single
  command from the repository root.
- **FR-002**: The backend MUST expose a liveness endpoint that returns service
  status without checking dependencies.
- **FR-003**: The backend MUST expose a readiness endpoint that returns service
  status including dependency health (or confirmation that no dependencies are
  required).
- **FR-004**: Both health endpoints MUST return responses in a structured JSON
  format with status and service name fields.
- **FR-005**: The mobile app MUST be able to start with Expo Go and display the
  backend health status without manual code changes.
- **FR-006**: A shared types package MUST exist with foundational type
  definitions (station ID, user ID, partner ID, ID generation utility).
- **FR-007**: CI MUST run linting on every pull request for all code areas.
- **FR-008**: CI MUST run tests on every pull request for all code areas.
- **FR-009**: CI MUST build a Docker image of the backend service on every pull
  request.
- **FR-010**: The development environment MUST NOT require a database or
  external services to run.
- **FR-011**: Environment configuration MUST be documented in `.env.example`
  with secrets marked and excluded from version control.
- **FR-012**: The monorepo MUST support running backend and frontend
  independently without cross-contamination of dependencies.
- **FR-013**: Branch protection rules MUST require passing lint and test checks
  before pull requests can be merged.
- **FR-014**: The backend MUST output logs in structured JSON format with
  timestamp, level, message, and service fields.

### Key Entities *(include if feature involves data)*

- **StationId**: Unique identifier for EV charging stations, used across
  services for consistent referencing.
- **UserId**: Unique identifier for registered driver accounts.
- **PartnerId**: Unique identifier for infrastructure partner accounts.
- **Health Status**: Runtime status indicator for the backend service (alive,
  ready, error states).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A new developer can go from `git clone` to a running backend
  service with responding health endpoints in under 10 minutes.
- **SC-002**: A new developer can launch both backend and mobile app and confirm
  end-to-end connectivity in under 15 minutes total.
- **SC-003**: CI pipeline (lint + test + build) completes in under 5 minutes
  for the initial codebase.
- **SC-004**: The monorepo structure supports running backend and frontend
  independently without environment conflicts on macOS, Linux, and Windows.
- **SC-005**: All CI checks pass on the initial PR with zero configuration
  changes required after the initial setup.

## Assumptions

- Developers have Rust (stable toolchain) and Node.js (with pnpm) installed on
  their machines.
- The target development operating systems are macOS and Linux; Windows support
  via WSL is acceptable.
- No database or external services are required in Phase 1; all services are
  self-contained.
- Docker is available in CI but not required for local development.
- Expo Go on a physical device or emulator can reach the local backend via
  network.
- Environment configuration is minimal and documented in `.env.example` with
  sensible defaults.
- Team size during Phase 1 is small (1-3 developers), so complex multi-developer
  scenarios are not a concern.
