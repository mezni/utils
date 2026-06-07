# Feature Specification: Monorepo and CI/CD Setup

**Feature Branch**: `001-monorepo-ci-cd`

**Created**: 2026-06-07

**Status**: Draft

**Input**: User description: "from your context and docs/* files and docs/planning/roadmap.md read sprint 1.1"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Initialize Monorepo Workspace (Priority: P1)

A developer clones the repository and runs the respective build commands. Both Rust and JavaScript/TypeScript projects compile without errors, and shared crates are accessible to all services.

**Why this priority**: The monorepo workspace is the foundation that every other component depends on. Without it, no backend service or frontend application can be built or tested.

**Independent Test**: Can be fully tested by running `cargo build --all` and `npm install` from the repository root — both must complete without errors.

**Acceptance Scenarios**:

1. **Given** a fresh clone of the repository, **When** the developer runs `cargo build --all`, **Then** all Rust crates and services compile without errors
2. **Given** a fresh clone of the repository, **When** the developer runs `npm install`, **Then** all JS/TS dependencies are installed without errors
3. **Given** the Rust workspace is configured, **When** the developer runs `cargo test -p ev-core`, **Then** NanoID generation tests pass (prefixes are correct, IDs are unique)

---

### User Story 2 - Set Up CI/CD Pipelines (Priority: P1)

A developer pushes code changes to any branch. GitHub Actions automatically run the relevant checks — Rust linting and tests for backend changes, frontend linting and builds for JS/TS changes. The developer sees pass/fail status within minutes.

**Why this priority**: Without CI, the team has no automated quality gates. Code can break silently, and integration issues are discovered too late.

**Independent Test**: Can be fully tested by pushing a trivial change to each path-scoped directory and verifying the corresponding workflow triggers. A breaking change (e.g., clippy violation) must cause the relevant workflow to fail.

**Acceptance Scenarios**:

1. **Given** a Rust crate change is pushed, **When** CI runs, **Then** the path-scoped workflow for that crate triggers and runs `cargo fmt --check`, `cargo clippy`, and `cargo test`
2. **Given** a frontend package change is pushed, **When** CI runs, **Then** the path-scoped workflow triggers and runs `npm lint` and `npm build`
3. **Given** a push that introduces a clippy warning, **When** CI runs, **Then** the Rust workflow fails and reports the warning

---

### User Story 3 - Configure Local Development Environment (Priority: P2)

A developer sets up their local environment using Docker Compose and environment file examples. PostgreSQL starts, and the service containers are healthy.

**Why this priority**: While development can start with manual database setup, Docker Compose provides a reproducible environment that eliminates "works on my machine" issues.

**Independent Test**: Can be fully tested by running `docker compose -f infra/compose/docker-compose.yml up -d` and verifying all containers are healthy.

**Acceptance Scenarios**:

1. **Given** Docker Compose is configured, **When** the developer runs `docker compose up -d`, **Then** PostgreSQL starts and passes its health check
2. **Given** environment example files exist, **When** the developer copies them to `.env`, **Then** the service configuration is valid (required variables are documented)

---

### Edge Cases

- Missing system dependencies: When Rust, npm, or Docker are not installed, the developer must see clear installation instructions in the onboarding guide
- Network failure during `npm install`: Must not leave the workspace in a partially configured state — a second attempt must work
- CI workflow triggers on unrelated changes: Path filters must ensure backend CI does not run on frontend-only changes and vice versa

## Out of Scope

The following items are explicitly excluded from Sprint 1.1 and will be addressed in later sprints:

- **Keycloak / authentication setup** — Sprint 2.x (Phase 2)
- **GIS sync trigger** — Sprint 6.x (Phase 6)
- **Clickstream Service** — Sprint 5.x (Phase 5)
- **Mobile app deployment (TestFlight / Play Store)** — Post-Phase 1
- **Production TLS / domain configuration** — Sprint 1.6 (hardening) or Phase 2
- **Database migrations beyond the scaffold structure** — Sprint 1.2

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The repository MUST have a Cargo workspace at the root that includes all Rust crates and services
- **FR-002**: The Cargo workspace MUST declare shared dependency versions so all crates use the same library versions
- **FR-003**: The repository MUST have a npm workspace at the root that includes all JS/TS apps and packages
- **FR-004**: The npm workspace MUST define root-level scripts for building, testing, and linting all apps
- **FR-005**: A shared `tsconfig.base.json` MUST provide base TypeScript configuration inherited by all apps
- **FR-006**: The `ev-core` crate MUST provide NanoID generation functions for all entity types (USR, PRT, STN, CHG, REV, EVT)
- **FR-007**: The `ev-core` crate MUST export shared enums for connector types and charger statuses
- **FR-008**: The `ev-db` crate MUST provide a PostgreSQL connection pool factory and pagination data structures
- **FR-009**: Six CI/CD workflow files MUST exist in `.github/workflows/` covering the full workspace and each path-scoped component
- **FR-010**: The full workspace CI workflow MUST run Rust linting (fmt, clippy) and testing on every push
- **FR-011**: The full workspace CI workflow MUST run frontend linting and building on every push
- **FR-012**: Path-scoped CI workflows MUST only trigger when files in their scope change
- **FR-013**: Path-scoped CI workflows for Rust services MUST include a PostgreSQL service container for integration tests
- **FR-014**: Environment variable example files MUST exist in `infra/env/` documenting all required variables per service
- **FR-015**: Two Docker Compose files MUST exist in `infra/compose/`: `docker-compose.yml` (development, includes pgadmin) and `docker-compose.prod.yml` (production, no pgadmin). Both define postgres and Rust service placeholders.
- **FR-016**: A `.gitignore` MUST exist at the repository root excluding build artifacts, dependencies, and IDE files
- **FR-017**: A `.dockerignore` MUST exist at the repository root excluding files not needed in Docker builds
- **FR-018**: All CI workflows MUST configure npm dependency caching via `actions/cache` targeting `~/.npm` to keep install times under 2 minutes

### Key Entities *(include if feature involves data)*

- **Rust Workspace**: Collection of crates and services sharing dependency versions via `Cargo.toml` at the repository root
- **npm Workspace**: Collection of apps and packages sharing dependency management via `package.json#workspaces`
- **CI Workflow**: GitHub Actions workflow file defining automated checks triggered by pushes and pull requests
- **Shared Crate**: Reusable Rust library (ev-core for NanoIDs and types, ev-db for database utilities) consumed by all services

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: `cargo build --all` completes in under 5 minutes on a fresh CI runner
- **SC-002**: `npm install` completes in under 2 minutes on a fresh CI runner
- **SC-003**: All six CI workflows complete within 10 minutes of a push
- **SC-004**: A path-scoped CI workflow for a Rust service runs in under 8 minutes including PostgreSQL container startup
- **SC-005**: New developers can set up and verify their environment using only the onboarding guide, without asking for help, in under 30 minutes

## Clarifications

### Session 2026-06-07

- Q: How should Docker Compose handle dev vs production differences? → A: Two files — `docker-compose.yml` (dev, includes pgadmin) and `docker-compose.prod.yml` (prod, no pgadmin).
- Q: Should the CI workflows use npm dependency caching? → A: Use `actions/cache` for `~/.npm` to meet the 2-minute install target.
- Q: What items should be explicitly declared out of scope for Sprint 1.1? → A: Standard deferred items: Keycloak/auth, GIS sync trigger, Clickstream Service, mobile app deployment, production TLS.

## Assumptions

- All developers have Rust toolchain (1.95+), Node.js (20.20+), npm (10.8+), and Docker (24+) installed
- The CI environment (GitHub Actions ubuntu-latest) has all required tools pre-installed or cached
- The PostgreSQL test container image (postgis/postgis:16-3.4) is publicly available and does not require authentication
- The repository remote (origin) is already configured — this sprint does not cover initial git push
- All six workflows are separate files — CI configuration does not need to be consolidated or templated
