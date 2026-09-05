# Feature Specification: Infrastructure & Multi-Schema Engine

**Feature Branch**: `001-infra-multi-schema-engine`

**Created**: 2026-09-05

**Status**: Draft

**Input**: User description: "read docs/PLAN.md sprint 1"

## Clarifications

### Session 2026-09-05

- Q: How should the system signal that it is ready and healthy to a developer? → A: Health endpoint plus a startup readiness log line.
- Q: What should the application do when the automatic migrations fail during container startup? → A: Retry while the database is unavailable, then stop the application with a clear error if migrations ultimately fail.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Containerized Development Environment (Priority: P1)

As a developer, I want a containerized environment with an application service and a PostgreSQL database service so that I can start the entire system consistently with a single command, regardless of what is installed on my host machine.

**Why this priority**: Every later sprint — domain data models, API routers, and the seeding CLI — depends on a repeatable environment being up first. Without it, nothing else can be exercised or trusted.

**Independent Test**: On a machine with only the container runtime installed, run the documented environment startup command; the application service starts and reports a listening API, and the database service is healthy. This delivers a working baseline for every subsequent sprint.

**Acceptance Scenarios**:

1. **Given** a clean host with the container tooling available, **When** the developer runs the documented startup command, **Then** both the application and database services start and reach a healthy state without manual steps.
2. **Given** a running environment, **When** the developer restarts the application service alone, **Then** the persisted database data remains intact.
3. **Given** a running environment, **When** the developer tears it down and recreates it, **Then** it boots back to the same healthy baseline without code or configuration changes.

---

### User Story 2 - Multi-Schema Migration Framework (Priority: P1)

As a developer, I want database schema creation and versioning handled by an automated migration framework that covers every domain schema so that the database structure is reproducible from scratch, kept in sync across environments, and reviewable in version control.

**Why this priority**: Schema isolation is the project's core architectural guarantee (see docs/BRIEF.md and docs/ARCHITECTURE.md). Migrations are the only mechanism that makes that guarantee reproducible, so they must exist before any domain modeling begins.

**Independent Test**: From an empty database, apply the full migration history; all domain schemas and their version marker are created without error. This delivers reproducible schema management independent of any API or data code.

**Acceptance Scenarios**:

1. **Given** a fresh PostgreSQL instance with no application schemas, **When** the developer applies all migrations, **Then** every configured domain schema exists and the migration history is recorded as up to date.
2. **Given** an environment already at the latest migration, **When** the developer re-applies the migrations, **Then** the operation is a controlled no-op with no errors (idempotent).
3. **Given** a migration that creates schema objects, **When** it is reviewed in the repository, **Then** it appears as a versioned, ordered migration script and is inspectable.
4. **Given** an environment at an earlier revision, **When** `upgrade to latest` is run, **Then** only the pending revisions are applied to reach the current state.

---

### User Story 3 - Configurable Database Connectivity (Priority: P2)

As a developer, I want the application's database connection to be driven by environment configuration so that I can point the system at different database instances (local container, remote host, CI) without changing application code.

**Why this priority**: The connection and session layer built in Sprint 1 is consumed by every domain model afterwards, and configuration-driven connectivity keeps later environments (CI, staging demos) painless. It is lower priority than a bootable container and working migrations, but remains in Sprint 1.

**Independent Test**: Start the environment with a non-default database configuration value; the application connects to the intended target and migrations run against it. This delivers environment-agnostic connectivity on its own.

**Acceptance Scenarios**:

1. **Given** the documented default configuration, **When** the environment starts, **Then** the application connects to the local database using the default connection string.
2. **Given** an overridden database configuration, **When** the environment starts, **Then** the application uses the overridden value without any source code changes.
3. **Given** a connected application, **When** it performs database operations, **Then** it uses an asynchronous, pooled connection rather than a single blocking connection.
4. **Given** the application starts before the database is ready, **When** it attempts to connect, **Then** it retries within a bounded window and connects once the database becomes available; if the database never becomes available, it stops with a clear error (FR-014).

---

### Edge Cases

- What happens when the database service is not yet accepting connections when the application boots? The application retries while the database is unavailable, then stops with a clear error if migrations do not succeed (US3/AC4, FR-014).
- What happens when the environment is started on a host whose default port is already in use? The container runtime fails to bind and Docker Compose aborts with a clear port-binding error (e.g., "port is already allocated"); the environment is not left partially running.
- What happens when migrations are run against an empty database vs. a partially migrated one? (US2/AC1, AC2, AC4)
- How does the system behave on teardown and recreation when persistent data exists?
- What if a migration script is edited after it has already been applied? The startup runner surfaces the Alembic revision/checksum mismatch as a clear startup failure (FR-014) rather than silently continuing.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST provide a repeatable containerized environment consisting of at least an application service and a PostgreSQL service.
- **FR-002**: System MUST expose a single documented command that starts the full environment and a corresponding command that tears it down.
- **FR-003**: System MUST keep database data persistent across application service restarts.
- **FR-004**: System MUST provide a configuration-driven database connection with a sensible default for local development.
- **FR-005**: System MUST support overriding the database configuration through environment variables without code changes.
- **FR-006**: System MUST provide an asynchronous database engine with connection pooling and reusable session management for application code.
- **FR-007**: System MUST apply database migrations that create all six domain schemas on a fresh instance: `catalog`, `inventory`, `crm`, `usage`, `billing`, and a dedicated `dunning` schema.
- **FR-008**: System MUST record migration history in the repository as reviewable, versioned scripts.
- **FR-009**: System MUST support idempotent application of migrations (re-running at the latest revision is a no-op).
- **FR-010**: System MUST expose the application under the canonical `app` module path (e.g., `app.main`, `app.cli.seed`) for consistent run and import commands.
- **FR-011**: System MUST run baseline migrations automatically on container startup and validate that they succeed against PostgreSQL before the application is considered ready (failure handling per FR-014).
- **FR-012**: System MUST expose an HTTP health endpoint that reports application readiness and database connectivity.
- **FR-013**: System MUST emit a startup readiness log line once migrations succeed and the application is listening.
- **FR-014**: System MUST retry database connectivity during startup while the database is unavailable, and MUST stop the application with a clear error if migrations ultimately fail.

### Key Entities *(include if feature involves data)*

- **Domain Schemas**: The six PostgreSQL schemas that isolate bounded contexts: `catalog`, `inventory`, `crm`, `usage`, `billing`, and `dunning`. Creation and versioning of these schemas is Sprint 1's core deliverable.
- **Migration History**: The versioned, ordered set of schema changes that bring any environment's database in line with the repository head.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A developer can start the full environment with one documented command on a clean machine and confirm, via the HTTP health endpoint and the startup readiness log line, that the system is ready in under 5 minutes.
- **SC-002**: Migrations apply from a completely empty database to the latest state with zero errors and no manual intervention.
- **SC-003**: The repository contains a version-controlled migration history that any developer can read.
- **SC-004**: A developer can change the database target using only environment variables and observe the application connect to the new target in under 5 minutes.
- **SC-005**: Restarting the application service alone preserves all database data.
- **SC-006**: Applying migrations to an already-current environment completes quickly and produces no schema changes.

## Assumptions

- Developers have the container runtime and orchestration tooling available on their host machines.
- Python 3.12 and PostgreSQL 16 are the agreed runtime versions (per docs/ARCHITECTURE.md).
- The local development database requires no external secrets management in Phase 1.
- The project constitution is not yet ratified; no governance constraints apply to this specification.
- Out of scope for this feature, though enabled by it: domain data modeling, API routers, and the seeding CLI (later sprints).