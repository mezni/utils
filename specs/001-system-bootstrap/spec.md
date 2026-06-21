# Feature Specification: System Bootstrap & Enforcement Kernel

**Feature Branch**: `001-system-bootstrap`

**Created**: 2026-06-21

**Status**: Draft

**Input**: User description: "Initialize BorneMap project with monorepo structure, CI enforcement pipeline, database schemas, and service skeletons"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Monorepo Initialization (Priority: P1)

Developers clone the repository and see a complete project structure with all necessary directories, packages, and configuration files in place, enabling immediate development work.

**Why this priority**: The monorepo is the foundation for all development work. Without proper structure, developers cannot proceed with any features.

**Independent Test**: Navigate to the repository root, verify the directory structure matches the specification, and confirm all expected files exist.

**Acceptance Scenarios**:

1. **Given** a fresh clone of the repository, **When** the developer runs `ls`, **Then** they see directories: `apps/packages`, `services`, `tools`, `infrastructure`, `docs`, `spec`
2. **Given** a fresh clone, **When** the developer runs `ls apps/packages`, **Then** they see `ui-kit`, `domain-types`, `client-core`
3. **Given** a fresh clone, **When** the developer runs `ls services`, **Then** they see `auth-service`, `driver-service`, `admin-service`
4. **Given** a fresh clone, **When** the developer runs `cat .github/workflows/ci.yml`, **Then** the CI pipeline exists with 9 stages

---

### User Story 2 - CI Enforcement Pipeline (Priority: P1)

The CI pipeline executes 9 mandatory stages with hard-stop on any failure, enforcing code quality, dependencies, identity formats, and SQLx compile-time verification.

**Why this priority**: CI enforcement is critical for maintaining architectural integrity and preventing violations of the constitution.

**Independent Test**: Run `make ci` or `./tools/ci_guard.sh` and verify all 9 stages pass without any failures.

**Acceptance Scenarios**:

1. **Given** a push to main, **When** the CI pipeline runs, **Then** it executes stages: format_check → type_check → dependency_graph_validation → identity_validation → schema_validation → sqlx_compile_check → analytics_write_gate → integration_tests → build_success
2. **Given** a schema validation failure, **When** the CI pipeline runs, **Then** it hard-stops at stage 5 and reports the error
3. **Given** a SQLx failure, **When** the CI pipeline runs, **Then** it hard-stops at stage 6 and reports the error
4. **Given** a successful pipeline run, **When** the developer checks the summary, **Then** all 9 stages show "passed" status

---

### User Story 3 - Database Schemas Bootstrapped (Priority: P1)

All three databases (platform_db, analytics_db, keycloak_db) are initialized with proper schema definitions, migrations, and verification scripts.

**Why this priority**: Database schemas are the foundation for all data storage. Without them, no application data can be stored.

**Independent Test**: Connect to each database and verify all tables and constraints exist.

**Acceptance Scenarios**:

1. **Given** a database connection to platform_db, **When** running schema verification, **Then** tables exist: users, gis, inventory (inventory is a schema within platform_db owned by admin-service)
2. **Given** a database connection to analytics_db, **When** running schema verification, **Then** tables exist: raw_events (append-only event log as primary model), with optional derived tables
3. **Given** a database connection to keycloak_db, **When** inspecting the database, **Then** it contains Keycloak tables (not application data), and auth-service cannot write directly to keycloak_db schema
4. **Given** a schema verification script, **When** executed, **Then** it reports all tables created successfully
5. **Given** an inventory CRUD operation, **When** a station is created/updated/deleted, **Then** an event is emitted to trigger GIS sync via event bus

---

### User Story 4 - Service Skeletons Created (Priority: P1)

Three microservices (auth-service, driver-service, admin-service) are created with basic structure, health endpoints, and proper configuration files.

**Why this priority**: Service skeletons enable parallel development across services without blocking each other.

**Independent Test**: Start each service and verify health endpoints respond correctly.

**Acceptance Scenarios**:

1. **Given** the auth-service skeleton, **When** running `cargo run --bin auth-service`, **Then** it responds to GET /health with status 200 and JSON body { "status": "ok", "timestamp": "2026-06-21T12:00:00Z", "service": "auth-service" }
2. **Given** the driver-service skeleton, **When** running `cargo run --bin driver-service`, **Then** it responds to GET /health with status 200 and JSON body { "status": "ok", "timestamp": "2026-06-21T12:00:00Z", "service": "driver-service" }
3. **Given** the admin-service skeleton, **When** running `cargo run --bin admin-service`, **Then** it responds to GET /health with status 200 and JSON body { "status": "ok", "timestamp": "2026-06-21T12:00:00Z", "service": "admin-service" }
4. **Given** any service skeleton, **When** inspecting the directory, **Then** it contains Cargo.toml, main.rs, and configuration files
5. **Given** any service, **When** checking configuration, **Then** it has hard-coded port binding: auth-service on 3000, driver-service on 3001, admin-service on 3002

---

### User Story 5 - SpecKit Compliance (Priority: P1)

All documentation follows SpecKit standards with proper enforcement layers, plan/workflow structure, and versioning.

**Why this priority**: SpecKit provides a structured approach to feature development and ensures traceability across the project.

**Independent Test**: Verify all SpecKit markers are present in documentation and no violations exist.

**Acceptance Scenarios**:

1. **Given** any feature specification, **When** inspecting the file, **Then** it contains SpecKit start/end markers
2. **Given** any implementation plan, **When** inspecting the file, **Then** it contains the constitution check gate
3. **Given** any plan document, **When** checking compliance, **Then** all mandatory sections are present
4. **Given** the project configuration, **When** inspecting `.specify/extensions.yml`, **Then** all hooks are properly configured
5. **Given** the Rust workspace configuration, **When** inspecting root Cargo.toml, **Then** it explicitly maps all services and packages (ui-kit, domain-types, client-core, auth-service, driver-service, admin-service)
6. **Given** any Rust package, **When** inspecting domain-types, **Then** it contains only serde types, event schemas, and no backend framework dependencies (actix-web, sqlx, tokio)
7. **Given** an inventory CRUD operation, **When** inspecting the implementation, **Then** it emits events to trigger GIS sync via event bus (NOT synchronous triggers)

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST create a monorepo structure with packages: `apps/packages/ui-kit`, `apps/packages/domain-types`, `apps/packages/client-core`
- **FR-002**: System MUST create three microservices: `auth-service`, `driver-service`, `admin-service` in `services/`
- **FR-003**: System MUST create validation tools: 06 tools scripts (01_validate_identity.sh, 02_validate_deps.sh, 03_validate_analytics_gate.sh, 04_validate_schema.sh, 05_sqlx_policy_check.sh, 06_ci_guard_final.sh)
- **FR-004**: System MUST implement CI pipeline with exactly 9 stages: format_check, type_check, dependency_graph_validation, identity_validation, schema_validation, sqlx_compile_check, analytics_write_gate, integration_tests, build_success
- **FR-005**: System MUST enforce hard-stop on any CI stage failure
- **FR-006**: System MUST bootstrap platform_db with three schemas: users, gis, inventory
- **FR-007**: System MUST bootstrap analytics_db with three schemas: telemetry_events, analytics_events, system_events
- **FR-008**: System MUST bootstrap keycloak_db with Keycloak tables (no application data)
- **FR-009**: System MUST create service skeletons with health endpoints: GET /health on port 3000, 3001, 3002
- **FR-010**: System MUST implement SQLx offline verification with `cargo sqlx prepare --check`
- **FR-011**: System MUST create infrastructure scripts: provision_db.sh, deploy.sh, migrate.sh
- **FR-012**: System MUST create a Keycloak realm export file for future use
- **FR-013**: System MUST set up Redis configuration
- **FR-014**: System MUST define a Rust workspace root Cargo.toml mapping all services and packages explicitly
- **FR-015**: System MUST enforce domain-types isolation rule: domain-types MUST NOT depend on any backend framework (actix-web, sqlx, tokio)
- **FR-016**: System MUST provide deterministic CI entrypoint via `make ci` or `./tools/ci_guard.sh`
- **FR-017**: System MUST enforce inventory data domain separation: inventory is a schema within platform_db owned by admin-service, not a standalone service
- **FR-018**: System MUST enforce analytics write gate: admin-service can only read from analytics_db, must go through BUS → GIS worker → events for writes
- **FR-019**: System MUST define CI stage outputs with JSON schema validation requirement
- **FR-020**: System MUST enforce health endpoint schema: GET /health MUST return JSON { "status": "ok", "timestamp": ISO8601, "service": "service-name" }
- **FR-021**: System MUST enforce port binding: auth-service MUST bind to port 3000, driver-service to 3001, admin-service to 3002, hard-coded in configuration files
- **FR-022**: System MUST define event propagation model: inventory CRUD → emits event → event bus → GIS worker → updates GIS schema (event-driven, NOT synchronous triggers)
- **FR-023**: System MUST enforce analytics_db schema consistency: analytics_db MUST contain raw_events (append-only event log) as primary model, with derived tables optional
- **FR-024**: System MUST enforce keycloak_db ownership: auth-service MUST NOT directly write to keycloak_db schema, only via Keycloak admin API

### Key Entities *(include if feature involves data)*

- **Repository Structure**: Directory tree containing packages, services, tools, infrastructure, docs, and spec directories
- **CI Pipeline**: 9-stage workflow with hard-stop on failure
- **Database Schemas**: Platform DB (users, gis, inventory - inventory is a schema within platform_db owned by admin-service), Analytics DB (raw_events append-only event log as primary model, with optional derived tables), Keycloak DB (Keycloak tables, auth-service cannot write directly)
- **Service Skeletons**: Three Rust microservices with health endpoints returning JSON { "status": "ok", "timestamp": ISO8601, "service": "service-name" }
- **Validation Tools**: Shell scripts for identity validation, dependency validation, analytics gate validation, schema validation, SQLx policy check
- **Rust Workspace**: Explicit Cargo.toml root workspace mapping all services and packages
- **Domain-Types Isolation**: Shared contracts package with no backend framework dependencies
- **Event Propagation Model**: Inventory CRUD → event emission → event bus → GIS worker → GIS schema updates (event-driven, not synchronous triggers)
- **CI Entrypoint**: Deterministic entrypoint via `make ci` or `./tools/ci_guard.sh`
- **Port Binding**: Hard-coded port assignments (auth: 3000, driver: 3001, admin: 3002)

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Repository clone completes with 100% directory structure matching specification
- **SC-002**: CI pipeline executes all 9 stages successfully on every push
- **SC-003**: All three database schemas are created and verified without errors
- **SC-004**: All three service skeletons respond to health endpoint with status 200
- **SC-005**: SpecKit compliance verification passes with no violations

## Assumptions

- Target development language is Rust (based on constitution and service skeleton requirements)
- Target platform is Linux server for services, web for frontend applications
- Database systems: PostgreSQL for platform_db and analytics_db, Keycloak DB (PostgreSQL) for identity
- CI platform is GitHub Actions (based on .github/workflows/ci.yml)
- Testing framework is cargo test
- Containerization not required for MVP (optional infrastructure/docker-compose)
- Redis is not required for MVP (optional infrastructure/redis)
