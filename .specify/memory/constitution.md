# BorneMap Constitution

**Version**: 1.15.2
**Date**: 2026-06-21
**Status**: Active

## Preamble

BorneMap is a microservices platform for EV charging station management. This constitution defines the architectural rules, constraints, and enforcement mechanisms that ensure system integrity, security, and maintainability across all services.

## Core Principles

### 1. Service Topology Lock
- **Rule**: Exactly three microservices MUST exist: auth-service (3000), driver-service (3001), admin-service (3002)
- **Consequence**: No additional services, no port changes, no topology modifications
- **Enforcement**: Hard-stop CI gates on configuration validation

### 2. Identity Dual System
- **Rule**: Two independent identity systems: Keycloak UUID (human) and Platform nanoid(12) with PREFIX (business)
- **Rule**: NO mixing allowed - UUID in users table only, nanoid(12) with PREFIX in entity tables (STA/CHG/OPR/EVT)
- **Consequence**: Prevents data corruption and enforces business logic
- **Enforcement**: Static analysis validation

### 3. Data Ownership
- **Rule**: Every data domain has exactly one owning service
- **Rule**: Cross-service writes forbidden
- **Data Domains**:
  - users → auth-service
  - gis → driver-service
  - inventory → admin-service
  - analytics → driver-service (write only), admin-service (read only)
- **Enforcement**: Database roles + CI analytics gate validation

### 4. Contract-First
- **Rule**: Contract definition → Backend implementation → Frontend implementation
- **Rule**: domain-types MUST NOT depend on backend frameworks (actix-web, sqlx, tokio)
- **Consequence**: Independent teams can work on contracts and implementations in parallel
- **Enforcement**: Dependency validation with AST analysis

### 5. SQLx Compile-Time Verification
- **Rule**: All SQL queries MUST be compile-time verified via SQLx
- **Rule**: NO dynamic SQL construction
- **Consequence**: Compile-time detection of SQL errors before deployment
- **Enforcement**: CI sqlx_compile_check stage

### 6. CI Enforcement
- **Rule**: 9-stage CI pipeline with hard-stop on any failure
- **Rule**: NO partial success allowed
- **Stages**: format_check → type_check → dependency_graph_validation → identity_validation → schema_validation → sqlx_compile_check → analytics_write_gate → integration_tests → build_success
- **Enforcement**: Deterministic exit codes, artifact passing, no bypass

### 7. Forbidden Edges
- **Rule**: NO service→service imports, NO frontend→backend imports, NO shared-domain→services
- **Rule**: NO ui-kit→client-core, NO circular dependencies
- **Consequence**: Enforces bounded context boundaries
- **Enforcement**: AST-based dependency validation

### 8. Single-Writer Analytics
- **Rule**: driver-service ONLY can write to analytics_db
- **Rule**: admin-service and auth-service can ONLY read from analytics_db
- **Enforcement**: Database roles + CI analytics gate validation
- **Mechanism**: ADMIN → BUS → ADB (NOT direct write)

### 9. Runtime Topology Enforcement
- **Rule**: NO extra HTTP servers in worker crates
- **Rule**: NO service spawning in tests
- **Rule**: Port bindings locked (3000/3001/3002), NO drift
- **Enforcement**: CI runtime topology check

### 10. Migration Drift Detection
- **Rule**: Migration files MUST match compiled schemas
- **Rule**: NO schema divergence between migrations and code
- **Enforcement**: CI migration drift detection and schema hash validation

### 11. Identity Location Rules
- **Rule**: UUID MUST ONLY appear in users table and Keycloak mapping layer
- **Rule**: UUID MUST NOT appear in any other entity tables
- **Rule**: nanoid(12) MUST use PREFIX (STA/CHG/OPR/EVT) in entity tables
- **Enforcement**: Static analysis validation

## Service Boundaries

### auth-service (Port 3000)
- **Owned Data**: users schema
- **Functions**: Authentication, user profile management, Keycloak integration
- **No Access**: gis, inventory, analytics (read-only for analytics via BUS)

### driver-service (Port 3001)
- **Owned Data**: gis schema (OSM staging, curated stations, spatial functions), analytics_db (write-only)
- **Functions**: GIS operations, telemetry ingestion, analytics write, OSM ETL
- **No Access**: users (read-only via Keycloak), inventory (read-only for nearby search)

### admin-service (Port 3002)
- **Owned Data**: inventory schema, analytics_db (read-only)
- **Functions**: Station CRUD, charger CRUD, partner management, inventory sync
- **No Access**: users (read-only via Keycloak), gis (write-only via events), analytics (write-only via BUS)

## Identity System

### Keycloak UUID (Human)
- **Purpose**: User-facing identifiers (emails, IDs for support)
- **Format**: UUID v4
- **Table**: users.user_id
- **Scope**: ONLY users table and Keycloak mapping layer

### Platform nanoid(12) with PREFIX (Business)
- **Purpose**: Internal business entity identifiers
- **Format**: PREFIX + 12 alphanumeric characters
- **Prefixes**:
  - STA - Station (inventory.stations)
  - CHG - Charger (inventory.chargers)
  - CON - Connector (inventory.connectors)
  - PRT - Partner (inventory.partners)
  - EVT - Event (raw_events table)
- **Scope**: Entity tables ONLY, NO user table

## Enforcement Mechanisms

### CI Pipeline (9 Stages)
1. format_check - Cargo fmt verification
2. type_check - Cargo clippy linting
3. dependency_graph_validation - AST-based forbidden edge detection
4. identity_validation - UUID/nanoid usage validation
5. schema_validation - Database schema consistency
6. sqlx_compile_check - SQLx offline verification
7. analytics_write_gate - Single-writer analytics enforcement
8. integration_tests - cargo test execution
9. build_success - cargo build verification

### Static Analysis
- **Dependency validation**: Syn-based AST parsing
- **Identity validation**: Regex + context-aware pattern matching
- **Runtime topology**: Service spawning detection, port binding enforcement

### Database Enforcement
- PostgreSQL roles: bornemap_admin, bornemap_driver, bornemap_auth, bornemap_analytics_writer, bornemap_analytics_reader
- Schema ownership: Explicit GRANT statements
- Single-writer enforcement: ROW-level security (optional, future)

## Compliance Process

1. **Design Phase**: All designs must pass constitution check gates before implementation
2. **Implementation Phase**: CI enforcement prevents violations from reaching production
3. **Review Phase**: All code changes must pass constitution validation
4. **Audit Phase**: Regular compliance audits for data ownership, identity separation, etc.

## Violation Handling

1. **CI Violation**: Hard-stop, required fix before merge
2. **Design Violation**: Rejection, redesign required
3. **Runtime Violation**: Immediate rollback, fix required
4. **Severity Levels**:
   - **Critical**: Service topology, identity separation, data ownership
   - **High**: Forbidden edges, contract-first, single-writer analytics
   - **Medium**: SQLx policy, CI enforcement, migration drift
   - **Low**: Documentation, code style, formatting

## Amendment Process

1. **Proposal**: Formal proposal with justification and impact analysis
2. **Stakeholder Review**: All team members must review and approve
3. **Version Bump**: Increment version number
4. **Migration Path**: Clear path for existing code to adapt

## Version History

- **v1.15.2** (2026-06-21): Updated with runtime topology enforcement, migration drift detection, identity location rules
- **v1.15.1** (2026-06-20): Updated with enforcement kernel enhancements
- **v1.15.0** (2026-06-19): Initial constitution for BorneMap project
