<!--
  Sync Impact Report
  Version change: (template) → 1.0.0
  Modified principles: N/A (initial fill from template)
  Added sections: Core Principles (5 principles), Technology Stack & Constraints,
    Development Workflow, Governance
  Removed sections: None
  Templates requiring updates:
    - .specify/templates/spec-template.md: ✅ no changes needed
    - .specify/templates/plan-template.md: ⚠ plan-template has "Constitution Check"
      gate — ensure it cross-references principles by name
    - .specify/templates/tasks-template.md: ✅ no changes needed
  Follow-up TODOs: None
-->

# BorneMap Constitution

## Core Principles

### I. Service Ownership & Data Isolation

Every service owns exactly one database schema. No service may write directly to
another service's schema. Cross-service reads are permitted only via dedicated
read-only database roles and exclusively against materialized views. The
three-service topology (auth-service, driver-service, admin-service) is frozen —
no fourth service may be added during the validation phase.

**Rationale**: Schema-level isolation prevents accidental data corruption across
service boundaries and keeps each service independently deployable.

### II. Spatial-First Design

All geospatial queries MUST target materialized views, never base tables. GiST
indexing is mandatory on all spatial columns. PostGIS is the single source of
truth for all location data. The materialized view refresh strategy must
guarantee read consistency without blocking concurrent queries.

**Rationale**: EV charging discovery is fundamentally a spatial problem. Every
architectural decision starts from the geospatial access pattern.

### III. Idempotent Data Operations

All data ingestion and synchronization operations MUST be idempotent. Running
the same import twice MUST produce identical state with zero duplicate records.
Updates MUST overwrite safely via versioning or upsert patterns. Every sync
operation MUST be recorded in a sync_jobs audit trail.

**Rationale**: External geospatial sources (OSM, OCPI, etc.) may be re-imported
multiple times. Non-idempotent ingestion would corrupt the inventory over time.

### IV. Strict Entity Hierarchy

Partner → Station → Charger → Connector hierarchy is non-negotiable. Foreign key
enforcement MUST be strict across every level of the hierarchy. No dangling
connectors or chargers may exist. All record identifiers MUST use the typed
prefix + nanoid(12) format (e.g., PAR-, STA-, CHR-, CON-).

**Rationale**: EV charging infrastructure follows a clear physical hierarchy.
Enforcing it at the database level prevents orphan records and guarantees data
integrity.

### V. Observability & Audit

All ingestion and sync operations MUST be logged via sync_jobs. Query latency
tracking is required for the driver-service. Error capture is mandatory for all
ingestion failures. Structured logging MUST be used across all services. No
observability data may be treated as optional or best-effort.

**Rationale**: A geospatial platform without observability is blind to
performance degradation and data quality issues that directly impact user
experience.

## Technology Stack & Constraints

PostgreSQL 16 with PostGIS is mandatory for the primary database. Redis is
optional for caching (owned by driver-service). Backend services MUST use Rust
1.85+. Frontend applications MUST use Node.js 22+. Keycloak 25+ is required for
identity and access management. Traefik 3+ is the API gateway. All local
development MUST use Docker Compose with deterministic schema initialization on
startup. No dependency on external users schema — the identity model is
self-contained.

## Development Workflow

Features follow a spec-first workflow: specification → plan → tasks →
implementation. All pull requests MUST reference the originating spec
requirements. Tests MUST be written and confirmed failing before implementation
code is written. Commits MUST occur after each logical group of changes. The
constitution, ADRs, and architecture docs MUST be kept in sync with any
principle-altering decisions.

## Governance

This constitution supersedes all other development practices. Amendments require
an Architecture Decision Record (ADR) and team approval. Versioning follows
semantic versioning (MAJOR.MINOR.PATCH). A compliance review MUST be performed
at the end of each sprint to verify all principles are upheld. Violations of
non-negotiable principles (Service Ownership, Spatial-First, Idempotent
Operations, Strict Hierarchy) require immediate remediation before the next
sprint begins.

**Version**: 1.0.0 | **Ratified**: 2026-06-20 | **Last Amended**: 2026-06-20
