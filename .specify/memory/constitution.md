<!--
## Sync Impact Report
**Version Change**: Initial → 1.0.0
**Modified Principles**: N/A (initial version)
**Added Sections**: All sections
**Removed Sections**: N/A
**Templates Requiring Updates**:
  ✅ plan-template.md - Reviewed, constitution check aligned
  ✅ spec-template.md - Reviewed, requirements structure aligned
  ✅ tasks-template.md - Reviewed, task categorization aligned
**Follow-up TODOs**: None
-->

# BorneMap Platform Constitution

## Core Principles

### I. Pragmatic Architecture
System architecture MUST use the minimum number of services that correctly separate responsibilities. Every new service MUST be justified via an approved Architecture Decision Record (ADR) proving no existing service can own the responsibility. Services are added only when scale, complexity, or operational boundaries justify separation.

**Rationale**: Pragmatic architecture minimizes operational complexity, reduces operational burden, and keeps the platform maintainable for a single operator.

### II. Single Source of Truth
Every entity has exactly one authoritative database table. All other representations (derived tables, cached data, derived layers) MUST be read-only. All writes MUST flow through the authoritative owner. Cross-schema access MUST be explicitly permitted and documented.

**Rationale**: Eliminates data inconsistency, simplifies data flow, reduces debugging complexity, and enforces clear ownership.

### III. Simple Operations
The platform MUST be operable by a single person. Infrastructure and operations MUST require minimal tooling. Every operational procedure MUST have a documented runbook. Automated deployment, monitoring, and health checks MUST be maintained.

**Rationale**: Operational simplicity reduces cost, increases reliability, and enables quick problem resolution. One-person operability maximizes flexibility.

### IV. Domain Separation by Schema
Business logic (stations, partners), users (profiles, favorites, reviews), GIS (spatial enrichment, OSM data), and analytics (events, aggregates) MUST be separated into distinct database schemas. Cross-schema communication MUST be explicit and controlled. Services MUST write to only their owned schema.

**Rationale**: Schema separation enforces clear boundaries, prevents schema pollution, simplifies migrations, and enables independent evolution.

### V. Build for Current Scale
The platform MUST target current needs first, not future scale. No premature optimization. Resources MUST be allocated to features that deliver immediate value. Infrastructure complexity MUST grow only when justified by actual demand and operational complexity.

**Rationale**: Builds valuable functionality faster, reduces technical debt, and avoids over-engineering. Complexity is added incrementally when needed.

### VI. Public Access First
Anonymous browsing MUST always work. No feature MUST be gated behind authentication unless the feature specifically requires user identity (e.g., favorites, profile management). Authentication MUST be an upgrade path, not a barrier to entry.

**Rationale**: Maximizes user engagement, lowers friction, and captures maximum user base immediately. Features that don't require login MUST remain accessible.

### VII. RTL & Arabic Built-In
Arabic language support with Right-to-Left layout MUST work correctly on every screen, in every application, without workarounds. RTL failures are Class A bugs. Language support MUST be planned from the start, not added as an afterthought.

**Rationale**: Arabic is a primary language for the target market (Tunisia). RTL support is a non-negotiable requirement that should never break or require special handling.

### VIII. Visual Consistency
All visual values (colors, spacing, typography, shadows, radius) MUST be defined in design tokens. Hardcoding visual values MUST be prohibited. All applications MUST consume tokens from the shared `packages/ui` package.

**Rationale**: Ensures visual consistency across web, mobile, and dashboard applications. Token system provides single source of truth for design.

## Non-Negotiable Rules

The following rules MUST NEVER be violated:

- **inventory.station** is the authoritative source of truth for station data
- **Public access** to station discovery never requires authentication
- **Tokens** (JWTs) MUST NOT be stored in localStorage or AsyncStorage
- **Arabic RTL** layout MUST work correctly on every screen in every application
- **Only Traefik** exposes public network ports
- **Keycloak** owns all authentication and authorization
- **No additional services** can be added without an approved ADR
- **Cross-schema access** beyond explicitly permitted rules is prohibited

## Additional Constraints

### Schema Ownership
- **inventory schema**: Owned by Admin Service. Write only. Migrations owned by Admin Service.
- **users schema**: Owned by Driver Service. Write only. Migrations owned by Driver Service.
- **gis schema**: Owned by triggers. Write only. Read by Driver Service for spatial queries.
- **analytics schema**: Owned by Clickstream Service. Write only. Migrations owned by Clickstream Service.

### Service Responsibilities
- **Keycloak**: Authentication, JWT issuance, role management, OAuth2 providers
- **Driver Service**: Public discovery, authenticated driver features (favorites, reviews, profiles)
- **Admin Service**: Partner and station management, manual availability updates, review moderation
- **Clickstream Service**: Analytics event ingestion, validation, persistence
- **Traefik**: Edge routing, TLS termination, rate limiting, public port exposure (80, 443)

### Authentication & Authorization
- **Public drivers**: Implicit role, no JWT required for discovery
- **Registered drivers**: Keycloak role `registered_driver`, JWT with user claims
- **Partners**: Keycloak role `partner`, JWT with `partner_id` claim, scoped to own resources
- **Admins**: Keycloak role `admin`, full platform access

### Database Constraints
- **PostgreSQL 16** with **PostGIS** extension
- **NanoID** (prefixes: USR-, STN-, CHG-, REV-, PRT-, EVT-)
- **Migration strategy**: Versioned migrations, run on service startup
- **Cross-schema access**: Permitted access documented in constitution section IX

### Frontend Constraints
- **Design tokens**: All visual values from `packages/ui/src/tokens/`
- **Typography**: Plus Jakarta Sans (web/mobile), Inter (dashboard)
- **Accessibility**: WCAG 2.1 AA minimum for all web applications
- **Localization**: Arabic (RTL), French, English - mandatory for all screens

### Operational Constraints
- **Deployment**: Manual following documented runbook. No automated CI/CD pipelines.
- **Health checks**: All services MUST implement health check endpoints
- **Environment configuration**: Stored in host-managed `.env` files, never committed
- **Secrets**: Stored on host only, never in images or committed files
- **Docker Compose**: Production uses `docker-compose.prod.yml`, dev uses `docker-compose.yml`

## Development Workflow

### Sprint Planning
- Sprints are 2 weeks (hardening sprints are 1 week)
- Each phase contains multiple sprints
- Phases build in layers: UI → Data → Services → Integration → Infrastructure
- Nothing is thrown away between phases. Mock data is replaced, not rebuilt
- Each phase ends with a hardening sprint that must pass before the next phase begins

### Phase Structure
- **Phase 1**: Four apps with mock data (UI only, no backend)
- **Phase 2**: Database foundation (all schemas, OSM data, seeds)
- **Phase 3**: Backend services (services running, endpoints tested)
- **Phase 4**: Authentication & user management (Keycloak, JWT, auth flows)
- **Phase 5**: Connect apps to services (mock replaced with real data)
- **Phase 6**: GIS synchronization (trigger-based sync, spatial queries)
- **Phase 7**: Clickstream analytics (events tracked, aggregates, reporting)
- **Phase 8**: Traefik production runtime (TLS, routing, production Compose)
- **Phase 9**: Features, hardening, launch readiness (reviews, favorites, profile, hardened)

### Definition of Done
A task is complete when:
- ✅ Code is implemented and passes all tests
- ✅ No Class A bugs remain (Class A = blocks correctness, security, or access)
- ✅ Documentation is updated to reflect changes
- ✅ All linting and formatting rules are satisfied
- ✅ Code review is complete with approval
- ✅ All components pass integration tests
- ✅ Arabic RTL layout is verified in Arabic language
- ✅ Accessibility compliance is verified (for web apps)

### Code Review Requirements
- All changes MUST be reviewed by at least one other developer
- Reviewers MUST verify:
  - Compliance with core principles
  - No Class A bugs introduced
  - Follows existing code style
  - Appropriate error handling
  - Documentation updates included
  - No security issues (secret handling, auth, input validation)

### Testing Requirements
- **Class A bugs**: Block correctness, security, or user access. MUST be resolved before phase closes.
- **Class B bugs**: Degrade quality but don't block. SHOULD be resolved before phase closes.
- **Class C bugs**: Nice-to-have improvements. No mandatory phase target.

Testing is OPTIONAL unless explicitly requested in the feature specification.

## Governance

### Amendment Procedure
Any amendment to this constitution MUST:
1. Be documented in a separate amendment file
2. Specify the changed principles and rationale
3. Propose a version bump according to semantic versioning rules
4. Reference related ADRs or decisions
5. Outline migration impact on existing code and documentation
6. Be approved by the project stakeholders

### Versioning Policy
- **MAJOR**: Backward incompatible governance changes or principle removals
- **MINOR**: New principles or sections added, or material expansions
- **PATCH**: Clarifications, wording improvements, non-semantic refinements

### Compliance Review
All Pull Requests MUST be reviewed for:
- Constitution compliance verification
- Class A bug identification
- Architectural decisions alignment
- Schema ownership enforcement
- Authentication and authorization correctness
- RTL and accessibility compliance
- Security best practices

### ADR Requirement
An ADR MUST be created for:
- Adding a new service to the architecture
- Changing authentication or authorization model
- Modifying database schema design
- Changing user role model
- Introducing new cross-schema access patterns

ADR documents are immutable and never edited after creation.

### Runbook Dependency
Every complex operational procedure MUST have a documented runbook in `ops/`. Runbooks MUST include:
- Step-by-step instructions
- Expected outcomes
- Common errors and troubleshooting
- Success criteria

### Reporting and Metrics
- **Sprint status**: Documented in `docs/project/phases/phase-NN-status.md`
- **Bug tracking**: Class A/B/C bugs tracked in `docs/project/bugs.md`
- **Decision tracking**: All ADRs maintained in `docs/adr/`
- **Scope management**: Scope document reviewed at phase boundaries

### Conflict Resolution
When principles conflict:
1. Non-negotiable rules (Section II) have highest priority
2. Pragmatic Architecture (Principle I) guides resolution
3. Single Source of Truth (Principle II) enforces data consistency
4. Simple Operations (Principle III) favors simplicity
5. If still unresolved, consult stakeholder approval

**Version**: 1.0.0 | **Ratified**: 2026-06-05 | **Last Amended**: 2026-06-05
