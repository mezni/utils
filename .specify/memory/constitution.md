<!--
  Sync Impact Report
  Version change: (template) → 1.0.0
  Modified principles:
    - [PRINCIPLE_1_NAME] → "I. Validation Before Optimization (NON-NEGOTIABLE)"
    - [PRINCIPLE_2_NAME] → "II. Technical Stack Governance (LOCKED)"
    - [PRINCIPLE_3_NAME] → "III. API & Service Architecture"
    - [PRINCIPLE_4_NAME] → "IV. Data Architecture Standards"
    - [PRINCIPLE_5_NAME] → "V. Development & Environment Discipline"
    - [SECTION_2_NAME] → "Additional Constraints"
    - [SECTION_3_NAME] → "Development Workflow & Quality Gates"
    - [GOVERNANCE_RULES] → Fully populated governance section
  Added sections:
    - All 5 principles (initial population)
    - Section 2: Additional Constraints
    - Section 3: Development Workflow & Quality Gates
    - Governance section fully populated
  Removed sections: None
  Templates requiring updates:
    - .specify/templates/plan-template.md ✅ No changes required (generic Constitution Check)
    - .specify/templates/spec-template.md ✅ No changes required
    - .specify/templates/tasks-template.md ✅ No changes required
  Follow-up TODOs: None
-->

# BorneMap Constitution

## Core Principles

### I. Validation Before Optimization (NON-NEGOTIABLE)

- Strategic operational rule: **"validation before optimization"**
- System scaling, high-availability deployments, and advanced caching infrastructure
  layers SHALL ONLY be executed following explicitly validated market demand
- In-memory cache (Redis) is DEPRECATED until validated demand
- Message broker (RabbitMQ) is DEPRECATED until validated demand
- All infrastructure complexity MUST be justified by a proven need; simplicity
  is the default

### II. Technical Stack Governance (LOCKED)

The following technology stack is LOCKED for MVP and initial rollouts. Deviations
require a constitutional amendment:

- **Backend Services**: Rust via Actix-web (modular workspace divided into
  discrete worker domains)
- **Database Layer**: PostgreSQL with PostGIS extension for spatial computations
- **Mobile Client**: React Native via Expo Go framework (Targeting SDK 51)
- **Admin Portal**: React (Vite / Next.js layout engine)
- **Dev Engine Runtime**: Node.js v24.16.0 / npm v11.13.0

### III. API & Service Architecture

- The main business router gateway is strictly designated as **`api-service`**.
  Generic references to `api/` are DEPRECATED to ensure strict architectural
  decoupling from `auth-service` and future microservices
- Frontend and backend systems communicate via structured HTTP REST endpoints
  prefixed under version control mappings (`/api/v1`)
- Service isolation MUST be maintained; each service owns a discrete domain
  boundary with no circular dependencies

### IV. Data Architecture Standards

- All database entity IDs MUST strictly adhere to the `XXX-nanouuid` pattern
  (e.g., `stn-e3b0c442`, `prv-k9x2m47a`, `chg-7b2a19f4`)
- Standard UUIDv4 strings or sequential auto-incrementing integers are STRICTLY
  RESTRICTED from direct public API exposures
- Location telemetry metrics MUST utilize PostGIS geometry coordinates
  (SRID 4326)
- Default client map views MUST natively anchor on the geographic center bounds
  of Tunis, Tunisia (`36.8065`, `10.1815`)

### V. Development & Environment Discipline

- Local engineering loops MUST utilize Docker Compose to run single-purpose
  database instances (`/deployments`), minimizing hardware overhead in
  virtualized environments
- Mobile application bundling routines MUST utilize secure external tunneling
  overlays (`--tunnel`) or virtual network bridged interfaces to push JavaScript
  binaries to physical testing hardware in VirtualBox environments
- Troubleshooting mobile client render trees MUST use completely offline maps
  with hardcoded spatial coordinate bounds before testing async network layer
  dependencies
- Development tracking specifications, structural schema dictionaries, and
  onboarding maps MUST be kept in sync under `/spec` and `/docs` to facilitate
  clean, context-aware parsing by AI coding assistants

## Additional Constraints

- Only upon explicitly validated market demand: system scaling, high-availability
  deployments, caching infrastructure, and message brokers
- No Redis or RabbitMQ until validation gates are passed
- All third-party dependencies MUST be reviewed for license compatibility and
  long-term maintainability
- Containerized database instances under `/deployments` are the sole approved
  local development backend

## Development Workflow & Quality Gates

- **Environment Isolation**: Docker Compose runs single-purpose database
  instances; no local daemon installations outside containers
- **UI Diagnostics**: Offline maps with hardcoded coordinates first, async
  network testing later (isolate network adapter issues before introducing
  network dependencies)
- **Documentation Sync**: All files under `/spec` and `/docs` MUST remain
  current with the codebase; stale documentation is a blocking quality gate
- Public API changes MUST be versioned under `/api/v1` and documented before
  merge

## Governance

This constitution supersedes all ad hoc practices and informal conventions.

**Amendment Procedure:**
1. Document the rationale for the change
2. Obtain project lead approval
3. Provide a migration plan for any breaking changes
4. Update the constitution version number accordingly

**Versioning Policy (Semantic):**
- **MAJOR**: Backward incompatible governance/principle removals or
  redefinitions
- **MINOR**: New principle/section added or materially expanded guidance
- **PATCH**: Clarifications, wording, typo fixes, non-semantic refinements

**Compliance Review:**
- All code reviews MUST verify compliance with constitutional principles
- Implementation plans MUST include a Constitution Check gate before Phase 0
- Any deviation from constitutional constraints MUST be documented as a
  complexity tracking entry in the implementation plan
- The constitution SHOULD be reviewed quarterly for continued relevance

**Version**: 1.0.0 | **Ratified**: 2026-05-27 | **Last Amended**: 2026-05-27
