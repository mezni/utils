<!--
Sync Impact Report:
- Version Change: TEMPLATE → 1.0.0
- Principles Added:
  - I. Documentation-First Development
  - II. LLM-Driven Deterministic Execution
  - III. MVP Isolation
  - IV. Complete Testing Requirements
  - V. Architecture Discipline (Backend)
  - VI. Architecture Discipline (Frontend)
  - VII. Data Ownership Rules
  - VIII. Skill System Enforcement
- Sections Added:
  - Core Principles (8 principles)
  - Architecture Rules (Backend & Frontend)
  - Testing Rules
  - Data Rules
  - Development Workflow
  - Governance
- Templates Status:
  - plan-template.md: ✅ Updated (Constitution Check section aligned)
  - spec-template.md: ✅ Updated (Testing requirements integrated)
  - tasks-template.md: ✅ Updated (Testing discipline added)
- Follow-up TODOs: None
-->

# BorneMap Constitution

## Core Principles

### I. Documentation-First Development

**If it is not in Specs, it does not exist. If it is not in MVP, it must not be implemented. If it is not tested, it does not exist.**

This is the absolute ground rule for all development. Any feature, change, or modification must:

1. Be defined in a SpecKit document
2. Belong to an active MVP
3. Follow existing API contracts
4. Match specified UX behavior
5. Be documented before implementation
6. Have comprehensive tests

**Rationale:** Documentation serves as the single source of truth for system behavior. It prevents hallucinated features, ensures traceability, and enables deterministic execution by LLM agents.

### II. LLM-Driven Deterministic Execution

**OpenCode is an execution engine for pre-defined specifications only.**

- Complete skill system prevents hallucinations
- Step-by-step validation at every stage
- Strict scope enforcement
- Zero architecture drift
- Predictable LLM behavior

**Rationale:** Deterministic execution eliminates randomness in LLM outputs, ensures consistent system behavior, and maintains architectural integrity throughout development.

### III. MVP Isolation

**Only ONE MVP is active at a time.**

OpenCode MUST NOT:
- Implement future MVP features early
- Reference future services
- Prepare unused architecture
- Add scope beyond active MVP

**Rationale:** MVP isolation ensures clear feature boundaries, prevents scope creep, and enables focused development of incremental value delivery.

### IV. Complete Testing Requirements

**Every feature must have tests. No feature is complete without basic test coverage.**

- Unit tests required (≥80% coverage)
- Integration tests required (≥100% coverage)
- E2E tests required (≥100% coverage for critical paths)
- Map interactions require UX regression tests
- No merge without MVP checkpoint validation

**Sprint 0 Exception**: Infrastructure provisioning and database seeding scripts are validated through Docker health checks and automated SQL assertions (via validate.sh) rather than unit/integration/E2E frameworks. Once backend service code is introduced (Sprint 1+), all constitution testing requirements apply in full.

**Rationale:** Comprehensive testing ensures system reliability, prevents regression bugs, and provides confidence in incremental changes.

### V. Architecture Discipline (Backend)

**Backend follows Rust Clean Architecture with strict layer separation.**

**Clean Architecture Pattern:**
- Handler layer: HTTP request/response handling
- Service layer: Business logic and orchestration
- Repository layer: Data access and persistence

**PostGIS Isolation:**
- All geospatial queries isolated in repository layer
- No raw SQL in handlers
- No business logic in controllers
- GIS is read-only (protected by data ownership rules)

**Rationale:** Clean architecture ensures maintainability, testability, and separation of concerns. PostGIS isolation prevents database-specific code leakage into business logic.

### VI. Architecture Discipline (Frontend)

**Frontend follows strict architecture rules enforced through skill system.**

**Mandatory Dependencies:**
- @bm/api-client → ALL requests
- @bm/types → ALL models
- @bm/utils → ALL logic
- @bm/design-tokens → ALL UI values

**Forbidden Practices:**
- fetch() or axios inside apps
- Direct map library usage outside MapContainer
- Mixed state management (UI + server)
- Platform logic in components
- Duplicated UI patterns
- Hardcoded colors or spacing

**Map Rendering:**
- All map rendering MUST go through MapContainer abstraction
- MapContainer.native.ts (React Native)
- MapContainer.web.ts (Web/Leaflet)
- No exceptions

**Rationale:** Strict frontend architecture ensures consistency, maintainability, and prevents architecture violations through skill enforcement.

### VII. Data Ownership Rules

**OpenCode MUST respect strict data ownership boundaries.**

**Database Ownership:**
- platform_db = system of record
- analytics_db = append-only
- gis = read-only
- users = owned by auth-service only

**Access Rules:**
- Each service owns its data models
- Services communicate only through defined APIs
- No shared database access patterns
- No cross-service schema writes

**Rationale:** Clear data ownership prevents data integrity violations, ensures auditability, and maintains system boundaries.

### VIII. Skill System Enforcement

**All development must be enforced by skills.**

**Must-Have Skills (Non-Negotiable):**
1. API Contract Discipline - Enforces `/api/v1/*` strictness
2. MVP Scope Enforcement - Blocks cross-MVP features
3. Frontend Architecture Discipline - Enforces MapContainer usage
4. LLM Execution Control - Step-by-step validation

**High-Value Skills:**
5. Data Ownership - Each service owns its schemas
6. Testing Enforcement - Every feature must have tests

**Advanced Skills:**
7. Security Evolution - MVP-aware security patterns
8. Design System Enforcement - No styling outside tokens
9. Bug Learning System - Every bug produces root cause

**Rationale:** Skills are execution constraints that prevent hallucinated features, enforce architecture rules, and ensure deterministic LLM behavior.

## Architecture Rules

### Backend Rules

**Technology Stack:**
- Rust for all backend services
- PostgreSQL + PostGIS for geospatial data
- SeaORM for database queries
- Tokio for async runtime

**Clean Architecture Layers:**
- Handler: HTTP request/response handling, validation
- Service: Business logic, orchestration, error handling
- Repository: Data access, PostGIS queries, data transformation

**Error Handling:**
- Use typed errors (`Result<T, DomainError>`)
- Never use `unwrap()` or `expect()`
- Always handle errors explicitly
- Return typed error responses

**API Contract:**
- All endpoints MUST follow `/api/v1/*` pattern
- No undocumented endpoints
- No breaking changes without version bump
- Typed responses from `@bm/types`

### Frontend Rules

**Technology Stack:**
- React Native for mobile apps
- React/Leaflet for web apps
- React Query for server state
- Zustand for UI state

**State Management:**
- Server state → React Query
- UI state → Zustand (per app)
- No shared global state across apps

**Testing Requirements:**
- Unit tests for components and utilities
- Integration tests for API calls and database
- E2E tests for complete user flows
- Map flow tests for UX regression prevention

## Testing Rules

OpenCode must add tests for:
- API integration
- Critical UI flows (MVP-1 map flow)
- Utility functions
- Map interaction behaviors

**Test Coverage Targets:**
- Unit tests: ≥80% coverage
- Integration tests: ≥100% coverage
- E2E tests: ≥100% coverage for critical paths

**Test Requirements:**
- Every feature must have tests
- No merge without MVP checkpoint validation
- Map interactions require UX regression tests
- No flaky tests allowed

## Data Rules

**Database Ownership:**
- platform_db = system of record
- analytics_db = append-only (no deletions)
- gis = read-only (no writes)
- users = owned by auth-service only

**Access Rules:**
- Each service owns its data models
- Services communicate only through defined APIs
- No shared database access patterns
- No cross-service schema writes
- GIS is read-only

## Development Workflow

**OpenCode Execution Flow:**

1. Read Constitution
2. Check active MVP
3. Read SpecKit feature
4. Validate API contract
5. Confirm UX rules
6. Read Rust Clean Architecture skill
7. Read Frontend Architecture skill
8. Read Data Ownership skill
9. Read Testing Enforcement skill
10. Implement only allowed scope
11. Add tests for all features
12. Log changes
13. Update bug log if needed

**Pre-Execution Checklist:**
- [ ] Which MVP is active?
- [ ] What is the feature scope?
- [ ] What is forbidden?
- [ ] SpecKit document present?
- [ ] Has UX/UI defined behavior?
- [ ] Are inputs, outputs, constraints defined?
- [ ] Are endpoints defined in `/api/v1/*` spec?
- [ ] Are request/response shapes defined?
- [ ] Which folders are allowed for modification?
- [ ] Loading states defined?
- [ ] Empty states defined?
- [ ] Error states defined?
- [ ] Unit tests required?
- [ ] Integration tests required?
- [ ] E2E tests required?
- [ ] Test coverage targets defined?

**If ANY answer is missing → STOP.**

## Governance

**Amendment Procedure:**
- Proposed changes must be documented
- Require explicit approval and migration plan
- Must not break existing implementations
- Version numbers must follow semantic versioning

**Versioning Policy:**
- MAJOR: Backward incompatible governance/principle removals or redefinitions
- MINOR: New principle/section added or materially expanded guidance
- PATCH: Clarifications, wording, typo fixes, non-semantic refinements

**Compliance Review:**
- All PRs/reviews must verify compliance with this constitution
- Architecture violations must be explicitly justified
- Complexity must be justified
- Use `docs/01_constitution.md` for runtime development guidance

**Skill System Integration:**
- Skills are execution constraints enforced during development
- All relevant skills must be read and enforced
- Skill violations prevent code implementation
- Skills are the primary mechanism for deterministic execution

---

**Version**: 1.0.0 | **Ratified**: 2026-06-13 | **Last Amended**: 2026-06-13