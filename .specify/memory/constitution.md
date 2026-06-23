<!--
# SYNC IMPACT REPORT
## Version Change: 0.0.0 → 1.0.0
## Modified Principles: N/A (initial version)
## Added Sections:
- Core Principles (5 principles)
- Architecture Standards
- Development Workflow
- Governance

## Templates Requiring Updates:
- ✅ .specify/templates/plan-template.md (Constitution Check section exists)
- ✅ .specify/templates/spec-template.md (Requirements structure aligned)
- ✅ .specify/templates/tasks-template.md (Test-driven tasks aligned)

## Follow-up TODOs:
- None - all placeholders filled
-->

# BorneMap Constitution

## Core Principles

### I. Clean Architecture (NON-NEGOTIABLE)

The system MUST enforce strict layering with inward-only dependencies:

```
presentation → application → domain → infrastructure
```

**Rules:**
- Domain MUST be pure Rust (zero frameworks, no IO, no HTTP)
- Application MUST NOT access database directly
- Infrastructure MUST contain ALL SQLx operations and business logic must remain in domain
- Presentation MUST contain ONLY HTTP handlers and response mapping

**Rationale:**
Strict separation of concerns ensures testability, maintainability, and prevents architecture drift. Pure domain logic can be tested without database or HTTP layers, while infrastructure handles all external concerns.

---

### II. External Identity Model

The system uses ONLY external identifiers as primary keys:

| Entity | ID Format |
|---|---|
| Partners | PRT-\<nanoid(12)> |
| Stations | STA-\<nanoid(12)> |
| Chargers | CHR-\<nanoid(12)> |

**Rules:**
- NO UUIDs anywhere in the system
- `id` is the ONLY public identifier exposed in APIs
- `id` is immutable and globally unique per entity type
- All relationships use `id` as foreign keys

**Rationale:**
External IDs prevent internal schema leakage, stabilize API contracts, and simplify frontend integration while maintaining type safety and database efficiency.

---

### III. API Contract Compliance

ALL APIs MUST follow a standardized success/error contract:

**Success:**
```json
{
  "success": true,
  "data": {},
  "error": null
}
```

**Error:**
```json
{
  "success": false,
  "data": null,
  "error": {
    "code": "ERROR_CODE",
    "message": "Human readable message"
  }
}
```

**Rules:**
- All endpoints MUST use `/api/v1` versioning
- No raw framework responses allowed (no Actix responses, no HTTP status codes as API contract)
- No untyped errors allowed
- Only `id` is exposed externally in API responses

**Rationale:**
Consistent API contracts enable predictable client behavior, simplify frontend integration, and ensure stable interfaces across versions.

---

### IV. Domain Purity

The domain layer MUST contain ONLY business rules and invariants:

**Domain Rules:**
- NO database operations
- NO HTTP handling
- NO external IO
- NO framework usage
- Pure Rust structs with validation logic

**Infrastructure Layer Rules:**
- Infrastructure handles SQLx operations and repository implementations
- Infrastructure MUST NOT contain business logic
- All persistence logic is encapsulated in infrastructure

**Rationale:**
Domain purity ensures business logic is testable, reusable, and independent of infrastructure details. This enables easier testing and future infrastructure changes.

---

### V. Test-Driven Development (NON-NEGOTIABLE)

All features MUST be developed with tests written before implementation:

**Testing Requirements:**

**Backend Tests:**
- Unit tests for domain logic (mandatory)
- Integration tests for API endpoints (mandatory)
- Repository tests for database operations (mandatory)

**Frontend Tests:**
- Component tests for UI primitives
- API mock tests for transport layer
- React Query behavior tests for server state

**Rules:**
- NO feature is complete without tests
- Tests MUST validate business rules, not just implementation
- Tests MUST fail before implementation begins (Red-Green-Refactor cycle)
- Tests cover: functional correctness, edge cases, error handling

**Rationale:**
Test-driven development catches bugs early, provides living documentation, and ensures code meets requirements before implementation.

---

## Architecture Standards

### Technology Stack

**Backend:**
- Actix-Web (presentation layer only)
- SQLx (infrastructure layer only)
- Tokio (async runtime)
- Rust 1.75+

**Frontend:**
- React 18+
- TypeScript 5+
- TailwindCSS
- React Router (routing)
- React Query (server state management)

**Database:**
- PostgreSQL 16+
- Schema namespace: `ev`
- No surrogate keys
- `id` is PRIMARY KEY everywhere

**Infrastructure:**
- Docker for containerization
- Docker Compose for orchestration

### Layer Responsibilities

**Presentation Layer:**
- HTTP routing
- Request validation
- Response mapping
- NO business logic

**Application Layer:**
- Use-case orchestration
- Workflow coordination
- Command/query dispatch
- NO database access
- NO direct HTTP logic

**Domain Layer:**
- Business rules and invariants
- Entity logic
- Value objects
- Domain events
- NO framework usage
- NO IO operations

**Infrastructure Layer:**
- SQLx operations
- Repository implementations
- Database migrations
- External API calls
- NO business logic

---

## Development Workflow

### Speckit-Driven Development

The system follows a specification-first pipeline:

1. **Specification First:**
   - Define feature behavior
   - Define data model
   - Define API contract

2. **Architecture Design:**
   - Map feature to Clean Architecture layers
   - Validate against constitution principles

3. **Implementation:**
   - Write backend first (domain → application → infrastructure → presentation)
   - Then frontend structure
   - Then database migrations

4. **Validation:**
   - Run tests (must pass)
   - Security review
   - Linting and formatting

**NO implementation is allowed before specification is complete.**

### Code Organization

**Backend (Rust):**
```
services/admin-service/
├── src/
│   ├── presentation/    # HTTP handlers, routing
│   ├── application/     # use-cases, orchestrations
│   ├── domain/          # business logic, entities
│   ├── infrastructure/  # SQLx, repositories
│   ├── config/          # configuration management
│   ├── db/              # database pool, connections
│   ├── middleware/      # request/response middleware
│   └── common/          # shared utilities, error types
├── migrations/          # SQLx migrations
└── Cargo.toml
```

**Frontend (React/TS):**
```
apps/admin-dashboard/
├── src/
│   ├── pages/           # routing layer only
│   ├── features/        # business UI logic
│   ├── components/      # pure UI primitives
│   ├── api/             # transport layer
│   ├── hooks/           # custom React hooks
│   ├── types/           # TypeScript types
│   └── utils/           # utilities
├── package.json
└── vite.config.ts
```

**Shared Crates:**
```
crates/
├── platform-core/       # error system, result types, config, ID utilities
└── platform-db/         # SQLx pool, migrations, repository implementations
```

### Testing Strategy

**Backend:**
- Unit tests: Domain entities, business logic
- Integration tests: API endpoints, repository operations
- Repository tests: Database CRUD operations, queries

**Frontend:**
- Component tests: UI primitives, layout components
- API mock tests: Transport layer, apiClient
- React Query tests: Server state behavior

**Test Coverage Requirements:**
- All domain logic MUST have unit tests
- All API endpoints MUST have integration tests
- Critical user flows MUST have end-to-end tests

---

## Governance

### Compliance Enforcement

All changes MUST pass compliance checks:

**Architecture Check:**
- [ ] Clean Architecture respected (inward dependencies only)
- [ ] Domain layer is pure (no frameworks, no IO)
- [ ] Infrastructure contains NO business logic
- [ ] Presentation contains ONLY HTTP logic

**API Check:**
- [ ] Uses `/api/v1` versioning
- [ ] Follows standardized response format
- [ ] No raw framework responses
- [ ] Only `id` exposed externally

**Identity Check:**
- [ ] No UUIDs anywhere
- [ ] Only external IDs used
- [ ] Consistent ID formats (PRT/STA/CHR)
- [ ] Cascading deletes defined

**Database Check:**
- [ ] No surrogate keys
- [ ] `id` is PRIMARY KEY everywhere
- [ ] Schema under `ev` namespace
- [ ] Migrations are forward-only

**Frontend Check:**
- [ ] NO `fetch()` in components
- [ ] React Query used for server state
- [ ] API client is single entry point
- [ ] No transport logic in UI

**Observability Check:**
- [ ] Request ID present in all operations
- [ ] Structured logging enabled
- [ ] Tracing with correlation IDs
- [ ] No sensitive data in logs

### Amendment Process

1. **Proposal:**
   - Document proposed change with rationale
   - Identify affected principles and sections
   - Provide migration plan if breaking change

2. **Review:**
   - Architecture review required
   - Impact analysis across all files
   - Team consultation if affecting multiple stakeholders

3. **Approval:**
   - Major amendments require senior architect approval
   - Minor amendments require team consensus

4. **Implementation:**
   - Update constitution
   - Update all affected templates and documentation
   - Version increment per semantic versioning

5. **Communication:**
   - Notify all stakeholders of changes
   - Update onboarding materials
   - Document new requirements in governance files

### Versioning Policy

Constitution follows semantic versioning:

- **MAJOR:** Backward-incompatible principle removals or redefinitions
- **MINOR:** New principles added or significant expansions
- **PATCH:** Clarifications, wording improvements, non-semantic refinements

**Version Change Protocol:**
- Document rationale for version change
- Update last amended date
- Update sync impact report

### Constitution Hierarchy

The constitution is the highest authority document:

1. Constitution (this document) - absolute rules
2. Core documentation (architecture, API standards, etc.) - specific guidance
3. Epic specifications - feature-specific requirements
4. Implementation code - actual implementation

**Rule:**
Lower layers MUST NEVER override higher layers.
If any conflict exists, constitution overrides all others.

**Version**: 1.0.0 | **Ratified**: 2026-06-23 | **Last Amended**: 2026-06-23
