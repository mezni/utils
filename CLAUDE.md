# PROJECT MASTER PROMPT

## Project Overview

**Name**: EV Dashboard Platform Kernel (E001)
**Description**: Full-stack EV infrastructure dashboard with CRUD operations for Partners, Stations, and Chargers
**Status**: Design Complete, Implementation Tasks Generated (103 tasks ready)
**Current Phase**: Ready to begin implementation (awaiting explicit instruction)

---

## 🚀 Recommended Actions (1, 2, 3)

### Action 1: Install Rust Security Skill (Highly Recommended)

```bash
npx skills add mohitmishra786/low-level-dev-skills@rust-security -g -y
```

**Why**: This skill covers low-level Rust security practices including memory safety in database operations, SQL injection prevention, input validation, secure error handling, and privilege separation - all critical for the database layer.

**Where to Use**: When implementing security-critical code in:
- Database connection pool (T011)
- SQLx queries (T040)
- Input validation for all entities (T045)
- Error handling that handles sensitive data

---

### Action 2: Create Documentation Directory Structure

```bash
mkdir -p docs/decisions
```

**Purpose**: Establish ADR (Architecture Decision Records) structure for documenting significant technical decisions.

**ADR Files to Create**:
```
docs/decisions/
├── ADR-001-use-postgresql-for-ev-dashboard.md
├── ADR-002-deterministic-id-generation.md
├── ADR-003-soft-delete-strategy.md
├── ADR-004-cascade-delete-rules.md
└── ADR-005-status-enum-implementation.md
```

**When to Create**: Start writing ADRs before starting implementation (one per major architectural decision)

**ADR Template**:
```markdown
# ADR-XXX: [Title]

## Status
Accepted | Superseded by ADR-YYY | Deprecated

## Date
YYYY-MM-DD

## Context
[Brief description of the problem or requirement]

## Decision
[What we decided to do]

## Alternatives Considered
### Alternative 1
- Pros
- Cons
- Rejected because...

### Alternative 2
- Pros
- Cons
- Rejected because...

## Consequences
- Positive: [What benefits we gain]
- Negative: [What trade-offs we accept]
- Risk: [Potential issues]
```

---

### Action 3: Load All Essential Skills at Project Start

**For Every New Session**, load these skills in this order:

```bash
# 1. UI/UX Design Intelligence
/skill ui-ux-pro-max

# 2. Rust Best Practices (Clean Code, Tests, Documentation)
/skill rust-best-practices

# 3. Rust Async Patterns (Actix-Web + Tokio)
/skill rust-async-patterns

# 4. Documentation and ADRs
/skill documentation-and-adrs

# 5. Find-Skills (for discovering new capabilities)
/skill find-skills

# 6. (Optional) Rust Security (after installing with Action 1)
/skill rust-security
```

**When to Use**:
- Start of new session: Load all 5 core skills
- When implementing specific features: Load relevant skills
- When searching for tools: Use find-skills skill

---

## 📚 Essential Skills (Always Available)

### Skill 1: UI/UX Pro Max
**Purpose**: Design beautiful, accessible user interfaces
**When to Use**: Creating React components, designing pages, choosing color schemes, implementing charts
**Focus Areas**:
- Clean, modern design
- Accessibility (contrast, keyboard nav, screen readers)
- Mobile-first responsive design
- Dark mode support
- Performance optimization
- Chart/data visualization

### Skill 2: Rust Best Practices
**Purpose**: Apply idiomatic Rust code, clean architecture, testing strategies
**When to Use**: Writing or reviewing any Rust code
**Focus Areas**:
- Clean code architecture (Chapters 1-4)
- Testing strategies (Chapter 5)
- Documentation standards (Chapter 8)
- Error handling patterns (Chapter 4)
- Performance optimization (Chapter 3)

### Skill 3: Rust Async Patterns
**Purpose**: Async Rust programming with Tokio, error handling, tracing
**When to Use**: All async database operations, Actix-Web handlers, concurrency patterns
**Focus Areas**:
- Tokio runtime and async/await
- Repository pattern with async traits
- Error propagation with `?`
- Tracing for debugging
- Performance best practices

### Skill 4: Documentation and ADRs
**Purpose**: Document decisions, APIs, and code intent
**When to Use**: Writing ADRs, documenting public APIs, creating README
**Focus Areas**:
- ADR lifecycle and templates
- Comments vs documentation
- API documentation patterns
- README structure
- Changelog maintenance

### Skill 5: Find-Skills
**Purpose**: Discover and install agent skills from the ecosystem
**When to Use**: When you need a skill for a specific domain
**Commands**:
- `npx skills find [query]` - Search for skills
- `npx skills add <owner/repo@skill> -g -y` - Install skill globally

---

## 🏗️ Architecture Overview

### Clean Architecture Layers

```
┌─────────────────────────────────────┐
│ Presentation Layer                  │
│ ────────────────────────────────────│
│ • Actix-Web HTTP handlers           │
│ • Route definitions                 │
│ • Request validation                │
│ • Response mapping                  │
│ • Middleware (logging, error mapper)│
└─────────────────────────────────────┘
              ↓ depends on
┌─────────────────────────────────────┐
│ Application Layer                   │
│ ────────────────────────────────────│
│ • Use-case orchestration            │
│ • Command/query dispatch            │
│ • Business logic validation         │
│ • DTOs (request/response)            │
└─────────────────────────────────────┘
              ↓ depends on
┌─────────────────────────────────────┐
│ Domain Layer                        │
│ ────────────────────────────────────│
│ • Business rules and invariants     │
│ • Entities (Partner, Station, Charger)│
│ • Repository traits                 │
│ • Domain services                   │
│ • Value objects (Status)            │
│ • NO frameworks, NO IO, NO HTTP     │
└─────────────────────────────────────┘
              ↓ implemented by
┌─────────────────────────────────────┐
│ Infrastructure Layer                │
│ ────────────────────────────────────│
│ • SQLx repositories                 │
│ • Database migrations               │
│ • Deterministic ID generation       │
│ • Response mappers                  │
│ • External IO (database, HTTP)      │
│ • NO business logic                 │
└─────────────────────────────────────┘
```

### Key Architectural Principles

1. **Clean Architecture (Constitution I)**
   - Strict layering with inward-only dependencies
   - Domain is pure (no frameworks, no IO, no HTTP)
   - Application orchestrates use-cases
   - Infrastructure handles ALL IO

2. **External Identity Model (Constitution II)**
   - ONLY external IDs used (PRT-<12-char>, STA-<12-char>, CHR-<12-char>)
   - NO UUIDs anywhere
   - `id` is the ONLY public identifier in APIs
   - IDs are immutable and globally unique

3. **API Contract Compliance (Constitution III)**
   - All endpoints use `/api/v1` versioning
   - Standardized success/error format
   - No raw framework responses
   - Only `id` exposed externally

4. **Domain Purity (Constitution IV)**
   - Domain contains ONLY business rules and invariants
   - NO database operations, NO HTTP handling
   - Infrastructure contains NO business logic

5. **Test-Driven Development (Constitution V)**
   - Tests MUST be written BEFORE implementation
   - All domain logic MUST have unit tests
   - All API endpoints MUST have integration tests
   - Tests cover: functional correctness, edge cases, error handling

---

## 💾 Data Model

### Entities

#### Partner
- **ID**: PRT-<12 alphanumeric characters> (deterministic from seed)
- **Fields**: id, name, status, is_valid, created_by, updated_by, created_at, updated_at, deleted_at
- **Status**: ACTIVE, INACTIVE, MAINTENANCE, DISABLED
- **Soft Delete**: deleted_at timestamp
- **Cascade**: Hard delete → CASCADE to stations

#### Station
- **ID**: STA-<12 alphanumeric characters> (deterministic from seed)
- **Fields**: id, partner_id, name, location, status, created_by, updated_by, created_at, updated_at, deleted_at
- **Status**: ACTIVE, INACTIVE, MAINTENANCE, DISABLED
- **Soft Delete**: deleted_at timestamp
- **Cascade**: Hard delete → CASCADE to chargers

#### Charger
- **ID**: CHR-<12 alphanumeric characters> (deterministic from seed)
- **Fields**: id, station_id, status, power_rating, created_by, updated_by, created_at, updated_at, deleted_at
- **Status**: ACTIVE, INACTIVE, MAINTENANCE, DISABLED
- **Power Rating**: 1-1000 kW
- **Soft Delete**: deleted_at timestamp

### Relationships

```
Partner (PRT-xxx)
    │ 1 → N
    ▼
Station (STA-xxx)
    │ 1 → N
    ▼
Charger (CHR-xxx)
```

### Key Data Rules

1. **Deterministic ID Generation**: Hash-based nanoid from string seed (NOT random)
   - Implementation: `format!("PRT-{}", deterministic_nanoid(seed, 12))`
   - Seed source: created_by field
   - Infrastructure layer only

2. **Soft Delete Strategy**: deleted_at TIMESTAMP NULL
   - Rows active ONLY when `deleted_at IS NULL`
   - All queries MUST filter by `deleted_at IS NULL`
   - Application layer control, NOT database-level

3. **Cascade Delete Rules**:
   - Hard delete: CASCADE (ON DELETE CASCADE in database)
   - Soft delete: NO cascade (children remain active)

4. **Status Enum**: Unified across all entities (ACTIVE, INACTIVE, MAINTENANCE, DISABLED)
   - Default: ACTIVE
   - Used for filtering and display

5. **Admin Dependency**: created_by and updated_by FK to admins table (assumed to exist in separate system module, no auth system in scope)

---

## 🌐 API Contract

### Base Path

```
/api/v1
```

### Standard Response Format

**Success**:
```json
{
  "success": true,
  "data": {},
  "error": null
}
```

**Error**:
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

### Endpoints

#### Dashboard
- `GET /api/v1/dashboard/kpis` - KPI metrics (partners_count, stations_count, chargers_count)

#### Partners
- `GET /api/v1/partners` - List active partners (pagination supported)
- `POST /api/v1/partners` - Create partner
- `GET /api/v1/partners/{id}` - Get partner by ID
- `DELETE /api/v1/partners/{id}` - Hard delete partner (CASCADE)
- `PUT /api/v1/partners/{id}` - Soft delete or undelete partner

#### Stations
- `GET /api/v1/stations` - List active stations (pagination supported)
- `POST /api/v1/stations` - Create station
- `GET /api/v1/stations/{id}` - Get station by ID
- `DELETE /api/v1/stations/{id}` - Hard delete station (CASCADE)
- `PUT /api/v1/stations/{id}` - Soft delete or undelete station

#### Chargers
- `GET /api/v1/chargers` - List active chargers (pagination supported)
- `POST /api/v1/chargers` - Create charger
- `GET /api/v1/chargers/{id}` - Get charger by ID
- `DELETE /api/v1/chargers/{id}` - Hard delete charger
- `PUT /api/v1/chargers/{id}` - Update status, soft delete, or undelete

---

## 📁 Project Structure

```
BorneMap/
├── services/
│   └── admin-service/              # Backend (Rust + Actix-Web)
│       ├── src/
│       │   ├── presentation/       # HTTP handlers, routing
│       │   ├── application/        # Use-cases, orchestrations
│       │   ├── domain/             # Business logic, entities
│       │   ├── infrastructure/     # SQLx, repositories
│       │   ├── config/             # Configuration management
│       │   ├── db/                 # Database pool, connections
│       │   ├── middleware/         # Request/response middleware
│       │   └── common/             # Shared utilities, error types
│       ├── migrations/             # SQLx migrations
│       └── Cargo.toml
│
├── apps/
│   └── admin-dashboard/            # Frontend (React + TypeScript)
│       ├── src/
│       │   ├── pages/              # Routing layer
│       │   ├── features/           # Business UI logic
│       │   ├── components/         # Pure UI primitives
│       │   ├── api/                # Transport layer
│       │   ├── hooks/              # React Query hooks
│       │   ├── types/              # TypeScript types
│       │   └── utils/              # Utilities
│       ├── package.json
│       └── vite.config.ts
│
├── crates/
│   ├── platform-core/              # Shared Rust crate
│   │   ├── src/
│   │   │   ├── error/
│   │   │   ├── result/
│   │   │   ├── config/
│   │   │   ├── id/
│   │   │   └── validation/
│   │   └── Cargo.toml
│   └── platform-db/                # Shared Rust crate
│       ├── src/
│       │   ├── pool/
│       │   ├── migration/
│       │   └── transaction/
│       └── Cargo.toml
│
├── infrastructure/
│   ├── docker/
│   │   ├── postgres/
│   │   ├── admin-service/
│   │   └── admin-dashboard/
│   ├── postgres/
│   ├── observability/
│   └── network/
│
├── docs/
│   ├── core/                       # Core documentation
│   │   ├── constitution.md
│   │   ├── architecture.md
│   │   └── api-standards.md
│   └── epics/
│       └── E001-dashboard-core/
│
├── specs/
│   └── 001-ev-dashboard/
│       ├── spec.md                 # User stories, requirements
│       ├── plan.md                 # Implementation plan
│       ├── research.md             # Technical research
│       ├── data-model.md           # Entity definitions
│       ├── quickstart.md           # Setup guide
│       ├── contracts/
│       │   ├── api.yaml
│       │   └── dashboard.yaml
│       ├── tasks.md                # Implementation tasks (103 tasks)
│       └── checklists/
│           └── requirements.md
│
├── docker-compose.yml
└── Cargo.toml
```

---

## 🎯 Implementation Status

### Tasks.md Summary

**Total Tasks**: 103
**Parallel Opportunities**: 48 (46.6%)
**Test Tasks**: 43 (TDD-marked)
**MVP Scope**: Phases 1-3 (45 tasks: Setup + Foundational + User Story 1)

### Phase Breakdown

| Phase | Tasks | Purpose | Status |
|-------|-------|---------|--------|
| **Phase 1: Setup** | 7 | Project initialization | ⏸️ Waiting for start |
| **Phase 2: Foundational** | 9 | Core infrastructure (BLOCKS all stories) | ⏸️ Waiting for start |
| **Phase 3: US1 - Dashboard** | 29 | View Dashboard Overview 🎯 MVP | ⏸️ Waiting for start |
| **Phase 4: US2 - Partners** | 17 | Manage Partners (P2) | ⏸️ Waiting for start |
| **Phase 5: US3 - Stations** | 17 | Manage Stations (P3) | ⏸️ Waiting for start |
| **Phase 6: US4 - Chargers** | 18 | Manage Chargers (P4) | ⏸️ Waiting for start |
| **Phase 7: Polish** | 6 | Cross-cutting concerns | ⏸️ Waiting for start |

### Recommended Implementation Order

1. **Start with Phase 1: Setup** (T001-T007)
   - Create project structure
   - Initialize Rust workspace
   - Setup frontend and Docker

2. **Complete Phase 2: Foundational** (T008-T016) ⚠️ BLOCKS ALL STORIES
   - Database schema and migrations
   - Deterministic ID generation
   - SQLx pool
   - Base error system
   - Domain models

3. **Implement User Story 1: Dashboard** (T017-T045) 🎯 MVP
   - Complete CRUD for all entities
   - Hard delete with CASCADE
   - Soft delete and undelete
   - All handlers, routes, DTOs

4. **Validate MVP independently**

5. **Proceed to User Stories 2-4 incrementally**

---

## 🧪 Testing Strategy (TDD Approach)

### Backend Testing

**Unit Tests** (rust-best-practices Chapter 5):
- Domain entity validation
- Business logic in services
- Helper functions
- One assertion per test when possible

**Integration Tests**:
- Repository implementations
- API endpoints
- End-to-end user journeys

**Test Naming**:
```rust
#[tokio::test]
async fn test_partner_service_create_should_succeed_when_name_is_valid() {
    // Arrange
    // Act
    // Assert
}
```

### Frontend Testing

**Component Tests** (ui-ux-pro-max style):
- UI primitives
- Form components
- Layout components

**API Mock Tests**:
- apiClient behavior
- Transport layer
- Error handling

**React Query Tests**:
- Server state behavior
- Cache invalidation
- Query results

---

## 📝 Documentation Standards

### Code Comments (rust-best-practices Chapter 8)

**DO**:
```rust
// Comments explain WHY, not WHAT
// We use deterministic ID generation from string seed to ensure:
// 1. Consistency across instances and environments
// 2. Reproducible test scenarios
// 3. No UUIDs (violates constitution II)
pub fn generate_partner_id(seed: &str) -> String {
    // Implementation
}
```

**DON'T**:
```rust
// Don't comment self-explanatory code
// Increment counter by 1
counter += 1;

// Don't leave commented-out code
// const oldImplementation = () => { ... }  ← Delete it
```

### Doc Comments (rust-best-practices Chapter 8)

**For public APIs**:
```rust
/// Creates a new partner organization.
///
/// # Arguments
/// * `name` - Partner organization name (required, 1-200 characters)
/// * `created_by` - Administrator ID who created this partner
/// * `updated_by` - Administrator ID who last updated this partner
///
/// # Returns
/// Returns `Result<Partner, AppError>` with the created partner or an error
///
/// # Example
/// ```
/// let service = PartnerService::new(repository);
/// let partner = service.create("Example EV Network", "admin1", "admin1").await?;
/// ```
async fn create(
    &self,
    name: String,
    created_by: String,
    updated_by: String,
) -> Result<Partner, AppError>
```

### ADR Requirements (documentation-and-adrs)

**Create ADRs for**:
- Choosing PostgreSQL + SQLx (ADR-001)
- Deterministic ID generation (ADR-002)
- Soft delete strategy (ADR-003)
- Cascade delete rules (ADR-004)
- Status enum implementation (ADR-005)

**Format**: `docs/decisions/ADR-XXX-title.md`

---

## 🔐 Security Considerations

### High Priority (Use rust-security skill)

1. **SQL Injection Prevention**
   - Use SQLx parameterized queries
   - Never concatenate user input into SQL strings
   - Validate all external input

2. **Input Validation**
   - Validate partner_id, station_id, charger_id formats
   - Validate power_rating range (1-1000 kW)
   - Validate name length (1-200 characters)

3. **Error Handling**
   - Never expose sensitive database errors
   - Use generic error messages for production
   - Log detailed errors internally

4. **Database Security**
   - Use connection pooling with appropriate limits
   - Use environment variables for credentials
   - Rotate database credentials periodically

5. **Authentication** (out of scope for E001 but documented)
   - Future: integrate with external admins table
   - Use proper session management
   - Implement rate limiting

### Medium Priority

- Implement CORS configuration
- Add rate limiting for API endpoints
- Use HTTPS in production
- Implement request logging (already using tracing)

---

## 📊 Constitution Compliance Checklist

### I. Clean Architecture ✅
- [x] Strict layering: presentation → application → domain → infrastructure
- [x] Domain is pure (zero frameworks, no IO, no HTTP)
- [x] Application layer orchestrates use-cases, no DB access
- [x] Infrastructure contains ALL SQLx operations, no business logic
- [x] Presentation contains ONLY HTTP handlers and response mapping

### II. External Identity Model ✅
- [x] Only external IDs used (PRT/STA/CHR)
- [x] NO UUIDs anywhere
- [x] `id` is the ONLY public identifier in APIs
- [x] IDs are immutable and globally unique
- [x] All relationships use `id` as foreign keys
- [x] Cascading deletes: Partners → Stations → Chargers

### III. API Contract Compliance ✅
- [x] All endpoints use `/api/v1` versioning
- [x] Standardized success/error format
- [x] No raw framework responses
- [x] Only `id` exposed externally

### IV. Domain Purity ✅
- [x] Domain contains ONLY business rules and invariants
- [x] NO database operations in domain
- [x] NO HTTP handling in domain
- [x] NO external IO in domain
- [x] NO framework usage in domain
- [x] Infrastructure handles SQLx and repository implementations, no business logic

### V. Test-Driven Development ✅
- [x] Unit tests for domain logic (mandatory)
- [x] Integration tests for API endpoints (mandatory)
- [x] Repository tests for database operations (mandatory)
- [x] Frontend tests required (component, API mock, React Query)

---

## 🎨 UI/UX Best Practices (ui-ux-pro-max)

### Accessibility (Priority 1)
- Contrast 4.5:1 for normal text
- Alt text for meaningful images
- Keyboard navigation support
- ARIA labels for icon-only buttons
- Screen reader support (VoiceOver/Screen Reader)

### Touch & Interaction (Priority 2)
- Minimum 44×44px touch targets
- 8px+ spacing between touch targets
- Loading feedback during async operations
- Error messages near problem fields

### Performance (Priority 3)
- WebP/AVIF images
- Lazy loading for below-fold content
- Reduce layout shift (CLS < 0.1)
- Optimize database queries
- Add pagination for list endpoints

### Style Selection (Priority 4)
- Match product type (SaaS dashboard)
- Consistent style across all pages
- SVG icons (no emoji)
- Consistent color palette
- Responsive design (mobile-first)

### Layout & Responsive (Priority 5)
- Mobile-first breakpoints (375 / 768 / 1024 / 1440)
- Readable font size (minimum 16px)
- No horizontal scroll on mobile
- Consistent 4pt/8pt spacing system
- Touch-friendly spacing

### Typography & Color (Priority 6)
- Line-height 1.5 for body text
- Consistent font pairing
- Semantic color tokens (not raw hex)
- Dark mode support (light/dark variants together)

---

## 🚦 Quick Start Commands

### Backend
```bash
cd services/admin-service
cargo build          # Build
cargo test           # Run tests
cargo clippy         # Lint
cargo doc --open     # Generate docs
```

### Frontend
```bash
cd apps/admin-dashboard
npm install          # Install dependencies
npm run dev          # Start dev server
npm test             # Run tests
npm run build        # Build for production
```

### Docker
```bash
docker-compose build      # Build all services
docker-compose up -d      # Start all services
docker-compose ps         # Check status
docker-compose logs -f    # View logs
docker-compose down       # Stop all services
```

### Database
```bash
docker-compose exec postgres psql -U admin -d platform_db
```

---

## 🎯 Current Status & Next Steps

### What's Complete ✅
- [x] Full system specification (spec.md)
- [x] Implementation plan (plan.md)
- [x] Data model (data-model.md)
- [x] Research findings (research.md)
- [x] API contracts (api.yaml)
- [x] Tasks.md with 103 implementation tasks
- [x] Constitution ratified
- [x] Skills installed (ui-ux-pro-max, rust-best-practices, rust-async-patterns, documentation-and-adrs, find-skills)
- [x] Recommended actions documented (1, 2, 3)

### What's Ready to Start ⏸️
- [ ] Project structure created
- [ ] Cargo workspace initialized
- [ ] Docker infrastructure configured
- [ ] Phase 1: Setup (T001-T007)
- [ ] Phase 2: Foundational (T008-T016)
- [ ] Phase 3: User Story 1 (T017-T045)

### ⚠️ IMPORTANT
**Do NOT start implementation yet.**

Wait for explicit instruction from me before beginning work.

### When Ready to Start

**Load Skills First**:
```
/skill ui-ux-pro-max
/skill rust-best-practices
/skill rust-async-patterns
/skill documentation-and-adrs
/skill find-skills
```

**Then Start with**:
```
Action 1: npx skills add mohitmishra786/low-level-dev-skills@rust-security -g -y
Action 2: mkdir -p docs/decisions
Action 3: Create ADR-001.md, ADR-002.md, etc.
```

**Then Begin**:
```
Phase 1: T001 - T007 (Setup)
Phase 2: T008 - T016 (Foundational)
Phase 3: T017 - T045 (User Story 1 - MVP)
```

---

## 📞 Session Notes

**Session Date**: YYYY-MM-DD
**Branch**: 001-ev-dashboard
**Status**: ⏸️ Waiting for explicit instruction to start implementation

---

## 🔧 Tooling & Configuration

### Rust Toolchain
- **Version**: 1.75+
- **Format**: rustfmt
- **Lint**: clippy
- **Test**: cargo test
- **Docs**: cargo doc

### Node.js Toolchain
- **Version**: 18+
- **Package Manager**: npm or yarn
- **Test**: vitest / cypress
- **Build**: Vite

### Database
- **Version**: PostgreSQL 16+
- **Schema**: ev namespace
- **Migration Tool**: SQLx
- **Pool**: sqlx::PgPoolOptions

---

## 📚 External References

### Constitution
- `docs/core/constitution.md`

### Core Documentation
- `docs/core/architecture.md`
- `docs/core/api-standards.md`
- `docs/core/data-modeling.md`

### Epic Documentation
- `docs/epics/E001-dashboard-core/epic.md`
- `docs/epics/E001-dashboard-core/spec.md`
- `docs/epics/E001-dashboard-core/data-model.md`
- `docs/epics/E001-dashboard-core/research.md`
- `docs/epics/E001-dashboard-core/tasks.md`

### Specifications
- `specs/001-ev-dashboard/spec.md` - User stories & requirements
- `specs/001-ev-dashboard/plan.md` - Implementation plan
- `specs/001-ev-dashboard/data-model.md` - Entity definitions
- `specs/001-ev-dashboard/contracts/api.yaml` - API contracts

### Quickstart
- `specs/001-ev-dashboard/quickstart.md`

---

## ✅ Session Checklist

Before each session, ensure:

- [ ] Load all 5 essential skills (ui-ux-pro-max, rust-best-practices, rust-async-patterns, documentation-and-adrs, find-skills)
- [ ] Review this master prompt
- [ ] Check current branch (001-ev-dashboard)
- [ ] Check git status
- [ ] Verify project structure exists
- [ ] Review tasks.md to know what to work on
- [ ] Apply recommended actions (1, 2, 3) if needed
- [ ] Create or update ADRs as decisions are made
- [ ] Write documentation alongside code (TDD + documentation-first)

**Remember**: Wait for explicit instruction before starting implementation. This prompt is for context and guidance, not to auto-start work.
