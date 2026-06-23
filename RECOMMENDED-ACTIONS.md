# Recommended Actions for E001 EV Dashboard

## Immediate Actions (Recommended)

### Action 1: Install Rust Security Skill

```bash
npx skills add mohitmishra786/low-level-dev-skills@rust-security -g -y
```

**Why**: Covers low-level Rust security practices including memory safety, SQL injection prevention, input validation, secure error handling, and privilege separation.

**When to Use**: Database layer, SQLx queries, input validation, error handling.

---

### Action 2: Create Documentation Directory Structure

```bash
mkdir -p docs/decisions
```

**Purpose**: Establish ADR (Architecture Decision Records) structure.

**ADR Files to Create**:
```
docs/decisions/
├── ADR-001-use-postgresql-for-ev-dashboard.md
├── ADR-002-deterministic-id-generation.md
├── ADR-003-soft-delete-strategy.md
├── ADR-004-cascade-delete-rules.md
└── ADR-005-status-enum-implementation.md
```

**When to Create**: Before starting implementation, write ADRs for major architectural decisions.

**When to Update**: After making a decision, write an ADR explaining:
- What the decision was
- Why it was chosen
- What alternatives were considered
- What consequences it has

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

## What These Actions Achieve

### Action 1: Rust Security
- ✅ Memory safety in database operations
- ✅ SQL injection prevention
- ✅ Input validation
- ✅ Secure error handling
- ✅ Privilege separation

### Action 2: Documentation Structure
- ✅ Capture architectural decisions (ADRs)
- ✅ Preserve historical context
- ✅ Help agents understand past decisions
- ✅ Prevent re-deciding
- ✅ Enable future maintenance

### Action 3: Skills Loading
- ✅ Consistent knowledge access
- ✅ Best practices enforcement
- ✅ Code quality standards
- ✅ Documentation guidelines
- ✅ Tool discovery capability

---

## Workflow Integration

### Start of Every Session

1. **Load Skills** (Action 3)
   ```
   /skill ui-ux-pro-max
   /skill rust-best-practices
   /skill rust-async-patterns
   /skill documentation-and-adrs
   /skill find-skills
   ```

2. **Review Master Prompt**
   - Read CLAUDE.md for project context
   - Check current tasks from tasks.md
   - Verify branch (001-ev-dashboard)

3. **Apply Actions** (if needed)
   - Install Rust security skill (Action 1)
   - Create docs/decisions/ structure (Action 2)

4. **Start Implementation**
   - Follow tasks.md in order
   - Apply best practices from loaded skills
   - Write ADRs when making decisions
   - Document code as you go

---

## Skills Reference

### Skill 1: UI/UX Pro Max
**Purpose**: Design beautiful, accessible user interfaces
**When to Use**: React components, pages, charts, colors, animations
**Coverage**: 161 color palettes, 57 font pairings, 99 UX guidelines, 25 chart types

### Skill 2: Rust Best Practices
**Purpose**: Apply idiomatic Rust code, clean architecture, testing strategies
**When to Use**: All Rust code
**Coverage**: Clean code (Chapters 1-4), Testing (Chapter 5), Documentation (Chapter 8)

### Skill 3: Rust Async Patterns
**Purpose**: Async Rust programming with Tokio, error handling, tracing
**When to Use**: All async operations, Actix-Web, databases
**Coverage**: Tokio runtime, futures, tasks, channels, performance

### Skill 4: Documentation and ADRs
**Purpose**: Document decisions, APIs, and code intent
**When to Use**: ADRs, API docs, README
**Coverage**: ADR lifecycle, inline documentation, API patterns

### Skill 5: Find-Skills
**Purpose**: Discover and install agent skills
**When to Use**: Need specialized capabilities
**Commands**: `npx skills find [query]`, `npx skills add <package> -g -y`

### Skill 6: Rust Security (Optional)
**Purpose**: Low-level Rust security practices
**When to Use**: Database layer, SQLx, input validation
**Coverage**: Memory safety, SQL injection, secure error handling

---

## Project Context

**Name**: EV Dashboard Platform Kernel (E001)
**Branch**: 001-ev-dashboard
**Status**: ⏸️ Design Complete, Waiting for Implementation

**What's Complete**:
- ✅ Full system specification
- ✅ Implementation plan (103 tasks)
- ✅ Data model with entities
- ✅ API contracts (OpenAPI)
- ✅ Research findings
- ✅ Tasks.md with clear phases
- ✅ All 5 essential skills installed
- ✅ This action guide created

**What's Ready**:
- [ ] Action 1 executed (Rust security skill installed)
- [ ] Action 2 executed (docs/decisions/ structure created)
- [ ] Action 3 executed (skills loaded)
- [ ] Phase 1: Setup (T001-T007) - ⏸️ Waiting
- [ ] Phase 2: Foundational (T008-T016) - ⏸️ Waiting
- [ ] Phase 3: User Story 1 (T017-T045) - ⏸️ Waiting

**Wait for Explicit Instruction**: Do NOT start implementation yet.

---

## Implementation Order (When Ready)

1. **Phase 1: Setup** (T001-T007)
   - Create project structure
   - Initialize Rust workspace
   - Setup Docker infrastructure
   - Configure linting

2. **Phase 2: Foundational** (T008-T016) ⚠️ BLOCKS ALL STORIES
   - Database schema and migrations
   - Deterministic ID generation
   - SQLx pool
   - Base error system
   - Domain models
   - Status value object
   - Pagination utilities

3. **Phase 3: User Story 1** (T017-T045) 🎯 MVP
   - Complete CRUD for all entities
   - Hard delete with CASCADE
   - Soft delete and undelete
   - All handlers, routes, DTOs
   - Complete test suite

4. **Validate MVP independently**

5. **Proceed to User Stories 2-4**

---

## Files Reference

**Master Prompt**: `CLAUDE.md` - Complete project context and guidelines

**Task List**: `specs/001-ev-dashboard/tasks.md` - 103 implementation tasks

**Specification**: `specs/001-ev-dashboard/spec.md` - User stories & requirements

**Plan**: `specs/001-ev-dashboard/plan.md` - Implementation strategy

**Data Model**: `specs/001-ev-dashboard/data-model.md` - Entity definitions

**API Contracts**: `specs/001-ev-dashboard/contracts/api.yaml` - OpenAPI spec

**Quickstart**: `specs/001-ev-dashboard/quickstart.md` - Setup guide

---

**Remember**: Wait for explicit instruction before starting implementation. These actions are recommended but should be executed only when told to proceed.
