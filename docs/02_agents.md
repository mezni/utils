# OpenCode Execution Brain

## Version: 1.0
## Status: Active
## Core Philosophy: Documentation is the system. Code is just its execution.

---

## 🧠 PURPOSE

This file defines how OpenCode must behave when working on BorneMap.

**OpenCode is NOT an architect.**
**OpenCode is NOT a product designer.**
**OpenCode is an EXECUTION ENGINE for pre-defined specifications only.**

---

## 🚦 SKILL SYSTEM INTEGRATION

OpenCode MUST always enforce these skills:

### 🔴 MUST HAVE SKILLS

**1. API Contract Discipline**
- Enforces `/api/v1/*` strictness
- Ensures typed responses
- Prevents breaking changes
- Location: `skills/api-contract-discipline/skill.md`

**2. MVP Scope Enforcement**
- Enforces active MVP scope
- Blocks cross-MVP features
- Prevents scope creep
- Location: `skills/mvp-scope-enforcement/skill.md`

**3. Frontend Architecture Discipline**
- MapContainer is ONLY map abstraction
- No direct API calls
- Strict state separation (UI: Zustand, Server: React Query)
- Platform logic in adapters only
- Location: `skills/frontend-architecture-discipline/skill.md`

**4. LLM Execution Control**
- Enforces step-by-step execution
- Requires validation before coding
- Prevents jumping ahead
- Complete validation checklist
- Location: `skills/llm-execution-control/skill.md`

### 🟠 HIGH VALUE SKILLS

**5. Data Ownership**
- Each service owns its schemas
- No cross-schema writes
- GIS is read-only
- Analytics is append-only
- Location: `skills/data-ownership/skill.md`

**6. Testing Enforcement**
- Every feature must have tests
- Unit + Integration + E2E required
- No merge without MVP checkpoint validation
- Map interactions must have UX regression tests
- Location: `skills/testing-enforcement/skill.md`

### 🟡 ADVANCED SKILLS

**7. Security Evolution**
- MVP-aware security patterns
- Input sanitization consistency
- API abuse prevention
- Strict logging boundaries
- Location: `skills/security-evolution/skill.md`

**8. Design System Enforcement**
- No styling outside tokens
- No duplicated UI patterns
- Consistent spacing/typography
- Platform consistency rules
- Location: `skills/design-system-enforcement/skill.md`

**9. Bug Learning System**
- Every bug produces root cause
- Prevention rules created
- ADR updates for structural bugs
- No repeated bugs allowed
- Location: `skills/bug-learning-system/skill.md`

---

## 🔄 COMPLETE EXECUTION FLOW

### Step 1 — Read Constitution
- Read [docs/01_constitution.md](../docs/01_constitution.md)
- Understand core principles
- Identify forbidden behaviors

### Step 2 — Check Active MVP
- Read [docs/execution/active-mvp.md](../docs/execution/active-mvp.md)
- Confirm active MVP
- Identify forbidden features

### Step 3 — Read SpecKit
- Find relevant specification
- Identify inputs, outputs, constraints
- Validate API contracts
- Check UX requirements

### Step 4 — Validate API Contract
- Check [docs/api/driver-service.md](../docs/api/driver-service.md)
- Verify endpoint definitions
- Validate request/response shapes
- Confirm versioning

### Step 5 — Confirm UX Rules
- Check [docs/design/05_map-interactions.md](../docs/design/05_map-interactions.md)
- Confirm interaction patterns
- Verify state management rules
- Check loading/empty/error states

### Step 6 — Read Rust Clean Architecture
- Read [skills/rust-clean-architecture/skill.md](../skills/rust-clean-architecture/skill.md)
- Verify layer separation
- Check repository pattern
- Validate PostGIS isolation

### Step 7 — Read Frontend Architecture
- Read [skills/frontend-architecture-discipline/skill.md](../skills/frontend-architecture-discipline/skill.md)
- Verify MapContainer usage
- Check state separation
- Validate API client usage

### Step 8 — Read Data Ownership
- Read [skills/data-ownership/skill.md](../skills/data-ownership/skill.md)
- Verify schema ownership
- Check cross-service access
- Validate schema boundaries

### Step 9 — Read Testing Enforcement
- Read [skills/testing-enforcement/skill.md](../skills/testing-enforcement/skill.md)
- Verify test requirements
- Check coverage targets
- Validate MVP checkpoint

### Step 10 — Implement Backend (if applicable)
- Follow Rust Clean Architecture
- Implement in correct layers
- Use Result<T, DomainError>
- Isolate PostGIS in repository

### Step 11 — Implement Frontend
- Use MapContainer for maps
- Use @bm/api-client for API
- Separate UI state (Zustand) from server state (React Query)
- Use design tokens for styling

### Step 12 — Add UX Polish
- Implement skeleton loading states
- Create empty states
- Implement error states
- Add retry options
- Haptics on mobile
- Smooth transitions

### Step 13 — Add Tests
- Write unit tests
- Write integration tests
- Write E2E tests
- Test UX regression
- Validate test coverage

### Step 14 — Validate Constraints
- Check [docs/execution/07_scope-guard.md](../docs/execution/07_scope-guard.md)
- Check architecture compliance
- Verify API contract adherence
- Confirm frontend rules
- Validate MVP scope
- Check data ownership
- Verify testing requirements

### Step 15 — Log Changes
- Update [docs/execution/04_done-log.md](../docs/execution/04_done-log.md)
- Log completed tasks
- Document changes
- Record assumptions

---

## 🚫 FORBIDDEN BEHAVIORS

### Architecture Violations
- Create new services
- Modify system architecture
- Add endpoints outside spec
- Extend MVP scope without instruction
- Write code outside source/
- Bypass @bm/api-client
- Use fetch() or axios inside apps
- Access users schema directly
- Access keycloak_db
- Introduce new libraries without approval
- Implement features not in active MVP

### Backend Violations
- Business logic in handlers
- SQL inside controllers
- Direct database access from frontend
- Cross-service schema writes
- Mixed DB + logic layers
- No PostGIS isolation

### Frontend Violations
- Direct map library usage outside MapContainer
- Direct API calls in components
- Mixed state management (UI + server)
- Platform logic in components
- Duplicate UI patterns
- Inline styling system

### Testing Violations
- Features without tests
- Missing integration tests
- Missing E2E tests
- No MVP checkpoint validation
- Flaky tests
- No UX regression tests

---

## 📋 REQUIRED PRE-EXECUTION CHECKLIST

Before writing ANY code, OpenCode MUST confirm:

### MVP Context
- [ ] Which MVP is active?
- [ ] What is the feature scope?
- [ ] What is forbidden?

### SpecKit Validation
- [ ] SpecKit document present?
- [ ] Has UX/UI Pro Max defined behavior?
- [ ] Are inputs, outputs, constraints defined?

### API Contract
- [ ] Are endpoints defined in /api/v1/* spec?
- [ ] Are request/response shapes defined?

### Allowed Scope
- [ ] Which folders are allowed for modification?
- [ ] No cross-boundary violations?

### UX Constraints
- [ ] Loading states defined?
- [ ] Empty states defined?
- [ ] Error states defined?

### Architecture Validation
- [ ] Read Rust Clean Architecture
- [ ] Read Frontend Architecture
- [ ] Read Data Ownership
- [ ] Read Testing Enforcement

### Testing Requirements
- [ ] Unit tests required
- [ ] Integration tests required
- [ ] E2E tests required
- [ ] Test coverage targets

**If ANY answer is missing → STOP.**

---

## 🧱 CORE PRINCIPLE

**Execution is not development tracking. It is MVP constraint enforcement.**

OpenCode MUST follow the skill system execution flow, not create features at will.

---

## ⚡ RESULT OF THIS FILE

This ensures:

- ✅ Zero architecture drift
- ✅ No frontend/backend mixing
- ✅ Strict MVP execution
- ✅ Predictable LLM behavior
- ✅ Clean OpenCode output
- ✅ Feature drift prevention
- ✅ Architectural integrity
- ✅ Complete test coverage
- ✅ Strict scope enforcement
- ✅ Deterministic execution

---

*This document is the system. All code execution must align with these rules.*