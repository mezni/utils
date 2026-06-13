# BorneMap Documentation Index

## Version: 1.0
## Purpose: Navigation hub for humans + LLM agents
## Core Philosophy: Documentation is the system. Code is just its execution.

---

## 🧭 1. SYSTEM OVERVIEW

**BorneMap is a MVP-driven, LLM-executed EV charging platform.**

All development follows strict layers:

**Constitution → MVP → Specs → Execution → Code → Bugs → Fixes**

---

## 🧠 2. CORE PRINCIPLE

**If it is not in Specs, it does not exist.**
**If it is not in MVP, it must not be implemented.**

---

## 🤖 3. SKILL SYSTEM (NEW)

**Skills are execution constraints for LLM behavior:**

### 🔴 MUST HAVE (Non-Negotiable)

**1. API Contract Discipline**
- Enforces `/api/v1/*` strictness
- Ensures typed responses
- Prevents breaking changes
- Single source of truth: @bm/types
- **Location:** `skills/api-contract-discipline/skill.md`

**2. MVP Scope Enforcement**
- Enforces active MVP scope
- Blocks cross-MVP features
- Prevents scope creep
- Strict blocking of future features
- **Location:** `skills/mvp-scope-enforcement/skill.md`

**3. Frontend Architecture Discipline**
- MapContainer is ONLY map abstraction
- No direct API calls
- Strict state separation (UI: Zustand, Server: React Query)
- Platform logic in adapters only
- **Location:** `skills/frontend-architecture-discipline/skill.md`

**4. LLM Execution Control**
- Enforces step-by-step execution
- Requires validation before coding
- Prevents jumping ahead
- Complete validation checklist
- **Location:** `skills/llm-execution-control/skill.md`

### 🟠 HIGH VALUE

**5. Data Ownership**
- Each service owns its schemas
- No cross-schema writes
- GIS is read-only
- Analytics is append-only
- **Location:** `skills/data-ownership/skill.md`

**6. Testing Enforcement**
- Every feature must have tests
- Unit + Integration + E2E required
- No merge without MVP checkpoint
- Map interactions must have UX regression tests
- **Location:** `skills/testing-enforcement/skill.md`

### 🟡 ADVANCED

**7. Security Evolution**
- MVP-aware security patterns
- Input sanitization consistency
- API abuse prevention
- Strict logging boundaries
- **Location:** `skills/security-evolution/skill.md`

**8. Design System Enforcement**
- No styling outside tokens
- No duplicated UI patterns
- Consistent spacing/typography
- Platform consistency rules
- **Location:** `skills/design-system-enforcement/skill.md`

**9. Bug Learning System**
- Every bug produces root cause
- Prevention rules created
- ADR updates for structural bugs
- No repeated bugs allowed
- **Location:** `skills/bug-learning-system/skill.md`

---

## 📁 4. DOCUMENTATION STRUCTURE

### 4.1 Root Governance

| File | Purpose |
|------|---------|
| [Constitution](./01_constitution.md) | System rules (architecture + constraints) |
| [Agents](./02_agents.md) | OpenCode execution rules |
| [Implementation Plan](./03_implementation-plan.md) | MVP roadmap |
| [Skill Loader](../skills/AGENTS.md) | Master skill orchestration |

---

### 4.2 MVP LAYER

Each MVP defines a full vertical slice.

**[Active MVP → docs/mvp/mvp-1-discovery.md](./mvp/mvp-1-discovery.md)** ← CURRENT

- [mvp-1-discovery.md](./mvp/mvp-1-discovery.md)
- [mvp-2-operations.md](./mvp/mvp-2-operations.md)
- [mvp-3-identity.md](./mvp/mvp-3-identity.md)
- [mvp-4-analytics.md](./mvp/mvp-4-analytics.md)
- [mvp-5-hardening.md](./mvp/mvp-5-hardening.md)
- [mvp-6-production.md](./mvp/mvp-6-production.md)

---

### 4.3 FEATURE SPECS (EXECUTION CONTRACTS)

**Purpose:** Defines exact implementation contracts for OpenCode

**Includes:**
- API contracts
- UX behavior
- Edge cases
- Acceptance criteria

- [station-discovery/](./specs/station-discovery/)
- [nearby-search/](./specs/nearby-search/)
- [station-detail/](./specs/station-detail/)
- [map-interactions/](./specs/map-interactions/)
- [auth-flow/](./specs/auth-flow/)
- [admin-crud/](./specs/admin-crud/)
- [analytics-events/](./specs/analytics-events/)

---

### 4.4 API CONTRACTS

**Rule:** All APIs MUST follow `/api/v1/*`

- [api/overview.md](./api/overview.md)
- [api/versioning.md](./api/versioning.md)
- [api/driver-service.md](./api/driver-service.md)
- [api/admin-service.md](./api/admin-service.md)
- [api/auth-service.md](./api/auth-service.md)

---

### 4.5 DATABASE SCHEMA

**Ownership rules:**
- `inventory` → admin-service
- `users` → auth-service
- `gis` → read-only
- `analytics` → append-only

- [schema/overview.md](./schema/overview.md)
- [schema/inventory.md](./schema/inventory.md)
- [schema/gis.md](./schema/gis.md)
- [schema/users.md](./schema/users.md)
- [schema/analytics.md](./schema/analytics.md)

---

### 4.6 ARCHITECTURE

- [architecture/overview.md](./architecture/overview.md)
- [architecture/frontend.md](./architecture/frontend.md)
- [architecture/backend.md](./architecture/backend.md)
- [architecture/services.md](./architecture/services.md)
- [architecture/data-model.md](./architecture/data-model.md)
- [architecture/network-model.md](./architecture/network-model.md)

---

### 4.7 DESIGN SYSTEM (UX PRO MAX)

**Purpose:** Defines how the product feels, not how it works

- [design/design-system.md](./design/design-system.md)
- [design/ux-principles.md](./design/ux-principles.md)
- [design/mobile-patterns.md](./design/mobile-patterns.md)
- [design/map-interactions.md](./design/map-interactions.md)
- [design/motion.md](./design/motion.md)
- [design/empty-error-states.md](./design/empty-error-states.md)

---

### 4.8 EXECUTION CONTROL (CRITICAL FOR OPENCODE)

**Purpose:** Defines what OpenCode is allowed to work on RIGHT NOW

- [execution/active-mvp.md](./execution/active-mvp.md)
- [execution/sprint-backlog.md](./execution/sprint-backlog.md)
- [execution/in-progress.md](./execution/in-progress.md)
- [execution/done-log.md](./execution/done-log.md)
- [execution/release-notes.md](./execution/release-notes.md)

---

### 4.9 BUG & FEEDBACK LOOP

**Purpose:** Captures runtime failures, prevents repeated LLM mistakes, enforces learning loop

- [bugs/bug-log.md](./bugs/bug-log.md)
- [bugs/known-issues.md](./bugs/known-issues.md)
- [bugs/regression-tests.md](./bugs/regression-tests.md)
- [bugs/fix-history.md](./bugs/fix-history.md)

---

### 4.10 TESTING

- [testing/strategy.md](./testing/strategy.md)
- [testing/unit.md](./testing/unit.md)
- [testing/integration.md](./testing/integration.md)
- [testing/e2e.md](./testing/e2e.md)
- [testing/map-flow-tests.md](./testing/map-flow-tests.md)

---

### 4.11 OBSERVABILITY

- [observability/logging.md](./observability/logging.md)
- [observability/metrics.md](./observability/metrics.md)
- [observability/tracing.md](./observability/tracing.md)
- [observability/alerts.md](./observability/alerts.md)

---

## 🚀 5. MVP QUICK ACCESS (CURRENT WORK)

### 🔵 ACTIVE MVP

**MVP-1 → Discovery Core**

**Includes:**
- Map view
- Station markers
- Nearby search
- Station detail
- Basic analytics events

**Forbidden:**
- auth
- dashboard
- admin
- partner flows

---

## 🧩 6. LLM EXECUTION FLOW

**When OpenCode runs:**

1. Read Constitution
2. Check Active MVP
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

---

## ⚠️ 7. SYSTEM GUARANTEE

This documentation system guarantees:

- ✅ No feature drift across MVPs
- ✅ No architectural corruption
- ✅ Strict LLM execution safety
- ✅ Predictable frontend + backend evolution
- ✅ Traceable bugs and fixes
- ✅ Complete skill enforcement
- ✅ Deterministic execution
- ✅ Zero architecture violations

---

## 🧠 8. FINAL RULE

**The documentation is the system.**
**The code is just its execution.**

---

## Quick Links

- [Active MVP](./mvp/mvp-1-discovery.md)
- [API Documentation](./api/overview.md)
- [Latest Architecture Decisions](./adr/)
- [Testing Strategy](./testing/strategy.md)
- [Skill System](../skills/AGENTS.md)

## Contribute

All documentation should be maintained in accordance with the project constitution, implementation plan, and skill system.