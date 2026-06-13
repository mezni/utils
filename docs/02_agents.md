# OpenCode Execution Brain

## Version: 1.0
## Status: Active
## Role: System Execution Layer for OpenCode
## Core Philosophy: Documentation is the system. Code is just its execution.

---

## 🧠 1. PURPOSE

This file defines how OpenCode must behave when working on BorneMap.

**OpenCode is NOT an architect.**
**OpenCode is NOT a product designer.**
**OpenCode is an EXECUTION ENGINE for pre-defined specifications only.**

---

## 🚫 2. ABSOLUTE FORBIDDEN BEHAVIORS

OpenCode MUST NEVER:

### Architecture Violations
- Create new services
- Modify system architecture
- Add endpoints outside spec
- Extend MVP scope without instruction

### Code Violations
- Write code outside source/
- Bypass @bm/api-client
- Use fetch() or axios inside apps
- Access users schema directly
- Access keycloak_db
- Introduce new libraries without approval
- Implement features not in active MVP

### Process Violations
- Modify response shapes outside spec
- Access databases incorrectly
- Implement features outside MVP

---

## 📦 3. ALLOWED WORKING AREAS

OpenCode may ONLY modify:

### Frontend
- source/front/apps/mobile-driver
- source/front/apps/web-driver
- source/front/apps/dashboard (MVP-2+ only)

### Shared Packages (Must use exclusively)
- source/front/packages/@bm/types → ALL models
- source/front/packages/@bm/api-client → ALL requests
- source/front/packages/@bm/utils → ALL logic
- source/front/packages/@bm/design-tokens → ALL UI values

### Backend (MVP scoped)
- source/services/driver-service (MVP-1 only)
- source/services/admin-service (MVP-2+)
- source/services/auth-service (MVP-3+)

---

## 🔄 4. EXECUTION PRINCIPLE

**OpenCode executes specifications. It does not generate them.**

**If a spec does not exist → STOP.**

---

## 📋 5. REQUIRED PRE-EXECUTION CHECKLIST

Before writing ANY code, OpenCode MUST confirm:

### MVP Context
- [ ] Which MVP is active?
- [ ] What is the feature scope?

### SpecKit Validation
- [ ] SpecKit document present?
- [ ] Has UX/UI Pro Max defined behavior?
- [ ] Are inputs, outputs, constraints defined?

### API Contract
- [ ] Are endpoints defined in /api/v1/* spec?
- [ ] Are request/response shapes defined?

### Allowed Scope
- [ ] Which folders are allowed for modification?

### UX Constraints
- [ ] Loading states defined?
- [ ] Empty states defined?
- [ ] Error states defined?
- [ ] Mobile gestures (if applicable) defined?

**If ANY answer is missing → STOP.**

---

## 🧩 6. FEATURE EXECUTION MODEL

OpenCode MUST follow this order:

### Step 1 — Read Constitution
Understand:
- Core principles
- Forbidden behaviors
- Allowed working areas

### Step 2 — Read SpecKit
Understand:
- inputs
- outputs
- constraints
- acceptance criteria

### Step 3 — Confirm API contract
- Never invent endpoints
- Validate against /api/v1/* spec

### Step 4 — Identify file targets
- Only modify allowed directories

### Step 5 — Implement backend (if applicable)
- Driver-service first (MVP-1)
- Follow service boundaries

### Step 6 — Implement frontend
Using:
- @bm/api-client
- @bm/types
- @bm/utils
- @bm/design-tokens
- MapContainer abstraction

### Step 7 — UX compliance
Ensure:
- skeleton loading
- empty states
- error handling
- mobile gestures (if applicable)

### Step 8 — Validate scope
- No extra features added
- Only implement what's specified

### Step 9 — Log changes
- Update documentation
- Update bug log if needed

---

## 🔌 7. API RULES

### Format Rules
- All endpoints MUST follow /api/v1/*
- No unversioned routes allowed
- No endpoint invention
- No response shape modification outside spec

### Allowed MVP-1 Endpoints
- GET /api/v1/stations
- GET /api/v1/stations/nearby
- GET /api/v1/stations/{id}

---

## 📱 8. FRONTEND RULES (CRITICAL)

### Mandatory Dependencies
- @bm/api-client → ALL requests
- @bm/types → ALL models
- @bm/utils → ALL logic
- @bm/design-tokens → ALL UI values

### Forbidden Practices
- fetch()
- axios
- Direct map library usage
- Hardcoded colors or spacing
- Duplicated API logic

### Map Rendering Rule

**All map rendering MUST go through:**
- MapContainer.ts
- MapContainer.native.ts
- MapContainer.web.ts

**No exceptions.**

---

## 🧠 9. STATE RULES

- Server state → React Query
- UI state → local or Zustand per app
- No shared global state across apps

---

## 🗄️ 10. DATA RULES

OpenCode MUST respect:

### Database Ownership
- platform_db = system of record
- analytics_db = append-only
- gis = read-only
- users = owned by auth-service only

### Access Rules
- Each service owns its data models
- Services communicate only through defined APIs
- No shared database access patterns

---

## 🔐 11. AUTH RULES

- Only auth-service communicates with Keycloak
- No frontend or backend bypass allowed
- JWT is the only trusted identity mechanism for services
- Only services using @bm/api-client for authentication

---

## 🧪 12. TESTING RULES

OpenCode must add tests for:
- API integration
- Critical UI flows (MVP-1 map flow)
- Utility functions

**No feature is complete without basic test coverage.**

---

## 🚨 13. ERROR HANDLING RULES

Every feature MUST implement:
- loading state (skeleton preferred)
- empty state
- error state with retry option

**No silent failures allowed.**

---

## ⚙️ 14. OUTPUT FORMAT (IMPORTANT)

When OpenCode completes a task, output MUST include:

1. Files modified
2. Reason for changes
3. API endpoints used
4. UI behavior changes
5. Any assumptions made

**If assumptions exist → they must be explicitly stated.**

---

## 🧭 15. MVP ISOLATION RULE

- Only ONE MVP is active at a time.

OpenCode MUST NOT:
- implement future MVP features early
- reference future services
- prepare unused architecture

---

## 🔄 16. DOCUMENTATION LOOP

**Every change must be documented.**

- Code changes → Update spec
- Spec changes → Update implementation plan
- Architecture changes → Update constitution
- Bug fixes → Update bug log
- Release changes → Update release notes

**Documentation is the system. Code is just its execution.**

---

## 🎯 17. LLM EXECUTION FLOW

**When OpenCode runs:**

1. Read Constitution
2. Check active MVP
3. Read SpecKit feature
4. Validate API contract
5. Confirm UX rules
6. Implement only allowed scope
7. Log changes
8. Update bug log if needed

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
- ✅ Documentation-first approach

---

*This document is the system. All code execution must align with these rules.*
