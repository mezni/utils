# Constitution

## Version: 1.0
## Status: Active
## Core Philosophy: Documentation is the system. Code is just its execution.

---

## 🧠 1. CORE PRINCIPLE

**If it is not in Specs, it does not exist.**
**If it is not in MVP, it must not be implemented.**
**If it is not tested, it does not exist.**

This is the absolute ground rule for all development. Any feature, change, or modification must be:

1. Defined in a SpecKit document
2. Belong to an active MVP
3. Follow existing API contracts
4. Match specified UX behavior
5. Be documented before implementation
6. Have comprehensive tests

---

## 🚫 2. ABSOLUTE FORBIDDEN BEHAVIORS

OpenCode MUST NEVER:

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

### Skills (Must always enforce)
- api-contract-discipline
- mvp-scope-enforcement
- frontend-architecture-discipline
- llm-execution-control
- data-ownership
- testing-enforcement

---

## 🔄 4. EXECUTION PRINCIPLE

**OpenCode executes specifications. It does not generate them.**

If a spec does not exist → STOP.

Implementation must follow this order:

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

---

## 📋 5. REQUIRED PRE-EXECUTION CHECKLIST

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
- [ ] Read Rust Clean Architecture skill
- [ ] Read Frontend Architecture skill
- [ ] Read Data Ownership skill
- [ ] Read Testing Enforcement skill

### Testing Requirements
- [ ] Unit tests required
- [ ] Integration tests required
- [ ] E2E tests required
- [ ] Test coverage targets

**If ANY answer is missing → STOP.**

---

## 🧱 6. MVP ISOLATION RULE

- Only ONE MVP is active at a time.

OpenCode MUST NOT:
- implement future MVP features early
- reference future services
- prepare unused architecture
- Add scope beyond active MVP

---

## 📱 7. FRONTEND RULES (CRITICAL)

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

### Map Rendering
**All map rendering MUST go through:**
- MapContainer.ts
- MapContainer.native.ts
- MapContainer.web.ts

**No exceptions.**

---

## 🧠 8. STATE RULES

- Server state → React Query
- UI state → local or Zustand per app
- No shared global state across apps

---

## 🗄️ 9. DATA RULES

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

## 🔐 10. AUTH RULES

- Only auth-service communicates with Keycloak
- No frontend or backend bypass allowed
- JWT is the only trusted identity mechanism for services

---

## 🧪 11. TESTING RULES

OpenCode must add tests for:
- API integration
- Critical UI flows (MVP-1 map flow)
- Utility functions

**No feature is complete without basic test coverage.**

---

## 🚨 12. ERROR HANDLING RULES

Every feature MUST implement:
- loading state (skeleton preferred)
- empty state
- error state with retry option

**No silent failures allowed.**

---

## ⚙️ 13. OUTPUT FORMAT (IMPORTANT)

When OpenCode completes a task, output MUST include:

1. Files modified
2. Reason for changes
3. API endpoints used
4. UI behavior changes
5. Any assumptions made

**If assumptions exist → they must be explicitly stated.**

---

## 🔄 14. DOCUMENTATION IS SYSTEM

**Every change must be documented.**

- Code changes → Update spec
- Spec changes → Update implementation plan
- Architecture changes → Update constitution
- Bug fixes → Update bug log
- Release changes → Update release notes

**Documentation is the system. Code is just its execution.**

---

## 🎯 15. MVP ISOLATION RULE

**Only ONE MVP is active at a time.**

OpenCode MUST NOT:
- Implement future MVP features early
- Reference future services
- Prepare unused architecture
- Add scope beyond active MVP

---

## 🧠 16. CORE EXECUTION PRINCIPLE

OpenCode is a deterministic implementation engine driven by SpecKit, constrained by Constitution, and enforced by Skills.

---

## ⚡ RESULT OF THIS FILE

This constitution ensures:

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
- ✅ Complete skill enforcement

---

*This constitution is the system. All code execution must align with these principles.*