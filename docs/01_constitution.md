# BorneMap Constitution

## Version: 1.0
## Status: Active
## Core Philosophy: Documentation is the system. Code is just its execution.

---

## 🧠 1. CORE PRINCIPLE

**If it is not in Specs, it does not exist.**
**If it is not in MVP, it must not be implemented.**

This is the absolute ground rule for all development. Any feature, change, or modification must be:

1. Defined in a SpecKit document
2. Belong to an active MVP
3. Follow existing API contracts
4. Match specified UX behavior
5. Be documented before implementation

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

### Process Violations
- Implement features not in active MVP
- Introduce new libraries without approval
- Modify response shapes outside spec
- Access databases incorrectly

---

## 📦 3. ALLOWED WORKING AREAS

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

If a spec does not exist → STOP.

Implementation must follow this order:
1. Read Constitution
2. Check active MVP
3. Read SpecKit feature
4. Validate API contract
5. Confirm UX rules
6. Implement only allowed scope
7. Log changes
8. Update bug log if needed

---

## 📋 5. PRE-EXECUTION CHECKLIST

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
- [ ] No cross-boundary violations?

### UX Constraints
- [ ] Loading states defined?
- [ ] Empty states defined?
- [ ] Error states defined?
- [ ] Mobile gestures (if applicable) defined?

**If ANY answer is missing → STOP.**

---

## 🎯 6. MVP ISOLATION RULE

- Only ONE MVP is active at a time
- OpenCode MUST NOT implement future MVP features early
- OpenCode MUST NOT reference future services
- OpenCode MUST NOT prepare unused architecture

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
- Only services using @bm/api-client for authentication

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
- Loading state (skeleton preferred)
- Empty state
- Error state with retry option

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

## ⚖️ 15. VIOLATION CONSEQUENCES

Any violation of the constitution results in:

1. **Immediate scope halt** - Stop all work on violation
2. **Code review rejection** - Changes will not be merged
3. **Documentation requirement** - Must document why violation occurred
4. **Process correction** - Must follow proper workflow for future
5. **Learning loop** - Must be logged and prevented from recurring

---

## 🔄 16. AMENDMENT PROCESS

Constitutional changes require:

1. Written proposal
2. Architecture review
3. Stakeholder approval
4. Documentation update
5. Team communication
6. OpenCode agent reload

---

## 🎯 17. SUCCESS GUARANTEE

This constitution ensures:

- ✅ Zero architecture drift
- ✅ No frontend/backend mixing
- ✅ Strict MVP execution
- ✅ Predictable LLM behavior
- ✅ Clean OpenCode output
- ✅ Feature drift prevention
- ✅ Architectural integrity

---

*This constitution is the system. All code execution must align with these principles.*
