# Scope Guard

## Version: 1.0
## Status: Active
## Core Philosophy: Documentation is the system. Code is just its execution.

---

## 🎯 PURPOSE

**Prevents MVP scope drift.**

This is the ultimate guard against building features outside the active MVP scope.

---

## 🚫 SCENE CHECK

**Ask yourself:**
- Is this feature in the active MVP?
- Is this feature in the forbidden list?
- Is this feature in the allowed list?

**If it's not in the allowed list → DO NOT BUILD IT.**

---

## 🧭 CURRENT MVP: MVP-1 (Discovery Core)

**Status:** ACTIVE
**Timeline:** June 1 - June 20, 2026

---

## ✅ ALLOWED FEATURES (MVP-1)

### Map System

**✅ ALLOWED:**
- [x] Map view on mobile
- [x] Map view on web
- [x] Station markers
- [x] User location tracking
- [x] Map pan and zoom
- [x] Marker tap interaction
- [x] MapContainer abstraction
- [x] Map performance optimization

**❌ FORBIDDEN:**
- [ ] Map clustering (not in MVP-1)
- [ ] Route planning (not in MVP-1)
- [ ] Offline maps (not in MVP-1)
- [ ] 3D maps (not in MVP-1)

---

### Station Discovery

**✅ ALLOWED:**
- [x] View all active stations
- [x] Station list view
- [x] Nearby station search
- [x] Distance-based sorting
- [x] Radius filtering
- [x] Station detail views

**❌ FORBIDDEN:**
- [ ] Station filtering (not in MVP-1)
- [ ] Station sorting (not in MVP-1)
- [ ] Advanced search (not in MVP-1)
- [ ] Station filtering by amenities (not in MVP-1)

---

### Station Details

**✅ ALLOWED:**
- [x] Station name and location
- [x] Status indicators
- [x] Charger information
- [x] Connector types display
- [x] Mobile bottom sheet
- [x] Web side panel

**❌ FORBIDDEN:**
- [ ] Booking system (not in MVP-1)
- [ ] Payment integration (not in MVP-1)
- [ ] User reviews (not in MVP-1)
- [ ] Station photos (not in MVP-1)

---

### API

**✅ ALLOWED:**
- [x] GET /api/v1/stations
- [x] GET /api/v1/stations/nearby
- [x] GET /api/v1/stations/{id}
- [x] POST /api/v1/events

**❌ FORBIDDEN:**
- [ ] POST /api/v1/stations (not in MVP-1)
- [ ] PUT /api/v1/stations/{id} (not in MVP-1)
- [ ] DELETE /api/v1/stations/{id} (not in MVP-1)
- [ ] Authentication endpoints (not in MVP-1)

---

### Design System

**✅ ALLOWED:**
- [x] Design tokens package
- [x] Color system
- [x] Typography scale
- [x] Spacing system
- [x] Radius system

**❌ FORBIDDEN:**
- [ ] Complex component library (not in MVP-1)
- [ ] Animation system (not in MVP-1)
- [ ] Theming engine (not in MVP-1)

---

### Testing

**✅ ALLOWED:**
- [x] Unit tests
- [x] Integration tests
- [x] E2E tests (in progress)

**❌ FORBIDDEN:**
- [ ] Performance testing (not in MVP-1)
- [ ] Load testing (not in MVP-1)
- [ ] Security testing (not in MVP-1)

---

## 🚫 FORBIDDEN FEATURES (MVP-1)

### Architecture (ABSOLUTE FORBIDDEN)

**❌ NEVER:**
- [ ] Add new services (driver-service only for MVP-1)
- [ ] Modify system architecture
- [ ] Change database schema
- [ ] Modify API contracts
- [ ] Add new dependencies

**Consequences:**
- Architecture violation
- Code review rejection
- Documentation requirement
- Process correction

---

### Authentication (ABSOLUTE FORBIDDEN)

**❌ NEVER:**
- [ ] Add authentication endpoints
- [ ] Implement user login
- [ ] Add JWT tokens
- [ ] Create user management
- [ ] Add Keycloak integration

**Consequences:**
- Security breach
- MVP scope violation
- Architecture violation
- Immediate rollback

---

### Admin Features (ABSOLUTE FORBIDDEN)

**❌ NEVER:**
- [ ] Create admin dashboard
- [ ] Add station CRUD operations
- [ ] Add user management
- [ ] Add partner management
- [ ] Add operational workflows

**Consequences:**
- MVP scope violation
- Architecture violation
- Features not in MVP-1

---

### Partner Features (ABSOLUTE FORBIDDEN)

**❌ NEVER:**
- [ ] Add partner dashboards
- [ ] Add partner-specific features
- [ ] Add partner management
- [ ] Add partner analytics

**Consequences:**
- MVP scope violation
- Architecture violation
- Features not in MVP-1

---

### Analytics UI (ABSOLUTE FORBIDDEN)

**❌ NEVER:**
- [ ] Create analytics dashboards
- [ ] Add analytics visualizations
- [ ] Add analytics reports
- [ ] Add analytics management

**Consequences:**
- MVP scope violation
- Architecture violation
- Features not in MVP-1

---

### Frontend Patterns (ABSOLUTE FORBIDDEN)

**❌ NEVER:**
- [ ] Use fetch() directly
- [ ] Use axios directly
- [ ] Access database from frontend
- [ ] Use direct map library in UI
- [ ] Add new frontend packages

**Consequences:**
- Architecture violation
- Security risk
- Maintenance issues
- Code quality degradation

---

## 🚨 BLOCKER TRIGGERS

### Auto-Blockers (Architecture Violations)

**If any of these occur → EXECUTION HALTED:**

1. **New service created**
   - Driver-service only for MVP-1
   - No admin-service yet
   - No auth-service yet

2. **API contract changed**
   - /api/v1/* pattern maintained
   - No new endpoints
   - No response shape changes

3. **Architecture changed**
   - No new components outside scope
   - No database changes
   - No platform changes

4. **Backend DB access from frontend**
   - API client is sole frontend backend communication
   - No fetch() or axios
   - No direct database access

---

### Manual Blockers (Scope Violations)

**If any of these occur → EXECUTION REQUESTED → REVIEW REQUIRED:**

1. **Feature outside scope**
   - No admin features
   - No auth features
   - No partner features
   - No advanced search

2. **Design system deviation**
   - No hardcoded values
   - All from @bm/design-tokens
   - No custom styling

3. **Testing deviation**
   - All required tests
   - No skipped tests
   - No fake tests

---

## 🎯 SCOPE VALIDATION

### Pre-Implementation Check

**Before starting any task, ask:**

1. **Is this task in the allowed list?**
   - ✅ If yes, proceed
   - ❌ If no, check if it's in the forbidden list

2. **Is this task in the forbidden list?**
   - ✅ If no, proceed
   - ❌ If yes, STOP and request approval

3. **Is this task out of scope for MVP-1?**
   - ✅ If no, proceed
   - ❌ If yes, STOP and request approval

---

### Post-Implementation Check

**After completing any task, ask:**

1. **Did I add any forbidden features?**
   - ❌ If yes, fix immediately
   - ✅ If no, proceed

2. **Did I violate any architecture rules?**
   - ❌ If yes, fix immediately
   - ✅ If no, proceed

3. **Did I scope creep?**
   - ❌ If yes, rollback changes
   - ✅ If no, proceed

---

## 🚦 SCOPE WARNING INDICATORS

### Yellow Flags (Request Review)

- Feature not clearly in scope
- Feature extends MVP scope slightly
- Feature has minor dependencies outside scope
- Feature requires validation

**Action:** Request review and approval

---

### Red Flags (STOP EXECUTION)

- Feature clearly outside scope
- Feature conflicts with MVP-1
- Feature violates architecture rules
- Feature has no justification

**Action:** STOP execution, request approval

---

## 🔄 SCOPE EVOLUTION

### MVP-1 → MVP-2

**Scope Expands To Include:**
- Admin dashboard
- Station CRUD operations
- User management
- Partner management
- Operational workflows

**Allowed Architecture Changes:**
- Admin-service introduction
- Additional API endpoints
- Enhanced database schema
- New frontend packages

---

### MVP-2 → MVP-3

**Scope Expands To Include:**
- Authentication system
- User registration/login
- JWT-based authorization
- Keycloak integration

**Allowed Architecture Changes:**
- Auth-service introduction
- User management
- Authentication endpoints
- RBAC implementation

---

## 🧠 SCOPE GUARD BEST PRACTICES

### For Developers

1. **Always check this file before coding**
   - Verify task is in allowed list
   - Confirm task is not in forbidden list
   - Check if task extends scope

2. **Ask for clarification if unsure**
   - When in doubt, ask
   - Better to ask than build wrong
   - Documentation is the system

3. **Report scope violations immediately**
   - Don't build forbidden features
   - Report when in doubt
   - Don't wait until review

---

### For OpenCode (LLM)

1. **Read scope before implementation**
   - Check allowed features
   - Check forbidden features
   - Verify task is in MVP-1

2. **Validate before coding**
   - Ask if task is in scope
   - Request clarification if needed
   - Never build out of scope

3. **Log scope decisions**
   - Document any scope questions
   - Record scope clarifications
   - Update scope if needed

---

## 📊 SCOPE COMPLIANCE

### Current Compliance Status

**Allowed Features Implemented:** 100% ✅

**Forbidden Features Attempted:** 0 ✅

**Scope Violations:** 0 ✅

**Architecture Violations:** 0 ✅

---

## 🎯 SCOPE GUARD ACTIVE STATUS

**Scope Guard Status: ACTIVE**

**Current MVP:** MVP-1 (Discovery Core)

**Scope Guard Valid:** Until MVP-1 completion or explicit scope change

**Review Required:** No

**Next Review:** After MVP-1 completion or MVP-2 start

---

*Scope drift is the enemy of MVP completion. This guard prevents building features that don't belong to the current MVP.*