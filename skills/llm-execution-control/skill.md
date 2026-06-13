# LLM Execution Control Skill — BorneMap

## Purpose
Turn OpenCode into a deterministic system through step-by-step execution with strict validation gates.

---

## 🎯 Core Philosophy

**OpenCode is not a creative coder. OpenCode is an execution engine for pre-defined specifications.**

---

## 🚦 Execution Flow

### Step 1 — Read Constitution

**Before ANY code:**
- Read [docs/01_constitution.md](../docs/01_constitution.md)
- Understand core principles
- Identify forbidden behaviors

**Required Check:**
- [ ] Architecture principles understood
- [ ] Forbidden behaviors acknowledged
- [ ] Allowed working areas identified

---

### Step 2 — Check Active MVP

**Verify current scope:**
- Read [docs/execution/active-mvp.md](../docs/execution/active-mvp.md)
- Confirm active MVP
- Identify forbidden features

**Required Check:**
- [ ] Active MVP identified
- [ ] Forbidden features documented
- [ ] MVP scope confirmed

---

### Step 3 — Read SpecKit

**Understand requirements:**
- Find relevant specification
- Identify inputs, outputs, constraints
- Validate API contracts
- Check UX requirements

**Required Check:**
- [ ] Specification read
- [ ] Inputs/outputs defined
- [ ] Constraints understood
- [ ] No ambiguity

---

### Step 4 — Validate API Contract

**Confirm API requirements:**
- Check [docs/api/driver-service.md](../docs/api/driver-service.md)
- Verify endpoint definitions
- Validate request/response shapes
- Confirm versioning

**Required Check:**
- [ ] API contract validated
- [ ] All endpoints defined
- [ ] Response shapes confirmed
- [ ] No endpoint invention

---

### Step 5 — Confirm UX Rules

**Verify UX requirements:**
- Check [docs/design/05_map-interactions.md](../docs/design/05_map-interactions.md)
- Confirm interaction patterns
- Verify state management rules
- Check loading/empty/error states

**Required Check:**
- [ ] UX rules confirmed
- [ ] Interaction patterns understood
- [ ] State rules validated
- [ ] UX states defined

---

### Step 6 — Implement Backend (if applicable)

**Follow Rust Clean Architecture:**
- Read [skills/rust-clean-architecture/skill.md](rust-clean-architecture/skill.md)
- Implement in correct layers:
  - Handler (API layer)
  - Service (business logic)
  - Repository (DB access)
- Use Result<T, DomainError>
- Isolate PostGIS in repository

**Required Check:**
- [ ] Layer separation correct
- [ ] No business logic in handlers
- [ ] No SQL in services
- [ ] No PostGIS in services
- [ ] Error handling proper

---

### Step 7 — Implement Frontend

**Follow Frontend Architecture:**
- Read [skills/frontend-architecture-discipline/skill.md](frontend-architecture-discipline/skill.md)
- Use @bm/api-client for API calls
- Use MapContainer for maps
- Separate UI state (Zustand) from server state (React Query)
- Use design tokens for styling

**Required Check:**
- [ ] No direct API calls
- [ ] MapContainer used correctly
- [ ] State separation correct
- [ ] Design tokens used

---

### Step 8 — Add UX Polish

**Follow UX Pro Max:**
- Read [skills/ui-ux-pro-max/skill.md](ui-ux-pro-max/skill.md)
- Implement skeleton loading states
- Create empty states
- Implement error states
- Add retry options
- Haptics on mobile
- Smooth transitions

**Required Check:**
- [ ] Loading states present
- [ ] Empty states present
- [ ] Error states present
- [ ] Retry functionality
- [ ] Smooth animations

---

### Step 9 — Validate Constraints

**Final validation:**
- Read [docs/execution/07_scope-guard.md](../docs/execution/07_scope-guard.md)
- Check architecture compliance
- Verify API contract adherence
- Confirm frontend rules
- Validate MVP scope

**Required Check:**
- [ ] No architecture violations
- [ ] API contract followed
- [ ] Frontend rules respected
- [ ] MVP scope intact

---

### Step 10 — Log Changes

**Update documentation:**
- Update [docs/execution/04_done-log.md](../docs/execution/04_done-log.md)
- Log completed tasks
- Document changes
- Record assumptions

**Required Check:**
- [ ] Done-log updated
- [ ] Changes documented
- [ ] Assumptions recorded

---

## 🔒 Enforcement Rules

### Step-by-Step Execution

**Never skip steps:**

```
1. Read Constitution → STOP if unclear
2. Check Active MVP → STOP if scope mismatch
3. Read SpecKit → STOP if not defined
4. Validate API Contract → STOP if missing
5. Confirm UX Rules → STOP if not defined
6. Implement Backend → STOP if violations
7. Implement Frontend → STOP if violations
8. Add UX Polish → STOP if incomplete
9. Validate Constraints → STOP if violations
10. Log Changes → STOP if incomplete
```

### Pre-Implementation Validation

**Before ANY code generation:**

1. **Validate SpecKit:**
   - [ ] Specification exists?
   - [ ] Inputs/outputs defined?
   - [ ] Constraints documented?
   - [ ] API contracts defined?

2. **Validate MVP Scope:**
   - [ ] Feature in active MVP?
   - [ ] Feature not in forbidden list?
   - [ ] All dependencies available?
   - [ ] No cross-MVP features?

3. **Validate Architecture:**
   - [ ] Architecture rules followed?
   - [ ] Layer separation correct?
   - [ ] No forbidden patterns?
   - [ ] No architecture violations?

**IF ANY validation fails → STOP and fix**

---

### Stop Conditions

**Execution STOPS if:**

1. **SpecKit missing:**
   - Feature not in spec
   - No inputs/outputs defined
   - No constraints documented

2. **API contract missing:**
   - Endpoint not defined
   - No request/response shapes
   - No versioning

3. **MVP scope violation:**
   - Feature outside scope
   - Cross-MVP features
   - Future MVP features

4. **Architecture violation:**
   - New services added
   - Architecture changed
   - Layer separation broken

5. **Frontend rule violation:**
   - Direct API calls
   - Direct map usage
   - Inline styling
   - State mixing

6. **UX rule violation:**
   - No loading states
   - No empty states
   - No error states
   - No retry

---

## 🎯 Deterministic Execution

### Execution History

**Track every execution run:**
- Read [docs/execution/06_llm-execution-runs.md](../docs/execution/06_llm-execution-runs.md)
- Record run number
- Document scope
- Record result
- Note issues

**Execution Format:**
```markdown
RUN #004
Date: 2026-06-14
Scope: Station detail view implementation
Result: Completed
Issues:
  - None
Lessons Learned:
  - Use proper error handling
Prevention Rules Added:
  - DomainError for all errors
```

---

## 📋 Validation Checklists

### Architecture Compliance

**Backend:**
- [ ] Layer separation correct (handler → service → repository)
- [ ] No business logic in handlers
- [ ] No SQL in services
- [ ] No PostGIS in services
- [ ] Error handling uses Result<T, DomainError>
- [ ] No panics in production

**Frontend:**
- [ ] No direct API calls
- [ ] MapContainer used correctly
- [ ] State separation (UI: Zustand, Server: React Query)
- [ ] Platform logic in adapters
- [ ] Design tokens used
- [ ] No inline styling

---

### API Contract Compliance

- [ ] All endpoints follow /api/v1/* pattern
- [ ] All responses typed
- [ ] All endpoints defined
- [ ] No endpoint invention
- [ ] Response shapes match @bm/types
- [ ] Versioning maintained

---

### UX Requirements Compliance

- [ ] Loading states present
- [ ] Empty states present
- [ ] Error states present
- [ ] Retry functionality
- [ ] Smooth animations
- [ ] Haptics on mobile

---

## 🚦 Execution Flow Diagram

```
START
  ↓
1. READ CONSTITUTION
  ↓
2. CHECK ACTIVE MVP
  ↓
3. READ SPECSKIT
  ↓
4. VALIDATE API CONTRACT
  ↓
5. CONFIRM UX RULES
  ↓
6. IMPLEMENT BACKEND
  ↓
7. IMPLEMENT FRONTEND
  ↓
8. ADD UX POLISH
  ↓
9. VALIDATE CONSTRAINTS
  ↓
10. LOG CHANGES
  ↓
END
```

---

## 🧠 Deterministic Execution Checklist

**Before Starting ANY Implementation:**

- [ ] Constitution read and understood
- [ ] Active MVP confirmed
- [ ] SpecKit read
- [ ] API contract validated
- [ ] UX rules confirmed
- [ ] All validation checks passed

**During Implementation:**

- [ ] Follow step-by-step execution
- [ ] Validate at each step
- [ ] Stop on violations
- [ ] Fix violations before proceeding

**After Implementation:**

- [ ] All validation checks passed
- [ ] Architecture compliance verified
- [ ] API contract adherence confirmed
- [ ] Done-log updated
- [ ] Execution run documented

---

## 🚫 Forbidden Patterns

### 1. Skipping Validation Steps

```typescript
// ❌ WRONG - Skipping validation
function implementFeature() {
  // ❌ Skip constitution check
  // ❌ Skip MVP scope check
  // ❌ Skip spec read
  // ❌ Skip API validation
  // ❌ Skip UX rules
  // Direct implementation
}
```

### 2. Implementing Without SpecKit

```rust
// ❌ WRONG - No specification
fn handle_stations(/*...*/) -> Result<ApiResponse<StationDto>, ApiError> {
  // ❌ No spec read
  // ❌ No API contract validated
  // Direct implementation
}
```

### 3. Scope Expansion

```typescript
// ❌ WRONG - Cross-MVP feature
function handle_auth(/*...*/) -> Result<LoginResponse, ApiError> {
  // ❌ No spec for MVP-1
  // ❌ No validation
  // Direct implementation
}
```

---

## 🎯 Deterministic Execution Success

**OpenCode is deterministic when:**

- [ ] Steps followed in order
- [ ] Validations performed at each step
- [ ] Stops on violations
- [ ] Architecture rules followed
- [ ] MVP scope maintained
- [ ] Documentation updated
- [ ] Execution history maintained

---

*This skill turns OpenCode into a deterministic execution engine, not a creative coder.*