# Skill System Loader — BorneMap

## Version: 1.0
## Status: Active
## Core Philosophy: Skills are not documentation. They are execution constraints for LLM behavior.

---

## 🎯 PURPOSE

**Turn OpenCode into a deterministic execution engine through skill enforcement.**

This file orchestrates all LLM execution through strict skill constraints.

---

## 🚦 SKILL HIERARCHY

**When skills conflict:**

```
security > architecture > documentation
```

---

## 🧠 SKILL LIST

### 🔴 MUST HAVE (Non-Negotiable)

These skills must ALWAYS be enforced:

1. **[API Contract Discipline](./api-contract-discipline/)**
   - Enforces /api/v1/* strictness
   - Ensures typed responses
   - Prevents breaking changes
   - Requirement: All endpoints must be typed and versioned

2. **[MVP Scope Enforcement](./mvp-scope-enforcement/)**
   - Enforces active MVP scope
   - Blocks cross-MVP features
   - Prevents scope creep
   - Requirement: Only active MVP features allowed

3. **[Frontend Architecture Discipline](./frontend-architecture-discipline/)**
   - Enforces MapContainer usage
   - Prevents direct API calls
   - Enforces state separation
   - Requirement: MapContainer abstraction mandatory

4. **[LLM Execution Control](./llm-execution-control/)**
   - Enforces step-by-step execution
   - Requires validation before coding
   - Prevents jumping ahead
   - Requirement: Complete all validation steps

### 🟠 HIGH VALUE

These skills should be enforced when possible:

5. **[Data Ownership](./data-ownership/)**
   - Enforces database ownership
   - Prevents cross-service corruption
   - Enforces strict schema boundaries
   - Requirement: Each service owns its schemas

6. **[Testing Enforcement](./testing-enforcement/)**
   - Enforces test coverage
   - Requires MVP checkpoint validation
   - Prevents code without tests
   - Requirement: All features must have tests

### 🟡 ADVANCED

These skills provide additional safety:

7. **[Security Evolution](./security-evolution/)**
   - Enforces security patterns
   - Prevents common security issues
   - Adapts security to MVP progression
   - Requirement: Security based on MVP stage

8. **[Design System Enforcement](./design-system-enforcement/)**
   - Enforces design token usage
   - Prevents hardcoded styling
   - Ensures UI consistency
   - Requirement: All styling through tokens

9. **[Bug Learning System](./bug-learning-system/)**
   - Enforces bug root cause analysis
   - Prevents repeated bugs
   - Turns bugs into prevention
   - Requirement: Every bug produces prevention rule

---

## 🔄 EXECUTION FLOW

### Before ANY Code Generation

**LLM MUST CHECK ALL SKILLS:**

```
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
6. READ RUST CLEAN ARCHITECTURE
   ↓
7. READ FRONTEND ARCHITECTURE
   ↓
8. READ DATA OWNERSHIP
   ↓
9. READ TESTING ENFORCEMENT
   ↓
10. VALIDATE CONSTRAINTS
    ↓
11. IMPLEMENT CODE
    ↓
12. ADD TESTS
    ↓
13. VALIDATE AGAINST ALL SKILLS
```

---

## 🚫 CONFLICT RESOLUTION

### Skill Priority

**When skills conflict:**

1. **Security > Architecture > Documentation**

   **Example:**
   - Security says "add authentication" → Architecture says "MVP-1 no auth"
   - **Resolution:** Architecture wins (MVP-1 scope constraint)

2. **Documentation > Architecture**

   **Example:**
   - Architecture says "new service" → Documentation says "no ADR"
   - **Resolution:** Documentation wins (must document before change)

3. **Architecture > Code**

   **Example:**
   - Code says "direct DB access" → Architecture says "use repository"
   - **Resolution:** Architecture wins (layer separation)

---

## 🧪 VALIDATION GATES

### Pre-Implementation Validation

**Before writing ANY code:**

```
✅ Constitution Read
   ✅ Active MVP Checked
   ✅ SpecKit Read
   ✅ API Contract Validated
   ✅ UX Rules Confirmed
   ✅ Rust Architecture Checked
   ✅ Frontend Architecture Checked
   ✅ Data Ownership Checked
   ✅ Testing Requirements Checked
   ✅ All Validation Passed
   → Proceed to Implementation
```

### Post-Implementation Validation

**After writing ANY code:**

```
❌ Architecture Violation Detected
   → STOP and Fix

❌ API Contract Violation Detected
   → STOP and Fix

❌ Frontend Rule Violation Detected
   → STOP and Fix

❌ Data Ownership Violation Detected
   → STOP and Fix

❌ Testing Missing
   → STOP and Add Tests

❌ MVP Scope Violation Detected
   → STOP and Fix
```

---

## 📋 COMPLETE CHECKLIST

**Before Implementing Any Feature:**

- [ ] Constitution read and understood
- [ ] Active MVP confirmed
- [ ] SpecKit read
- [ ] API contract validated
- [ ] UX rules confirmed
- [ ] Rust architecture rules followed
- [ ] Frontend architecture rules followed
- [ ] Data ownership rules followed
- [ ] Testing requirements checked
- [ ] No architecture violations
- [ ] No API contract violations
- [ ] No frontend violations
- [ ] No data ownership violations
- [ ] No testing violations
- [ ] No MVP scope violations

**After Implementing Any Feature:**

- [ ] Constitution validated
- [ ] Active MVP validated
- [ ] SpecKit validated
- [ ] API contract validated
- [ ] UX rules validated
- [ ] Rust architecture validated
- [ ] Frontend architecture validated
- [ ] Data ownership validated
- [ ] Testing validated
- [ ] MVP scope validated
- [ ] Done-log updated
- [ ] Documentation updated

---

## 🎯 LLM EXECUTION MODEL

### Deterministic Execution

**OpenCode MUST follow this flow:**

1. **Read Constitution**
   - Understand core principles
   - Identify forbidden behaviors

2. **Check Active MVP**
   - Confirm MVP scope
   - Identify forbidden features

3. **Read SpecKit**
   - Understand requirements
   - Identify inputs/outputs/constraints

4. **Validate API Contract**
   - Verify endpoint definitions
   - Confirm request/response shapes

5. **Confirm UX Rules**
   - Validate interaction patterns
   - Check state management rules

6. **Read Rust Clean Architecture**
   - Verify layer separation
   - Check repository pattern
   - Validate PostGIS isolation

7. **Read Frontend Architecture**
   - Verify MapContainer usage
   - Check state separation
   - Validate API client usage

8. **Read Data Ownership**
   - Verify schema ownership
   - Check cross-service access
   - Validate schema boundaries

9. **Read Testing Enforcement**
   - Verify test requirements
   - Check coverage targets
   - Validate MVP checkpoint

10. **Validate Constraints**
    - Check all rules
    - Identify violations
    - Fix violations if any

11. **Implement Code**
    - Follow all architecture rules
    - Use correct patterns
    - Implement correctly

12. **Add Tests**
    - Write unit tests
    - Write integration tests
    - Write E2E tests

13. **Validate Again**
    - Check all rules
    - Verify test coverage
    - Confirm MVP scope

14. **Log Changes**
    - Update done-log
    - Record issues
    - Update documentation

---

## 🚫 FORBIDDEN EXECUTION PATTERNS

### Pattern 1: Skipping Validation

```rust
// ❌ WRONG - No validation
function implementFeature() {
  // ❌ Skip constitution
  // ❌ Skip MVP check
  // ❌ Skip spec read
  // ❌ Skip API validation
  // ❌ Skip UX rules
  // ❌ Skip architecture checks
  // ❌ Skip testing requirements
  // ❌ No validation
  // Direct implementation
}
```

### Pattern 2: Violating Architecture

```typescript
// ❌ WRONG - Architecture violation
function StationMarker({ station }) {
  // ❌ Direct map usage
  import MapView from 'react-native-maps';

  // ❌ Direct API call
  fetch('/api/v1/stations');

  // ❌ No tests
  // No validation
}
```

### Pattern 3: Testing After Code

```rust
// ❌ WRONG - Tests after code
fn handle_update_station(/*...*/) -> Result<ApiResponse<()>, ApiError> {
  // ❌ Code first
  sqlx::query(/*...*/).execute(/*...*/).await?;

  // ❌ No tests
}

// ✅ CORRECT - Tests first
#[cfg(test)]
mod tests {
  #[test]
  fn test_update_station(/*...*/) {
    // ✅ Write tests first
  }
}

fn handle_update_station(/*...*/) -> Result<ApiResponse<()>, ApiError> {
  // ✅ Code after tests
  // ✅ Follows all rules
}
```

---

## 📊 SKILL COMPLIANCE METRICS

### Current Compliance

**MVP-1 Compliance Status:** ✅ COMPLIANT

**Skills Enforced:**
- ✅ API Contract Discipline
- ✅ MVP Scope Enforcement
- ✅ Frontend Architecture Discipline
- ✅ LLM Execution Control
- ✅ Data Ownership
- ✅ Testing Enforcement

**Violations:**
- ❌ None detected

**Compliance Rate:** 100%

---

## 🔄 CONTINUOUS VALIDATION

### Real-Time Monitoring

**During Implementation:**

```
Every Line of Code:
  → Check architecture rules
  → Check API contract
  → Check frontend rules
  → Check data ownership
  → Check testing requirements

Every Feature Complete:
  → Run all validation checks
  → Verify all tests passing
  → Confirm MVP scope
  → Validate architecture
  → Update documentation
```

---

## 🎯 FINAL PRINCIPLE

**Skills are not documentation. They are execution constraints for LLM behavior.**

This skill system ensures:

- ✅ Deterministic execution
- ✅ Zero architecture violations
- ✅ Complete test coverage
- ✅ Strict scope enforcement
- ✅ Consistent API contracts
- ✅ Proper frontend architecture
- ✅ Correct data ownership
- ✅ Comprehensive testing

---

*This skill loader orchestrates all LLM execution through strict skill constraints, turning OpenCode into a deterministic system.*