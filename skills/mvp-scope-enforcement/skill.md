# MVP Scope Enforcement Skill — BorneMap

## Purpose
Prevent the biggest LLM failure mode: implementing features from future MVPs into current MVP.

---

## 🚫 The Problem

**LLM Failure Mode:**
> "Adding features from future MVPs into current MVP"

This silently expands scope, creates technical debt, and delays actual MVP completion.

---

## 🎯 Core Philosophy

**MVP-1 is not a sandbox. MVP-1 is a vertical slice.**

You build exactly what's needed for MVP-1. Nothing more. Nothing less.

---

## 🔒 Core Rules

### 1. Only Active MVP Scope Allowed

**Check before ANY implementation:**

```rust
// ❌ WRONG - Cross-MVP feature in MVP-1
// Handling authentication in MVP-1
pub async fn handle_login(
    LoginRequest { email, password }: LoginRequest,
) -> Result<LoginResponse, ApiError> {
    // ❌ MVP-1 doesn't have auth yet!
    authenticate_user(email, password).await?;
    Ok(LoginResponse { token: "..." })
}

// ✅ CORRECT - Only MVP-1 scope
pub async fn handle_get_stations(
    Query(params): Query<GetStationsQuery>,
) -> Result<ApiResponse<Vec<StationDto>>, ApiError> {
    // ✅ Only MVP-1 discovery features
    stations_service::get_all(params).await
        .map_err(|e| map_error(e))
}
```

**Scope Enforcement:**
- [ ] Only active MVP features
- [ ] No cross-MVP features
- [ ] No future MVP features
- [ ] No "nice to have" features

---

### 2. Strict Blocking of Cross-MVP Logic

**Cross-MVP logic is forbidden:**

```rust
// ❌ WRONG - Cross-MVP authentication in MVP-1
// handlers/auth.rs
pub async fn handle_login(/*...*/) -> Result<LoginResponse, ApiError> {
    // ❌ MVP-1 doesn't have auth yet!
    auth_service::login(/*...*/).await
}

// ❌ WRONG - Cross-MVP user management in MVP-1
// handlers/users.rs
pub async fn handle_get_users(/*...*/) -> Result<Vec<UserDto>, ApiError> {
    // ❌ MVP-1 doesn't have user management yet!
    users_service::get_all(/*...*/).await
}
```

**MVP Scope Examples:**

| MVP | Scope | Forbidden |
|-----|-------|-----------|
| MVP-1 | Map discovery, station search | Auth, admin, user management |
| MVP-2 | Station CRUD, user management | Auth, analytics dashboards |
| MVP-3 | Authentication, user management | Station CRUD, partner flows |

**Enforcement:**
- [ ] Check active MVP before implementation
- [ ] Verify feature is in allowed scope
- [ ] Block cross-MVP features

---

### 3. Feature Classification Required

**Every feature must be classified:**

```typescript
// ✅ CORRECT - Proper classification
// Feature: Station detail view
// MVP: MVP-1
// Type: Core feature (Required)
// Priority: High
// Scope: MVP-1 (Discovery Core)

// Feature: User authentication
// MVP: MVP-1
// Type: Future feature (Forbidden)
// Priority: N/A
// Scope: MVP-3 (Identity)
```

**Feature Classification Fields:**
1. **MVP:** Which MVP does this belong to?
2. **Type:** Core feature, enhancement, optional, future
3. **Priority:** Critical, important, optional, low
4. **Scope:** Which parts of the MVP

---

## 🚧 MVP Scope Enforcement Rules

### MVP-1 (Discovery Core) — CURRENT

**ALLOWED:**
- ✅ Map view (mobile + web)
- ✅ Station markers
- ✅ User location tracking
- ✅ Station discovery API
- ✅ Nearby station search
- ✅ Station detail view
- ✅ Basic analytics events

**FORBIDDEN:**
- ❌ Authentication / Login
- ❌ User management
- ❌ Admin dashboard
- ❌ Station CRUD operations
- ❌ Partner management
- ❌ Analytics dashboards
- ❌ Payment integration
- ❌ Booking system
- ❌ User reviews

---

### MVP-2 (Operations) — FUTURE

**ALLOWED:**
- ✅ Station CRUD operations
- ✅ Station status updates
- ✅ User management
- ✅ Admin dashboard
- ✅ Partner management
- ✅ Operational workflows
- ✅ Operational analytics

**FORBIDDEN:**
- ❌ Authentication (MVP-1 only)
- ❌ Station discovery (MVP-1 only)
- ❌ Map-only interface
- ❌ User-facing station discovery

---

### MVP-3 (Identity) — FUTURE

**ALLOWED:**
- ✅ Authentication service
- ✅ User registration
- ✅ JWT-based sessions
- ✅ Authorization
- ✅ User profile management

**FORBIDDEN:**
- ❌ Station management (MVP-2 only)
- ❌ Operational dashboards (MVP-2 only)
- ❌ Station discovery (MVP-1 only)

---

## 📋 Feature Classification Checklist

**Before implementing ANY feature:**

- [ ] Feature classified (MVP, type, priority, scope)
- [ ] Feature in active MVP allowed scope
- [ ] Feature not in forbidden scope
- [ ] Cross-MVP features identified
- [ ] Decision made (implement or ADR required)

---

## 🔒 Cross-MVP Detection

### Red Flags

**Detect cross-MVP features:**

1. **User mentions "users" or "users table":**
   - ❌ Cross-MVP → Requires ADR

2. **Mentions "admin" or "admin dashboard":**
   - ❌ Cross-MVP → Requires ADR

3. **Authentication/authorization logic:**
   - ❌ Cross-MVP → Requires ADR

4. **Payment processing:**
   - ❌ Cross-MVP → Requires ADR

5. **User settings/preferences:**
   - ❌ Cross-MVP → Requires ADR

6. **Complex filtering/sorting:**
   - ❌ Cross-MVP → Requires ADR

### ADR Required

**If feature is cross-MVP, require ADR:**

```markdown
# ADR-MVP1-XXX: Adding [Feature] to MVP-1

## Context
LLM suggested adding [feature] to MVP-1

## Decision
❌ REJECTED - [Feature] belongs to MVP-3, not MVP-1

## Rationale
- MVP-1 scope is discovery only
- [Feature] requires [dependency] not available in MVP-1
- Adding now creates technical debt

## Alternative Considered
- ✅ Keep MVP-1 scope pure
- ✅ Add to MVP-3 when auth is ready

## Next Steps
- Remove [feature] from MVP-1 scope
- Add to MVP-3 specification
- No code changes needed
```

---

## 🚦 Scope Enforcement Rules

### Before Implementation

**Every implementation must:**

1. **Identify Feature:**
   ```typescript
   // Feature: Station detail view
   // MVP: MVP-1
   // Type: Core feature
   // Priority: High
   ```

2. **Check Scope:**
   ```rust
   // ✅ MVP-1 allowed
   // ✅ Feature in allowed scope

   // ❌ MVP-1 forbidden
   // ❌ Feature not in allowed scope
   ```

3. **Validate Dependencies:**
   ```rust
   // ✅ All dependencies in MVP-1
   // ❌ Dependency outside MVP-1
   ```

4. **Get Approval:**
   - ✅ Feature approved for MVP-1
   - ❌ Feature requires ADR

---

### During Implementation

**While coding:**

1. **Monitor Scope:**
   - [ ] Staying within MVP-1 scope
   - [ ] No cross-MVP features
   - [ ] No future MVP features

2. **Check Dependencies:**
   - [ ] All dependencies available
   - [ ] No MVP-2+ dependencies
   - [ ] No future service dependencies

3. **Verify Completeness:**
   - [ ] Feature fully implemented
   - [ ] No partial implementations
   - [ ] Ready for MVP-1 release

---

### After Implementation

**After completing code:**

1. **Scope Review:**
   - [ ] Feature is MVP-1 only
   - [ ] No cross-MVP features
   - [ ] No future MVP features

2. **Documentation Update:**
   - [ ] Update scope-guard.md
   - [ ] Update sprint backlog
   - [ ] Update done-log.md

3. **Release Validation:**
   - [ ] MVP-1 release ready
   - [ ] No scope violations
   - [ ] No technical debt

---

## 🚫 Forbidden Patterns

### 1. Cross-MVP Features in MVP-1
```rust
// ❌ WRONG - User management in MVP-1
pub async fn handle_get_users(/*...*/) -> Result<Vec<UserDto>, ApiError> {
    // ❌ Cross-MVP: User management belongs to MVP-2
    users_service::get_all(/*...*/).await
}
```

### 2. "Nice to Have" Features
```rust
// ❌ WRONG - Advanced filtering in MVP-1
pub async fn handle_search(
    Query(params): Query<SearchQuery>,
) -> Result<ApiResponse<Vec<StationDto>>, ApiError> {
    // ❌ "Nice to have" in MVP-1
    // Should be MVP-2+ enhancement
    stations_service::search(params).await
}
```

### 3. Future MVP Features
```rust
// ❌ WRONG - Authentication in MVP-1
pub async fn handle_login(/*...*/) -> Result<LoginResponse, ApiError> {
    // ❌ Future MVP: Authentication belongs to MVP-3
    auth_service::login(/*...*/).await
}
```

---

## 🎯 MVP Scope Enforcement Checklist

**Before Implementing Any Feature:**

- [ ] Feature classified (MVP, type, priority, scope)
- [ ] Feature in active MVP allowed scope
- [ ] Feature not in forbidden scope
- [ ] Cross-MVP features identified
- [ ] Decision made (implement or ADR required)
- [ ] All dependencies in MVP-1
- [ ] No future MVP dependencies

**After Implementing Any Feature:**

- [ ] Feature is MVP-1 only
- [ ] No cross-MVP features
- [ ] No future MVP features
- [ ] Documentation updated
- [ ] Scope-guard verified
- [ ] Done-log updated

---

## 📊 MVP Scope Compliance

### Current MVP-1 Compliance

**Scope Enforcement Status:** ✅ COMPLIANT

**Features Implemented:**
- ✅ Map view (mobile + web)
- ✅ Station markers
- ✅ Station discovery API
- ✅ Nearby search
- ✅ Station detail view

**Scope Violations:**
- ❌ None detected

**Cross-MVP Features Blocked:**
- ❌ Authentication (blocked, requires MVP-3)
- ❌ User management (blocked, requires MVP-2)
- ❌ Admin dashboard (blocked, requires MVP-2)

---

## 🔄 Scope Evolution Rules

### MVP-1 → MVP-2

**Scope Expands:**
- Add station CRUD operations
- Add user management
- Add admin dashboard
- Add operational workflows

**Allowed Changes:**
- New services (admin-service)
- New endpoints
- Enhanced database schema
- New frontend packages

**Blocked Changes:**
- Authentication (MVP-3 only)
- Analytics dashboards (MVP-4+ only)

---

### MVP-2 → MVP-3

**Scope Expands:**
- Add authentication service
- Add user registration/login
- Add JWT authorization
- Add Keycloak integration

**Allowed Changes:**
- New service (auth-service)
- Authentication endpoints
- User management
- Authorization system

**Blocked Changes:**
- Station CRUD (MVP-2 only)
- Operational dashboards (MVP-4+ only)

---

## 🧠 MVP Scope Enforcement Logic

**Before ANY Implementation:**

```
1. Identify Feature
   ↓
2. Classify Feature
   - Which MVP?
   - What type?
   - What priority?
   ↓
3. Check Active MVP
   - Is feature in allowed scope?
   - Is feature in forbidden scope?
   ↓
4. Validate Dependencies
   - All dependencies available?
   - No future dependencies?
   ↓
5. Decision
   - ✅ Implement (in scope)
   - ❌ Block (cross-MVP)
   - ❌ ADR required (unclear)
```

---

*This skill prevents scope creep and ensures MVP-1 stays pure and deliverable.*