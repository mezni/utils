# Sprint 12: Authorization Expansion & Fine-Grained ACL

**Duration**: 2026-06-27  
**Status**: 🚧 In Progress  
**Focus**: Evolve authorization from simple RBAC to a reusable authorization foundation

## Scope

- **shared/bornemap-auth/**: Expand role system and normalize JWT claims
- **shared/bornemap-authz/** (new): Create shared authorization crate
- **services/auth-service/**: Update authorization logic and extractors
- **future driver-service/** and **admin-service/**: Prepare for future services

## Goals

1. **Expand the role system beyond ADMIN**
   - Implement canonical role model: REGISTERED_DRIVER, PARTNER, ADMIN
   - Ensure type safety and normalization

2. **Centralize authorization logic**
   - Create shared authorization crate
   - Generalize role enforcement
   - Prepare for policy-based access control

3. **Introduce resource ownership**
   - Implement ownership authorization independent of HTTP handlers
   - Support partner-owned resources

4. **Update frontend authorization**
   - Extend authentication state with normalized roles
   - Implement role-aware UI rendering

## Objectives

### 1. Expand the Role Model

**Task**: Implement a canonical role model

**Supported Roles**:
- REGISTERED_DRIVER
- PARTNER
- ADMIN

**Requirements**:
- Strongly typed enum
- Parsing from JWT claims
- Serialization
- Normalization
- Exhaustive matching

### 2. Normalize JWT Role Claims

**Task**: Ensure JWT generation and validation always use canonical role values

**Requirements**:
- Normalize role names
- Reject unknown roles
- Reject missing role claims
- Prevent legacy role strings
- Keep backward compatibility only if explicitly configured

### 3. Create Shared Authorization Crate

**Task**: Create `shared/bornemap-authz/`

**Responsibilities**:
- Authorization helpers
- Role guards
- Ownership guards
- Future policy engine

**Goal**: Single authorization entry point for every service

### 4. Generalize Role Enforcement

**Task**: Replace the Admin-only guard with generic authorization

**Requirements**:
- `require_role(...)` for one role
- `require_any_role(...)` for multiple allowed roles
- Reusable across services
- No service-specific logic

**Example Usages**:
- Admin only
- Partner only
- Driver only
- Admin + Partner
- Admin + Driver

### 5. Introduce Resource Ownership Authorization

**Task**: Implement ownership authorization independent from HTTP handlers

**Requirements**:
- Owner-based authorization
- Admin bypass
- Reusable helper
- No database access inside guard

**Example Ownership Types**:
- User
- Partner
- System

### 6. Add Authorization Policies

**Task**: Introduce lightweight policy objects for complex authorization

**Examples**:
- UpdateStationPolicy
- DeleteStationPolicy
- ManagePartnerPolicy

**Responsibilities**:
- Combine role checks
- Combine ownership checks
- Support future business rules
- Independent from transport (HTTP)

### 7. Update CurrentUser Extractor

**Task**: Enhance CurrentUser with additional context

**Requirements**:
- Expose: user_id, role, subject, optional partner_id
- Validate JWT
- Parse normalized roles
- Reject malformed claims
- Expose typed authorization context

### 8. Add Role-Aware Helper Methods

**Task**: Provide convenience helpers

**Examples**:
- `is_admin()`
- `is_partner()`
- `is_registered_driver()`

**Requirements**: Internally rely on the canonical role model

### 9. Prepare Partner Ownership Model

**Task**: Introduce authorization primitives for partner-owned resources

**Examples**:
- Station
- Charger
- Pricing
- Opening hours

**Note**: No database migration required yet

### 10. Update Frontend Authorization

**Task**: Extend frontend authentication state

**Requirements**:
- Store normalized role
- Decode JWT safely
- Centralized authorization helpers
- Role-aware UI rendering

**Note**: Frontend authorization is UX only; backend remains authoritative

### 11. Implement Role-Protected Routes

**Task**: Support routes protected by allowed roles

**Examples**:
- Admin pages
- Partner dashboard
- Driver-only pages

**Requirements**:
- Unauthorized users redirected to access-denied page

### 12. Documentation

**Task**: Update documentation with:
- Role hierarchy
- Authorization architecture
- Ownership model
- Policy guidelines
- Examples for new services

## Deliverables

### Shared
- [ ] Canonical Role enum
- [ ] JWT role normalization
- [ ] Authorization helpers
- [ ] Generic role guard
- [ ] Ownership guard
- [ ] Policy interfaces

### Backend
- [ ] Updated CurrentUser extractor
- [ ] Generic role enforcement
- [ ] Centralized authorization module
- [ ] Policy-ready authorization architecture

### Frontend
- [ ] Normalized role handling
- [ ] Role-aware UI rendering
- [ ] Protected routes
- [ ] Unauthorized page

## Out of Scope

- Station ownership persistence
- Partner CRUD
- Driver service implementation
- Full ACL permission matrix
- Attribute-Based Access Control (ABAC)
- External authorization engines (e.g. OpenFGA, OPA)

## Done When

- [ ] Canonical role model implemented
- [ ] JWT roles are normalized and validated
- [ ] Authorization logic is centralized in bornemap-authz
- [ ] Generic role enforcement replaces admin-specific guards
- [ ] Resource ownership authorization is implemented
- [ ] Policy-based authorization foundation exists
- [ ] Backend remains the sole authority for access control
- [ ] Frontend performs role-aware rendering only
- [ ] Architecture is ready for partner-owned resources in the next sprint
- [ ] Documentation reflects the new authorization architecture

## Architecture Overview

### New Shared Crate Structure

```
shared/
├── bornemap-auth/           # Enhanced with role normalization
│   ├── src/
│   │   ├── jwt_validator.rs
│   │   ├── rbac.rs          # Enhanced role model
│   │   └── lib.rs
│   └── Cargo.toml
└── bornemap-authz/          # New shared authorization crate
    ├── src/
    │   ├── authorization.rs
    │   ├── guards.rs
    │   ├── ownership.rs
    │   ├── policies.rs
    │   └── lib.rs
    └── Cargo.toml
```

### Updated Backend Architecture

```
HTTP Layer
    ↓
Authentication Middleware (JWT validation with normalized roles)
    ↓
CurrentUser Extractor (enhanced with subject, partner_id)
    ↓
Authorization Middleware (bornemap-authz)
    ↓
Role/Ownership Guards
    ↓
Policy Evaluation
    ↓
Protected Route Handler
```

### Frontend Authorization Flow

```
Route Guard → Role Check → Ownership Check → Policy Check → Component
    ↓
Auth Store (normalized roles, subject info)
    ↓
API Client (role-aware requests)
    ↓
Permission Components (conditional rendering)
```

## Implementation Plan

### Phase 1: Core Role System (Days 1-2)
1. Implement canonical Role enum in bornemap-auth
2. Add JWT role normalization
3. Update JWT validation to reject non-canonical roles
4. Update CurrentUser extractor to use normalized roles

### Phase 2: Shared Authorization Crate (Days 3-4)
1. Create bornemap-authz crate structure
2. Implement generic role guards
3. Implement ownership guards
4. Create policy interfaces

### Phase 3: Backend Integration (Days 5-6)
1. Replace admin-specific guards with generic ones
2. Add ownership authorization to admin endpoints
3. Implement policies for station management
4. Update auth-service to use new authorization system

### Phase 4: Frontend Updates (Days 7-8)
1. Update auth store to handle normalized roles
2. Implement role-protected routes
3. Create permission-aware UI components
4. Add unauthorized page

### Phase 5: Testing & Documentation (Days 9-10)
1. Comprehensive testing of authorization system
2. Update documentation
3. Integration testing with frontend
4. Performance testing

## Key Decisions

1. **Role Hierarchy**: ADMIN > PARTNER > REGISTERED_DRIVER (with admin bypass)
2. **Ownership Model**: Each resource has an owner (User, Partner, System)
3. **Policy Evaluation**: Simple role + ownership checks now, extensible for business rules
4. **Frontend Separation**: UI rendering only, backend remains authoritative

## Risk Assessment

**High Risk Areas**:
- JWT role normalization breaking existing tokens
- Performance impact of ownership checks
- Frontend-backend role synchronization

**Mitigation Strategies**:
- Gradual rollout with feature flags
- Caching for ownership checks
- Comprehensive test coverage

## Success Metrics

- 100% test coverage for authorization components
- No performance degradation (< 10ms overhead for auth checks)
- All existing routes continue to work
- New authorization system is used for all new endpoints