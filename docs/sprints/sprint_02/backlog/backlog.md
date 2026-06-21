# Sprint 1 — Identity & Security Core

**Status**: NOT_STARTED
**Constitution Version**: 1.15.2
**Dependencies**: Sprint 0 (CI pipeline, service skeletons, DB bootstrap)

---

## Must Have (Exit Criteria)

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S1-001 | Configure Keycloak realm `bornemap` | team | NOT_STARTED |
| S1-002 | Create Keycloak clients (mobile-driver, web-driver, admin-dashboard) | team | NOT_STARTED |
| S1-003 | Define hard-coded roles (driver, partner, admin) | team | NOT_STARTED |
| S1-004 | Implement JWT validation middleware (shared-infra/jwt.rs) | team | NOT_STARTED |
| S1-005 | Implement shared JWT module with Keycloak JWKS verification | team | NOT_STARTED |
| S1-006 | Implement JWT validation in auth-service middleware | team | NOT_STARTED |
| S1-007 | Implement JWT validation in driver-service middleware | team | NOT_STARTED |
| S1-008 | Implement JWT validation in admin-service middleware | team | NOT_STARTED |
| S1-009 | Create `users.user_profiles` table (UUID PK, role, preferences, timestamps) | team | NOT_STARTED |
| S1-010 | Implement JIT provisioning (upsert user on first valid JWT) | team | NOT_STARTED |
| S1-011 | Implement Traefik forward-auth JWT validation | team | NOT_STARTED |
| S1-012 | Configure gateway route protection by role | team | NOT_STARTED |
| S1-013 | Implement RBAC middleware (role extraction + route guard) | team | NOT_STARTED |
| S1-014 | Implement resource ownership checks (ABAC layer) | team | NOT_STARTED |
| S1-015 | Implement auth-service Keycloak sync endpoint | team | NOT_STARTED |
| S1-016 | Implement audit logging for login success/failure, token rejection | team | NOT_STARTED |

## Should Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S1-017 | Create integration tests for JWT validation | team | NOT_STARTED |
| S1-018 | Create integration tests for JIT provisioning | team | NOT_STARTED |
| S1-019 | Create CI identity validation gate (UUID detection) | team | NOT_STARTED |
| S1-020 | Create CI Keycloak dependency gate | team | NOT_STARTED |
| S1-021 | Create CI RBAC coverage check | team | NOT_STARTED |
| S1-022 | Create CI session consistency check | team | NOT_STARTED |

## Nice to Have

| ID | Task | Owner | Status |
|----|------|-------|--------|
| S1-023 | Create Keycloak realm export (infrastructure/keycloak/realm-bornemap.json) | team | NOT_STARTED |
| S1-024 | Add token blacklist support (logout enforcement) | team | NOT_STARTED |
| S1-025 | Add token replay detection (jti tracking) | team | NOT_STARTED |

## CI Additions (Sprint 1)

| ID | Gate | Rule |
|----|------|------|
| CI-1.1 | Identity Validation Gate | FAIL if users schema contains non-UUID IDs or nanoid in auth tables |
| CI-1.2 | Keycloak Dependency Gate | FAIL if any service other than auth-service calls Keycloak Admin API |
| CI-1.3 | RBAC Coverage Check | FAIL if any endpoint missing role guard |
| CI-1.4 | Session Consistency Check | FAIL if JWT role != platform_db role mapping |

## Exit Criteria

Sprint 1 is COMPLETE ONLY IF:
- [ ] Keycloak fully integrated with realm `bornemap`
- [ ] JWT validation working across all 3 services
- [ ] `user_profiles` synchronized correctly (JIT provisioning)
- [ ] RBAC enforced everywhere (no bypass routes exist)
- [ ] Identity validation CI gate passes
- [ ] Keycloak gate enforced
- [ ] Session consistency verified
