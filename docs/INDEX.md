# BorneMap Documentation Index

**Generated:** June 2026  
**Status:** Complete - Pre-Sprint Phase

---

## 📋 Core Documentation

### 1. **SYSTEM_STATE.md** (8.1 KB)
**Current architecture state and component status**

- System overview & mission
- Service topology (Auth, Driver, Admin)
- Frontend applications (mobile, web, dashboard)
- Database state (platform_db, keycloak_db, analytics_db)
- API contract status
- Technology stack approval
- Identity system configuration
- Sprint execution state
- Known issues & blockers
- Governance model

**Use When:** You need to understand the current state of each component and what's implemented.

---

### 2. **architecture.md** (21 KB)
**Detailed system design & topology**

- System context diagram
- Service topology (3 services only)
  - Auth Service (:3000)
  - Driver Service (:3001)
  - Admin Service (:3002)
- Frontend applications (3 apps)
- Data schema architecture
  - platform_db (gis, inventory, users schemas)
  - keycloak_db (isolated)
  - analytics_db (write-only)
- Caching strategy (Redis)
- API gateway routing (Traefik)
- Identity & authentication flow
- Data flow examples
- Deployment topology (Docker Compose)
- Architectural constraints & rules
- Scaling & HA notes

**Use When:** You need to understand how the system is designed, data flows, and service boundaries.

---

### 3. **auth-flow.md** (28 KB)
**Authentication & authorization deep dive**

- Keycloak configuration
  - Realm: bornemap
  - 3 OAuth2 clients (mobile, web, dashboard)
  - Role model (driver, partner, admin)
- Registration flow
- Login flow (OAuth2 Authorization Code)
- Token exchange
- Authenticated request flow
- JWT token structure
- Refresh token flow
- Logout flow
- Role-based access control (RBAC)
- Security considerations
  - Token storage (memory vs localStorage vs Keychain)
  - JWT validation
  - CORS & CSRF
  - Rate limiting
- Error scenarios
- Integration points
- Testing auth flows
- Configuration & secrets

**Use When:** You're implementing authentication, need JWT details, or troubleshooting auth flows.

---

### 4. **api-contracts.md** (17 KB)
**OpenAPI contract specifications**

- OpenAPI structure & location
- Gateway routing (Traefik proxy)
- Auth Service API (:3000)
  - /register, /login, /refresh, /logout, /me, /profile
- Driver Service API (:3001)
  - /stations, /search, /chargers, /favorites, /reviews
- Admin Service API (:3002)
  - Partners CRUD, Stations CRUD, Chargers CRUD, Analytics, Audit logs
- Common response schemas
- Error response format
- Pagination schema
- Entity ID format (PREFIX-nanoid(12))
- Authentication & authorization
- Token format & claims
- Role-based access
- Validation rules
- Rate limiting
- API versioning

**Use When:** Implementing API endpoints or understanding request/response contracts.

---

### 5. **roadmap_status.md** (10 KB)
**Sprint progress tracking & timeline**

- Overall progress status
- Sprint planning (7 sprints)
  - SPRINT-001: Auth Service (CRITICAL, gating)
  - SPRINT-002: Driver Service Spatial API
  - SPRINT-003: Admin Service Partner Management
  - SPRINT-004: Mobile App (Expo)
  - SPRINT-005: Web Driver App (React)
  - SPRINT-006: Dashboard Portal (React)
  - SPRINT-007: Integration & E2E Testing
- Feature completion matrix
- Known blockers & risks
- Dependency graph
- Testing strategy
- Timeline estimate (~42 days total)
- Success metrics
- Post-validation expansion candidates

**Use When:** Planning sprints, understanding dependencies, or checking progress.

---

### 6. **sprint_backlog.md** (11 KB)
**Deferred work, backlog items, and technical debt**

- Outstanding tasks (pre-sprint)
  - Infrastructure setup (6 items)
  - Database migrations (6 items)
  - OpenAPI specs (6 items)
  - Shared code libraries (10 items)
  - CI/CD pipeline (6 items)
- Deferred features (out of scope for validation)
  - OCPP integration
  - Payment processing
  - Smart charging
  - Real-time telemetry
  - Event-driven architecture
  - Advanced analytics
  - Service mesh
  - Kubernetes
  - Native mobile features
  - LDAP/AD federation
  - i18n support
  - Advanced caching
- Known technical debt
- Documentation gaps
- Testing gaps
- Backlog by priority
- Sprint readiness checklist
- Risk assessment
- External dependencies
- Future enhancements

**Use When:** Understanding scope boundaries, planning post-validation work, or reviewing blockers.

---

### 7. **GUARDRAILS.md** (Inherited from governance)
**Execution standards & code quality rules**

Found in root project governance, enforced via SpecKit CI validator.

Key rules:
- Non-negotiable blockers (unwrap, raw SQL, any, localStorage, etc.)
- Architecture rules (3-service topology, API-first, dependency direction)
- Identity rules (Keycloak model, JWT validation, token storage)
- Database rules (schema isolation, transactions, cross-schema prevents)
- Redis rules (service ownership, cache invalidation)
- Frontend rules (API access, UX states, map interactions)
- Shared code boundaries
- Security rules
- Build system rules
- Session discipline

**Use When:** Reviewing code, checking architecture compliance, or enforcing standards.

---

## 🚀 How to Use This Documentation

### For Architects
1. Start with **architecture.md** (understand system design)
2. Review **SYSTEM_STATE.md** (current status)
3. Check **roadmap_status.md** (sprint dependencies)

### For Backend Engineers (Rust/Actix)
1. Read **architecture.md** (service boundaries)
2. Study **api-contracts.md** (endpoint specs)
3. Review **auth-flow.md** (authentication details)
4. Check **GUARDRAILS.md** (code standards)

### For Frontend Engineers (React/Expo)
1. Read **architecture.md** (service topology)
2. Study **api-contracts.md** (API endpoints)
3. Review **auth-flow.md** (token handling)
4. Check **GUARDRAILS.md** (frontend rules)

### For DevOps/Infrastructure
1. Start with **architecture.md** (deployment topology)
2. Review **sprint_backlog.md** (infrastructure tasks)
3. Check **roadmap_status.md** (timeline)

### For QA/Testing
1. Read **roadmap_status.md** (testing strategy)
2. Study **api-contracts.md** (API expectations)
3. Review **auth-flow.md** (authentication scenarios)
4. Check **sprint_backlog.md** (testing gaps)

### For Project Managers
1. Start with **roadmap_status.md** (sprint planning)
2. Review **sprint_backlog.md** (blockers & risks)
3. Check **SYSTEM_STATE.md** (current status)

---

## 📚 Document Relationships

```
┌─────────────────────────────────────┐
│ SYSTEM_STATE.md                    │ ← Current state
│ (What we have now)                 │
└──────────────────────────────┬──────┘
                               │
    ┌──────────────────────────┼──────────────────────────┐
    │                          │                          │
    ↓                          ↓                          ↓
┌──────────────────┐  ┌─────────────────────┐  ┌──────────────────┐
│ architecture.md  │  │ roadmap_status.md  │  │ sprint_backlog.md│
│ (How it works)   │  │ (Where we're going)│  │ (What's deferred)│
└──────────────────┘  └─────────────────────┘  └──────────────────┘
        │                      │                        │
        ├──────────────────────┼────────────────────────┤
        │                      │                        │
        ↓                      ↓                        ↓
    ┌──────────────┐    ┌────────────────┐    ┌──────────────────┐
    │ api-          │    │ auth-flow.md   │    │ GUARDRAILS.md    │
    │ contracts.md  │    │ (Security)     │    │ (Standards)      │
    │ (Specs)       │    └────────────────┘    └──────────────────┘
    └──────────────┘
```

---

## ✅ Document Status

| Document | Size | Status | Last Updated |
|----------|------|--------|--------------|
| SYSTEM_STATE.md | 8.1 KB | ✅ Complete | June 2026 |
| architecture.md | 21 KB | ✅ Complete | June 2026 |
| auth-flow.md | 28 KB | ✅ Complete | June 2026 |
| api-contracts.md | 17 KB | ✅ Complete | June 2026 |
| roadmap_status.md | 10 KB | ✅ Complete | June 2026 |
| sprint_backlog.md | 11 KB | ✅ Complete | June 2026 |
| **TOTAL** | **95 KB** | ✅ Ready | June 2026 |

---

## 🎯 Next Steps

**Before Sprint Execution:**
1. Review **SYSTEM_STATE.md** (verify understanding)
2. Review **roadmap_status.md** (understand sprint sequence)
3. Review **sprint_backlog.md** (identify pre-sprint blockers)
4. Provision infrastructure (per backlog checklist)
5. Generate OpenAPI clients (TypeScript & Rust)

**During Sprint Execution:**
1. Reference **api-contracts.md** (implement per spec)
2. Check **GUARDRAILS.md** (code review against standards)
3. Reference **auth-flow.md** (authentication scenarios)
4. Update **SYSTEM_STATE.md** (track progress)

**Post-Sprint:**
1. Update **SYSTEM_STATE.md**
2. Update **roadmap_status.md**
3. Update **sprint_backlog.md**
4. Review for next sprint

---

## 📖 Reading Order Recommendations

**Time: 15 minutes (Overview)**
1. SYSTEM_STATE.md (skim)
2. roadmap_status.md (read sprint titles)

**Time: 45 minutes (Full Understanding)**
1. architecture.md (full read)
2. roadmap_status.md (full read)
3. SYSTEM_STATE.md (detailed review)

**Time: 2+ hours (Deep Dive)**
1. All 6 documents in full detail
2. Cross-reference between documents
3. Study data flows and service boundaries

---

**Ready to execute sprints! See `roadmap_status.md` to begin SPRINT-001.**
