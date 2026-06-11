# OpenCode Execution Rules — BorneMap

**Version:** 1.0
**Scope:** All AI coding agents (OpenCode or equivalent)

---

## 1. Execution Principle

BorneMap is a constitution-driven system. Code generation tools do not decide architecture — they only implement it.

All execution MUST follow:
- Constitution (v1.0)
- ADR decisions
- Repository structure rules

---

## 2. Agent Role

An OpenCode-like agent is a **deterministic implementation engine**, not an architect.

### Allowed
- Generate code inside /source
- Refactor existing modules
- Implement features from specs
- Generate tests
- Fix bugs
- Update documentation in /docs

### Forbidden
- Designing architecture changes
- Changing database schema without migration file + ADR
- Modifying service boundaries
- Introducing new services
- Changing authentication model
- Creating files outside /source

---

## 3. Repository Boundary Rules

| Path | Purpose |
|---|---|
| /source | runtime applications (ONLY allowed code) |
| /docs | architecture, ADRs, specs |
| /infra | deployment, Docker, Traefik |
| /scripts | tooling, migrations |

**Forbidden:**
- Writing runtime code outside /source
- Modifying /infra without explicit instruction
- Editing /docs unless updating documentation only
- Creating hidden or duplicate service folders

---

## 4. Service Boundaries

| Service | Domain |
|---|---|
| driver-service | discovery only |
| admin-service | management only |
| clickstream-service | analytics ingestion |
| auth-gateway | identity abstraction |

**RULE:** A service MUST NOT import or depend on another service's internal logic.

---

## 5. Database Rules

| DB | Purpose | Rules |
|---|---|---|
| platform_db | system of record | normalized, PostGIS, source of truth |
| analytics_db | append-only events | INSERT only, no UPDATE/DELETE |
| keycloak_db | identity | NEVER accessed directly |

### Schema Ownership

| Schema | Owner |
|---|---|
| inventory | admin-service |
| gis | driver-service (read-only) |
| users | auth-gateway (MVP-3) |

---

## 6. Authentication Rules

- Keycloak is internal-only
- No frontend direct access
- Authentication must go through backend gateway
- All services MUST validate JWT before processing requests
- Partner scoping: `WHERE partner_id = JWT.partner_id`

---

## 7. API Rules

- ALL endpoints MUST follow `/api/v1/*`
- No versionless APIs
- No mixed versioning during MVP-1 to MVP-3
- No undocumented endpoints

---

## 8. Code Generation Rules

- Small, composable modules
- No monolithic service files
- Strict separation: controllers, services, repositories, domain models
- No duplicated business logic across services
- Shared code: DTOs, validation helpers, utilities only
- Services MUST NOT call each other directly (HTTP API or event system only)

---

## 9. UX-First Rules

- Mobile Driver App is primary product surface
- Skeleton loaders instead of spinners
- Optimistic UI updates
- Gesture-first navigation
- Haptic feedback for primary actions
- Animations via Reanimated only
- No hardcoded design tokens
- MapContainer.tsx is the ONLY file allowed to handle map platform differences

---

## 10. Data Integrity

- platform_db = source of truth
- GIS = read-only derived data
- analytics = immutable event log
- Foreign keys must always be enforced
- No orphan records allowed in inventory domain
- Soft delete only for infrastructure entities

---

## 11. MVP Enforcement

| MVP | Allowed scope |
|---|---|
| MVP-1 | discovery only |
| MVP-2 | admin features |
| MVP-3 | auth + RBAC |
| MVP-4 | analytics |
| MVP-5 | performance |
| MVP-6 | infra + production |

**RULE:** No feature from a future MVP may be implemented early without explicit ADR approval.

### 11.1 Delivery Model

- MVP-driven incremental delivery
- Strict vertical slices (not layer-first)
- Each MVP MUST end with a stabilization sprint
- No cross-MVP feature leakage

### 11.2 Vertical Slice Rule

Each MVP MUST include:
- Backend (service + DB)
- Frontend (app / UI)
- Integration (end-to-end wiring)
- Stabilization (testing + polish)

### 11.3 Global Execution Flow (per MVP)

```
Design → Backend → Frontend → Integration → Testing → Stabilization
```
Applied per MVP, not globally parallel.

---

## 12. Security Rules

- No external DB access from clients
- No bypassing backend validation
- No secret leakage in logs
- All inputs validated at service layer

---

## 13. Testing Rules

- Unit tests for services
- Integration tests for APIs
- E2E for discovery flow

---

## 14. Non-Negotiable Rules

- All runtime code under /source
- No API gateway
- /api/v1/* mandatory
- platform_db is authoritative
- analytics is append-only
- GIS is read-only
- Keycloak is internal-only
- No service overlap allowed
- No architecture changes without ADR
