# BorneMap Guardrails

**Status**: System Safety Layer
**Scope**: Runtime protection, failure prevention, and operational boundaries

---

## 1. Architectural Guardrails

### 1.1 Service Topology Guard
- Exactly 3 services: `auth-service`, `driver-service`, `admin-service`
- No additional services, no renames, no merging
- CI rejects any topology drift

### 1.2 Database Topology Guard
- Only `platform_db`, `analytics_db`, `keycloak_db` exist
- No additional databases permitted
- Schema ownership is absolute

### 1.3 Identity Guard
- Users: UUID only (Keycloak sub)
- Entities: PREFIX-nanoid(12) only
- No cross-format mixing allowed

---

## 2. Data Access Guardrails

### 2.1 Analytics Write Guard
- ONLY `driver-service` may write to `analytics_db`
- `admin-service`: READ ONLY
- `auth-service`: NO ACCESS

### 2.2 Cross-Schema Write Guard
- No service may write outside its owned schema
- Violation = HARD STOP in CI

### 2.3 Soft Delete Guard
- All inventory operations use soft delete
- Hard deletes on inventory tables = FAIL

---

## 3. Dependency Guardrails

### 3.1 Import Guard
- No service-to-service imports
- No frontend-to-backend imports
- No circular dependencies

### 3.2 Package Boundary Guard
- `ui-kit`: UI only (no API, no logic)
- `domain-types`: contracts only (no runtime logic)
- `client-core`: transport only (no business logic)
- `shared-infra`: infra only (no business logic)

---

## 4. Query Safety Guardrails

### 4.1 SQLx Guard
- All queries MUST be compile-time verified
- No runtime SQL string construction
- No dynamic query generation

### 4.2 Spatial Query Guard
- Max radius: 50km
- Max results: 500
- Max bbox area: predefined constant
- All PostGIS queries must have LIMIT clause

### 4.3 Analytics Query Guard
- `admin-service`: READ ONLY queries only
- No INSERT/UPDATE/DELETE on analytics_db
- SQLx compile-time queries only

---

## 5. Security Guardrails

### 5.1 Auth Guard
- JWT validation required on all endpoints
- Issuer MUST match realm `bornemap`
- Expired tokens rejected
- Role claim required

### 5.2 Rate Limit Guard

| Role    | Limit       |
|---------|-------------|
| public  | 60 req/min  |
| driver  | 300 req/min |
| partner | 200 req/min |
| admin   | 500 req/min |

### 5.3 Payload Guard
- Max export rows: 1000
- Max response payload: 1MB
- Pagination enforced on all list endpoints

---

## 6. Operational Guardrails

### 6.1 Migration Guard
- Forward-only migrations
- No destructive rollback
- SQLx compatibility required
- CI validation required

### 6.2 Deployment Guard
- CI must pass fully before deploy
- Schema version must match across environments
- Health checks required post-deploy

### 6.3 Rollback Guard
- Last-known-good deployment snapshot available
- Automated rollback on health check failure
- Rollback script required for every migration

---

## 7. Enforcement Actions

| Violation                      | Action       |
|--------------------------------|--------------|
| New service introduced        | REJECT       |
| analytics_db write violation  | REJECT       |
| Identity format violation     | REJECT       |
| Dependency cycle detected     | REJECT       |
| SQLx safety violation         | REJECT       |
| Cross-schema write detected   | REJECT       |
| Missing rate limit            | CORRECT      |
| Missing ownership check       | CORRECT      |

---

## 8. Observability Guardrails

- All API errors MUST use structured format
- No raw stack traces in production
- All logs must be structured JSON
- No PII or raw tokens in logs

---

**Version**: 1.0 | **Status**: Active | **Enforcement**: CI + Runtime
