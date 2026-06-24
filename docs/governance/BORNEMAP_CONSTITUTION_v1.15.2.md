# BorneMap Constitution — v1.15.2 (Canonical)

**Status**: System of Record
**Supersedes**: v1.15.1
**Scope**: Full platform architecture, identity, data governance, and enforcement rules

---

## 1. PROJECT IDENTITY & MISSION

**Name**: BorneMap
**Mission**: EV charging station discovery and management platform for the Tunisian market.

**Optimization Objective**:
Deterministic execution with strict architectural enforcement and zero uncontrolled system expansion.

---

## 2. CORE SYSTEM INVARIANTS (NON-NEGOTIABLE)

### 2.1 Service Count Constraint

Exactly three services only:

| Service | Port | Responsibility |
|---------|------|----------------|
| `auth-service` | 3000 | Authentication + user profiles |
| `driver-service` | 3001 | GIS + telemetry + analytics write |
| `admin-service` | 3002 | Inventory + analytics read |

No additional services may be introduced. No service splitting or duplication allowed.

### 2.2 Architecture Immutability Rule

The following require a Constitution upgrade:

- new services
- new databases
- new event pipelines
- new shared modules
- new ownership domains

Any violation = ARCHITECTURE DRIFT.

### 2.3 Identity Dual-System Rule

Two independent identity systems MUST NEVER overlap.

| System | Type | Purpose |
|--------|------|---------|
| Keycloak | UUID (sub) | Human identity |
| Platform Entities | PREFIX-nanoid(12) | Business objects |

**Rules:**
- Users MUST NOT use nanoid
- Entities MUST NOT use UUID

### 2.4 Entity ID Standard

All platform entity IDs MUST follow: `PREFIX-nanoid(12)`

- **PREFIX** = uppercase 3-letter domain tag
- **nanoid(12)** = unique identifier
- **hyphen `-`** is mandatory

**Valid examples:**
```
STA-abc123def456
CHG-k9x2lm8q1v7z
OPR-x91kd82m4p0a
EVT-q8m1z7p3x6n2
```

**Invalid examples:**
```
STA_nanoid(12)
STAabc123def456
UUID usage in entity system
```

---

## 3. DATABASE ARCHITECTURE

### 3.1 `platform_db`

**Schemas:** `users`, `gis`, `inventory`

**Ownership:**

| Schema | Owner |
|--------|-------|
| `users` | auth-service |
| `gis` | driver-service |
| `inventory` | admin-service |

**`users` schema tables:**
- `user_profiles` — auth-service has exclusive READ/WRITE access

### 3.2 `keycloak_db`

- Identity provider storage only
- No application logic allowed

### 3.3 `analytics_db`

| Service | Access |
|---------|--------|
| driver-service | READ/WRITE |
| admin-service | READ ONLY |
| auth-service | NO ACCESS |

Frontend MUST NEVER access `analytics_db`.

---

## 4. DATA OWNERSHIP RULE

All data domains are strictly owned by a single service.

- Cross-service database writes are forbidden
- Ownership transfer requires Constitution upgrade

---

## 5. FRONTEND ARCHITECTURE

### 5.1 Package Structure

```
source/packages/
  ui-kit         UI ONLY: components, layouts, tokens, accessibility primitives
  domain-types   Contract layer: DTOs, API contracts, event schemas, entity IDs
  client-core    Transport layer: API clients, React Query wrappers, auth/session
```

### 5.2 Rules

- `ui-kit` → `domain-types` → `client-core` (dependency direction enforced)
- No business logic in `ui-kit` or `client-core`
- No API access from `ui-kit`

---

## 6. BACKEND ARCHITECTURE

### 6.1 Clean Architecture (Rust)

| Layer | Responsibility | Allowed Dependencies |
|-------|---------------|---------------------|
| `domain/` | Pure logic, entities, value objects, errors | None |
| `application/` | Use-case orchestration, DTO mapping | domain |
| `infrastructure/` | SQLx, Redis, external integrations | domain, application |
| `presentation/` | HTTP, validation, response mapping | application |

### 6.2 Shared Modules

**`shared-domain`**: Pure domain primitives only (entity IDs, DTOs, event contracts). No infrastructure logic.

**`shared-infra`**: JWT parsing, DB connection utilities, serialization helpers. No business logic, orchestration, or domain rules.

### 6.3 Dependency Graph Lock

**Frontend:**
```
ui-kit → domain-types → client-core
```

**Backend:**
```
services → shared-domain → shared-infra
```

**Forbidden:**
- service → service imports
- frontend → backend imports
- circular dependencies

Violation = HARD FAILURE.

---

## 7. API OWNERSHIP RULE

| Service | Owned APIs |
|---------|-----------|
| auth-service | authentication APIs, user profile APIs |
| driver-service | GIS APIs, telemetry ingestion, nearby search APIs |
| admin-service | inventory APIs, analytics dashboards |

Business endpoints MUST NOT be duplicated.

Operational endpoints (allowed everywhere): `/health`, `/ready`, `/live`, `/metrics`

---

## 8. TRUST BOUNDARY MODEL

No service trusts another service's runtime state.

**Trusted sources:**
- Keycloak JWT
- SQLx compile-time validation
- `domain-types` contracts
- schema validation

**Untrusted sources:**
- service-to-service state
- cached data
- client input
- external payloads

---

## 9. IDENTITY SYSTEM

### 9.1 Human Identity

- **Source**: Keycloak UUID (sub)
- **Field name**: `user_uuid`
- **Used in**: auth-service, user_profiles

### 9.2 Business Identity

- **Format**: `PREFIX-nanoid(12)`
- **Used for**: stations, chargers, operators, events, all platform entities

### 9.3 Identity Separation Rule

- Users = UUID only
- Entities = PREFIX-nanoid only
- No mixing allowed

### 9.4 Analytics Identity Rule

**Valid:**
```json
{ "user_uuid": "uuid", "operator_id": "OPR-abc123def456" }
```

**Invalid:**
- merged identity fields
- ambiguous `actor_id` fields
- cross-format identifiers

---

## 10. DRIVER-SERVICE ANALYTICS PIPELINE

### 10.1 Pipeline Stages

1. **Ingestion** — receives events, validates schema, extracts idempotency key (no DB access)
2. **Domain** — normalization, enrichment, validation, deduplication (no DB access)
3. **Persistence** — ONLY layer allowed to write `analytics_db`

### 10.2 Event Authority

`driver-service` is the authoritative owner of event schemas. `domain-types` contains published versions only.

### 10.3 Event Contract (Canonical)

```json
{
  "event_type": "string",
  "schema_version": 1,
  "user_uuid": "uuid | null",
  "idempotency_key": "string",
  "payload": {},
  "timestamp": "ISO-8601"
}
```

### 10.4 Event Flow

All services → `driver-service` ingestion API → validation + enrichment → `analytics_db` write

**Endpoint**: `POST /api/v1/telemetry/events`

---

## 11. SQLx ENFORCEMENT RULE

All queries MUST be compile-time verified. CI MUST run:

```
cargo sqlx prepare --check
```

Failure = HARD STOP.

---

## 12. EVENT SYSTEM RULES

- `schema_version` required
- `idempotency_key` required
- replay-safe events required
- `driver-service` deduplicates all events

---

## 13. FAILURE ISOLATION MODEL

Failures MUST NOT propagate across services. Shared infrastructure failures are platform-level.

---

## 14. MIGRATION GOVERNANCE

| Schema | Owner |
|--------|-------|
| `users` | auth-service |
| `gis` + `analytics_db` | driver-service |
| `inventory` | admin-service |

**Rules:**
- forward-only migrations
- no destructive rollback
- SQLx compatibility required
- CI validation required

---

## 15. CI / ENFORCEMENT RULES

**HARD FAIL CONDITIONS:**
- `analytics_db` write violation
- identity violation
- service topology change
- SQLx failure
- schema mismatch
- dependency violation
- migration violation

---

## 16. KNOWN INHERITED BUGS

| ID | Issue | Fix |
|----|-------|-----|
| KNOWN-001 | Test stations leaking | filter `is_test = FALSE` |
| KNOWN-002 | Missing `deleted_at` | required field |
| KNOWN-003 | Duplicate nearby endpoint | driver-service owns |
| KNOWN-004 | CI grep brittle | regex-safe enforcement |

---

## 17. GOVERNANCE HIERARCHY

1. **SDEC v3.0** (highest authority)
2. **BorneMap Constitution v1.15.2**
3. Architecture docs
4. Sprint artifacts
5. LLM output (lowest authority)

---

## 18. SPRINT OUTPUT REQUIREMENTS

Every sprint MUST produce:

- `SYSTEM_STATE.md`
- `roadmap_status.md`
- `sprint_state.json`
- `sprint_review.md`
- `validation_report.md`
- `follow_up.md`

---

## 19. DEPLOYMENT TOPOLOGY

```
Edge: Traefik API Gateway :80/:443
                │
                ├── auth-service :3000
                ├── driver-service :3001
                └── admin-service :3002
```

**Version**: 1.15.2 | **Ratified**: 2026-06-24 | **Last Amended**: 2026-06-24
