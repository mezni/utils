# BorneMap Project Constitution — v1.15.2 (Canonical)

**Status**: System of Record
**Supersedes**: v1.15.1
**Scope**: Full platform architecture, identity, data governance, and enforcement rules

---

## 1. Project Identity & Mission

**Name**: BorneMap
**Mission**: EV charging station discovery and management platform for the Tunisian market.

**Optimization Objective**: Deterministic execution with strict architectural enforcement and zero uncontrolled system expansion.

---

## 2. Core System Invariants (Non-Negotiable)

### 2.1 Service Count Constraint

Exactly three services only:

| Service       | Port |
|---------------|------|
| auth-service  | 3000 |
| driver-service| 3001 |
| admin-service | 3002 |

- No additional services may be introduced
- No service splitting or duplication allowed

### 2.2 Architecture Immutability Rule

The following require a Constitution upgrade:
- New services
- New databases
- New event pipelines
- New shared modules
- New ownership domains

**Violation = ARCHITECTURE DRIFT**

### 2.3 Identity Dual-System Rule

Two independent identity systems MUST NEVER overlap:

| System          | Type              | Purpose              |
|-----------------|-------------------|----------------------|
| Keycloak        | UUID (sub)        | Human identity       |
| Platform Entities| PREFIX-nanoid(12) | Business objects     |

**Rules**:
- Users MUST NOT use nanoid
- Entities MUST NOT use UUID

### 2.4 Entity ID Standard (Canonical)

All platform entity IDs MUST follow: `PREFIX-nanoid(12)`

Where:
- `PREFIX` = uppercase 3-letter domain tag
- `nanoid(12)` = unique identifier
- `-` (hyphen) is mandatory

**Valid examples**:
- `STA-abc123def456`
- `CHG-k9x2lm8q1v7z`
- `OPR-x91kd82m4p0a`
- `EVT-q8m1z7p3x6n2`

**Invalid examples**:
- `STA_nanoid(12)` (underscore)
- `STAabc123def456` (no hyphen)
- UUID usage in entity system

---

## 3. Service Topology

| Service        | Port | Responsibility                                             |
|----------------|------|------------------------------------------------------------|
| auth-service   | 3000 | Authentication + user profiles                             |
| driver-service | 3001 | GIS + telemetry + analytics write                          |
| admin-service  | 3002 | Inventory + analytics read                                 |

**No additional services permitted.**

---

## 4. Database Architecture

### 4.1 platform_db

**Schemas**: `users`, `gis`, `inventory`

**Ownership**:

| Schema    | Owner          |
|-----------|----------------|
| users     | auth-service   |
| gis       | driver-service |
| inventory | admin-service  |

**users schema tables**: `users`, `user_profiles`

auth-service has exclusive READ/WRITE access.

### 4.2 keycloak_db

- Identity provider storage only
- No application logic allowed

### 4.3 analytics_db

| Service        | Access    |
|----------------|-----------|
| driver-service | READ/WRITE|
| admin-service  | READ ONLY |
| auth-service   | NO ACCESS |

**Frontend MUST NEVER access analytics_db.**

---

## 5. Data Ownership Rule

All data domains are strictly owned by a single service.
- Cross-service database writes are forbidden
- Ownership transfer requires Constitution upgrade

---

## 6. Frontend Architecture

### 6.1 Package Structure

```
apps/packages/
  ui-kit/
  domain-types/
  client-core/
```

### 6.2 ui-kit

**UI ONLY**: components, layouts, tokens, accessibility primitives.
- No logic or API access.

### 6.3 domain-types

**Contract layer**: DTOs, API contracts, event schemas, entity IDs.
- No runtime logic allowed.

### 6.4 client-core

**Transport layer**: API clients, React Query wrappers, auth/session handling, DTO mapping.
- No business logic allowed.

---

## 7. Backend Architecture

### 7.1 shared-domain

Pure domain primitives only: entity IDs, DTOs, event contracts.
- No infrastructure logic.

### 7.2 shared-infra

**Allowed**: JWT parsing, DB connection utilities, serialization helpers.
**Forbidden**: business logic, orchestration, domain rules.

---

## 8. API Ownership Rule

| Service        | APIs                                                   |
|----------------|--------------------------------------------------------|
| auth-service   | Authentication APIs, user profile APIs                 |
| driver-service | GIS APIs, telemetry ingestion, nearby search APIs      |
| admin-service  | Inventory APIs, analytics dashboards                   |

- Business endpoints MUST NOT be duplicated
- Operational endpoints allowed everywhere: `/health`, `/ready`, `/live`, `/metrics`

---

## 9. Trust Boundary Model

No service trusts another service's runtime state.

**Trusted sources**: Keycloak JWT, SQLx compile-time validation, domain-types contracts, schema validation.

**Untrusted sources**: service-to-service state, cached data, client input, external payloads.

---

## 10. Identity System

### 10.1 Human Identity

- **Source**: Keycloak UUID (sub)
- **Field name**: `user_uuid`
- **Used in**: auth-service, user_profiles

### 10.2 Business Identity

- **Format**: `PREFIX-nanoid(12)`
- **Used for**: stations, chargers, operators, events, all platform entities

### 10.3 Identity Separation Rule

- Users = UUID only
- Entities = PREFIX-nanoid only
- No mixing allowed

### 10.4 Analytics Identity Rule

**Valid**:
```json
{
  "user_uuid": "uuid",
  "operator_id": "OPR-abc123def456"
}
```

**Invalid**: merged identity fields, ambiguous actor_id fields, cross-format identifiers.

---

## 11. Driver-Service Analytics Pipeline

### 11.1 Pipeline Stages

1. **Ingestion Layer** — receives events, validates schema, extracts idempotency key (no DB access)
2. **Domain Layer** — normalization, enrichment, validation, deduplication (no DB access)
3. **Persistence Layer** — ONLY layer allowed to write analytics_db

### 11.2 Event Authority

driver-service is the authoritative owner of event schemas. domain-types contains published versions only.

### 11.3 Event Contract (Canonical)

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

### 11.4 Event Flow

All services → driver-service ingestion API → validation + enrichment → analytics_db write

**Endpoint**: `POST /api/v1/telemetry/events`

---

## 12. Dependency Graph Lock

**Frontend**: `ui-kit → domain-types → client-core`
**Backend**: `services → shared-domain → shared-infra`

**Forbidden**:
- service → service imports
- frontend → backend imports
- circular dependencies

**Violation = HARD FAILURE**

---

## 13. Shared Module Governance

| Package       | Content                                      |
|---------------|----------------------------------------------|
| ui-kit        | UI only                                      |
| domain-types  | Contracts only                               |
| client-core   | Transport only                               |
| shared-infra  | Must NOT include business logic/orchestration |

---

## 14. SQLx Enforcement Rule

All queries MUST be compile-time verified. CI MUST run: `cargo sqlx prepare --check`

**Failure = HARD STOP**.

---

## 15. Event System Rules

- `schema_version` required
- `idempotency_key` required
- replay-safe events required
- driver-service deduplicates all events

---

## 16. Failure Isolation Model

Failures MUST NOT propagate across services. Shared infrastructure failures are platform-level.

---

## 17. Migration Governance

| Service        | Schema Owner    |
|----------------|-----------------|
| auth-service   | users schema    |
| driver-service | gis + analytics_db |
| admin-service  | inventory schema |

**Rules**: forward-only migrations, no destructive rollback, SQLx compatibility required, CI validation required.

---

## 18. CI / Enforcement Rules

**HARD FAIL CONDITIONS**:
- analytics_db write violation
- identity violation
- service topology change
- SQLx failure
- schema mismatch
- dependency violation
- migration violation

---

## 19. Known Inherited Bugs

| ID          | Issue                    | Rule                       |
|-------------|--------------------------|----------------------------|
| KNOWN-001   | test stations leaking    | filter `is_test = FALSE`   |
| KNOWN-002   | missing deleted_at       | required field             |
| KNOWN-003   | duplicate nearby endpoint| driver-service owns        |
| KNOWN-004   | CI grep brittle          | regex-safe enforcement     |

---

## 20. Governance Hierarchy

1. SDEC v3.0 (highest authority)
2. BorneMap Constitution v1.15.2
3. Architecture docs
4. Sprint artifacts
5. LLM output (lowest authority)

---

## 21. Sprint Output Requirements

Every sprint MUST produce:
- `SYSTEM_STATE.md`
- `roadmap_status.md`
- `sprint_state.json`
- `sprint_review.md`
- `validation_report.md`
- `follow_up.md`

---

**Version**: 1.15.2 | **Status**: Canonical | **Scope**: Full platform
