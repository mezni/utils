# SpecKit Enforcement Layer v1.1 (Hardened + CI-Deterministic)

**Status**: Canonical Runtime Layer
**Role**: Convert constitutional rules into deterministic CI gates, compile-time guarantees, and static analysis rules.

---

## 1. Purpose

Convert BorneMap constitutional rules into:
- Deterministic CI gates
- Compile-time guarantees (SQLx + Rust + TS)
- AST-based static analysis rules
- Strict data ownership enforcement
- Zero cross-domain leakage architecture

---

## 2. CI Pipeline DAG (Strict Order)

1. `format_check`
2. `type_check`
3. `dependency_graph_validation`
4. `identity_validation`
5. `schema_validation`
6. `sqlx_compile_check`
7. `analytics_write_gate`
8. `integration_tests`
9. `build_success`

**Any failure = HARD STOP**

---

## 3. Service Topology Validation

**Allowed services ONLY**:
- `auth-service`
- `driver-service`
- `admin-service`

```
EXPECTED=("auth-service" "driver-service" "admin-service")
```

**FAIL if**:
- Extra service exists
- Renamed service exists
- Missing service exists

---

## 4. Identity System (Unified + Strict)

### 4.1 User Identity
- UUID only (Keycloak sub)
- RFC4122 format
- Allowed only in: `auth-service`, `platform_db.users`

### 4.2 Business Entity Identity
Format: `PREFIX-[a-z0-9]{12}`

| Prefix | Entity   |
|--------|----------|
| STA    | Station  |
| CHG    | Charger  |
| OPR    | Partner  |
| EVT    | Event    |

### 4.3 CI Enforcement Rule
```bash
grep -R "UUID" backend/  | block if used in business entities
grep -R "nanoid" backend/ | block if used in users schema
```

### 4.4 AST Enforcement
Must validate:
- Rust AST (syn / cargo-expand)
- TypeScript AST (ts-morph / eslint plugin)

---

## 5. Dependency Graph Enforcement (Strict DAG)

### 5.1 Allowed Structure
```
frontend:
  ui-kit → domain-types → client-core

backend:
  shared-domain → shared-infra
  services → shared-domain + shared-infra
```

### 5.2 Forbidden Edges
- `shared-domain → services` ❌
- `service → service` ❌
- `frontend → backend` ❌
- `ui-kit → client-core` ❌
- `domain-types → runtime logic` ❌

### 5.3 CI Implementation
- Rust: `cargo metadata` + `syn` graph analysis
- TypeScript: `madge` or `ts-morph` dependency tree

---

## 6. Analytics Write Gate (Critical)

### 6.1 Rule
**Only** `driver-service` may WRITE to `analytics_db`.

### 6.2 Access Matrix

| Service        | Access    |
|----------------|-----------|
| driver-service | WRITE     |
| admin-service  | READ ONLY |
| auth-service   | NONE      |

### 6.3 CI Enforcement
```bash
grep -R "analytics_db" backend/
FAIL if file path != driver-service
```

### 6.4 Required Pipeline
driver-service MUST enforce:
- `idempotency_key` validation
- Duplicate suppression
- `schema_version` validation
- Replay-safe ingestion
- Event normalization

---

## 7. SQLx Enforcement Layer (Hardened)

### 7.1 Rule
- ALL SQL must be compile-time verified
- No runtime query construction
- No dynamic SQL strings

### 7.2 CI Checks
```bash
cargo sqlx prepare --check
```

### 7.3 Forbidden Patterns
```sql
INSERT INTO analytics_db
UPDATE analytics_db
DELETE FROM analytics_db
```
**outside driver-service module path → FAIL**

### 7.4 Migration Isolation

| Service        | Allowed Schema |
|----------------|----------------|
| auth-service   | users          |
| driver-service | gis            |
| admin-service  | inventory      |

**Cross-schema migration = FAIL**

---

## 8. Schema Validation Layer

### 8.1 platform_db Rules
- **users schema**: `id = UUID ONLY`, must match Keycloak sub
- **inventory schema**: `id TEXT PRIMARY KEY`, `CHECK (id ~ '^(STA|CHG|OPR)-[a-z0-9]{12}$')`
- **gis schema**: ingestion-only, external OSM source allowed, no manual mutation outside driver-service

---

## 9. Frontend Boundary Enforcement

| Package      | Content                        |
|--------------|--------------------------------|
| ui-kit       | UI ONLY — no API, no logic     |
| domain-types | types ONLY — no runtime logic  |
| client-core  | transport ONLY — API + mapping |

---

## 10. Analytics Pipeline (Strict)

### 10.1 Ingestion Endpoint
`POST /api/v1/telemetry/events` — ONLY in driver-service

### 10.2 Event Schema
```json
{
  "event_type": "string",
  "user_id": "UUID | null",
  "payload": {},
  "schema_version": 1,
  "timestamp": "ISO-8601",
  "idempotency_key": "string"
}
```

### 10.3 Required Processing Steps
driver-service MUST:
1. Validate schema
2. Enrich location
3. Attach session metadata
4. Deduplicate events
5. Ensure replay safety
6. Write to analytics_db

---

## 11. Failure Modes (Strict)

**HARD FAIL IF**:
- Service topology violated
- analytics_db written outside driver-service
- UUID used in business entities
- nanoid used in users
- Dependency cycles detected
- SQLx unsafe query detected
- Schema drift detected
- Frontend/backend coupling detected

---

## 12. CI Master Pipeline

```
01_format_check
02_type_check
03_dependency_graph_validation
04_identity_validation
05_schema_validation
06_sqlx_compile_check
07_analytics_write_gate
08_integration_tests
09_build_success
```

---

## 13. System Guarantees

After applying this layer:
- **Architecture**: 3-service strict topology, no hidden services
- **Data**: single writer analytics model, strict schema ownership
- **Identity**: UUID vs nanoid fully separated, no cross-domain leakage
- **Compilation**: SQLx compile-time safety enforced, no runtime SQL allowed
- **Structural**: no circular dependencies, no frontend/backend leakage

---

**Version**: 1.1 | **Status**: Hardened | **CI-Integrated**: Yes | **Architecture-Stable**: Yes
