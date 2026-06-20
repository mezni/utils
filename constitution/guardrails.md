# BorneMap — Guardrails Spec
**Version:** 2.1
**Date:** June 2026
**Supersedes:** v2.0

---

## 0. Purpose

These guardrails define non-negotiable execution constraints for all LLM outputs, sprint execution, CI validation, runtime file generation, and architecture evolution.

Enforced by: `tools/validate.sh` + `tools/ci_guard.sh` + `tools/sprint_engine.sh`

---

## 1. Absolute System Constraints (Hard Block Rules)

### 1.1 Architecture Expansion Prohibition

No new services beyond:
- auth-service (:3000)
- driver-service (:3001)
- admin-service (:3002)

### 1.2 Forbidden Infrastructure

Strictly prohibited in all phases:
- Kafka, RabbitMQ, NATS
- Distributed tracing backends (Jaeger, OpenTelemetry collector stacks)
- Service mesh (Istio, Linkerd)
- Kubernetes manifests (deferred post-validation)
- Autoscaling systems
- Event-sourced architectures

### 1.3 Domain Restrictions (Product Constraints)

The system SHALL NOT include:
- Payments, billing, invoicing
- Hardware charger communication (OCPP)
- Real-time telemetry streaming pipelines
- Grid optimization / smart charging
- Cryptocurrency / wallet systems

### 1.4 Database Expansion Constraint

Only permitted:
- `platform_db` (PostgreSQL 16 + PostGIS)
- `keycloak_db` (managed by Keycloak)
- `analytics_db` (PostgreSQL, owned by admin-service)
- Redis

No additional databases or caching layers permitted.

### 1.5 Directory Root Rule

Monorepo root is `bornemap/` — **no `source/` prefix on any path.**

`shared/` = Rust Cargo workspace crates only.
`packages/` = TypeScript shared packages only.
These are never interchanged.

---

## 2. Execution Constraints (LLM Behavior)

### 2.1 Phase Isolation Rule

Only ONE sprint phase is active at any time.

The LLM MUST NOT:
- Execute future-phase logic
- Generate future-phase artifacts
- Pre-build architecture outside the current phase

### 2.2 Allowed Output Constraint

Outputs MUST strictly match the `allowed_outputs` list for the current phase (defined in SDEC master prompt Section 7).

Anything outside this list is an invalid output and must not be produced.

### 2.3 No Implicit File Creation Rule

The system MUST NOT:
- Invent new file paths
- Introduce undocumented artifacts
- Extend sprint structure dynamically

All files must exist in `file_structure.md` or the current sprint definition.

### 2.4 Constitutional Conflict Rule

If the LLM detects a conflict between any two governance documents:
- **Do not silently resolve it**
- Surface it immediately using the HARD BLOCK format
- Halt until the human resolves it

---

## 3. Sprint Execution Rules

### 3.1 Sprint Authority Hierarchy

1. SDEC Master Prompt (highest)
2. Constitution
3. Guardrails
4. `docs/architecture.md`
5. `sprints/<id>/state/sprint_state.json`
6. LLM output (lowest)

### 3.2 Sprint State Mutation Rule (Critical)

The LLM MUST NOT modify:
- `current_phase`
- `allowed_outputs`
- `blocked_outputs`

These fields are ONLY modified by `tools/sprint_engine.sh`.

### 3.3 Transition Control Rule

Sprint phase transitions occur ONLY when:
- `tools/validate.sh` passes all artifact checks
- `tools/ci_guard.sh` passes all CI gates
- Artifact checksums in `checksum_manifest.json` are valid

The LLM signals readiness with "Ready for transition validation." It does not trigger the transition itself.

---

## 4. Data & Identity Guarantees

### 4.1 Entity ID Rule (Strict)

All IDs MUST match: `<PREFIX>-<nanoid(12)>`

Allowed prefixes: `USR`, `OPR`, `STA`, `CHG`

Hard rules:
- Generated only via shared nanoid(12) utility
- No manual IDs
- No semantic encoding in IDs — prefix is classification only

### 4.2 Schema Isolation Rule

Each service MUST:
- Own its schema boundary exclusively
- NOT directly read or write another service's schema tables
- Communicate cross-service only via HTTP API contracts

Schema ownership:
- `users` → auth-service
- `inventory` → admin-service
- `gis` → driver-service
- `analytics` → admin-service (in `analytics_db`)

Enforced by CI Gate 3.

### 4.3 Soft-Delete Rule

The following tables MUST have a `deleted_at TIMESTAMPTZ` column:
- `partner_profiles`
- `stations`
- `chargers`

No hard deletes on these entities.

### 4.4 Test Data Isolation Rule

All production queries against `stations` MUST include:
```sql
WHERE s.is_test = FALSE
```
Missing this filter is a HARD BLOCK condition (KNOWN-001).

---

## 5. Contract-First Enforcement (OpenAPI Rule)

### 5.1 Mandatory Contract Ordering

Before any implementation code:
1. OpenAPI spec MUST exist in `sprints/<id>/api/openapi.yaml`
2. OpenAPI spec MUST pass schema validation
3. Only then may implementation proceed

### 5.2 Contract Lock Rule

Once a sprint enters IMPLEMENTATION phase:
- OpenAPI files become immutable
- Changes require a sprint reset or a new CONTRACT phase iteration

---

## 6. CI / Validation Guardrails

Every commit and PR MUST pass all of the following:

### Gate 1 — API Validation
- OpenAPI schema validation via `spectral` or equivalent
- Every route in Actix-web has a matching OpenAPI path
- Every OpenAPI path has a matching handler

### Gate 2 — Backend Validation
- `cargo check` — no compile errors
- `cargo test` — all tests pass
- `sqlx prepare --check` — compile-time query safety
- Migration consistency check

### Gate 3 — Schema Isolation
```bash
grep -rE "SELECT .* FROM.*(users\.|inventory\.|gis\.)" services/<wrong-service>/
```
Any cross-schema access = HARD FAIL.

### Gate 4 — Identity Validation
- All IDs matching `[A-Z]{3}-` must match `[A-Z]{3}-[a-zA-Z0-9]{12}`

### Gate 5 — Architecture Compliance
- No imports from forbidden infrastructure
- No service directories beyond the three permitted

### Gate 6 — Security
- Every non-public endpoint must have JWT middleware
- Role check must be explicit per handler

### Gate 7 — Test Coverage
- `domain/` = 100% coverage
- `api/` handlers = ≥ 90% coverage
- Failure = HARD BLOCK

### Gate 8 — Doc Drift
- `docs/architecture.md` timestamp checked against last schema migration
- OpenAPI ↔ Actix-web route parity enforced

---

## 7. Artifact Integrity Rule

Each sprint MUST maintain:
- `artifacts/generated_files_index.md` — list of every generated file
- `artifacts/checksum_manifest.json` — SHA256 hash of every generated file

Rules:
- Every generated file must have a checksum entry
- No orphan artifacts (files without index entry)
- Drift detection is mandatory before phase transition

---

## 8. Bug & Failure Memory Rule

### 8.1 Bug Tracking Is Mandatory

All failures discovered during TESTING or any phase MUST be recorded immediately in `bugs/active.md` using the standard bug format (defined in SDEC master prompt Section 10).

### 8.2 Resolution Rule

Resolved bugs MUST:
1. Move entry to `bugs/resolved.md`
2. Have a corresponding regression test committed
3. Log the recurrence risk in `bugs/regression_log.md`

### 8.3 Regression Rule

Any bug that recurs after resolution MUST be logged in `bugs/regression_log.md` with the sprint and phase it reappeared in.

### 8.4 Known Bug Watch List

Every sprint review must explicitly verify the inherited known bugs (Section 7 of constitution.md) are not reintroduced.

---

## 9. Output Discipline Rule

The system MUST:
- Output ONLY requested sprint artifacts for the current phase
- Avoid commentary outside execution scope
- Never explain unless asked
- Never expand scope for completeness
- Never skip sprint boundaries
- Begin every generated file with the standard header (phase, sprint, skill)

---

## 10. Seven-Skill Pipeline Rule

All seven professional skills (UX/UI, Rust Architecture, Postgres, Keycloak/Security, Testing, Documentation, Security Review) are always active.

They form a single pipeline. If any layer is broken, the sprint is invalid regardless of how many other layers pass.

---

## 11. Failure Modes

### Hard Fail (sprint blocked until revalidation)
- Invalid phase output
- Unauthorized file creation
- Schema isolation violation
- Service boundary breach
- OpenAPI-first bypass
- Missing `WHERE s.is_test = FALSE` in production query
- Constitutional document conflict

### Soft Fail (flagged, can proceed with acknowledgment)
- Missing GitHub sync
- Incomplete story mapping
- Stale issue state

### Recovery
- Automatic reconciliation via `tools/reconcile.sh`
- Rollback to last valid `sprint_state.json` checkpoint
- Re-sync GitHub projection

---

## 12. Summary Principle

> If uncertain, the system MUST default to:
> **"Do nothing and await valid phase instruction."**
