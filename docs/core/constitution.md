# PLATFORM CONSTITUTION
## System Governance Specification (LLM-Enforceable)
## Version: 1.0

---

# 0. PURPOSE

This Constitution defines the immutable rules governing all system design, implementation, and evolution.

It is the highest authority in the system.

All documentation, code, schemas, APIs, and infrastructure MUST comply with it.

Any contradiction with lower-level documents is automatically resolved in favor of this Constitution.

---

# 1. HIERARCHY OF AUTHORITY

Rules apply in the following strict order:

1. constitution.md (this document)
2. core/ documentation
3. governance/ rules
4. epic specifications
5. implementation code

### Rule:
Lower layers MUST NEVER override higher layers.

---

# 2. ARCHITECTURAL LAW

## 2.1 Clean Architecture Mandate

The system MUST follow:

```text id="arch-001"
presentation → application → domain → infrastructure Dependency Rule:
Dependencies flow ONLY inward
Inner layers MUST NOT depend on outer layers
```

## 2.2 Layer Responsibilities

| Layer | Responsibility |
|---|---|
| presentation | HTTP, routing, controllers |
| application | use-case orchestration |
| domain | business logic + invariants |
| infrastructure | persistence + external IO |

## 2.3 Framework Isolation Law

The following restrictions apply:

* presentation MAY use Actix (or frontend frameworks)
* infrastructure MAY use SQLx
* domain MUST use ZERO frameworks
* application MUST remain framework-agnostic

---

# 3. IDENTITY & DATA LAW

## 3.1 Primary Identity Model

The system uses ONLY external identifiers as primary keys:

Format Rules:

| Entity | id format |
|---|---|
| Operators | PRT-\<nanoid(12)> |
| Stations | STA-\<nanoid(12)> |
| Chargers | CHR-\<nanoid(12)> |

## 3.2 Identity Rule

* `id` is the ONLY identifier in external API and system design
* `id` is immutable
* `id` is globally unique per entity type

## 3.3 Referential Integrity Law

All relationships MUST use `id`.

Cascading deletes MUST follow domain hierarchy:

```
Operators → Stations → Chargers
```

No orphan entities are allowed.

---

# 4. API CONTRACT LAW

## 4.1 Standard Response Format

### Success
```json
{
  "success": true,
  "data": {},
  "error": null
}
```

### Error
```json
{
  "success": false,
  "data": null,
  "error": {
    "code": "ERROR_CODE",
    "message": "Human readable message"
  }
}
```

## 4.2 API Rules

* All endpoints MUST use the standard response format
* No raw framework responses allowed
* No untyped errors allowed
* All APIs MUST use `/api/v1` versioning

## 4.3 Error Code Rules

Error codes MUST be:

* stable
* machine-readable
* enumerated

Examples:

* `VALIDATION_ERROR`
* `NOT_FOUND`
* `CONFLICT`
* `INTERNAL_ERROR`
* `DB_ERROR`

---

# 5. DOMAIN MODEL LAW

## 5.1 Entity Rules

Each entity MUST:

* have a unique `id`
* enforce invariants in domain layer only
* never contain persistence logic

## 5.2 Business Invariants

* Stations MUST belong to an Operator
* Chargers MUST belong to a Station
* Deletion MUST cascade downward

## 5.3 Domain Purity Rule

Domain layer MUST:

* contain NO IO
* contain NO database logic
* contain NO HTTP logic
* contain ONLY business rules

---

# 6. FRONTEND GOVERNANCE LAW

## 6.1 Data Access Rule

* No direct HTTP calls in UI components
* All API calls MUST go through `apiClient`
* React Query MUST be used for server state

## 6.2 Layer Separation

Frontend MUST be split into:

* pages (routing only)
* features (business UI logic)
* api (transport layer)
* components (pure UI)

---

# 7. BACKEND GOVERNANCE LAW

## 7.1 Actix Rule

* Actix is ONLY allowed in presentation layer
* Handlers MUST NOT contain business logic

## 7.2 Application Rule

Application layer MUST:

* orchestrate use-cases
* remain framework-free
* not perform persistence directly

## 7.3 Infrastructure Rule

Infrastructure MUST:

* handle SQLx operations
* implement repositories
* contain NO business logic

---

# 8. DATABASE LAW

## 8.1 Schema Rule

All tables MUST reside under:

```
ev
```

## 8.2 Identity Rule

* `id` is PRIMARY KEY
* No surrogate keys exist
* All relations use `id`

## 8.3 Migration Rule

* migrations are forward-only
* migrations are deterministic
* migrations are timestamp ordered
* no rollback dependency allowed

---

# 9. OBSERVABILITY LAW

## 9.1 Logging Requirements

* logs MUST be structured
* logs MUST include `request_id`
* logs MUST be consistent across layers

## 9.2 Tracing Requirements

* every request MUST have correlation ID
* propagation across all layers is mandatory

---

# 10. SHARED CRATES LAW

## 10.1 platform-core

MUST:

* contain pure utilities only
* define error + result primitives

MUST NOT:

* use SQLx
* use Actix
* perform IO

## 10.2 platform-db

MUST:

* manage SQLx pool
* implement repositories
* handle migrations

MUST NOT:

* contain business logic
* contain domain rules

---

# 11. GOVERNANCE LAW

## 11.1 Change Control

* All changes MUST be documented
* No silent architectural drift is permitted

## 11.2 Compatibility Rule

* Backward compatibility is mandatory unless explicitly deprecated
* Breaking changes require version increment

---

# 12. FORBIDDEN BEHAVIORS

The following are strictly prohibited:

* introducing new identity systems outside `id`
* bypassing layered architecture
* mixing domain logic with infrastructure
* direct API calls in UI components
* using surrogate keys or hidden identifiers

---

# 13. SYSTEM GUARANTEE

If compliant, the system guarantees:

* deterministic architecture behavior
* strict separation of concerns
* stable API contracts
* scalable epic-based expansion
* LLM-predictable code generation structure
