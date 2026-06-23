# ARCHITECTURAL DECISION RECORDS (ADR)

**Last Updated**: 2026-06-23  
**Authority**: Architecture Team  
**Related**: [Epic Lifecycle](./epic-lifecycle.md), [Change Policy](./change-policy.md)

---

## Overview

This document records all significant architectural decisions made in the BorneMap EV Dashboard Platform. An Architectural Decision Record (ADR) is a document that captures a decision made to resolve an important technical problem and its context, rationale, and consequences.

### Principles

1. **Immutability**: Accepted decisions are not changed; only superseded by new ADRs
2. **Reversibility**: Decisions are reversible (marked as SUPERSEDED) if needed
3. **Documentation**: Every decision that affects multiple teams or epics requires an ADR
4. **Accessibility**: All decisions must be findable and readable

---

## Decision Log

### ADR-001: External ID System

**Status**: Accepted  
**Date**: 2026-06-15  
**Author**: Architecture Team  
**Related Epic**: E001 (Dashboard Kernel)

#### Context

The system needs a way to identify partners, stations, and chargers. Traditional approaches include:
1. Numeric surrogate keys (e.g., 1, 2, 3)
2. UUIDs (e.g., `550e8400-e29b-41d4-a716-446655440000`)
3. Custom external IDs (e.g., `PRT-abc123def456`)

#### Decision

Use **external IDs only** for API exposure:
- Partners: `PRT-<12-char-base62>`
- Stations: `STA-<12-char-base62>`
- Chargers: `CHR-<12-char-base62>`

Do NOT expose numeric IDs or UUIDs in APIs.

#### Rationale

**Prevents Schema Leakage**
- Numeric IDs reveal system scale (e.g., "1000 partners" from ID 1000)
- External format is opaque to clients

**Stabilizes API Contract**
- External IDs don't change if we refactor internal schema
- Clients can reliably reference entities

**Simplifies Frontend Integration**
- Human-readable prefixes identify entity type
- Reduced confusion vs bare UUIDs
- Better for debugging (PRT-abc vs 550e8400...)

**Supports Multiple Identifier Spaces**
- Different entity types don't collide
- Future business entities can use unique prefixes

#### Consequences

**Positive**:
- ✅ Better API security (no schema inference)
- ✅ More maintainable (can refactor internal IDs)
- ✅ Easier debugging (readable prefixes)

**Negative**:
- ❌ Slight performance overhead (string comparison vs integer)
- ❌ Requires translation layer (external ↔ internal)

**Neutral**:
- Storage overhead minimal (16 bytes vs 8 bytes, negligible)

#### Implementation Notes

- IDs are **deterministic** (hash-based, not random)
- Generation happens in **infrastructure layer only**
- Database PRIMARY KEY is the external ID (no surrogate key)
- All queries use external ID

---

### ADR-002: Soft Delete with Audit Trail

**Status**: Accepted  
**Date**: 2026-06-15  
**Author**: Architecture Team  
**Related Epic**: E001

#### Context

Need to support entity deletion while maintaining audit trails and referential integrity. Options:
1. Hard delete (immediate removal, lose history)
2. Soft delete (mark as deleted, keep history)
3. Event sourcing (immutable log of all changes)

#### Decision

Implement **soft delete** with `deleted_at` timestamp:
- Instead of DELETE, perform UPDATE with `deleted_at = now()`
- All queries filter: WHERE deleted_at IS NULL
- Keeps historical data for audits

Hard delete available for GDPR compliance (separate API).

#### Rationale

**Maintains Audit Trails**
- Every deletion is timestamped
- Can reconstruct historical state
- Compliance with audit requirements

**Preserves Referential Data**
- Stations linked to deleted partners remain intact
- Can answer "what did this partner own?"
- Simplifies data recovery

**Balance Between Audit and Privacy**
- Soft delete for operational deletion
- Hard delete available for GDPR "right to be forgotten"

#### Consequences

**Positive**:
- ✅ Complete audit trail
- ✅ Data recovery possible
- ✅ Historical analysis enabled

**Negative**:
- ❌ Database grows indefinitely (need archival strategy)
- ❌ All queries need `deleted_at IS NULL` filter
- ❌ Risk of data leakage if not handled carefully

#### Implementation Notes

- Soft delete: UPDATE set deleted_at = now()
- Hard delete: DELETE (triggers CASCADE)
- Query pattern: WHERE deleted_at IS NULL
- Application layer enforces filtering

---

### ADR-003: Deterministic ID Generation

**Status**: Accepted  
**Date**: 2026-06-16  
**Author**: Architecture Team  
**Related Epic**: E001

#### Context

How to generate external IDs? Options:
1. Random (nanoid, UUID) - unpredictable but not reproducible
2. Deterministic (hash-based from seed) - reproducible but complex
3. Monotonic (auto-increment) - simple but reveals scale

#### Decision

Use **deterministic ID generation** from a seed string:
- Create hash from: entity-type + creation-context
- Use first 12 Base62 characters of hash
- Same seed always produces same ID

#### Rationale

**Reproducibility for Testing**
- Same test input always produces same ID
- Easier to write reproducible tests
- Better for seeding development databases

**Prevents ID Collisions**
- Hash collision probability negligible (256-bit hash → 12 chars)
- More reliable than pure random

**Infrastructure Layer Responsibility**
- ID generation is IO concern, not business logic
- Keeps domain pure

#### Consequences

**Positive**:
- ✅ Reproducible test IDs
- ✅ Deterministic behavior easier to reason about
- ✅ Better for data seeding

**Negative**:
- ❌ Slightly slower than random (hashing overhead)
- ❌ Requires understanding of seed format
- ❌ More complex than simple random

#### Implementation Notes

- Use BLAKE3 hash function
- Extract first 12 characters in Base62
- Seed format: "{entity_type}:{creation_context}"
- Happens only in infrastructure layer

---

### ADR-004: Clean Architecture Layers

**Status**: Accepted  
**Date**: 2026-06-15  
**Author**: Architecture Team  
**Related Epic**: E001

#### Context

Need clear separation of concerns across codebase. Options:
1. Layered architecture (clean, but can be rigid)
2. Hexagonal (ports/adapters, flexible)
3. Micro-kernels (feature-based, complex)

#### Decision

Use **Clean Architecture** with 4 layers:
```
Presentation (HTTP, UI)
    ↓
Application (Use cases, orchestration)
    ↓
Domain (Business rules, entities)
    ↓
Infrastructure (Database, external APIs)
```

Dependencies flow **inward only**.

#### Rationale

**Clear Boundaries**
- Each layer has single responsibility
- Easy to locate code
- Easy to test in isolation

**Framework Independence**
- Domain layer is pure Rust (no dependencies)
- Can swap frameworks (Actix → Axum)
- Easy testing without setup

**Scalability**
- Clear extension points
- New features follow established patterns
- Large teams can work in parallel

#### Consequences

**Positive**:
- ✅ Excellent testability (domain unit tests without setup)
- ✅ Framework-agnostic core
- ✅ Clear architecture documentation

**Negative**:
- ❌ More files/boilerplate than simple code
- ❌ Requires discipline (easy to violate layers)
- ❌ Steeper learning curve for new engineers

#### Implementation Notes

- See [Architecture Guide](../core/architecture.md) for details
- Enforce via code review
- Document layer responsibilities
- Use trait interfaces for layer communication

---

### ADR-005: Repository Pattern for Data Access

**Status**: Accepted  
**Date**: 2026-06-16  
**Author**: Architecture Team  
**Related Epic**: E001

#### Context

How to access data without coupling domain to database? Options:
1. Direct database access (simple, but couples domain)
2. Repository pattern (clean, requires boilerplate)
3. DAO pattern (similar to repository)

#### Decision

Use **Repository pattern**:
- Domain defines repository interfaces (traits)
- Infrastructure implements repositories
- Application uses repositories via interface

#### Rationale

**Dependency Inversion**
- Domain depends on abstraction, not concrete database
- Infrastructure provides implementation

**Testability**
- Can mock repositories in application tests
- Domain tests don't need database

**Flexibility**
- Easy to swap database (PostgreSQL → MongoDB)
- Can have multiple implementations for testing

#### Consequences

**Positive**:
- ✅ Domain completely decoupled from database
- ✅ Easy mocking for tests
- ✅ Simple to swap implementations

**Negative**:
- ❌ More code (trait + implementation)
- ❌ Slight performance overhead
- ❌ Requires discipline

#### Implementation Notes

- Domain: `pub trait PartnerRepository { ... }`
- Infrastructure: `impl PartnerRepository for PostgresPartnerRepository { ... }`
- Application injects via dependency injection
- Use `async_trait` for async methods

---

## Upcoming Decisions (Under Review)

These decisions are under consideration and not yet accepted:

### ADR-006: Authentication Strategy (PLANNED)
- Should we use JWT, sessions, or OAuth?
- Decision date: TBD

### ADR-007: Caching Strategy (PLANNED)
- Should we use Redis, in-memory cache, or none?
- Decision date: TBD

---

## ADR Template

Use this template for all new architectural decisions:

```
### ADR-NNN: <Short Title>

**Status**: [Draft / Under Review / Accepted / Superseded by ADR-XXX]
**Date**: YYYY-MM-DD
**Author**: [Name]
**Related Epic**: [E001, E002, etc.]
**Supersedes**: [ADR-XXX if applicable]

#### Context

<Describe the problem and why it matters>

#### Decision

<Clearly state the decision made>

#### Rationale

<Explain why this decision was made>

#### Consequences

**Positive**:
- ✅ 

**Negative**:
- ❌ 

**Neutral**:
- 

#### Implementation Notes

<How to implement this decision>
```

---

## Rules

### 7.1 When to Write an ADR

Write an ADR when:
- ✅ Decision affects multiple layers or epics
- ✅ Decision is expensive to reverse
- ✅ Decision impacts external APIs
- ✅ Decision differs from common patterns

Skip ADR for:
- ❌ Simple implementation choices (which function name)
- ❌ Minor refactoring
- ❌ Bug fixes
- ❌ Local module decisions

### 7.2 ADR Lifecycle

1. **Draft**: Proposal under discussion
2. **Under Review**: Shared with stakeholders
3. **Accepted**: Ratified by architecture team
4. **Superseded**: Replaced by newer ADR
5. **Deprecated**: No longer valid (keep for history)

### 7.3 ADR Numbers

- Sequential numbering: ADR-001, ADR-002, etc.
- Never reuse numbers
- Keep old ADRs even if superseded (with status marked)

### 7.4 Immutability Rule

- Accepted ADRs cannot be edited (only their status changed)
- If issues found, create new ADR superseding the old one
- Rationale: maintain historical accuracy

---

## ADR Index

| Number | Title | Status | Date | Epic |
|--------|-------|--------|------|------|
| ADR-001 | External ID System | Accepted | 2026-06-15 | E001 |
| ADR-002 | Soft Delete with Audit Trail | Accepted | 2026-06-15 | E001 |
| ADR-003 | Deterministic ID Generation | Accepted | 2026-06-16 | E001 |
| ADR-004 | Clean Architecture Layers | Accepted | 2026-06-15 | E001 |
| ADR-005 | Repository Pattern | Accepted | 2026-06-16 | E001 |

---

## See Also

- [Change Policy](./change-policy.md) - How to propose changes
- [Epic Lifecycle](./epic-lifecycle.md) - Epic decision process
- [Review Process](./review-process.md) - Code review standards
- [Constitution](../core/constitution.md) - Core principles
