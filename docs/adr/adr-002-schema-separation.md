# ADR-002: Schema Separation Over Database Separation

**Status:** Accepted  
**Decision Date:** 2026-01-16  
**Supersedes:** None  
**Related:** ADR-001

---

## Context

A single PostgreSQL database (ADR-001) requires data isolation. Two approaches considered:

1. **Schema Separation** — Four schemas within one database (inventory, users, gis, analytics)
2. **Database Separation** — Multiple PostgreSQL instances, each owning specific data domain

---

## Decision

Use **schema separation within a single database**. Data is logically isolated by PostgreSQL schema, not by separate database instances.

**Schemas:**
- `inventory` — Partners, stations, chargers (Admin Service writes)
- `users` — User accounts, profiles, favorites, reviews (Driver Service writes)
- `gis` — Spatial data and enrichment (Trigger writes)
- `analytics` — Events and aggregates (Clickstream Service writes)

Each schema has exclusive write access enforced by application-level middleware and documented cross-schema access rules.

---

## Rationale

### 1. Single Database Benefits (ADR-001)
Schema separation preserves single-database benefits while adding logical isolation.

### 2. Atomic Transactions
Many operations require atomic writes across schemas:
- Station write (inventory) triggers GIS sync (gis) in same transaction
- Analytics reads (analytics) can occur in same transaction as business queries

**Separate databases would break atomicity.**

### 3. Enforceability
Schema separation provides clear, enforceable boundaries:
- Services can be restricted to specific schemas via database roles
- Cross-schema access explicitly documented and reviewed
- Violations are code/query audits, not architectural assumptions

### 4. Operational Simplicity
- Simpler than multi-database replication
- Easier backup/restore (single database)
- Single transaction log
- Middleware enforces access rules

### 5. Migration Autonomy
Each service owns its schema migrations. Services are independent:
- Driver Service migrations don't affect Admin Service
- No coordination needed at database level
- Parallel deployment possible

---

## Consequences

### Positive
- ✅ Atomic transactions across domains
- ✅ Clear data ownership boundaries
- ✅ Enforceable cross-schema rules
- ✅ Service migration independence
- ✅ Simpler operational procedures
- ✅ No network partitions between "databases"

### Negative
- ❌ Services must still coordinate schema changes (at migration level)
- ❌ One service bug can affect another's schema (requires discipline)

### Mitigations
- **Discipline:** Cross-schema access rules documented and audited
- **Middleware:** Application middleware enforces scope isolation (e.g., partner scope)
- **Testing:** Integration tests verify no accidental cross-schema reads/writes

---

## Alternatives Considered

### 1. Separate PostgreSQL databases (one per schema)
**Rejected** because:
- Breaks atomic transactions across schemas (GIS sync, analytics aggregation)
- Adds operational complexity
- No isolation benefit (still all Postgres)
- Requires multi-database replication

### 2. Table prefixing within single schema
**Rejected** because:
- No logical isolation
- Harder to enforce access rules
- Violates DDD principle of bounded contexts
- Less clear data ownership

### 3. Views for access control
**Rejected** because:
- PostgreSQL row-level security is per-role, not per-service
- Views don't provide write isolation
- Harder to enforce at application layer

---

## Schema Responsibilities

| Schema | Owner | Writes | Reads | Purpose |
|--------|-------|--------|-------|---------|
| inventory | Admin Service | Admin | Admin, Driver, Trigger | Business entities |
| users | Driver Service | Driver | Driver, Admin (reporting) | User data |
| gis | Trigger | Trigger | Driver (spatial) | Spatial enrichment |
| analytics | Clickstream | Clickstream | Admin (reporting) | Event data |

**Critical Rule:** Each service reads only its own schema, plus shared schemas as documented.

---

## Enforcement

**At Application Layer:**
- Middleware extracts user role from JWT
- Role determines which schemas are accessible
- Partner scope enforced via partner_id claim
- Queries fail if accessing unpermitted schemas

**At Database Layer:**
- PostgreSQL roles restrict schema access
- Cross-schema views for required reads
- Trigger function handles GIS writes

---

## Questions & Answers

**Q: What if a service accidentally writes to the wrong schema?**
A: Application middleware prevents it. If schema access is truly needed, it must be documented and reviewed (cross-schema rule update).

**Q: Can we add a fifth schema later?**
A: Yes, but requires an ADR justifying why it can't be addressed in existing schemas.

**Q: Why not use a separate database for analytics?**
A: Atomic aggregation queries require consistency with business data. Separate database breaks atomicity.

---

**Document Version:** 1.0  
**Last Updated:** 2026-06-05
