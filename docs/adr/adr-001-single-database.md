# ADR-001: PostgreSQL + PostGIS as Single Database

**Status:** Accepted  
**Decision Date:** 2026-01-15  
**Related:** ADR-002, ADR-008

---

## Context

BorneMap requires:
- Geographic/GIS data storage (stations, OpenStreetMap data)
- Relational business data (partners, stations, chargers)
- User and profile data (accounts, favorites, reviews)
- Analytics event storage

Early architectural discussions considered multiple database technologies:
- Separate databases per domain (PostgreSQL for business, separate GIS database, separate analytics)
- Multiple specialized data stores (PostgreSQL + Elasticsearch + InfluxDB)
- Separate PostgreSQL instances with replication

### Constraints
- **Simplicity:** One person must be able to operate the platform
- **Atomicity:** Transactions must span business and GIS data
- **Current Scale:** 10,000s of stations, not millions

---

## Decision

Use a **single PostgreSQL 16 instance with PostGIS extension** as the universal data store for all application data.

**Scope:**
- Business entities (partners, stations, chargers)
- User data (accounts, profiles, relationships)
- Geographic/spatial data (OpenStreetMap, station locations)
- Analytics events (clickstream)

**Separation:** Achieve data isolation through **four PostgreSQL schemas**, not multiple databases (see ADR-002).

---

## Rationale

### 1. Atomic Transactions
GIS synchronization (see ADR-008) requires atomicity between `inventory.station` and `gis.station_locations`. A single database with triggers ensures:
- Writes are all-or-nothing
- No eventual consistency delays
- No network partitions between databases

### 2. Operational Simplicity
Single database means:
- One set of credentials
- One backup/restore procedure
- One replication pipeline (if needed)
- One connection pool to manage
- One service to monitor

Reduces complexity for one-person operations team.

### 3. PostGIS Capabilities
PostgreSQL + PostGIS is the defacto standard for spatial operations:
- Mature spatial indexing (GIST, BRIN)
- ST_Contains, ST_Distance, ST_DWithin for geospatial logic
- Seamless integration with business data in same database
- No impedance mismatch between relational and spatial queries

### 4. Cost & Licensing
- PostgreSQL is open source, no licensing costs
- PostGIS is open source
- Single instance is cheaper than multiple specialized databases
- Fits budgetary constraints for Tunisia-based startup

### 5. Transaction Consistency
Analytics aggregation (see ADR-004) can read `raw_events` in same transaction as business queries. Consistency guaranteed at database level.

---

## Consequences

### Positive
- ✅ Simple operational model
- ✅ Atomic transactions across domains
- ✅ Mature spatial capabilities
- ✅ Single backup/restore procedure
- ✅ No network partition issues
- ✅ Cost effective

### Negative
- ❌ Cannot infinitely scale horizontally (sharding required at very large scale)
- ❌ All workloads compete for single connection pool
- ❌ No isolated failure domains by data type

### Mitigations
- **Scale:** Build for current scale (ADR-005 — Principle 5). If traffic exceeds single database capacity, introduce read replicas (requires future ADR).
- **Isolation:** Schema separation (ADR-002) provides logical isolation and clear boundaries
- **Monitoring:** Query performance monitoring alerts to slow queries before they become problems

---

## Alternatives Considered

### 1. Separate PostgreSQL instances (one per schema)
**Rejected** because:
- Violates ADR-002 (schema separation is preferred)
- Adds operational complexity (three databases to manage)
- Breaks atomic transactions across schemas
- No cost benefit (three instances cost more than one)

### 2. PostgreSQL + Elasticsearch (for analytics)
**Rejected** because:
- Analytics queries don't require full-text search (no Elasticsearch benefit)
- Adds operational complexity
- Violates principle of pragmatic architecture
- Direct database writes suffice

### 3. PostgreSQL + InfluxDB (for time-series)
**Rejected** because:
- Analytics events are not true time-series (sparse, categorical)
- InfluxDB adds operational complexity
- PostgreSQL is adequate for analytics scale

### 4. Cloud-managed database (RDS, CloudSQL)
**Rejected** because:
- Platform must run on bare metal (ops team preference)
- Cloud database adds latency and cost
- Reduces operational autonomy

---

## Validation

This decision is validated by:
1. ✅ Single database operational model (ADR-006)
2. ✅ GIS trigger approach (ADR-008) requires atomic transactions
3. ✅ Direct analytics insert (ADR-004) works with single database
4. ✅ Schema separation enforced at database level (ADR-002)
5. ✅ No other ADRs contradict this choice

---

## Questions & Answers

**Q: What if we outgrow a single PostgreSQL instance?**
A: Build for current scale (principle 5). If traffic exceeds single instance, introduce read replicas (requires future ADR with load testing data).

**Q: Why not use MongoDB for flexibility?**
A: Spatial queries and atomic transactions require relational semantics. MongoDB lacks native spatial indexing and doesn't guarantee ACID across documents.

**Q: Can we add a cache layer later?**
A: Yes. Query performance monitoring will alert if caching is needed. Can add Redis read-through cache (requires ADR) without changing this decision.

---

**Document Version:** 1.0  
**Last Updated:** 2026-06-05
