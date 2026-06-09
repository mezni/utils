# ADR-005: Rust + Actix-web for backend (from MVP-2)

**Status:** Accepted
**Date:** 2026-06-09

## Context

MVP-1 uses json-server for rapid prototyping. From MVP-2 onward, the platform requires a production-grade backend with a database, spatial queries, and authentication. The language must be performant, memory-safe, and have strong ecosystem support for PostgreSQL.

## Decision

Use Rust with the Actix-web framework for all backend services from MVP-2 onward. Database access via sqlx with compile-time checked queries. json-server is replaced entirely — no gradual migration.

## Consequences

- Excellent performance and low memory footprint
- Compile-time SQL verification via sqlx macros
- Memory safety without garbage collection
- Faster development cycle than C++ or Go for this domain
- Team must be proficient in Rust
