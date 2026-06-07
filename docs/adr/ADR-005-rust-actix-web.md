# ADR-005: Rust + Actix-web for Backend Services

**Status**: Accepted
**Date**: 2026-06-07

## Context

Backend services need a runtime that is performant, type-safe, and produces small, statically-linked binaries for simple deployment.

## Decision

Use Rust with the Actix-web framework for all backend services.

## Rationale

- Rust provides memory safety without garbage collection
- Actix-web is one of the fastest HTTP frameworks available
- Static binaries simplify Docker images (no interpreter, no runtime dependencies)
- sqlx provides compile-time checked SQL queries
- Strong type system catches entire classes of bugs at compile time
- Team expertise and preference

## Consequences

- Longer compile times compared to dynamic languages
- Rust's ownership model has a learning curve
- sqlx requires a running database for compile-time query checking (mitigated by cargo sqlx prepare)
- Binary size is larger than an interpreted equivalent but smaller than a full runtime

## Compliance

- All services follow the same internal structure from the constitution (config, router, errors, handlers, db)
- All database queries use sqlx::query_as! or sqlx::query! macros
- No ORM permitted
