# ADR-009: Monorepo with source/ root

**Status:** Accepted
**Date:** 2026-06-09

## Context

The project has multiple frontend apps (Driver Web, Driver Mobile, Dashboard), backend services (driver-service, admin-service, clickstream-service), shared Rust crates, and shared frontend packages. Multiple repositories would add cross-repo coordination overhead, versioning complexity, and CI duplication.

## Decision

Use a single monorepo with `source/` as the root for all application code. `source/apps/` contains frontend applications, `source/services/` contains backend services, `source/packages/` contains shared frontend packages, `source/services/crates/` contains shared Rust crates. `database/` contains migrations and seeds. `docs/` contains all documentation.

## Consequences

- Single `git clone` for the entire platform
- Cross-project refactoring in a single commit
- Shared CI configuration and tooling
- Workspace tooling (pnpm for JS, cargo for Rust) manages dependencies
- Larger repository size, but well-structured with clear directory boundaries
