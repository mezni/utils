# ADR-009: Monorepo with Cargo and pnpm Workspaces

**Status**: Accepted
**Date**: 2026-06-07

## Context

The project contains Rust services, shared crates, JavaScript/TypeScript applications, and shared frontend packages. Options: monorepo with workspace tooling, or multiple repositories.

## Decision

Use a single monorepo with Cargo workspaces (Rust) and pnpm workspaces (JS/TS).

## Rationale

- Single source of truth for all code
- Atomic commits across backend and frontend changes
- Shared dependency versions via Cargo workspace.dependencies and pnpm workspace
- Consistent CI/CD configuration in one .github/workflows directory
- Easier onboarding: clone one repository, everything works
- Cargo workspaces enable shared crate development without publishing
- pnpm workspaces enable shared package development without npm publishing

## Consequences

- Repository size grows with every service and application
- CI must be path-scoped to avoid running all jobs on every change
- Root-level configuration files must handle both Rust and JS tooling
- Requires both Cargo and pnpm installed in CI environments

## Compliance

- All Rust code lives under crates/ or services/
- All JS/TS code lives under packages/ or apps/
- No service or application lives outside the monorepo
