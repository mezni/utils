# Research: Mock API and Design System Foundation

**Phase**: 0 — Research & Unknown Resolution
**Date**: 2026-06-09

## Decisions

### Decision 1: json-server for mock API
- **Decision**: Use json-server 0.17.x as the mock REST API
- **Rationale**: Zero-config REST API from a JSON file. Supports filtering, pagination, CRUD operations. Routes.json rewrites /api/* to /* for API prefix compliance. Matches constitution Principle 10 (API prefix consistency) and Principle 1 (MVP-first delivery).
- **Alternatives considered**: Express.js mock (more setup, more flexible — unnecessary for MVP-1), PostgREST (requires real PostgreSQL — overkill for mock), json-graphql-server (GraphQL — not aligned with REST API design)

### Decision 2: Design tokens as TypeScript + Tailwind config
- **Decision**: Define tokens in TypeScript files with a shared tailwind.config.base.js preset
- **Rationale**: TypeScript ensures type safety and compile-time errors for missing tokens. Tailwind preset extends tokens to all web apps. native.ts exports plain JS values for React Native StyleSheet. Matches constitution Principle 9 (Visual consistency).
- **Alternatives considered**: CSS custom properties (no type safety, no RN support), JSON token files (no compile-time checking), Style Dictionary (overkill for current scope)

### Decision 3: pnpm workspace
- **Decision**: Use pnpm workspace with pnpm-workspace.yaml
- **Rationale**: Native monorepo support, strict dependency isolation, fast installs. Matches project requirement for monorepo structure.
- **Alternatives considered**: npm workspaces (slower, less strict isolation), yarn workspaces (comparable but pnpm is the project standard), turborepo (adds unnecessary orchestration for MVP-1)

## Seed Data Strategy

### Partner Seed States
Three partners demonstrating all flag combinations:

| Partner | Type | is_verified | is_live | is_active | Purpose |
|---------|------|-------------|---------|-----------|---------|
| 1 | business | true | true | true | Fully operational — stations visible on map |
| 2 | business | true | false | true | Verified but no stations (yet) — not on map |
| 3 | personal | false | false | true | Awaiting verification — not on map |

### Tunisian Cities for Stations
Tunis, Sfax, Sousse, Ettadhamen, Kairouan, Bizerte, Gabès, Ariana, Gafsa, El Mourouj, Kasserine, Monastir, Hammamet, Nabeul, Medenine — 1 station per city, 15 total.

### Connector Distribution
24 chargers across 15 stations: mix of Type 2 (12), CCS (6), CHAdeMO (4), Type 1 (2). Power ranges 7-350 kW.
