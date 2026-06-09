# Research: MVP-1 Foundation Setup

**Phase**: Phase 0 — Outline & Research
**Date**: 2026-06-08

## Overview

Sprint 1.1 is well-specified by the constitution and user input. No unresolved technical unknowns required external research. This document confirms the established decisions.

## Decisions

### Mock API Approach

- **Decision**: json-server with routes.json for `/api` prefix
- **Rationale**: Specified in constitution (ADR-016). Zero-code REST API from a single JSON file. Fits MVP-1 need for a working backend without any service code.
- **Alternatives considered**: None — mandated by constitution.

### Design Token Delivery

- **Decision**: TypeScript token files exporting const objects + Tailwind config
- **Rationale**: Constitution specifies `tailwind.config.base.js` for web apps and `tokens/native.ts` for React Native. TypeScript gives type safety and IDE autocompletion.
- **Alternatives considered**: CSS custom properties (no type safety), JSON-only (no TypeScript validation).

### Package Manager & Workspace

- **Decision**: pnpm workspace with root-level scripts
- **Rationale**: pnpm is constitution-mandated. Workspace allows apps and packages to reference each other via `@borne/` namespace.
- **Alternatives considered**: npm workspaces (slower), Yarn workspaces (not specified in constitution).

### Seed Data Strategy

- **Decision**: 3 partners, 15 Tunisian stations with real city coordinates, 24 chargers
- **Rationale**: Sufficient data for all three apps to develop against. Real Tunisian cities (Tunis, Sfax, Sousse, etc.) make the map meaningful during development.
- **Alternatives considered**: Fewer stations (wouldn't cover map edge cases), random coordinates (not realistic).

### Nearby Station Filtering

- **Decision**: Client-side only in MVP-1 — server returns all stations
- **Rationale**: json-server has no spatial query support. Client-side Haversine formula or similar in the driver apps. Server-side spatial filtering arrives in MVP-2+ with PostGIS.
- **Alternatives considered**: None — json-server limitation accepted per constitution.

## Dependencies

- **json-server 0.x**: Stable, widely used mock REST API tool. No configuration beyond db.json and routes.json.
- **TypeScript 5.x**: Required for token file type safety.
- **pnpm 9.x**: Required by constitution for workspace management.
- **concurrently**: For running mock server + dashboard in parallel (root `dev` script).

## Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| json-server EOL/deprecation | Low | Medium | json-server is stable v0.x; MVP-2 replaces with Rust services anyway |
| Token file drift between web and mobile | Medium | Medium | Constitution mandates native.ts sync in same commit as colors.ts changes |
| Port conflicts (3001 in use) | Low | Low | Document how to change port in quickstart.md |
