# ADR-011: React + Vite for Web Applications

**Status**: Accepted

**Date**: 2026-01-01

## Context

Two web applications are required: Driver Web (public map discovery) and Dashboard (partner/admin management). Technology choices: React, Vue, Svelte; Vite or Webpack bundler.

## Decision

**Use React + Vite for both Driver Web App and Dashboard App.**

Both are initialized at `source/apps/driver-web/` and `source/apps/dashboard/`.

## Rationale

- **Component ecosystem**: React has mature ecosystem for maps (react-leaflet), tables, forms.
- **Vite**: Fast cold start, instant HMR, optimized builds. No eject needed.
- **Shared tokens**: Both apps consume design system from `source/packages/ui`. React makes monorepo patterns simple.
- **Tailwind integration**: Clean Tailwind + TypeScript + React development experience.
- **Team familiarity**: React is the team's established frontend skill.

## Consequences

- Both apps depend on Node 18+ and npm for builds.
- Tailwind is extended from shared base config in each app (`npm run dev` watches both app and packages/ui).
- Both apps must consume design tokens, never hardcode colors or spacing.
- No separate CSS-in-JS framework; Tailwind is the styling solution.

## References

- Constitution section 4: Frontend Applications
- Constitution section 5.4: Token Delivery
- Implementation Plan, Sprints 1.2 and 1.3: Dashboard and Driver Web
