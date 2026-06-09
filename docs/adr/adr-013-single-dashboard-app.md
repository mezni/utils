# ADR-013: Single Dashboard App for partner and admin

**Status:** Accepted
**Date:** 2026-06-09

## Context

Partner and Admin are separate roles with different navigation and data scopes. Two options: build separate dashboard apps, or build a single app with role-based views. Separate apps would duplicate the AppShell, sidebar, and shared components.

## Decision

Build a single Dashboard App serving both Partner and Admin roles. Navigation, screens, and data access are determined by the user's role (from JWT in MVP-3, from dev role switcher in MVP-1/2). The same codebase, same build, same deployment.

## Consequences

- Zero code duplication between admin and partner UIs
- Shared components, styling, and configuration
- Role-based rendering is a single conditional pattern
- Must carefully guard against leaking data across role boundaries
- Dev role switcher needed until MVP-3

