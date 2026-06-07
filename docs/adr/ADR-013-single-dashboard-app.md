# ADR-013: Single Dashboard App over Separate Partner and Admin Apps

**Status**: Accepted
**Date**: 2026-06-07

## Context

The platform needs a dashboard for partners (manage their own stations) and admins (manage everything). Options: separate apps for each role, or a single app with role-based views.

## Decision

Build a single Dashboard application that serves both Partner and Admin roles. Role is determined from the JWT on login; the UI adapts accordingly.

## Rationale

- Shared codebase reduces duplication (navigation, layout, tables, forms)
- Both roles use the same visual design tokens
- Partners and admins have overlapping functionality (station management differs only in scope)
- A single app is simpler to deploy (one build, one URL)
- JWT role claim drives visibility — no separate deployment or route configuration

## Consequences

- The codebase must handle two role configurations gracefully
- Partner-only views must never leak admin data (enforced by JWT partner_id claim)
- Testing must cover both role configurations

## Compliance

- Role enforcement is server-side via JWT claims
- UI adapts based on role — never trusts client-side role values alone
- No separate frontend application for partner vs admin
