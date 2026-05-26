# Research: Partner Dashboard — Multi-Tenant Views

**Date**: 2026-05-26 | **Plan**: [plan.md](./plan.md)

## Decisions

### Decision 1: Backend scoping approach — repository-layer `owner_id` injection

**Decision**: Reuse the existing JWT middleware that extracts `user_id` and `role` from the token. For partner-scoped endpoints, add a `get_partner_profile(user_id)` lookup that retrieves the partner's `partner_profile_id` (via `partner_profiles` table join), then inject that ID as `owner_id` into all repository queries via an additional `WHERE owner_id = $N` clause.

**Rationale**: This matches the existing multi-tenancy pattern described in Constitution Principle II. The backend already has the infrastructure; it only needs the partner-profile lookup step added to the request context.

**Alternatives considered**:
- Passing raw `user_id` as owner_id (rejected: the users table and partner_profiles table have a 1:N relationship after the multi-user clarification; we need the partner_profile_id, not the user_id)
- Frontend-side filtering (rejected: violates constitutional data isolation — filtering must happen at the database extraction tier)

### Decision 2: Partner dashboard app architecture

**Decision**: Build the partner dashboard as a standalone Vite + React app under `sources/frontend/apps/partner-dashboard/`, mirroring the admin portal's structure. Reuse the existing `@bornemap/ui` package, AppShell layout, and BaseMap component. The app has its own `App.tsx` with routes scoped to 4 sections (Overview, Stations, Chargers, Profile).

**Rationale**: The constitution and repo structure already define `apps/partner-dashboard/` as a standalone app with its own `main.tsx`. Sharing the admin portal's code would create tight coupling and route conflicts.

**Alternatives considered**:
- Single app with role-based views (rejected: violates modular monorepo principle; different build/deploy targets)
- Iframe embedding (rejected: poor UX, complex auth sharing)

### Decision 3: Multi-user partner support

**Decision**: Multiple `users` rows can share the same `partner_profile_id`. The JWT token carries individual `user_id` and `role`, but the `owner_id` filter uses the shared `partner_profile_id`. When a user authenticates, the middleware looks up their associated partner profile to determine the `owner_id`.

**Rationale**: Partner orgs naturally have multiple employees. This avoids requiring credential sharing while keeping the data isolation simple (one `owner_id` per partner org).

**Alternatives considered**:
- One user per partner (rejected by clarification)
- Role hierarchy within a partner org (deferred post-MVP0)

### Decision 4: BaseMap reuse

**Decision**: The partner dashboard imports the BaseMap component directly from the admin portal. Since both apps live in the same monorepo and `@bornemap/ui` is shared, the component can be moved to a shared location or imported via relative path.

**Rationale**: The BaseMap is identical in functionality — only the data source differs (scoped to partner's stations). Duplicating the component violates the DRY principle.

**Alternatives considered**:
- Duplicate BaseMap in partner-dashboard (rejected: maintenance burden)
- Extract BaseMap to `@bornemap/ui` (preferred but potentially scope-creep; consider during implementation if the import path is clean)
