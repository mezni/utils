# Architecture Decisions

## AD-001: Partner Deletion Strategy

**Date**: 2026-06-09
**Context**: When an admin attempts to delete a partner that owns stations, json-server has no referential integrity — deleting the partner would leave orphaned stations with no owning partner.
**Decision**: Block deletion in the Dashboard UI when a partner owns one or more stations. The delete modal is replaced with a warning message showing the station count and instructing the admin to delete those stations first.
**Rationale**: This matches real database behavior (foreign key constraint) that will be enforced in MVP-2. Blocking with a warning prevents data corruption and is safer than cascading deletion which could delete data unintentionally.
**Alternatives considered**: Cascade (delete all stations and chargers — risky, silent data loss), Allow (let json-server create orphaned records — messy, requires cleanup).

## AD-002: Icon Strategy

**Date**: 2026-06-09
**Context**: Dashboard app needed icons for sidebar navigation items (Home, Partners, Stations, Chargers, Availability).
**Decision**: Use simple Unicode symbols and CSS-styled elements instead of an icon library. The lightning bolt in the sidebar brand header is a custom SVG inline element.
**Rationale**: No icon library dependency keeps the bundle small. The MVP-1 icon set is small (5 icons). A proper icon library (e.g., Lucide, Phosphor) can be introduced in MVP-2 if needed.
**Alternatives considered**: Lucide, Phosphor, Heroicons, Font Awesome.

## AD-003: Client-Side Partner Visibility

**Date**: 2026-06-09
**Context**: Driver apps need to filter out stations belonging to partners that are not verified, not live, or not active.
**Decision**: Compute the visibility filter client-side. Fetch all partners and stations, filter stations where `partner.is_verified && partner.is_live && partner.is_active`.
**Rationale**: json-server cannot perform cross-resource joins. With fewer than 20 stations, the overhead of fetching all data client-side is negligible. The filter logic is simple and shared across both driver apps.
**Alternatives considered**: Server-side middleware (json-server doesn't support joins), Pre-filter stations in db.json (inflexible).

## AD-004: Dev Role Switcher

**Date**: 2026-06-09
**Context**: MVP-1 has no authentication. Partner view needs to be testable.
**Decision**: Add a dev-only role switcher at the bottom of the sidebar. It toggles between Admin View and Partner View. When Partner View is active, a partner selector dropdown appears. Labeled "Dev Only — removed in MVP-3".
**Rationale**: Enables testing of both admin and partner workflows without authentication. The explicit labeling and planned removal prevent it from becoming a permanent feature.
**Alternatives considered**: URL-based switching (less discoverable), localStorage flag (persists unexpectedly).

## DEC-002 — Dedicated PostgreSQL Instance for Keycloak

**Date**: 2026-06-09
**Context**: Original design used a `keycloak` schema inside the `ev_platform` database. This couples Keycloak's internal migrations, connection usage, and backup lifecycle with the application database.
**Decision**: Keycloak runs against a dedicated PostgreSQL container (`postgres-keycloak`) with its own database named `keycloak_db`. The application database (`ev_platform`) runs in a separate container (`postgres-app`). The two instances never share connections, schemas, or backup procedures.
**Alternatives considered**: Shared PostgreSQL instance with `keycloak` schema (removed), managed cloud database for Keycloak (rejected — bare metal strategy).
**Consequences**:
- Migration `0005_keycloak_schema.sql` is removed entirely
- Docker Compose gains a second PostgreSQL container
- Two separate backup procedures — one per database
- Keycloak environment variables point to `postgres-keycloak`
- Application services point to `postgres-app`
- Two pgAdmin connections needed in development
