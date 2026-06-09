# ADR-020: Partner operational flags (is_verified, is_live, is_active)

**Status:** Accepted
**Date:** 2026-06-09

## Context

Partner accounts have distinct lifecycle states: identity verification, operational readiness, and account status. A single status field would conflate these concerns. For example: a verified partner may be temporarily suspended, or a newly created partner may not yet have any stations.

## Decision

Use three independent boolean flags: `is_verified` (admin-approved identity), `is_live` (has visible stations, requires is_verified), `is_active` (account operationally enabled). Stations are visible only when all three are true. The constraint `is_live = false OR is_verified = true` is enforced at the database level.

## Consequences

- Clear separation of identity, operational, and account states
- Database constraint prevents invalid state combinations
- UI can display each flag independently with appropriate badges
- Each flag has a distinct lifecycle and who can set it
- Slightly more complex query logic (three flags instead of one status)
