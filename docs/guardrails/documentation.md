# Guardrail — Documentation

Applies to: all code in `source/`, all files in `docs/`

---

## Philosophy

Documentation is part of the code. An undocumented public function is an incomplete function. Documentation must explain *why*, not just *what* — the code already shows what.

---

## Rust documentation

Every `pub` item (function, struct, enum, trait, module) must have a doc comment. No exceptions.

```rust
/// Finds stations within `radius_m` metres of the given coordinates.
///
/// Queries the `mv_stations_geo` materialized view via PostGIS `ST_DWithin`.
/// Results are ordered by distance ascending.
///
/// # Errors
///
/// Returns [`AppError::Database`] if the query fails.
/// Returns an empty `Vec` (not an error) when no stations are within range.
///
/// # Example
///
/// ```rust,no_run
/// let stations = repo.find_nearby(36.8189, 10.1658, 5000).await?;
/// ```
pub async fn find_nearby(
    &self,
    lat: f64,
    lng: f64,
    radius_m: i32,
) -> Result<Vec<StationSummary>, AppError> {
```

Rules:
- First line: one sentence summary ending with a period.
- `# Errors` section: list every error variant the function can return and under what condition.
- `# Panics` section: required if the function can panic (which it should not — see Rust guardrail).
- `# Example` section: required on all public repository and service methods.
- Internal (`pub(crate)` or private) functions: doc comment optional but encouraged for non-obvious logic.
- Use `//` for inline comments explaining *why* a decision was made. Never comment the obvious.

```rust
// ST_DWithin uses a spheroid calculation — more accurate than ST_Distance for Tunisia's
// latitude range but ~15% slower. Acceptable given cached materialized view.
```

---

## Module-level documentation

Every module file (`mod.rs` or a named module) must have a module doc comment at the top:

```rust
//! Station repository — PostgreSQL implementation of [`StationRepository`].
//!
//! All queries target the `inventory` schema. The materialized view
//! `mv_stations_geo` is the primary read target for spatial queries;
//! `inventory.stations` is the write target.
//!
//! Cache invalidation is the responsibility of the caller (StationService),
//! not this module.
```

---

## TypeScript documentation

Use JSDoc for all exported functions, hooks, types, and components.

```typescript
/**
 * Fetches stations within `radius` metres of the given coordinates.
 *
 * Debounced by 300ms — safe to call on every map viewport change.
 * Falls back to AsyncStorage cache when the network request fails.
 *
 * @param options.lat  Latitude of the viewport centre
 * @param options.lng  Longitude of the viewport centre
 * @param options.radius  Search radius in metres (default: 5000)
 * @returns React Query result with `stations` array and `queryBoundingBox`
 *
 * @example
 * const { data, status } = useNearby({ lat: 36.8189, lng: 10.1658 });
 */
export function useNearby(options: NearbyOptions): UseQueryResult<NearbyResponse> {
```

Rules:
- All exported hooks, utilities, and components get JSDoc.
- `@param` and `@returns` are required on all non-trivial functions.
- `@example` required on all hooks and shared utilities.
- React components: document `props` via the TypeScript interface, not JSDoc params.

```typescript
interface StationCardProps {
  /** Station data to render. Must include coordinates for the map pin. */
  station: StationSummary;
  /** Called when the user taps the favourite icon. Requires authenticated session. */
  onFavourite?: (stationId: string) => void;
}
```

---

## docs/ directory structure

```
.specify/memory/
  constitution.md          # Architectural authority (never auto-generated)
docs/
  GUARDRAILS.md            # LLM entry-point — read first every session
  SYSTEM_STATE.md          # Current build state — updated every session
  roadmap_status.md        # MVP completion status
  sprint_backlog.md        # Current sprint tasks
  bug_tracker.md           # Active bugs, reproduction steps, and fixes
  adr/                     # Architecture Decision Records
    ADR-001-keycloak-single-realm.md
    ADR-002-...
  specs/
    mvp-1-admin-flow.md
    mvp-2-gis.md
    ...
  guardrails/              # Domain guardrails (see GUARDRAILS.md index)
  api/                     # Auto-generated OpenAPI specs (do not edit manually)
```

---

## ADR format

Every architectural decision that amends the constitution requires an ADR:

```markdown
# ADR-XXX — [Short title]

**Date:** YYYY-MM-DD
**Status:** Accepted | Superseded by ADR-YYY

## Context

What problem were we solving? What constraints existed?

## Decision

What did we decide to do?

## Consequences

**Positive:** ...
**Negative:** ...
**Risks:** ...
```

---

## SYSTEM_STATE.md format

Updated at the end of every builder session:

```markdown
# BorneMap — System State

Last updated: YYYY-MM-DD  Session: [session identifier]

## Built and verified

- [ ] MVP-1: Auth Service login/refresh endpoints
- [x] MVP-1: Keycloak realm + client configuration
- [x] MVP-1: users schema migration

## In progress

- MVP-2: Driver Service find_nearby endpoint (handler done, integration test pending)

## Known issues

- migration 003 has a typo in index name — fix before MVP-3

## Environment

- platform_db: migrations up to 005
- Keycloak: realm bornemap, clients configured
- Redis: not yet provisioned (MVP-5)
```

---

## Self-check before submitting

- [ ] Every new `pub` Rust item has a doc comment with `# Errors` section
- [ ] Every new exported TypeScript function/hook has JSDoc with `@param` and `@returns`
- [ ] `SYSTEM_STATE.md` updated to reflect work done this session
- [ ] Any architectural decision has a corresponding ADR in `docs/adr/`
- [ ] No TODO comments without a linked issue reference
- [ ] `cargo doc --no-deps` generates without warnings
