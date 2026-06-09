# Research: Dashboard Partner View

## R01 — Data-Scoping with RoleContext.selectedPartnerId

**Decision**: Each partner page reads `selectedPartnerId` from `useRole()` and passes it as API filter parameters.

**Rationale**: json-server supports `?partner_id=` filter queries natively. For chargers, two API calls are needed: first fetch partner's stations to get station IDs, then fetch chargers filtered by those station IDs. json-server supports `?station_id=STN001&station_id=STN002` syntax.

**Alternatives considered**: Fetch all data and filter client-side (wasteful for larger datasets), compute derived endpoints on the server (not possible with json-server), add a custom json-server middleware (over-engineered for MVP-1 scope).

## R02 — Latest Station Availability per Station

**Decision**: Compute client-side by grouping `station_availability` records by `station_id` and selecting the record with the latest `updated_at` for each group.

**Rationale**: station_availability is append-only. json-server cannot compute `DISTINCT ON` or window functions. Client-side grouping is a single O(n) pass over the array. The data set is small (at most a few hundred records).

**Alternatives considered**: json-server lowdb hooks to maintain a materialized view (adds complexity for MVP-1), storing current status in a separate field (would need a schema change), treating the latest POST response as current without refetching (fragile — other users may update).

## R03 — Availability Toggle UX

**Decision**: Pessimistic update with disabled toggle during API call, refetch on success, revert on error.

**Rationale**: Availability data must be accurate for drivers. An optimistic update showing "Available" after a failed POST is misleading. The 200-500ms API round-trip is fast enough that users won't notice the brief disabled state.

**Alternatives considered**: Optimistic update with rollback (faster UX but risk of incorrect data display), fire-and-forget with background refresh (user sees stale data longer), inline spinner (more complex UI for no real benefit).

## R04 — Partner Status Bar

**Decision**: Three horizontal badge groups: Verified/Awaiting, Live/Not Live, Active/Suspended. Uses StatusBadge-like styling (green for true, gray for neutral false, red for suspended).

**Rationale**: Clear visual grouping of the three independent operational flags. Matches the existing StatusBadge component convention. Easy to scan at a glance.

**Alternatives considered**: Single combined status text bar (loses detail of individual flags), icon-only indicators (harder to interpret at a glance), table format (takes too much vertical space in an overview screen).
