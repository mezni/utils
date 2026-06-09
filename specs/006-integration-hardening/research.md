# Phase 0 Research: Integration and Hardening

## R01 — Form Validation Audit

**Decision**: Audit all Dashboard forms (Add/Edit Partner, Station, Charger; Availability toggle). Required fields already have basic validation from Sprint 1.2/1.3. Lat/lng validation exists but needs verification.

**Rationale**: All admin forms were built with required field enforcement, but lat/lng range validation needs specific testing. The form submission pipeline should block invalid data before POST.

**Alternatives considered**: Adding a form validation library (out of scope — no new deps).

## R02 — ErrorState Coverage Audit

**Decision**: All 4 apps have ErrorState + retry on data fetch screens per Sprint 1.2/1.3/1.4/1.5 implementation. Verification needed by stopping json-server and testing each screen.

**Rationale**: ErrorState was a Sprint 1.2 requirement that was applied to all screens. The audit ensures no screen was missed during subsequent sprints.

**Alternatives considered**: N/A — this is verification only.

## R03 — Partner Deletion Behavior

**Decision**: Record in `docs/project/decisions.md`. Recommended approach: block deletion in Dashboard UI when a partner owns stations (check before deletion, show warning with station count). json-server has no referential integrity, so allowing deletion would leave orphaned stations.

**Rationale**: Blocking with a warning is safer and matches real database behavior expected in MVP-2 (foreign key constraint).

**Alternatives considered**: Cascade (delete all stations + chargers), Allow (let json-server handle it, leaving orphans).
