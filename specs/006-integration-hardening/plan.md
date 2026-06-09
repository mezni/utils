# Implementation Plan: Integration and Hardening

**Branch**: `006-integration-hardening` | **Date**: 2026-06-09 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/006-integration-hardening/spec.md`

## Summary

MVP-1 close-out sprint: full product loop verification across all 4 apps (Dashboard admin, Dashboard partner, Driver Web, Driver Mobile), edge case fix sweep (form validation, API offline handling, cross-browser/platform testing), and documentation (onboarding guide, mock API docs, MVP-1 status report). No new features — this sprint validates and hardens what exists.

## Technical Context

**Language/Version**: TypeScript 6.0 (Dashboard + Driver Web + Driver Mobile — existing projects)

**Primary Dependencies**: Vite 8, React 19, Tailwind 3 (web); Expo SDK 54, React Native (mobile); json-server (mock API) — all existing, no new deps

**Storage**: json-server at port 3001 — no change

**Testing**: Manual verification — cross-browser (Chrome, Firefox, Safari), cross-platform (iOS Simulator, Android Emulator), full product loop walkthrough

**Target Platform**: Web (modern browsers) + Mobile (iOS 15+ / Android 8+) — same as existing apps

**Project Type**: Integration & hardening across 4 existing projects

**Performance Goals**: All screens load within 3 seconds on broadband; zero crashes under any API condition

**Constraints**: No new features or dependencies; no automated E2E tests in MVP-1; all changes are bug fixes or documentation only

**Scale/Scope**: 4 apps, ~30 screens, full loop end-to-end, documentation under `docs/`

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| # | Principle | Check | Result |
|---|-----------|-------|--------|
| 1 | Dashboard-first delivery | All apps already built — hardening is after all deliveries | PASS |
| 2 | MVP-first delivery | No new features — only fixing what exists | PASS |
| 3 | Single source of truth | json-server remains sole data source | PASS |
| 4 | Visual consistency | No token changes — only form validation fixes | PASS |
| 5 | API prefix consistency | No API changes | PASS |
| 6 | No authentication | No auth changes in MVP-1 | PASS |
| 7 | Partner visibility | Already implemented — verified in full loop | PASS |

**Note**: Constitution (`constitution.md`) is still a template. No enforceable gates beyond project conventions.

**Gate verdict**: ALL PASS — proceed to Phase 0

## Project Structure

### Documentation (this feature)

```text
specs/006-integration-hardening/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output (separate command)
```

### Source Code (repository root)

No new source directories. Changes affect existing files in:

```text
source/apps/dashboard/src/
├── components/shared/Input.tsx           # Form validation fixes
├── components/shared/Modal.tsx           # Form submission validation hooks
├── pages/Partners/PartnersPage.tsx       # Partner CRUD verification
├── pages/Stations/StationsPage.tsx       # Lat/lng validation fix
├── pages/Chargers/ChargersPage.tsx       # Charger CRUD verification
└── pages/*/*.tsx                         # ErrorState audit across all screens

source/apps/driver-web/src/
└── pages/*.tsx                           # ErrorState audit

source/apps/driver-mobile/src/
└── screens/*.tsx                         # ErrorState audit, location denial fix

docs/
├── guides/onboarding.md                  # New — developer onboarding guide
├── api/mock-api.md                       # New — json-server API documentation
├── project/phases/mvp-01-status.md       # New — MVP-1 completion report
└── project/decisions.md                  # Updated — record cascade/block decision
```

**Structure Decision**: Changes are bug fixes and documentation only — no new projects or packages. Fixes are co-located with the affected components.

## Complexity Tracking

No constitution violations — complexity tracking not required.

---

## Phase 0: Research

### Unknowns & Research Tasks

| # | Unknown | Research Task |
|---|---------|---------------|
| R01 | Current state of form validation across all Dashboard forms | Audit all forms for missing validation — which fields allow empty/null/invalid values? |
| R02 | Current ErrorState coverage across all 4 apps | Audit all screens — which ones handle API errors? Which ones crash on API offline? |
| R03 | Partner deletion behavior | What happens when a partner with stations is deleted — json-server behavior? |

---

## Phase 1: Research Output

### R01 — Form Validation Audit

**Decision**: Audit all Dashboard forms (Add/Edit Partner, Station, Charger; Availability toggle). Required fields already have basic validation from Sprint 1.2/1.3. Lat/lng validation exists but needs verification.

**Rationale**: All admin forms were built with required field enforcement, but lat/lng range validation needs specific testing. The form submission pipeline should block invalid data before POST.

**Alternatives considered**: Adding a form validation library (out of scope — no new deps).

### R02 — ErrorState Coverage Audit

**Decision**: All 4 apps have ErrorState + retry on data fetch screens per Sprint 1.2/1.3/1.4/1.5 implementation. Verification needed by stopping json-server and testing each screen.

**Rationale**: ErrorState was a Sprint 1.2 requirement that was applied to all screens. The audit ensures no screen was missed during subsequent sprints.

**Alternatives considered**: N/A — this is verification only.

### R03 — Partner Deletion Behavior

**Decision**: json-server's default behavior cascades deletion — deleting a partner does NOT cascade to stations (foreign keys are not enforced). The decision to implement cascade or block in the UI is recorded in docs/project/decisions.md. Recommended: Block deletion in the UI when a partner has stations (check before deletion, show warning with station count).

**Rationale**: json-server has no referential integrity. A cascading delete would leave orphaned stations. Blocking with a warning is safer and matches real database behavior expected in MVP-2.

**Alternatives considered**: Cascade (delete all stations + chargers), Allow (let json-server handle it, leaving orphans).
