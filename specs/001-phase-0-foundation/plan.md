# Implementation Plan: Phase 0 — Foundation & Governance

**Branch**: `001-phase-0-foundation` | **Date**: 2026-05-22 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/001-phase-0-foundation/spec.md`

## Summary

Ship the governance scaffolding that every later phase depends on: a
top-level repository entry point linking the existing
`docs/` artefacts (Constitution, Roadmap, ADRs, Architecture overview,
Methodology, Operations), a `CONTRIBUTING.md` that consolidates branch
strategy + PR workflow + Definition of Done + ADR amendment procedure, a
GitHub PR template that forces declaration of roadmap phase and
constitutional principles, a `CODEOWNERS` file with a wildcard
Maintainers fallback, and branch-protection rules on `main`. No
application code, no CI jobs, no containers — those are Phase 1.

Technical approach: deliver a small set of plain-text artefacts at fixed
repository paths (mostly GitHub-native conventions:
`.github/PULL_REQUEST_TEMPLATE.md`, `.github/CODEOWNERS`, root-level
`README.md` and `CONTRIBUTING.md`). The repository-platform settings
(branch protection, auto-delete on merge) are configured **outside the
repository contents** in the GitHub UI/API and documented in the plan
runbook so they can be reproduced.

## Technical Context

> Phase 0 is a governance/documentation feature, not running software.
> Most "Technical Context" fields therefore resolve to "N/A — no code
> shipped". Fields that *do* apply describe the platform conventions
> and external configuration this feature relies on.

**Language/Version**: N/A (Markdown documents + GitHub configuration
files). Markdown follows GitHub Flavored Markdown.

**Primary Dependencies**: N/A in the code sense. Relies on GitHub
platform features: PR templates, CODEOWNERS, branch protection rules,
auto-delete-branch-on-merge setting, scheduled workflows (deferred to
Phase 1).

**Storage**: N/A.

**Testing**: Manual verification per `quickstart.md` and a link-integrity
check (markdown linter / link checker) that will be wired into CI in
Phase 1. At Phase 0 the checks are run by hand and recorded.

**Target Platform**: GitHub.com hosting the BorneMap repository.

**Project Type**: governance / documentation feature inside a polyrepo
project root.

**Performance Goals**: N/A.

**Constraints**:

- Top-level repository layout fixed by Constitution §"Repository
  structure" (`docs/`, `services/`, `frontend/`, `infra/`,
  `docker-compose.yml`, `Makefile`, `CONTRIBUTING.md`). Phase 0
  delivers only `CONTRIBUTING.md` and a top-level `README.md`; the
  other directories are out of scope for this feature (`docs/`
  already exists from the documentation commit).
- All governance documents MUST be readable offline (no required
  external links for canonical rules) — see spec Edge Cases.
- The feature MUST NOT add a runtime dependency, CI job, container,
  or service.

**Scale/Scope**: 7 new or modified files at the repository root and
under `.github/`. ~400–600 lines of Markdown total. Small team (solo to
≤5 Maintainers at ratification).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Constitution v1.0.0 principles I–VII evaluated against this feature:

| Principle | Applicability | Compliance |
|---|---|---|
| I. Clean Modular Architecture | N/A — no services shipped | ✅ pass (vacuous) |
| II. Domain Model Integrity | N/A — no domain entities | ✅ pass (vacuous) |
| III. Event Integrity via Outbox | N/A — no events produced | ✅ pass (vacuous) |
| IV. Soft-Delete Discipline | N/A — no DB schema | ✅ pass (vacuous) |
| V. Security & Identity | Partial — Phase 0 sets commit-provenance (DCO) and branch-protection rules that are the *governance side* of security; no application auth in scope | ✅ pass — FR-013, FR-016 strengthen security posture |
| VI. Observability | N/A — no runtime services | ✅ pass (vacuous) |
| VII. Quality & Testing Discipline (DoD) | Applies to the PR-template itself, which must surface the DoD checklist; no test categories are produced by this feature because no code ships | ✅ pass — FR-006 requires DoD checklist in PR template |

ADR Governance Rule: this feature does **not** affect any constitutional
boundary (no service responsibility, data ownership, identity provider,
event pipeline, soft-delete scope, deployment topology, or approved
stack change). No new ADR required.

**Result: PASS. No gate violations, no complexity-tracking entries.**

## Project Structure

### Documentation (this feature)

```text
specs/001-phase-0-foundation/
├── plan.md              # This file (/speckit.plan output)
├── spec.md              # Feature specification (/speckit.specify output)
├── research.md          # Phase 0 of /speckit.plan — research findings
├── data-model.md        # Phase 1 of /speckit.plan — governance entities
├── quickstart.md        # Phase 1 of /speckit.plan — verification runbook
├── contracts/
│   ├── pr-template.md   # PR-author surface contract
│   ├── codeowners.md    # CODEOWNERS schema contract
│   └── branch-protection.md  # main-branch ruleset contract
├── checklists/
│   └── requirements.md  # spec quality checklist (already exists)
└── tasks.md             # /speckit.tasks output — NOT created here
```

### Repository artefacts produced by this feature

This feature ships **files at fixed repository paths**, not under a
`src/` tree. The full target inventory:

```text
bornemap/                              (repository root)
├── README.md                          # NEW — top-level entry point (FR-001)
├── CONTRIBUTING.md                    # NEW — contributor guide (FR-004, FR-009, FR-014, FR-015, FR-016)
├── .github/
│   ├── PULL_REQUEST_TEMPLATE.md       # NEW — PR template (FR-005, FR-006)
│   └── CODEOWNERS                     # NEW — ownership + Maintainers fallback (FR-007, FR-008)
├── docs/                              # already exists (prior commit)
│   ├── README.md                      # already exists
│   ├── roadmap.md                     # already exists
│   ├── methodology.md                 # already exists
│   ├── architecture/overview.md       # already exists
│   ├── adr/                           # already exists (0001-0005 + template)
│   └── operations/deployment.md       # already exists
└── .specify/memory/constitution.md    # already exists, v1.0.0
```

Configuration applied **outside repository contents** (GitHub repo
settings, documented in `quickstart.md` so anyone can reproduce):

- Branch protection ruleset on `main` (FR-013).
- "Automatically delete head branches" repo setting enabled (FR-015 first
  half).
- Stale-branch flagging workflow — deferred to Phase 1 per research
  decision; documented in `CONTRIBUTING.md` as "flagged by Maintainers
  during weekly review until automated" (FR-015 second half).

**Structure Decision**: Phase 0 is a *governance feature*; it does not
fit any of the plan-template's standard layouts (single-project,
web-app, mobile+API). The selected structure is the **repository-root
artefacts** inventory above, mapped 1:1 to functional requirements in
the spec. No `src/` or `tests/` directories are created.

## Progress Tracking

- [X] Constitution Check: initial (PASS)
- [X] Phase 0 — Outline & Research
- [X] Phase 1 — Design & Contracts
- [X] Phase 2 — Task generation via /speckit.tasks
- [ ] Constitution Check: post-design (PASS) — run after T013 passes

## Complexity Tracking

> No Constitution Check violations. This table intentionally left empty.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| _(none)_  | _(none)_   | _(none)_                            |
