# Research: Phase 0 — Foundation & Governance

**Feature**: [spec.md](./spec.md)
**Plan**: [plan.md](./plan.md)
**Date**: 2026-05-22

## Purpose

This document resolves the two `NEEDS CLARIFICATION`-class technical
unknowns that the plan surfaced, plus consolidates best-practice
findings for the GitHub-native primitives this feature uses. All
clarifications about *what* the feature does were already resolved by
the `/speckit.clarify` session (recorded in `spec.md`).

## R-001 — How to enforce DCO sign-off in CI (Phase 1 dependency)

**Context**: spec FR-016 makes DCO `Signed-off-by:` trailers mandatory
on every commit and requires automated enforcement as a required status
check "when Phase 1 CI lands." Phase 0 must name the mechanism so
Phase 1 wires it without re-deciding.

**Decision**: Use a GitHub Action that runs on `pull_request` and
verifies every commit in the PR carries a `Signed-off-by:` trailer
matching the commit author. Specifically, adopt the open-source
`dcoapp/app` model as a GitHub Action workflow rather than installing
the DCO GitHub App.

**Rationale**:

- Lives in the repository (`.github/workflows/dco.yml`), so the rule is
  versioned alongside the code it protects — matches Constitution
  Principle VII ("ADR/changes versioned in repo").
- No third-party GitHub App installation, which would require org-level
  permissions and an external runtime; aligns with the on-premises /
  minimal-external-dependency posture in the Constitution.
- Re-runs on every push to the PR; failure produces a required status
  check that branch protection (FR-013) can require.
- Zero cost on GitHub-hosted runners for a project this size.

**Alternatives considered**:

- **DCO GitHub App (probot/dco)** — Rejected. Requires installing a
  GitHub App on the org or repo, adds an external trust boundary, and
  is opaque to repo-local audit.
- **Pre-commit hook only** — Rejected. Cannot be enforced; relies on
  contributor machine setup, no server-side gate.
- **Branch protection "require signed commits" setting** — Rejected as
  primary mechanism. That setting enforces *GPG signing*, not DCO
  sign-off. Per spec Q5 / FR-016, GPG signing is explicitly **not**
  required at Phase 0 and would need a new ADR.
- **Commit-message linter (e.g., commitlint)** — Rejected as primary
  mechanism. Commitlint is good at conventional-commits style but is
  not DCO-aware out of the box and conflates two unrelated concerns.
  We may add a separate commitlint check later for the Conventional
  Commits rule from `docs/methodology.md`, in a *different* status
  check.

**Phase 0 deliverable**: documented decision only. The actual workflow
file is **not** shipped by Phase 0 (no CI work this phase). Phase 1 task
list MUST include "Add `.github/workflows/dco.yml` per
research R-001" as a concrete task.

## R-002 — How to flag stale feature branches (>30 days idle)

**Context**: spec FR-015 requires stale branches to be flagged for
Maintainer review (not auto-deleted). Phase 0 must specify the
mechanism so the policy is reproducible.

**Decision**: A scheduled GitHub Actions workflow runs **weekly** on a
cron trigger, scans branches via the GitHub REST API for ones that
satisfy *all* of: (a) not `main`, (b) no commits in the last 30 days,
(c) no open PR. For each matching branch it opens (or updates) a single
tracking issue labeled `stale-branch` listing the branches, their last
commit author, and last commit date. Maintainers triage the issue
during their weekly review and decide whether to delete each branch.

**Rationale**:

- The "list in an issue" approach is non-destructive (matches spec FR-015
  intent — *flag*, do not auto-delete).
- A single rolling issue avoids issue-tracker noise; the issue body is
  refreshed weekly with the current candidate list.
- Stays inside the GitHub-native primitives; no external service.
- Weekly cadence is light on API quota and matches a typical Kanban
  review rhythm.

**Alternatives considered**:

- **Use third-party stale-branch bots (e.g., `actions/stale` for
  branches)** — Rejected. `actions/stale` targets *issues and PRs*, not
  branches; using it for branches requires wrappers and is fragile.
- **Auto-delete stale branches** — Rejected. Explicitly contradicts
  spec FR-015 / Q4 ("flagged for Maintainer review, not auto-deleted").
- **Manual weekly review only, no automation** — Rejected as the
  long-term answer (would let stale branches accumulate silently as
  the team grows), but is **the Phase 0 interim state** until the
  scheduled workflow ships in Phase 1.

**Phase 0 deliverable**: documented decision + interim manual process
in `CONTRIBUTING.md`. The scheduled workflow file is **not** shipped
by Phase 0. Phase 1 task list MUST include "Add
`.github/workflows/stale-branches.yml` per research R-002."

## R-003 — README.md best practices for a multi-service governance-heavy repo

**Context**: spec FR-001 requires the top-level README to be a navigable
entry point linking the Constitution, Roadmap, ADR index, Architecture
overview, Methodology, and CONTRIBUTING. Success criterion SC-001
requires a new contributor to find these in under 2 minutes.

**Decision**: Adopt a **navigation-first** README structure:

1. One-paragraph project mission (lifted from Constitution preamble).
2. **Status** badges section (placeholder for Phase 1 CI badges).
3. **Documentation** section — bulleted links to: Constitution, Roadmap,
   Methodology, Architecture overview, ADR index, Operations, and
   CONTRIBUTING. Every link is repo-relative.
4. **Quick start for developers** — short pointer that local dev
   instructions land in Phase 1 (with a TODO marker linking to the
   roadmap). No fake instructions.
5. **License + contact** — minimal footer.

**Rationale**:

- The README's job in this repo is *navigation*, not duplication.
  Duplicating Constitution content into README invites drift.
- A flat, bulleted "Documentation" section is the fastest scannable
  format for the 2-minute SC-001 target.
- Repo-relative links satisfy the Edge Case ("readable offline").

**Alternatives considered**:

- **Long, self-contained README** (mission + architecture diagram +
  install + contributing in one file) — Rejected. Inevitably drifts
  from the canonical documents; doubles the cross-reference surface
  FR-011 must police.
- **Minimal one-line README pointing to `docs/`** — Rejected. Fails
  SC-001 because a contributor needs to click twice (README → docs/ →
  target) instead of once.

## R-004 — PR template best practices for principle/phase declaration

**Context**: spec FR-005 + FR-006 require the PR template to force phase
and principle declaration, plus the DoD checklist from Constitution
Principle VII.

**Decision**: Use a **single** `.github/PULL_REQUEST_TEMPLATE.md`
(not a `PULL_REQUEST_TEMPLATE/` directory of multiple templates).
Structure:

1. `## Summary` — free text.
2. `## Phase / Principles` — required fields, fail loudly if blank:
   - `Phase: <0..12 or 5.5>`
   - `Principles: <comma-separated I..VII or "none — pure docs/test">`
3. `## Changes` — bulleted change list.
4. `## Tests` — checkbox grid for the seven test categories from
   Principle VII, each labelled "if applicable".
5. `## Definition of Done` — the five-item DoD checklist from
   Principle VII as checkboxes.
6. `## ADR` — small section: "Does this PR affect a constitutional
   boundary? If yes, link the ADR."

**Rationale**:

- A single template covers every PR. Per-type templates (multiple
  files under `PULL_REQUEST_TEMPLATE/`) require the author to *choose*
  via URL query, which is a discoverability failure at the moment the
  author most needs guidance.
- The Phase / Principles section is positioned *above* the change list
  so reviewers see it in the truncated PR preview on the GitHub
  dashboard — increases SC-003 and SC-006 compliance.
- Checkbox form for DoD makes review state visible at a glance and
  satisfies FR-006 verbatim.

**Alternatives considered**:

- **YAML form template** (`.github/PULL_REQUEST_TEMPLATE.yml`) —
  Rejected. GitHub supports YAML *issue* forms but **not** PR forms
  at the time of writing. PR templates are Markdown-only.
- **Per-area templates** (`feature.md`, `bugfix.md`, `docs.md`) —
  Rejected. Adds branching that drifts; the DoD applies to all PRs.

## R-005 — CODEOWNERS best practices with a wildcard fallback

**Context**: spec FR-007 / FR-008 require path-specific owners and a
wildcard Maintainers fallback. Spec Q1 confirmed the wildcard goes to a
maintainers group.

**Decision**: Place CODEOWNERS at `.github/CODEOWNERS`. Order rules
**most-specific first**, **wildcard last** (GitHub uses last-match
wins, so wildcard last means specific patterns override). Use a GitHub
**team** reference (`@mezni/bornemap-maintainers`) for the wildcard
fallback rather than an individual handle, so adding a Maintainer
later is a team membership change, not a CODEOWNERS commit.

Initial ruleset (Phase 0):

```
# Repository-governance documents — Maintainers approval required
/.specify/                         @mezni/bornemap-maintainers
/docs/adr/                         @mezni/bornemap-maintainers
/.github/                          @mezni/bornemap-maintainers
/CONTRIBUTING.md                   @mezni/bornemap-maintainers
/README.md                         @mezni/bornemap-maintainers

# Documentation
/docs/                             @mezni/bornemap-maintainers

# Wildcard fallback — every other path
*                                  @mezni/bornemap-maintainers
```

Service-specific owners (e.g., `/services/geo-service/`) will be added
in their respective phase as those services land. Phase 0 does not need
those entries because the corresponding directories do not yet exist.

**Rationale**:

- Team handle insulates CODEOWNERS from team-membership churn.
- Listing all current Phase-0-relevant paths under Maintainers prevents
  the wildcard from silently swallowing governance-critical paths.
- Order-aware design avoids the classic "wildcard above specific rule"
  bug where the specific rule never matches.

**Alternatives considered**:

- **Single wildcard line, nothing else** — Rejected. Fails to surface
  intent in code review; first reader cannot tell which paths are
  governance-critical vs. arbitrary.
- **Per-individual handles** — Rejected. Adding a Maintainer requires
  editing CODEOWNERS; team is one less moving part.

## R-006 — Branch protection ruleset construction

**Context**: spec FR-013 lists six enforced rules on `main`.

**Decision**: Use GitHub's **Rulesets** (the newer, hierarchical
replacement for classic Branch Protection Rules) targeting `main`. The
ruleset is configured via the GitHub UI/API and documented as an
exportable JSON snapshot stored at `docs/operations/branch-protection.json`
in a follow-up commit during Phase 1 (so it can be diffed in CI). For
Phase 0, the snapshot is captured in `contracts/branch-protection.md`
as a human-readable spec and applied manually.

Required ruleset clauses (1:1 with FR-013):

1. **Restrict pushes that create matching refs**: enabled (no direct
   push).
2. **Require a pull request before merging**: enabled; require
   **1** approval; require review from Code Owners; dismiss stale
   approvals on new commits.
3. **Require status checks to pass**: enabled; initial check list empty
   (filled in Phase 1 — see plan FR-013).
4. **Block force pushes**: enabled.
5. **Require linear history**: enabled.
6. **Do not allow bypass**: administrators included (no admin bypass).

**Rationale**:

- Rulesets are GitHub's current direction and expose JSON export for
  versioning, which Branch Protection Rules do not.
- Mapping is 1:1 with FR-013 so verification is mechanical.

**Alternatives considered**:

- **Classic Branch Protection Rules** — Rejected. Functional but
  deprecated direction; JSON export is awkward and CLI tooling is
  weaker.
- **No platform enforcement, rules in CONTRIBUTING only** — Rejected
  explicitly by spec Q2.

## R-007 — CONTRIBUTING.md best practices

**Context**: spec FR-004, FR-009, FR-014, FR-015, FR-016 all mandate
content for `CONTRIBUTING.md`. Best practice question is *organization*
and *length*.

**Decision**: Single `CONTRIBUTING.md` at repository root. Section
order:

1. **Welcome** (1 paragraph).
2. **Before you start** — required reading: Constitution + Roadmap.
3. **Repository governance** — Maintainer vs. Contributor roles
   (FR-014).
4. **How to propose a change** — pick up a roadmap card → branch →
   PR → review → merge.
5. **Branch strategy** — base branch, naming formats, lifecycle, merge
   style (FR-009, FR-015).
6. **Commit messages** — Conventional Commits (from methodology) **and**
   DCO `Signed-off-by:` trailer (FR-016).
7. **Pull-request workflow** — what the PR template asks for, how
   review works, what the Definition of Done means.
8. **Filing or amending an ADR** — link to MADR template + amendment
   procedure (consumes Constitution governance section by reference).
9. **Reporting issues** — quick pointer; full issue templates are out
   of scope for Phase 0.

**Rationale**:

- One file, scannable, navigable via a top ToC. Avoids splintering
  contributor guidance across multiple files.
- Section order matches the contributor's mental flow (read rules →
  open branch → open PR → handle review → maintain or amend).
- "Filing an ADR" gets its own section because spec edge case (PR
  conflicts with an accepted ADR) requires contributors to know the
  amendment path.

**Alternatives considered**:

- **Split into `docs/contributing/*.md` files** — Rejected for Phase 0
  (too many small files; harder to keep consistent). May be revisited
  if `CONTRIBUTING.md` grows past ~500 lines.

## R-008 — Auto-delete head branches setting

**Context**: spec FR-015 first half.

**Decision**: Enable the repository-level setting **"Automatically
delete head branches"** in GitHub repo settings → General. Documented
in `quickstart.md` so it is reproducible from a fresh fork.

**Rationale**: Single checkbox; zero cost; matches FR-015 verbatim.

**Alternatives**: None worth considering — the alternative is to not
enable it, which violates FR-015.

## Summary of Phase 0 unknowns resolution

| Unknown | Resolution | Phase 0 ships? | Deferred to |
|---|---|---|---|
| DCO enforcement mechanism | GitHub Actions workflow (R-001) | Decision only | Phase 1 |
| Stale branch flagging | Weekly scheduled workflow + tracking issue (R-002) | Decision only | Phase 1 |
| README structure | Navigation-first (R-003) | Yes | — |
| PR template structure | Single Markdown template (R-004) | Yes | — |
| CODEOWNERS structure | Specific-first, team wildcard (R-005) | Yes | — |
| Branch protection mechanism | GitHub Rulesets (R-006) | Yes (manual apply) | JSON snapshot in Phase 1 |
| CONTRIBUTING organization | Single file, 9 sections (R-007) | Yes | — |
| Auto-delete head branches | Repo setting toggle (R-008) | Yes (manual apply) | — |

All NEEDS CLARIFICATION items are resolved. Ready for Phase 1 design.
