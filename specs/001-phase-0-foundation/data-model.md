# Data Model: Phase 0 — Foundation & Governance

**Feature**: [spec.md](./spec.md)
**Plan**: [plan.md](./plan.md)
**Date**: 2026-05-22

## Note on "data model" for a governance feature

This feature ships no database schemas, no runtime entities, and no
persisted records. Its "entities" are **governance documents and
GitHub-platform configuration objects**. They have shape, identity,
relationships, and lifecycle just like data entities, so we model them
the same way to make verification mechanical.

## Entities

### 1. RepositoryEntryPoint (top-level README)

- **Identity**: file at repository path `README.md`.
- **Attributes**:
  - `mission` — single paragraph; MUST be consistent with Constitution
    preamble.
  - `documentation_links` — ordered list of repo-relative links to:
    Constitution, Roadmap, Methodology, Architecture overview, ADR
    index, Operations, CONTRIBUTING.
  - `status_badges_section` — placeholder for Phase 1 CI badges.
  - `quickstart_pointer` — note that local dev instructions land in
    Phase 1.
  - `license_and_contact` — minimal footer.
- **Relationships**: links **to** every entity below + every document
  under `docs/`.
- **Validation rules**:
  - Every `documentation_links` entry MUST be repo-relative.
  - Every link MUST resolve to an existing file in the repository
    (FR-011 / SC-002).
  - The link list MUST cover at minimum: Constitution, Roadmap,
    Methodology, Architecture overview, ADR index, Operations,
    CONTRIBUTING (FR-001).
- **Lifecycle**: Created in Phase 0. Updated whenever a new top-level
  governance document is added.

### 2. ContributingGuide (`CONTRIBUTING.md`)

- **Identity**: file at repository path `CONTRIBUTING.md`.
- **Attributes** (section presence is part of the schema):
  - `welcome` — 1 paragraph.
  - `before_you_start` — required reading list (Constitution, Roadmap).
  - `repository_governance` — Maintainer + Contributor role
    definitions (FR-014).
  - `how_to_propose_a_change` — workflow narrative.
  - `branch_strategy` — base branch, feature/ADR/hotfix naming
    formats, lifecycle (FR-009, FR-015), merge style.
  - `commit_messages` — Conventional Commits + DCO `Signed-off-by:`
    (FR-016).
  - `pull_request_workflow` — what the PR template asks, what review
    means, DoD explanation.
  - `filing_or_amending_adr` — links to MADR template + amendment
    procedure.
  - `reporting_issues` — short pointer.
- **Relationships**: references Constitution, Methodology, ADR
  template, Roadmap.
- **Validation rules**:
  - All sections above MUST be present (FR-004).
  - Branch strategy MUST be consistent with `docs/methodology.md`
    (FR-010).
  - MUST name current Maintainers (handle or team reference)
    (FR-014).
  - MUST state that ADR approval and Constitution amendments require
    Maintainer sign-off (FR-014).
  - MUST document `git commit -s` or equivalent for DCO (FR-016).
- **Lifecycle**: Created in Phase 0. Amended whenever the governance
  rules change (always together with a Constitution amendment when
  applicable).

### 3. PullRequestTemplate (`.github/PULL_REQUEST_TEMPLATE.md`)

- **Identity**: file at path `.github/PULL_REQUEST_TEMPLATE.md`.
- **Attributes** (section order is part of the schema — see
  `contracts/pr-template.md` for the binding contract):
  - `summary` — free text.
  - `phase_principles` — required fields: `Phase`, `Principles`.
  - `changes` — bulleted list.
  - `tests` — checkbox grid for the 7 test categories from Principle
    VII.
  - `definition_of_done` — 5-item checklist from Principle VII.
  - `adr` — small section asking if the change crosses a
    constitutional boundary.
- **Relationships**: referenced by every PR opened in the repository;
  read by reviewers; consumed by Phase 1 CI for parse-time validation.
- **Validation rules**:
  - All sections above MUST be present (FR-005, FR-006).
  - The 7 test categories MUST exactly match Constitution Principle
    VII (FR-006).
  - The 5 DoD items MUST exactly match Constitution Principle VII
    (FR-006).
- **Lifecycle**: Created in Phase 0. Re-validated whenever Principle
  VII changes.

### 4. CodeOwnersFile (`.github/CODEOWNERS`)

- **Identity**: file at path `.github/CODEOWNERS`.
- **Attributes**:
  - `rules` — ordered list of `(pattern, owners)` pairs. Order is
    semantically meaningful (GitHub: last match wins).
  - `maintainers_team` — the GitHub team referenced as wildcard
    fallback. Value at Phase 0: `@mezni/bornemap-maintainers`.
- **Relationships**: depends on the GitHub team existing; consumed by
  GitHub's PR-review-request mechanism.
- **Validation rules**:
  - The last rule MUST be the wildcard `*` (FR-008).
  - Wildcard owner MUST be the Maintainers team (FR-008, spec Q1).
  - Governance-critical paths (`/.specify/`, `/docs/adr/`, `/.github/`,
    `/CONTRIBUTING.md`, `/README.md`) MUST appear as explicit rules
    before the wildcard, with Maintainers as owner.
  - More-specific rules MUST appear before less-specific rules (so
    last-match-wins yields the most specific owner).
- **Lifecycle**: Created in Phase 0. Extended in every later phase that
  introduces a new service or top-level area.

### 5. MainBranchRuleset (GitHub Ruleset on `main`)

- **Identity**: a named ruleset in repository → Settings → Rules →
  Rulesets targeting branch `main`.
- **Attributes** (the six FR-013 clauses):
  - `restrict_pushes_creating_matching_refs` = enabled.
  - `require_pull_request` = enabled; `required_approving_review_count`
    = 1; `require_code_owner_review` = true;
    `dismiss_stale_reviews_on_push` = true.
  - `require_status_checks_to_pass` = enabled; `required_check_list`
    starts empty (Phase 0) and grows in Phase 1.
  - `block_force_pushes` = enabled.
  - `require_linear_history` = enabled.
  - `enforce_for_admins` = true (no bypass).
- **Relationships**: enforces the workflow declared by
  `PullRequestTemplate` and `CodeOwnersFile`.
- **Validation rules**: configuration MUST match all six FR-013
  clauses verbatim (FR-013).
- **Lifecycle**: Configured in Phase 0 via UI/API. Captured as JSON
  snapshot in Phase 1 (`docs/operations/branch-protection.json`) so it
  can be diffed in CI.

### 6. RepoSetting — AutoDeleteHeadBranches

- **Identity**: the boolean repository setting "Automatically delete
  head branches" in repo Settings → General.
- **Attributes**: `enabled` = true.
- **Relationships**: triggered automatically by GitHub on every PR
  merge; observable in the PR timeline.
- **Validation rules**: `enabled` MUST be true (FR-015 first half).
- **Lifecycle**: Set in Phase 0. Stable.

### 7. MaintainersTeam (GitHub Team)

- **Identity**: a GitHub team referenced by handle
  `@mezni/bornemap-maintainers`.
- **Attributes**:
  - `members` — list of GitHub user handles. At ratification:
    repository owner only.
  - `description` — "BorneMap repository Maintainers — merge rights on
    main, ADR/Constitution approval authority."
- **Relationships**: referenced by `CodeOwnersFile` wildcard;
  referenced by `MainBranchRuleset`'s code-owner-approval clause;
  named by `ContributingGuide.repository_governance`.
- **Validation rules**:
  - MUST exist before `CodeOwnersFile` is committed; GitHub silently
    drops unknown team references.
  - MUST contain ≥ 1 active member at all times.
- **Lifecycle**: Created in Phase 0. Members added as the team grows.

## State / lifecycle summary

There is no state machine for individual entities. The *aggregate*
state of Phase 0 has two values:

- **Not ratified** — one or more entities missing, validation fails,
  Phase 0 incomplete.
- **Ratified** — all seven entities exist and satisfy their validation
  rules. Phase 0 is complete; Phase 1 may begin.

The `quickstart.md` runbook executes the validation rules end-to-end
and produces a pass/fail result.

## Out of scope for Phase 0 data model

- Issue templates (`.github/ISSUE_TEMPLATE/`) — deferred.
- Discussion categories — deferred.
- Security policy (`SECURITY.md`) — recommended best practice but not
  required by spec; deferred to Phase 11 hardening.
- License file — not in spec; if needed it lands in a follow-up.
