# Feature Specification: Phase 0 — Foundation & Governance

**Feature Branch**: `001-phase-0-foundation`

**Created**: 2026-05-22

**Status**: Draft

**Input**: User description: "phase0_foundation - read phase 0 from docs/roadmap.md, use best practices for github speckit"

**Roadmap reference**: [docs/roadmap.md](../../docs/roadmap.md) §"Phase 0 — Foundation & Governance"

## Clarifications

### Session 2026-05-22

- Q: What is the default reviewer policy when a PR touches a path with no specific owner? → A: A repository-wide fallback CODEOWNERS entry assigns a maintainers group as the default reviewer for any path not covered by a more specific rule.
- Q: How strictly must `main` be protected at the repository platform level after Phase 0? → A: Strict protection — direct push forbidden; PR required; at least one code-owner approval required; required status checks (added incrementally as CI jobs land in Phase 1); linear history enforced; force-push forbidden; rules apply to administrators too.
- Q: What repository-governance role taxonomy does Phase 0 establish? → A: Two roles. **Maintainers** hold merge rights on `main`, are the approval authority for ADRs and Constitution amendments, and are the wildcard CODEOWNERS fallback. **Contributors** are everyone else who opens pull requests. Repository-governance roles are distinct from product roles (admin / operator / driver) defined in the Constitution.
- Q: What is the feature-branch lifecycle policy? → A: Branches are auto-deleted on merge. Branches that have been idle for more than 30 days with no open pull request are flagged for Maintainer review (not auto-deleted) so abandoned WIP is surfaced without forcing destruction of in-progress work.
- Q: What commit-provenance policy does Phase 0 establish? → A: Every commit MUST carry a DCO `Signed-off-by:` trailer. Enforcement is a required status check (the check itself is added when Phase 1 CI lands; the policy is binding immediately, on the honor system in the interim). GPG signing is not required at Phase 0; introducing it later requires an ADR.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — A new contributor can find the rules of the project (Priority: P1)

A new contributor lands on the BorneMap repository for the first time. Within
a few minutes they can locate the project's binding rules (the
Constitution), the architectural decisions already in force (ADRs), the
plan of work (Roadmap), and how to contribute (CONTRIBUTING).

**Why this priority**: Every subsequent phase depends on contributors
understanding the non-negotiable rules. Without this, every later PR risks
violating a constitutional boundary without anyone noticing in time.

**Independent Test**: A reviewer with no prior BorneMap context opens the
repository's `README` and can, in under 5 minutes, follow links to the
Constitution, the Roadmap, all five accepted ADRs, and the contributing
guide. No dead links, no missing files.

**Acceptance Scenarios**:

1. **Given** a fresh clone of the repository, **When** a contributor opens
   the top-level README, **Then** they see direct links to the
   Constitution, Roadmap, ADR index, and CONTRIBUTING document.
2. **Given** the published documentation tree, **When** the reader follows
   any cross-reference between Constitution, Roadmap, ADRs, and
   architecture overview, **Then** every link resolves and the target
   exists.

---

### User Story 2 — Every pull request is reviewed against the project rules (Priority: P1)

A contributor opens a pull request. The repository automatically presents
them with a PR template that forces them to declare the roadmap phase,
the constitutional principle(s) the change upholds, and the checklist
items from the Definition of Done. Reviewers are automatically requested
based on the area of the repository the change touches.

**Why this priority**: The Constitution's Definition of Done (Principle
VII) only protects the project if it is actually enforced at the review
gate. PR templates and code owners are the operational mechanism for
that enforcement.

**Independent Test**: Opening any pull request in the repository surfaces
a structured template; reviewers from the responsible area are
auto-requested; no PR can be merged without acknowledging the DoD
checklist.

**Acceptance Scenarios**:

1. **Given** the repository configuration, **When** a contributor opens
   a new pull request, **Then** the PR description is pre-filled with the
   project's standard template (summary, phase, principles, change list,
   test categories, Definition-of-Done checklist).
2. **Given** a pull request that touches a path with a designated owner,
   **When** the PR is opened, **Then** the designated owner is
   automatically requested as a reviewer.
3. **Given** a pull request that affects a constitutional boundary,
   **When** the author has not filed an ADR, **Then** the PR template's
   checklist surfaces the ADR requirement so reviewers can block the
   change.

---

### User Story 3 — Branch strategy is documented and consistent (Priority: P2)

Contributors know which branch to base work on, how to name a feature
branch, how long a branch should live, and how a change gets onto the
main line.

**Why this priority**: Trunk-based development with short-lived branches
is the assumption underlying the methodology document and the CI design
in Phase 1. Phase 0 must publish the rules so Phase 1 can enforce them.

**Independent Test**: A contributor reading CONTRIBUTING can, without
asking anyone, derive a correct branch name for a hypothetical change,
know the merge strategy, and know when their branch should be deleted.

**Acceptance Scenarios**:

1. **Given** CONTRIBUTING is published, **When** a contributor needs to
   start work on a roadmap phase, **Then** they can determine the
   correct branch name format and the merge style without further
   guidance.
2. **Given** an ADR is being drafted, **When** a contributor needs a
   branch name, **Then** the ADR branch naming convention is explicitly
   documented.

---

### Edge Cases

- What happens if the repository is cloned without internet access? All
  governance documents MUST be readable offline (no external link
  dependencies for canonical rules).
- What happens if a contributor opens a PR on a path that has no
  designated owner? The default reviewer policy MUST be defined so no PR
  can slip through without review.
- What happens if a PR template field is removed by the author? The
  Definition-of-Done items MUST still be enforceable by reviewers (i.e.,
  the rules live in the Constitution, not only in the template).
- What happens if a contributor proposes work that conflicts with an
  accepted ADR? The CONTRIBUTING guide MUST point them at the ADR
  amendment procedure.

## Requirements *(mandatory)*

### Functional Requirements

#### Repository structure & governance documents

- **FR-001**: The repository MUST present a top-level navigable entry
  point that links to the Constitution, Roadmap, ADR index,
  Architecture overview, Methodology, and CONTRIBUTING.
- **FR-002**: The Constitution MUST be present in the repository at the
  path declared by the project's Speckit configuration and MUST be the
  authoritative source of binding rules.
- **FR-003**: All five accepted Architecture Decision Records (ADR-0001
  through ADR-0005) MUST be present in the repository, each in
  "Accepted" status, and discoverable through an ADR index.
- **FR-004**: A CONTRIBUTING document MUST exist describing how to
  propose a change, the branch strategy, the pull-request workflow, the
  Definition of Done, and how to file or amend an ADR.

#### Pull-request workflow

- **FR-005**: Opening a pull request MUST present the author with a
  standard PR template that requires them to declare the roadmap phase
  affected and the constitutional principle(s) the change upholds.
- **FR-006**: The PR template MUST include the Definition-of-Done
  checklist items from Constitution Principle VII (tests, OpenAPI,
  security, observability, ADR if applicable).
- **FR-007**: The repository MUST define ownership of each major path so
  that pull requests touching that path automatically request review
  from the responsible owner.
- **FR-008**: A repository-wide fallback ownership rule MUST exist that
  assigns the designated **Maintainers group** (see Key Entities) as
  the default reviewer for any path not covered by a more specific
  ownership rule, so that every pull request automatically has at
  least one identified reviewer without relying on author judgment.
- **FR-014**: Repository-governance roles MUST be limited to two
  values at Phase 0: **Maintainer** and **Contributor**. The CONTRIBUTING
  document MUST name the current Maintainers (by handle or by reference
  to the Maintainers group) and state explicitly that ADR approval and
  Constitution amendments require Maintainer sign-off.

#### Branch & merge rules

- **FR-009**: The branch strategy MUST be documented in CONTRIBUTING and
  MUST specify: base branch, feature branch naming format, ADR branch
  naming format, hotfix branch naming format, and merge style.
- **FR-010**: The branch strategy MUST be consistent with the
  Methodology document (no contradictions between the two).
- **FR-013**: The `main` branch MUST be protected at the repository
  platform level with the following enforced rules from the end of
  Phase 0 onward:
  - Direct push to `main` is forbidden; all changes land via pull
    request.
  - At least one approving review from a code owner is required before
    merge.
  - Required status checks are enforced; the initial set MAY be empty
    at Phase 0 ratification and MUST grow as Phase 1 CI jobs land,
    without further amendment to this specification.
  - Linear history is required (no merge commits on `main`).
  - Force-push to `main` is forbidden.
  - The above rules MUST apply to repository administrators as well as
    regular contributors (no admin bypass).
- **FR-015**: The feature-branch lifecycle MUST be:
  - Branches are **auto-deleted on merge** (head branch removed once
    the PR merges to `main`).
  - Branches that have been idle for **more than 30 days** with no
    open pull request MUST be **flagged for Maintainer review** (not
    auto-deleted), so abandoned work is surfaced without destroying
    in-progress changes.
  - CONTRIBUTING MUST document both rules so contributors understand
    that their merged branches disappear automatically and that stale
    WIP branches will be reviewed.

#### Commit provenance

- **FR-016**: Every commit landing on `main` MUST carry a Developer
  Certificate of Origin (DCO) `Signed-off-by:` trailer in its commit
  message.
  - CONTRIBUTING MUST document the DCO and explain how contributors
    add the trailer (e.g., `git commit -s`).
  - The DCO requirement is binding from Phase 0 ratification onward.
    Automated enforcement as a required status check MUST be added
    when Phase 1 CI lands; in the interim, reviewers MUST enforce it
    manually.
  - GPG-signed commits are **not** required at Phase 0. Introducing a
    GPG-signing requirement later requires an ADR.

#### Cross-document consistency

- **FR-011**: Every cross-reference between Constitution, Roadmap, ADRs,
  Architecture overview, Methodology, and CONTRIBUTING MUST resolve to
  an existing target.
- **FR-012**: Phase 0 MUST NOT introduce any rule that contradicts the
  Constitution; if a contradiction is found, the Constitution wins and
  the Phase 0 artefact MUST be corrected.

### Key Entities

- **Constitution**: The authoritative, versioned set of binding rules
  for the project. Already ratified at v1.0.0 prior to this feature.
- **ADR (Architecture Decision Record)**: A dated, status-bearing
  document capturing one architectural decision and its consequences.
  Five accepted ADRs exist at the start of this feature.
- **Roadmap**: The phased program plan referenced by every Speckit
  feature.
- **Pull request**: The unit of change. Carries metadata declaring the
  roadmap phase, the principles upheld, and the Definition-of-Done
  checklist.
- **Path ownership rule**: A mapping from a repository path pattern to
  one or more reviewers who MUST be requested when a pull request
  touches that path.
- **Contributing guide**: The contributor-facing entry point that
  consolidates branch strategy, PR workflow, Definition of Done, and the
  ADR amendment procedure.
- **Maintainer**: A repository-governance role with merge rights on
  `main`, approval authority over ADRs and Constitution amendments, and
  membership in the wildcard CODEOWNERS fallback group. Distinct from
  product roles (admin / operator / driver).
- **Contributor**: A repository-governance role held by anyone who
  opens a pull request and is not a Maintainer. Contributors can
  propose ADRs and Constitution amendments but cannot approve them.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A new contributor can locate the Constitution, Roadmap,
  ADR index, and CONTRIBUTING document from the repository's top-level
  entry point in **under 2 minutes** without external help.
- **SC-002**: **100%** of cross-references between the Constitution,
  Roadmap, ADRs, Architecture overview, Methodology, and CONTRIBUTING
  resolve to existing targets when checked.
- **SC-003**: **100%** of newly opened pull requests after Phase 0
  ships include a populated phase declaration and principle declaration
  in the PR description (measured over the first 20 PRs).
- **SC-004**: **100%** of newly opened pull requests after Phase 0 ships
  have at least one reviewer automatically requested based on path
  ownership (measured over the first 20 PRs).
- **SC-005**: A contributor with no prior context can, after reading only
  CONTRIBUTING, correctly state the base branch, the feature-branch
  naming format, and the merge style for the project — verified by
  asking three independent reviewers.
- **SC-006**: Zero pull requests merged during the rest of the program
  fail to declare the constitutional principle(s) upheld (target
  measured at end of Phase 11).

## Assumptions

- The Constitution at `.specify/memory/constitution.md` is already
  ratified at v1.0.0 and is binding; Phase 0 consumes it, it does not
  re-author it.
- The project hosts on GitHub; "PR template", "code owners", and
  "branch protection" refer to standard GitHub mechanisms even though
  this specification does not name the implementation.
- The contributor population at Phase 0 ratification is small (solo to
  small team); ownership rules MUST be expressible in that context but
  MUST scale as the team grows.
- The documentation tree under `docs/` (Roadmap, Methodology,
  Architecture overview, ADRs, Operations) already exists and is the
  canonical home for project documentation; Phase 0 ensures these are
  discoverable, it does not re-create them.
- Phase 0 does **not** include CI configuration, container build, or
  any executable test infrastructure — those are Phase 1.
