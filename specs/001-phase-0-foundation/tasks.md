---

description: "Task list for Phase 0 — Foundation & Governance"
---

# Tasks: Phase 0 — Foundation & Governance

**Input**: Design documents in `specs/001-phase-0-foundation/`
**Prerequisites**: `plan.md`, `spec.md`, `research.md`, `data-model.md`, `contracts/`, `quickstart.md`
**Tests**: Phase 0 ships **no executable code**, so there are no unit/integration test tasks. Verification is performed via the `quickstart.md` runbook (manual at Phase 0; automated in Phase 1).
**Organization**: Tasks are grouped by user story (US1 P1, US2 P1, US3 P2) per `spec.md`.

## Format: `[ID] [P?] [Story?] Description`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks)
- **[Story]**: User-story tag (US1, US2, US3); omitted in Setup, Foundational, Polish
- Every task names an exact file path or a specific GitHub configuration surface

## Path Conventions

- Repository root for top-level files (`README.md`, `CONTRIBUTING.md`).
- `.github/` for GitHub-native configuration files.
- `docs/` already exists from the constitution-ratification commit.
- No `src/` or `tests/` — Phase 0 ships docs + GitHub config only.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: One-time prerequisite that all stories depend on. No code, no commands — these are GitHub-organisation actions.

- [ ] T001 Create the GitHub team `mezni/bornemap-maintainers` (organisation `mezni`, team slug `bornemap-maintainers`) and assign at least one initial member; record the team handle in `docs/methodology.md` if not already present (prerequisite for FR-008, FR-014, CODEOWNERS rule C-OWN-5)

**Checkpoint**: Maintainers team exists and is referencable in `.github/CODEOWNERS`.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Phase 0 has **no executable foundational layer**. The Constitution, ADRs 0001–0005, the Roadmap, the Methodology, and the Architecture overview were committed during constitution ratification (commit `8df191e` on `main`) and are treated as already-present inputs by the spec (FR-002, FR-003). No foundational task is required before user stories may begin.

**Checkpoint**: Confirm `docs/`, `.specify/memory/constitution.md`, and `docs/adr/0001-…0005-*` are present on the current branch. If any are missing, file a blocker and stop — Phase 0 cannot proceed.

---

## Phase 3: User Story 1 — A new contributor can find the rules of the project (Priority: P1) 🎯 MVP

**Goal**: Anyone landing on the repository can, in under five minutes, follow links from `README.md` to the Constitution, Roadmap, ADR index, Architecture overview, Methodology, and CONTRIBUTING — with zero dead links (FR-001, FR-011, SC-001, SC-002).

**Independent Test**: Execute `quickstart.md` Step 1 (README link inventory) and Step 7 (cross-document link integrity script). Both must pass with zero failures on a fresh clone of the branch.

### Implementation for User Story 1

- [ ] T002 [US1] Author top-level `README.md` (new file) with: one-paragraph mission consistent with the Constitution preamble; "Documentation" section linking to `.specify/memory/constitution.md`, `docs/roadmap.md`, `docs/methodology.md`, `docs/architecture/overview.md`, `docs/adr/README.md`, `docs/operations/deployment.md`, `CONTRIBUTING.md`; a "Status" note that the project is at Phase 0 (FR-001, FR-002, FR-003)

- [ ] T003 [US1] Update `docs/adr/README.md` if needed so each of ADR-0001…ADR-0005 is listed with "Accepted" status and links to its file under `docs/adr/`; confirm the index matches the actual files on disk (FR-003)

- [ ] T004 [US1] Run the link-check loop from `quickstart.md` Step 7 against the working tree; fix every dead `[text](path)` link reported and re-run until the script prints nothing (FR-011, SC-002)

**Checkpoint**: `README.md` exists, all seven documentation links resolve, ADR index lists all five ADRs as Accepted, link-check is clean. User Story 1 is independently testable and satisfies the Phase 0 MVP.

---

## Phase 4: User Story 2 — Every pull request is reviewed against the project rules (Priority: P1)

**Goal**: Every new PR is forced through the standard template (phase + principles + tests + DoD + ADR question), code-owners are auto-requested, and `main` is protected so no PR can land without a code-owner approval and DCO `Signed-off-by:` (FR-005, FR-006, FR-007, FR-008, FR-013, FR-014, FR-016).

**Independent Test**: Execute `quickstart.md` Steps 0, 3, 4, 5 in order. Each step's pass criteria must hold. Specifically: opening a throwaway PR must (a) pre-populate the template, (b) auto-request the Maintainers team, (c) be blocked by branch protection if you try to merge without an approving code-owner review.

### Implementation for User Story 2

- [ ] T005 [P] [US2] Create `.github/PULL_REQUEST_TEMPLATE.md` (new file) exactly matching the section order, titles, and field semantics defined in `specs/001-phase-0-foundation/contracts/pr-template.md` (Summary; Phase / Principles; Changes; Tests with the 7-item checkbox grid mirroring Constitution Principle VII; Definition of Done with the 5-item checklist; ADR Yes/No with link slot) (FR-005, FR-006)

- [ ] T006 [P] [US2] Create `.github/CODEOWNERS` (new file) exactly matching the ruleset in `specs/001-phase-0-foundation/contracts/codeowners.md`: governance-critical path rules first (`/.specify/`, `/docs/adr/`, `/.github/`, `/CONTRIBUTING.md`, `/README.md`), then `/docs/`, then the wildcard `*` → `@mezni/bornemap-maintainers` as the **last** non-comment line; satisfy invariants C-OWN-1..C-OWN-6 (FR-007, FR-008)

- [ ] T007 [US2] Configure the `main` branch ruleset in **GitHub UI → Repository → Settings → Rules → Rulesets** per `specs/001-phase-0-foundation/contracts/branch-protection.md`, applying all six clauses C-BR-1..C-BR-6: restrict direct pushes; require PR with ≥1 approving review and require code-owner review and dismiss stale approvals on push; require status checks (list empty at Phase 0); block force pushes; require linear history; empty bypass list (admins NOT in bypass); name the ruleset `main-protection`; target `refs/heads/main`; state Active (FR-013)

- [ ] T008 [US2] In **GitHub UI → Repository → Settings → General → Pull Requests**, enable "Automatically delete head branches" (FR-015 first half — this also serves Phase 5)

- [ ] T009 [US2] Verify the PR template + CODEOWNERS wiring end-to-end: push a throwaway commit to a temporary feature branch, open a draft PR, confirm the PR body is pre-populated with the template **in the exact section order from T005** and the Maintainers team is auto-requested as reviewer; close the draft and let auto-delete remove the branch (validates T005, T006, T007, T008 together) (SC-003, SC-004)

**Checkpoint**: A new PR cannot be opened without the template, cannot land without a code-owner approval, cannot be force-pushed, and the merged head branch self-deletes. User Story 2 is independently testable.

---

## Phase 5: User Story 3 — Branch strategy is documented and consistent (Priority: P2)

**Goal**: A contributor reading only `CONTRIBUTING.md` can derive (a) the base branch, (b) the correct branch name for a feature, ADR, or hotfix, (c) the merge style, (d) the branch-lifecycle rules (auto-delete on merge; >30d idle flagged), (e) the DCO sign-off requirement — without any contradiction with `docs/methodology.md` (FR-004, FR-009, FR-010, FR-014, FR-015, FR-016).

**Independent Test**: Execute `quickstart.md` Step 2 (CONTRIBUTING section inventory + cross-check vs. `docs/methodology.md`) and Step 6 (auto-delete confirmation). Both must pass.

### Implementation for User Story 3

- [ ] T010 [US3] Author top-level `CONTRIBUTING.md` (new file) with all nine sections enumerated in `quickstart.md` Step 2 (Welcome; Before you start — lists Constitution + Roadmap as required reading; Repository governance — defines Maintainer + Contributor roles, names current Maintainers, states ADR/Constitution amendments require Maintainer sign-off; How to propose a change; Branch strategy — base branch, feature/ADR/hotfix naming, merge style, auto-delete + stale >30d flagged; Commit messages — Conventional Commits + DCO `Signed-off-by:` with `git commit -s` example; Pull-request workflow — references PR template + DoD; Filing or amending an ADR — links MADR template + Constitution amendment policy; Reporting issues) (FR-004, FR-009, FR-014, FR-015, FR-016)

- [ ] T011 [US3] Cross-check `CONTRIBUTING.md` against `docs/methodology.md` for any contradiction on base branch name, feature branch format, ADR branch format, hotfix branch format, merge style, or branch-deletion policy; if a contradiction is found, update `CONTRIBUTING.md` (NOT the methodology — methodology is the authority unless the contradiction reveals a methodology bug, in which case file an ADR) until both documents agree (FR-010)

- [ ] T012 [US3] Re-run the link-check loop from `quickstart.md` Step 7 against the working tree (now including `CONTRIBUTING.md`); fix every dead link reported and re-run until the script prints nothing (FR-011)

**Checkpoint**: `CONTRIBUTING.md` covers every mandated section, contradicts nothing in `docs/methodology.md`, and links cleanly to the Constitution / Roadmap / ADRs. User Story 3 is independently testable.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: End-to-end ratification of Phase 0 and final cross-checks that span all three stories.

- [ ] T013 Execute `quickstart.md` Steps 0 through 8 in order on a fresh clone of the branch; record per-step pass/fail; do not declare ratification until every step passes (SC-001, SC-002, SC-003, SC-004, SC-005)

- [ ] T014 Manually re-check FR-012 (no contradiction with Constitution): for FR-008, FR-013, FR-014, FR-015, FR-016, re-read the Constitution and confirm no Principle I–VII or governance clause is violated; record the reviewer and date in the closing PR description (`quickstart.md` Step 8)

- [ ] T015 [P] Update `specs/001-phase-0-foundation/plan.md` "Progress Tracking" section: tick Phase 0 (Outline & Research), Phase 1 (Design & Contracts), Phase 2 (Task generation via /speckit.tasks), and after T013 passes also tick "Constitution Check: initial (PASS)" and "Constitution Check: post-design (PASS)"

- [ ] T016 [P] Update `docs/roadmap.md` Phase 0 row to status "Ratified" with the merge date, once the closing PR has been merged

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1, T001)**: No code dependency, but T006 references `@mezni/bornemap-maintainers` — that handle must resolve at the moment `.github/CODEOWNERS` is read by GitHub. T001 MUST complete before T006 is merged.
- **Foundational (Phase 2)**: No tasks; verification only.
- **User Stories (Phase 3, 4, 5)**: All three stories can begin in parallel once T001 is done. They touch **disjoint files** (US1 = `README.md` + `docs/adr/README.md`; US2 = `.github/PULL_REQUEST_TEMPLATE.md` + `.github/CODEOWNERS` + GitHub repo settings; US3 = `CONTRIBUTING.md`).
- **Polish (Phase 6)**: Depends on US1, US2, and US3 all being merged to the branch.

### User Story Dependencies

- **US1 (P1)**: No dependency on US2 or US3.
- **US2 (P1)**: No dependency on US1 or US3. Depends on Setup task T001 (Maintainers team must exist).
- **US3 (P2)**: No dependency on US1 or US2. **Soft dependency**: US3's `CONTRIBUTING.md` references the PR template and DCO — that is documentation pointing at US2's deliverables, not a build dependency, so US3 can be drafted in parallel and reviewed last.

### Within Each User Story

- US1: T002 → T003 (T002 introduces the README that T003's ADR-index changes are linked from) → T004 (link-check runs last on the new files).
- US2: T005 and T006 are file creations that can run in parallel ([P]); T007 and T008 are GitHub-UI configurations and require admin access; T009 verifies everything end-to-end and MUST run after T005, T006, T007, T008.
- US3: T010 → T011 (cross-check requires T010 to exist) → T012 (link-check runs last).

### Parallel Opportunities

- T005 and T006 are different files with no shared dependency → run in parallel.
- T015 and T016 are different files in the polish phase → run in parallel.
- US1, US2 (file creation tasks T005/T006), and US3 (T010) all touch disjoint files → if multiple contributors exist, they can run in parallel after T001.

---

## Parallel Example: User Story 2

```text
# After T001 (Maintainers team exists):
# T005 and T006 are independent file creations.
Task: "Create .github/PULL_REQUEST_TEMPLATE.md per contracts/pr-template.md"   # T005
Task: "Create .github/CODEOWNERS per contracts/codeowners.md"                  # T006

# T007 and T008 are GitHub UI changes; both require admin rights and can be
# performed in the same browser session.
# T009 must run last in this story (verifies T005/T006/T007/T008 together).
```

---

## Implementation Strategy

### MVP First (User Story 1 only)

1. Complete T001 (Maintainers team — this is also needed for US2 but is cheap).
2. Complete T002, T003, T004.
3. **STOP and VALIDATE**: Run `quickstart.md` Step 1 + Step 7 on the branch.
4. If clean, open a `feat(phase-0): land US1 — discoverable governance entry point` PR. This PR delivers the Phase 0 MVP: a contributor can find every binding rule from the README.

### Incremental Delivery

1. Land US1 (MVP) → reviewers can locate the rules they're applying.
2. Land US2 → every subsequent PR (including the US3 PR) is reviewed under the new template, code-owners, and `main` protection. This is the moment Phase 0's review enforcement turns on.
3. Land US3 → contributors have a single, authoritative `CONTRIBUTING.md`.
4. Land Polish phase → ratify Phase 0 (T013, T014); update plan + roadmap (T015, T016).

### Solo Strategy (realistic for this project)

The roadmap is a **solo build**. Run the phases sequentially in the priority order above (US1 → US2 → US3 → Polish). Open one PR per user story so the branch protection rules added in US2 actually get exercised by US3 and Polish.

---

## Notes

- Phase 0 ships **no executable code**, so there are no unit/integration/contract tests in the implementation tasks. The `quickstart.md` runbook is the verification surface.
- T001, T007, T008 are not text-file edits — they are **GitHub configuration actions**. Track them in the PR description with screenshots or settings URLs so the change is auditable.
- T007's required status checks list is **empty** at Phase 0 (Phase 1 will start populating it). Do not block T007 on waiting for CI to exist.
- Every file-creating task (T002, T005, T006, T010) creates a new file. Use the `Write` tool with absolute paths; verify the file exists before marking the task complete.
- Commit after each logical group (one commit per user story is acceptable; one commit per task is also acceptable). All commits MUST be signed off (`git commit -s`) per FR-016 — including the commits that land Phase 0 itself.
