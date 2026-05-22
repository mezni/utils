# Quickstart: Verify Phase 0 — Foundation & Governance

**Feature**: [spec.md](./spec.md)
**Plan**: [plan.md](./plan.md)
**Data model**: [data-model.md](./data-model.md)
**Date**: 2026-05-22

## Purpose

Mechanical, repeatable runbook to verify Phase 0 is ratified. Each
step maps to an entity in `data-model.md` and one or more functional
requirements in `spec.md`. Running the whole runbook end-to-end and
seeing every step pass means Phase 0 is **Ratified**.

This runbook is meant to be performed manually at Phase 0 (no CI yet)
and converted to automated checks in Phase 1.

## Prerequisites

- Local clone of the repository at the tip of `main` after Phase 0
  merges.
- A web browser with access to the GitHub repository UI as a user with
  `admin` permission (needed to verify repo settings, rulesets, and
  team membership).
- `git`, `grep`, `ls` available locally.

## Step 0 — MaintainersTeam exists

Maps to: data-model §7, FR-014.

1. Browse to GitHub → organisation `mezni` → Teams.
2. Confirm a team named `bornemap-maintainers` exists.
3. Confirm the team has **at least one** active member.
4. Confirm the team's description names it as the Maintainers team.

✅ Pass when all four conditions hold.

## Step 1 — README is the navigable entry point

Maps to: data-model §1, FR-001, SC-001, SC-002.

1. Open `README.md` at repository root.
2. Confirm it contains a one-paragraph mission consistent with the
   Constitution preamble.
3. Confirm a **Documentation** section with repo-relative links to:
   - `.specify/memory/constitution.md`
   - `docs/roadmap.md`
   - `docs/methodology.md`
   - `docs/architecture/overview.md`
   - `docs/adr/README.md`
   - `docs/operations/deployment.md`
   - `CONTRIBUTING.md`
4. Click each link in the GitHub web UI. Every link MUST resolve to an
   existing file with no 404.

✅ Pass when all 7 links resolve.

Quick local check:

```bash
# from repo root
for f in .specify/memory/constitution.md docs/roadmap.md \
         docs/methodology.md docs/architecture/overview.md \
         docs/adr/README.md docs/operations/deployment.md \
         CONTRIBUTING.md; do
    test -f "$f" || echo "MISSING: $f"
done
```

The script MUST print no output.

## Step 2 — CONTRIBUTING covers every mandated section

Maps to: data-model §2, FR-004, FR-009, FR-010, FR-014, FR-015, FR-016.

Open `CONTRIBUTING.md` and confirm **every** section below is present
and non-empty (titles MAY differ slightly but content MUST be there):

- [ ] Welcome
- [ ] Before you start (lists Constitution + Roadmap as required reading)
- [ ] Repository governance (defines Maintainer + Contributor; names
  current Maintainers; states ADR/Constitution amendment requires
  Maintainer sign-off — FR-014)
- [ ] How to propose a change
- [ ] Branch strategy (base branch, feature naming, ADR naming, hotfix
  naming, merge style — FR-009; auto-delete on merge + stale >30d
  flagged — FR-015)
- [ ] Commit messages (Conventional Commits + DCO `Signed-off-by:`
  with example `git commit -s` — FR-016)
- [ ] Pull-request workflow (references the PR template + Definition
  of Done)
- [ ] Filing or amending an ADR (links MADR template + Constitution
  amendment policy)
- [ ] Reporting issues

Then cross-check against `docs/methodology.md` for contradictions:

```bash
# Open both files side by side and scan for contradictions in:
#   - base branch name
#   - feature branch naming format
#   - merge style (squash / rebase / merge commit)
#   - branch deletion policy
```

✅ Pass when all sections present AND no contradiction with
`docs/methodology.md` (FR-010).

## Step 3 — PR template surfaces phase, principles, tests, DoD, ADR

Maps to: data-model §3, contracts/pr-template.md, FR-005, FR-006.

1. Open `.github/PULL_REQUEST_TEMPLATE.md`.
2. Confirm sections in this order:
   - `## Summary`
   - `## Phase / Principles` (with `Phase:` and `Principles:` lines)
   - `## Changes`
   - `## Tests` (7-item checkbox grid matching Principle VII categories)
   - `## Definition of Done` (5-item checklist matching Principle VII DoD)
   - `## ADR` (Yes/No checkboxes + space for ADR link)
3. Compare the 7 Tests items and 5 DoD items character-for-character
   against Constitution §VII.

End-to-end smoke test:

1. Push a trivial commit on a throwaway branch and open a draft PR.
2. Confirm the PR body is pre-populated with the template.
3. Confirm the Maintainers team is auto-requested as reviewer (because
   `.github/` triggers the explicit rule; if you used a different
   path, the wildcard fires).
4. Close the draft PR; delete the branch.

✅ Pass when the live PR shows all six sections, in order, and a
reviewer was auto-requested.

## Step 4 — CODEOWNERS satisfies invariants C-OWN-1..C-OWN-6

Maps to: data-model §4, contracts/codeowners.md, FR-007, FR-008.

1. Confirm `.github/CODEOWNERS` exists.
2. Confirm the **last non-comment, non-blank line** is the wildcard:
   ```
   *                                  @mezni/bornemap-maintainers
   ```
3. Confirm explicit Maintainers rules exist *before* the wildcard for
   each governance-critical path:
   - `/.specify/`
   - `/docs/adr/`
   - `/.github/`
   - `/CONTRIBUTING.md`
   - `/README.md`
4. In GitHub UI, open the CODEOWNERS file. GitHub renders a banner if
   any team reference is invalid; confirm **no warning** is shown.

Quick local check:

```bash
# wildcard must be last non-comment, non-blank line
grep -v '^#' .github/CODEOWNERS | grep -v '^$' | tail -1 \
  | grep -q '^\*' || echo "FAIL: wildcard is not the last rule"

# each governance-critical path must appear before the wildcard
for path in '/.specify/' '/docs/adr/' '/.github/' '/CONTRIBUTING.md' '/README.md'; do
    grep -q "^$path" .github/CODEOWNERS || echo "FAIL: missing rule for $path"
done
```

The script MUST print no output.

✅ Pass when all invariants C-OWN-1..C-OWN-6 hold.

## Step 5 — `main` branch ruleset applies all six FR-013 clauses

Maps to: data-model §5, contracts/branch-protection.md, FR-013.

1. In GitHub UI: Repository → Settings → Rules → Rulesets.
2. Confirm a ruleset (suggested name `main-protection`) exists,
   targets `refs/heads/main`, and is **Active**.
3. Verify each of the six clauses against the contract:
   - C-BR-1: Restrict pushes — enabled.
   - C-BR-2: Require PR; required approvals = 1; require code-owner
     review; dismiss stale approvals on push.
   - C-BR-3: Require status checks — enabled; required check list
     **may be empty** at Phase 0.
   - C-BR-4: Block force pushes — enabled.
   - C-BR-5: Require linear history — enabled.
   - C-BR-6: Bypass list — empty (admins NOT in bypass list).

Live test:

1. From a clean local clone, attempt `git push origin main` directly.
   GitHub MUST reject the push.
2. From a feature branch, open a PR and immediately try to merge it
   yourself without any review. GitHub MUST refuse (no approving
   review).

✅ Pass when both rejections occur.

## Step 6 — Auto-delete head branches is on

Maps to: data-model §6, FR-015 (first half).

1. GitHub UI: Repository → Settings → General.
2. Scroll to "Pull Requests" section.
3. Confirm "**Automatically delete head branches**" is **checked**.

Live test:

1. Open a throwaway PR.
2. Merge it (after satisfying Step 5's review requirement).
3. Confirm the head branch is **automatically deleted** after merge.

✅ Pass when the head branch disappears post-merge.

## Step 7 — Cross-document link integrity

Maps to: FR-011, SC-002.

Run a local link check. A simple, dependency-free version:

```bash
# from repo root: find every markdown link of the form [text](path)
# and verify the path exists (ignoring http/https links)
find . -path ./node_modules -prune -o -name '*.md' -print | while read f; do
    grep -oE '\]\([^)]+\)' "$f" | sed 's/^](//;s/)$//' | while read link; do
        # skip URLs
        echo "$link" | grep -qE '^(https?:|mailto:|#)' && continue
        # strip anchors
        target=$(echo "$link" | sed 's/#.*//')
        [ -z "$target" ] && continue
        # resolve relative to the file's directory
        dir=$(dirname "$f")
        [ -e "$dir/$target" ] || echo "DEAD: $f → $link"
    done
done
```

The script MUST print no output.

✅ Pass when zero dead links are reported.

## Step 8 — No contradiction with the Constitution

Maps to: FR-012.

For each FR newly introduced in the spec (FR-008, FR-013, FR-014,
FR-015, FR-016) re-read the Constitution and confirm the rule does
not contradict any Principle I–VII or the governance section.

Specifically:

- FR-013 (`main` protection) — extends Principle VII (DoD enforced at
  review gate); no contradiction.
- FR-014 (Maintainer/Contributor roles) — does not redefine product
  roles (admin/operator/driver); names a distinct repository-governance
  taxonomy; no contradiction.
- FR-015 (branch lifecycle) — Constitution has no rule on branch
  lifecycle; consistent with development-workflow section.
- FR-016 (DCO) — Constitution §V requires "no secret in repo"; DCO
  trailer is metadata, not a secret; no contradiction.

✅ Pass when every newly introduced rule is checked and reviewer
records "no contradiction found".

## Aggregate result

Phase 0 is **Ratified** when every step above passes on a fresh clone
of `main`. Record the runbook execution date and the executor's handle
in the PR that closes Phase 0.

If any step fails, file an issue tagged `phase-0` and fix before
declaring ratification.
