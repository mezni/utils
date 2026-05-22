# Contract: `main` Branch Ruleset

**Target**: GitHub Ruleset targeting branch `main`
**Configured via**: GitHub UI (Repository → Settings → Rules → Rulesets)
or REST API
**Source**: research R-006, spec FR-013, Q2

## Required clauses

The ruleset MUST contain **all six** of the following clauses. Each
clause maps 1:1 to a sub-bullet of FR-013.

| Clause | Setting | Maps to FR-013 sub-bullet |
|---|---|---|
| C-BR-1 | **Restrict creations / direct pushes** — direct push to `main` forbidden | "Direct push to `main` is forbidden" |
| C-BR-2 | **Require a pull request before merging** — `required_approving_review_count = 1`; `require_code_owner_review = true`; `dismiss_stale_reviews_on_push = true` | "At least one approving review from a code owner" |
| C-BR-3 | **Require status checks to pass** — enabled; **required check list starts empty** at Phase 0; grows as Phase 1 CI lands | "Required status checks … MAY be empty at Phase 0 … MUST grow as Phase 1 CI jobs land" |
| C-BR-4 | **Block force pushes** — enabled | "Force-push to `main` is forbidden" |
| C-BR-5 | **Require linear history** — enabled | "Linear history is required" |
| C-BR-6 | **Bypass list** — empty (administrators NOT in bypass list) | "Rules MUST apply to repository administrators as well … no admin bypass" |

## Auxiliary requirements

- The ruleset name SHOULD be `main-protection` for discoverability.
- The ruleset target ref MUST be exactly `refs/heads/main`.
- "Restrict deletions" SHOULD also be enabled to prevent accidental
  deletion of `main` (not explicitly required by FR-013 but
  consistent with intent).

## Versioning

A JSON export of the ruleset MUST be committed to
`docs/operations/branch-protection.json` no later than Phase 1, so
configuration drift is auditable via PR. Phase 0 does **not** need to
commit the JSON; the *applied state* is what matters here, and the
verification runbook (`quickstart.md`) confirms it.

## Verification

The `quickstart.md` runbook enumerates each clause and shows how to
verify it via the GitHub UI. A Phase-1 CI job MAY fetch the live
ruleset via API and diff it against the committed JSON; that check
is out of scope for Phase 0.

## Non-goals

- Classic Branch Protection Rules (deprecated direction).
- Required signed-commit (GPG) clause — explicitly rejected by spec
  Q5 / FR-016; requires an ADR to introduce.
- Required deployments / required workflow runs — none defined at
  Phase 0.
