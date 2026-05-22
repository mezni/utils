# Contract: CODEOWNERS Ruleset

**Path**: `.github/CODEOWNERS`
**Consumer**: GitHub PR-review-request engine
**Source**: research R-005, spec FR-007, FR-008, Q1

## Format rules

- One rule per line: `<pattern> <owner1> [<owner2> ...]`.
- Comments begin with `#` and are encouraged for governance-critical
  rules.
- Last matching rule wins (GitHub semantics). Therefore: **most
  specific first, wildcard last**.

## Required initial ruleset (Phase 0)

```text
# BorneMap CODEOWNERS — see spec FR-007/FR-008 and contracts/codeowners.md.
# Last matching rule wins; keep wildcard last.

# Governance-critical paths — Maintainers approval required
/.specify/                         @mezni/bornemap-maintainers
/docs/adr/                         @mezni/bornemap-maintainers
/.github/                          @mezni/bornemap-maintainers
/CONTRIBUTING.md                   @mezni/bornemap-maintainers
/README.md                         @mezni/bornemap-maintainers

# Project documentation
/docs/                             @mezni/bornemap-maintainers

# Wildcard fallback — every other path falls to Maintainers
*                                  @mezni/bornemap-maintainers
```

## Invariants

| ID | Rule |
|---|---|
| C-OWN-1 | The file MUST be present at `.github/CODEOWNERS`. |
| C-OWN-2 | The **last** non-comment, non-blank line MUST be the wildcard `*` rule. |
| C-OWN-3 | The wildcard owner MUST be the Maintainers team (`@mezni/bornemap-maintainers`). |
| C-OWN-4 | Each governance-critical path (`/.specify/`, `/docs/adr/`, `/.github/`, `/CONTRIBUTING.md`, `/README.md`) MUST be covered by a Maintainers-owned rule appearing before the wildcard. |
| C-OWN-5 | The Maintainers team MUST exist on GitHub before this file is committed (GitHub silently ignores unknown team references). |
| C-OWN-6 | When a new service or top-level area lands (e.g., `/services/geo-service/`), a more-specific rule MUST be added **above** the wildcard in the relevant phase's PR. |

## Verification

- Manually open a PR touching `/README.md` → Maintainers requested.
- Manually open a PR touching a path not covered by any specific rule
  (e.g., an arbitrary file in the repo root) → Maintainers requested
  via wildcard.
- A future Phase 1 CI job MAY add a CODEOWNERS lint check; not in
  scope for Phase 0.

## Non-goals

- Per-individual-handle ownership — rejected by R-005.
- Multiple owners per rule — allowed by GitHub but not used at
  Phase 0; revisit when the team grows.
