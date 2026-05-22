# Contract: Pull-Request Template Surface

**Path**: `.github/PULL_REQUEST_TEMPLATE.md`
**Consumers**: every PR author; every reviewer; Phase 1 CI parsing job
**Source**: research R-004, spec FR-005, FR-006

## Required structure (sections, order, content)

The PR template MUST render exactly the following sections in this
order. Section *titles* are part of the contract; deviation breaks any
CI parser added in Phase 1.

```markdown
## Summary

<free text>

## Phase / Principles

- Phase: <0..12 or 5.5>
- Principles: <comma-separated I..VII, or "none — pure docs/test">

## Changes

- ...

## Tests

(check all that apply; "N/A" if a category does not apply to this PR)

- [ ] unit
- [ ] integration
- [ ] transaction (if mutation)
- [ ] outbox (if event-producing)
- [ ] audit (if auditable)
- [ ] soft-delete (if infrastructure entity)
- [ ] spatial (if geo)

## Definition of Done

- [ ] Tests passing
- [ ] OpenAPI updated (or N/A)
- [ ] Security validated (Principle V)
- [ ] Logging/metrics/health verified (Principle VI)
- [ ] ADR filed (if constitutional boundary affected)

## ADR

Does this PR affect a constitutional boundary (service responsibility,
data ownership, identity provider, event pipeline, soft-delete scope,
deployment topology, approved stack)?

- [ ] No
- [ ] Yes — ADR link: <docs/adr/NNNN-...>
```

## Field semantics

| Field | Required | Validation (Phase 1 CI) |
|---|---|---|
| `Phase: <value>` | Yes | MUST match one of the roadmap phase identifiers |
| `Principles: <list>` | Yes | MUST be a comma-separated list of `I`..`VII`, or the literal string `none — pure docs/test` |
| `Tests:` checkbox grid | At least one checked or all marked N/A | Empty grid blocks merge |
| `Definition of Done:` checklist | All 5 items checked before merge | Unchecked items block reviewer approval |
| `ADR:` Yes/No | Exactly one of the two checkboxes ticked | Both unchecked → reviewer blocks |

## Versioning

The template is versioned together with the Constitution. If
Constitution Principle VII changes, this contract MUST be updated and
the PR template re-shipped in the same change.

## Non-goals

- Per-area templates (feature.md, bugfix.md, etc.) — rejected by
  R-004.
- YAML form fields — not supported by GitHub for PR templates.
- Auto-filled content (issue number, branch name) — left to future
  iteration; not required by spec.
