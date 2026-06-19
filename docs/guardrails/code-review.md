# Guardrail — Code Review

Applies to: all PRs, all LLM-generated output before it is accepted into the codebase

---

## Purpose

This file defines how to review code produced by an LLM builder session (or a human). The review process has two goals: catch violations before they reach the codebase, and teach the builder what correct output looks like.

Every piece of LLM output must pass this review before being committed. No exceptions for "small" changes.

---

## Review tiers

### Tier 1 — Blocking (fix before accepting anything)

These are hard failures. The output cannot be accepted until they are resolved.

**Architecture violations**
- [ ] A fourth microservice introduced without an ADR
- [ ] Any service other than Auth Service calling Keycloak APIs
- [ ] Any client code calling Keycloak token endpoints directly
- [ ] Any service writing to a schema it does not own
- [ ] Any application code connecting to `keycloak_db`
- [ ] A new endpoint not under `/api/v1/`
- [ ] Async event bus or queue introduced (prohibited during validation phase)

**Security violations**
- [ ] JWT stored in localStorage
- [ ] Any credential, secret, or key hardcoded in source
- [ ] An endpoint missing role validation that requires it
- [ ] `X-User-Id` or `X-User-Roles` headers trusted from the client (must come from Traefik only)

**Data integrity violations**
- [ ] Multi-table write without an explicit transaction
- [ ] Cache bust before `tx.commit()` instead of after
- [ ] `REFRESH MATERIALIZED VIEW` without `CONCURRENTLY`
- [ ] Raw SQL string concatenation instead of `sqlx::query!` macros
- [ ] Query on soft-deleted table missing `WHERE deleted_at IS NULL`

**Code quality blockers**
- [ ] `unwrap()` or `expect()` outside `#[cfg(test)]`
- [ ] `any` keyword in TypeScript
- [ ] API response consumed without Zod validation
- [ ] Missing error variant for a new failure mode

---

### Tier 2 — Required (fix in the same PR, not a follow-up)

These must be resolved before merging, but do not block review of other items.

**Tests**
- [ ] New service method without a unit test (happy + error path)
- [ ] New endpoint without an integration test (200, 401, 403, 422)
- [ ] New user-facing flow without an E2E test
- [ ] `data-testid` missing on a new interactive element

**Documentation**
- [ ] New `pub` Rust item without a doc comment and `# Errors` section
- [ ] New exported TypeScript function/hook without JSDoc
- [ ] `SYSTEM_STATE.md` not updated to reflect the session's work
- [ ] Architectural decision made without an ADR

**UX contract**
- [ ] Screen missing any of the four states (loading, success, empty, error)
- [ ] Shimmer not matching the shape of the target content
- [ ] Map interaction without 300ms debounce
- [ ] Form missing Zod validation or React Hook Form integration
- [ ] Hardcoded display string not going through i18n

**Database**
- [ ] New column without a migration file
- [ ] New `GEOMETRY` column without a GIST index
- [ ] New FK column without a btree index
- [ ] Migration edits an already-applied file instead of adding a new one

---

### Tier 3 — Recommended (note in review, fix in a follow-up)

These are quality improvements that don't block merging.

- Clippy warnings present but non-critical
- A function is too long (>40 lines in Rust service/handler layer) and could be split
- A test fixture is duplicated and could be extracted to a shared helper
- An i18n string is present but not yet translated into Arabic
- A component is complex enough to warrant a Storybook story
- `EXPLAIN ANALYZE` not included as a comment on a new index

---

## LLM output review workflow

When reviewing code produced by an LLM builder session, follow this sequence:

1. **Read the diff top-to-bottom once** without accepting or rejecting anything. Get the full picture first.

2. **Run Tier 1 checks.** If any Tier 1 violation is found, stop. Return the output to the builder with a specific correction prompt:

```
CORRECTION REQUIRED — Tier 1 violation:

[describe the exact violation]
[quote the offending line(s)]

Fix required:
[describe exactly what the correct implementation looks like]

Do not change anything else. Return only the corrected file(s).
```

3. **Run Tier 2 checks.** List all Tier 2 issues and return them together in one correction round (not one at a time).

4. **Run Tier 3 checks.** Log these as follow-up tasks in `docs/sprint_backlog.md` — do not block the current session.

5. **Verify tests pass locally** before marking the session output as accepted:
```bash
cargo test                  # Rust unit + integration
cargo clippy -- -D warnings # Zero warnings
vitest run                  # TypeScript unit
playwright test             # E2E (on full stack only)
```

6. **Update `docs/SYSTEM_STATE.md`** to reflect what was built and verified.

---

## Correction prompt templates

### Architecture violation
```
The output violates the BorneMap constitution.

Violation: [service X is calling Keycloak directly / writing to schema Y / etc.]
File: [path]
Line: [N]

The rule: [quote the relevant constitution rule]

Fix: [specific instruction]

Return only the corrected file. Do not add new features or refactor unrelated code.
```

### Missing test
```
The output is missing required tests.

What needs a test: [function/endpoint name]
Required test cases:
  - Happy path: [describe expected input and output]
  - Error path: [describe what should fail and how]
  - Auth: [401 when no token, 403 when wrong role]

Add these tests to [file path].
Return only the test file.
```

### Missing documentation
```
The following public items are missing documentation:

- [fn name] in [file] — needs doc comment with # Errors section
- [hook name] in [file] — needs JSDoc with @param and @returns

Add documentation only. Do not change implementation.
```

---

## What good output looks like

A well-formed builder session output:

- Touches exactly the files needed for the current task. Nothing more.
- Includes tests in the same output (not deferred to "a follow-up").
- Includes doc comments on every new public item.
- Ends with an update to `SYSTEM_STATE.md`.
- Has zero Clippy warnings and zero TypeScript errors.
- Does not refactor code outside the task scope.
- Does not introduce new dependencies without noting them explicitly.

If the output drifts outside task scope (refactoring unrelated files, adding features not in the spec), reject the out-of-scope portions and have the builder revert them.

---

## Self-check (reviewer)

Before marking a session output as accepted:

- [ ] All Tier 1 checks passed
- [ ] All Tier 2 checks passed or tracked as follow-ups
- [ ] Tests run locally and pass
- [ ] `SYSTEM_STATE.md` updated
- [ ] No new dependencies introduced without review
- [ ] No files outside the task scope were modified
