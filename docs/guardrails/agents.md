# Guardrail — Agent Orchestration

Applies to: all builder LLM sessions (Claude Code, Cursor, etc.)

---

## Session discipline

Every builder session MUST follow this exact sequence:

1. **Read `docs/GUARDRAILS.md`** completely before writing any code.
2. **Read the relevant domain guardrail** from `docs/guardrails/` for the task scope.
3. **Read `docs/SYSTEM_STATE.md`** to understand what exists and what does not.
4. **Read the relevant MVP spec** in `docs/specs/mvp-[N].md` (if one exists).
5. **Only then start writing code.**

No step may be skipped. If any of these files do not exist yet, the session should note the gap but proceed.

---

## Feed structure (for human operators feeding a builder LLM)

When starting a new builder session, provide files in this order:

```
1. docs/GUARDRAILS.md             # Master rules (always first)
2. docs/guardrails/<domain>.md    # Scoped to the task
3. docs/SYSTEM_STATE.md           # Current build reality
4. docs/specs/mvp-[N].md          # One bounded task spec
```

Never feed the builder:
- The entire constitution (it is too large and the key rules are already extracted into GUARDRAILS.md)
- Multiple MVP specs at once
- Unrelated domain guardrails

---

## Handoff protocol

When the builder completes a task or reaches a handoff point:

1. **Update tracking files** (all three):
   - `docs/SYSTEM_STATE.md` — mark built items, record known issues
   - `docs/roadmap_status.md` — update MVP status
   - `docs/sprint_backlog.md` — close completed tasks, advance backlog

2. **Self-check** against the relevant domain guardrail's self-check checklist.

3. **Run verification commands**:
   ```bash
   cargo test                    # Rust unit + integration
   cargo clippy -- -D warnings   # Zero warnings
   vitest run                    # TypeScript unit
   ```

4. **If tests fail**: fix before submitting. Do not submit broken output.

5. **If architecture violation discovered**: stop, document in SYSTEM_STATE.md as a known issue, do not proceed further on the violating path.

---

## Scope rules

- Work on exactly one MVP spec per session. No scope creep.
- Do not refactor code outside the current task scope.
- Do not add features not in the MVP spec.
- If you discover a bug in existing code while working on a task, log it in `SYSTEM_STATE.md` under "Known issues" but do not fix it unless the task spec explicitly includes it.
- If a dependency is missing or a prerequisite is not yet built, stop and report the blocker. Do not implement workarounds.

---

## Code generation rules

### When creating new files
- Follow the naming conventions in the constitution (Section V).
- Use the layer structure defined in `guardrails/rust.md`.
- Every new public function/item gets a doc comment (Rust) or JSDoc (TypeScript).
- Every new endpoint gets tests (integration test + E2E if user-facing).
- Every new interactive element gets a `data-testid` attribute.

### When modifying existing files
- Read the full file before editing. Do not assume you know its contents.
- Preserve existing patterns and conventions. If the file uses `query_as!`, do not change to `query!`.
- Do not reorder imports, reformat existing code, or make stylistic changes.

### Dependency management
- No new dependencies without explicit justification in the PR.
- If a new crate or npm package is needed, add it to the appropriate `Cargo.toml` or `package.json` and run the install command.
- Prefer standard library and first-party workspace crates/packages over third-party dependencies.

---

## Communication with the human operator

When blocked:
```
BLOCKER: [one-line description]
What I need: [specific decision, file, or configuration]
Why: [brief context]
```

When the task is complete:
```
DONE: [MVP-N task name]
Files modified:
  - source/services/[file] — [what was done]
  - docs/SYSTEM_STATE.md — [what was updated]
Verification:
  - cargo test: ✅
  - cargo clippy: ✅
  - vitest run: ✅
Next suggested: [MVP-N+1 or next task]
```

When introducing a Tier 1/2 violation (for human reviewers):
```
VIOLATION: [rule violated]
File: [path]
Reason: [why it was necessary or what needs to happen]
Fix plan: [how to resolve]
```

---

## Context window management

- If the task is large, split it into sub-tasks and request a new session per sub-task.
- Keep `SYSTEM_STATE.md` updated between sessions so each session starts with accurate context.
- Do not carry context from a previous session — re-read the files fresh each time.

---

## Self-check before handing off

- [ ] `GUARDRAILS.md` was read this session
- [ ] Only one MVP spec was worked on
- [ ] No out-of-scope files were modified
- [ ] `SYSTEM_STATE.md` updated
- [ ] All relevant tests pass
- [ ] No architecture violations introduced
- [ ] No TODOs without linked issues
- [ ] Doc comments / JSDoc added to all new public items
- [ ] `data-testid` added to all new interactive elements
