# BorneMap — Delivery OS
**Version:** 2.0
**Date:** June 2026
**Supersedes:** v1.0

---

## 1. System Architecture (Truth Model)

```
┌──────────────────────────────────────┐
│  sprints/<id>/backlog/               │
│  sprint_backlog.md  ← SOURCE OF TRUTH│
└──────────────┬───────────────────────┘
               │
               ▼
┌──────────────────────────────────────┐
│  sprint_engine.sh (STATE CORE)       │
│  - phase control                     │
│  - validation gates                  │
│  - transition rules                  │
└───────────┬──────────────────────────┘
            │
  ┌─────────┴──────────┐
  ▼                    ▼
┌──────────────┐  ┌────────────────────┐
│ GitHub Issues │  │ CI / Validation    │
│ (projection)  │  │ (hard enforcement) │
└──────────────┘  └────────────────────┘
            │
            ▼
┌──────────────────────────────────────┐
│ reconcile.sh                         │
│ (drift correction loop)              │
└──────────────────────────────────────┘
```

---

## 2. Core Principle

**GitHub is NOT the source of truth. It is a projection layer.**

The canonical backlog lives in:
```
sprints/<id>/backlog/sprint_backlog.md
```

`state/mapping.json` is the idempotency bridge between the backlog and GitHub Issues.

---

## 3. Backlog ID Format

```
EPIC-NNN     (e.g. EPIC-001)
FEAT-NNN     (e.g. FEAT-003)
STORY-NNN    (e.g. STORY-012)
```

GitHub Issue title format: `[STORY-NNN] Story title`
GitHub labels per issue: `sprint:<id>`, `phase:<phase>`, `status:<todo|in-progress|done|blocked>`

---

## 4. State Model (Canonical Schema)

`state/sprint_state.json` and `sprints/<id>/state/sprint_state.json` must always match.

```json
{
  "sprint_id": "sprint-001",
  "current_phase": "INGESTION",
  "entities": {
    "epics_total": 0,
    "features_total": 0,
    "stories_total": 0,
    "completed_stories": 0
  },
  "execution": {
    "active_story": null,
    "blocked": [],
    "in_progress": []
  },
  "sync": {
    "last_github_sync": null,
    "last_validation": null
  },
  "integrity": {
    "checksum": null,
    "drift_detected": false
  }
}
```

Phase values: `INGESTION | CONTRACT | ARCHITECTURE | IMPLEMENTATION | INTEGRATION | TESTING | REVIEW | DONE`

---

## 5. Delivery Loop (Runtime Cycle)

```
1. Human writes sprint_backlog.md
        ↓
2. LLM executes current phase (outputs artifacts)
        ↓
3. sprint_engine.sh validates phase artifacts
        ↓
4. reconcile.sh syncs GitHub Issues
        ↓
5. ci_guard.sh validates code + contracts
        ↓
6. reconcile.sh compares states, detects drift
        ↓
7. sprint state updated OR blocked
        ↓
8. Human triggers next phase via sprint_engine.sh
        ↓
9. repeat
```

---

## 6. Failure Model

### Hard Fail (sprint blocked)
- Invalid phase transition
- Schema isolation violation
- OpenAPI mismatch
- Service boundary breach
- Missing `WHERE s.is_test = FALSE` in production query

### Soft Fail (flagged, continue with acknowledgment)
- Missing GitHub sync
- Incomplete story mapping
- Stale issue state

### Recovery
- `tools/reconcile.sh` for GitHub drift
- Rollback to last valid `sprint_state.json` checkpoint
- Re-run `tools/validate.sh` after fix

---

## 7. What This System Is

| Component | Role |
|---|---|
| `sprint_backlog.md` | Canonical source of truth for all work |
| `sprint_engine.sh` | Phase state machine + transition control |
| `reconcile.sh` | GitHub Issues ↔ backlog drift correction |
| `ci_guard.sh` | Hard validation gate (build breaker) |
| `state/mapping.json` | Backlog ID → GitHub Issue # bridge |
| `state/sprint_state.json` | Live phase + story status |
