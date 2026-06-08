# Bug Tracker

Known bugs organized by severity and MVP. Resolved bugs archived at end of each MVP.

## Current Open Bugs

*(None yet — project starting.)*

## Classification

**Class A** — Blocks correctness, security, or user access. Must resolve before MVP closes.
- Wrong data returned by API
- Endpoint missing `/api` prefix
- Migration fails on clean database
- Map shows no stations when database has data
- Authentication bypass
- CORS blocking legitimate requests

**Class B** — Degrades quality, does not block. Resolve before target MVP closes.
- Slow query (>200ms on typical data)
- Missing validation message
- UI misalignment or visual bug
- Typo in error message
- Inefficient algorithm

**Class C** — Improvement or nice-to-have. No mandatory target.
- Refactor opportunity
- Minor UX polish
- Documentation gap
- Code comment clarity

---

## How to Report a Bug

1. Assign a classification (Class A, B, or C).
2. Describe the bug clearly with reproduction steps.
3. Note the affected MVP and component (Backend, Dashboard, Driver Web, Driver Mobile).
4. Track the status: Open, In Progress, Resolved.

**Example**:
```
### BUG-001: Missing /api prefix on health endpoint (Class A)

**Status**: Open

**MVP**: 1.1

**Component**: Backend

**Reproduction**: 
1. Start FastAPI service
2. Call GET /health (without /api prefix)
3. Expect 404 or redirect to /api/health

**Impact**: Violates constitution rule. Every endpoint must have /api prefix.

**Notes**: Check all routes in main.py.
```

---

## Archives

*Moved to archive when MVP closes.*
