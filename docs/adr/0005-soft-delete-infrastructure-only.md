# ADR-0005 — Soft delete on infrastructure entities only

- **Status**: Accepted
- **Date**: 2026-05-22
- **Deciders**: BorneMap core team
- **Tags**: data-lifecycle, retention

## Context

BorneMap manages two qualitatively different kinds of data:

1. **Infrastructure data** — companies, stations, chargers. These are
   long-lived business records, frequently referenced by other entities,
   and operators or admins must be able to "undelete" them or audit
   their lifecycle.
2. **Operational / transient data** — favorites, reviews, moderation
   records, outbox rows, audit logs. These either expire naturally,
   represent user actions, or are append-only artifacts. Keeping
   "deleted" copies of all of them creates index bloat, query
   complexity, and privacy risk without operational benefit.

Constitution Principle IV mandates a clean line between the two.

## Decision

We will apply **soft delete to infrastructure entities only**.

- The following tables carry `deleted_at TIMESTAMPTZ NULL`:
  - `companies`
  - `stations`
  - `chargers`
- All read queries on these tables MUST include `WHERE deleted_at IS
  NULL` unless the caller is an explicit admin/audit path that opts in
  with a documented flag.
- Cascading soft delete:
  - Deleting a `company` MUST soft-delete its `stations` and their
    `chargers` in the same transaction.
  - Deleting a `station` MUST soft-delete its `chargers`.
  - Each cascaded soft-delete produces its own outbox event so the
    audit log records the full effect (Principle III + Principle VII).
- Non-infrastructure tables (`favorites`, `reviews`, `moderation`,
  `outbox`, audit collections) MUST NOT carry `deleted_at`. They use
  hard delete or their own retention policy.

## Alternatives considered

- **Soft delete everywhere** — Rejected. Index/query complexity for
  little benefit on transient data; complicates GDPR-style deletion
  requests on user-generated content like reviews.
- **No soft delete; rely on backups for undelete** — Rejected.
  Operators must be able to restore a station without involving DBAs.
- **Versioned (history) tables for infra** — Rejected for MVP. Useful
  but adds significant complexity; can be added later via ADR if audit
  needs grow beyond the audit-log stream.

## Consequences

- **Positive**
  - Reversible operator/admin actions on infrastructure.
  - Audit log + soft-delete combine to give a full historical view.
  - Transient tables stay lean.
- **Negative**
  - Every infra-table read path must remember the `deleted_at IS NULL`
    filter. Forgetting it is a correctness bug.
  - Cascades must be implemented carefully to keep the outbox in step.
- **Follow-ups**
  - Phase 3: add `deleted_at` columns and partial indexes.
  - Phase 5b: implement cascade behavior and tests.
  - Repository pattern (or shared query helper) MUST default-apply the
    filter so opting out is the explicit case.

## Compliance check

- Schema review: only `companies`, `stations`, `chargers` carry
  `deleted_at`. CI fails if a non-infrastructure migration introduces
  the column.
- Soft-delete tests (Principle VII) MUST cover:
  - Default reads exclude soft-deleted rows.
  - Cascade soft-deletes propagate through the hierarchy.
  - Each cascade level emits its outbox event.
- Code review checklist explicitly cites this ADR for any new infra
  read path.
