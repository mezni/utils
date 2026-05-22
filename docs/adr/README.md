# Architecture Decision Records (ADRs)

Architectural decisions for BorneMap are recorded here as MADR-style
Architecture Decision Records.

> ADRs are required by the [Constitution](../../.specify/memory/constitution.md)
> for any change affecting a constitutional boundary (service
> responsibilities, data ownership, identity provider, event pipeline,
> soft-delete scope, deployment topology, approved stack).
> See Constitution §"Development Workflow & Governance".

## Index

| ID | Title | Status | Date |
|---|---|---|---|
| [0001](./0001-companies-as-top-level-grouping.md) | Drop `networks`; use companies as top-level grouping | Accepted | 2026-05-22 |
| [0002](./0002-geo-service-in-rust.md) | `geo-service` implemented in Rust | Accepted | 2026-05-22 |
| [0003](./0003-keycloak-sole-idp.md) | Keycloak as sole identity provider | Accepted | 2026-05-22 |
| [0004](./0004-outbox-pattern.md) | Outbox pattern for reliable event publishing | Accepted | 2026-05-22 |
| [0005](./0005-soft-delete-infrastructure-only.md) | Soft delete on infrastructure entities only | Accepted | 2026-05-22 |

## How to add a new ADR

1. Copy [`template.md`](./template.md) to
   `NNNN-short-slug.md` where `NNNN` is the next zero-padded id.
2. Fill in **Status**, **Context**, **Decision**, **Consequences**.
3. Open a PR on branch `adr/NNNN-short-slug`.
4. Update the index table above in the same PR.
5. If this ADR amends the Constitution, also bump
   `.specify/memory/constitution.md` (and its Sync Impact Report) in the
   same PR.

## Status values

- **Proposed** — under discussion, not yet binding.
- **Accepted** — binding. Code MUST comply.
- **Superseded by ADR-NNNN** — kept for history; the linked ADR governs.
- **Deprecated** — no longer in effect; no replacement.

A Proposed ADR MUST NOT be relied on for implementation decisions.
