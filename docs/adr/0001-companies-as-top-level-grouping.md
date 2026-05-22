# ADR-0001 — Drop `networks`; use companies as the top-level grouping

- **Status**: Accepted
- **Date**: 2026-05-22
- **Deciders**: BorneMap core team
- **Tags**: domain, data-model

## Context

Earlier drafts of BorneMap modeled charging infrastructure under a
`Network` aggregate that grouped one or more companies. In practice:

- Drivers think in terms of "the operator of this station," not the
  network it federates into.
- Operators self-manage as a single legal entity (a company), not as
  members of a network.
- "Network" introduced a level of indirection that complicated ownership,
  audit, soft-delete cascade, and authorization scoping.

The Constitution's domain model (Principle II) requires a single, clear
hierarchy with unambiguous ownership.

## Decision

We will drop the `Network` concept entirely. **Company** is the
top-level grouping for charging infrastructure. The hierarchy is fixed:

```
Company (CMP-<nanoid>)
└── Station (STA-<nanoid>)
    └── Charger (CHR-<nanoid>)
```

- Companies are created by Admin only.
- A station is owned by exactly one company OR by a private individual.
- A charger belongs to exactly one station.
- The term `network` MUST NOT be reintroduced as a top-level grouping
  in code, schema, or API.

## Alternatives considered

- **Keep `Network` above `Company`** — Rejected. Added complexity to
  ownership/scope guards with no driver- or operator-facing benefit.
- **Make `Network` a tag/attribute on `Company`** — Rejected.
  Tagging that requires hierarchical guarantees is a hierarchy in
  disguise; better to model needs explicitly via ADR if/when they arise.

## Consequences

- **Positive**
  - Simpler authorization: operator scope = "their company."
  - Simpler soft-delete cascade (company → stations → chargers).
  - Public-facing terminology matches operator self-description.
- **Negative**
  - Future federation across companies (if ever needed) will require a
    new ADR; the current model has no built-in slot for it.
- **Follow-ups**
  - All schema, OpenAPI, and UI labels MUST use "Company" terminology.
  - Reviewers MUST reject PRs that introduce `network` as a grouping.

## Compliance check

- Schema review: no `networks` table; no `network_id` foreign keys on
  `companies`, `stations`, or `chargers`.
- OpenAPI lint: no `network` resource path.
- Code review checklist explicitly cites this ADR.
