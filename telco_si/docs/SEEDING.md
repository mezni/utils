# Seeding — Synthetic Data Generation Engine (Planned)

> **Status: NOT IMPLEMENTED.** This page documents the *design target* only.
> The CLI seeder does not exist yet — it is scheduled for Sprint 4
> (see `docs/PLAN.md` and Epic 5). The six domain schemas are currently empty.

## Purpose

Generate realistic relational test data across all six schemas that is:

- **Schema-aligned** — matches the `SQLModel` entity definitions (planned, Sprint 2).
- **Relationally consistent** — respects foreign-key-like dependencies via global identifiers.
- **Lifecycle-representative** — includes healthy, delinquent, and suspended states.

## Topological Seeding Order

Ingestion strictly follows the relational dependency chain:

$$\text{Catalog} \longrightarrow \text{Inventory} \longrightarrow \text{CRM} \longrightarrow \text{Usage} \longrightarrow \text{Billing} \longrightarrow \text{Dunning}$$

Each stage depends on entities created in earlier stages (e.g., CDRs resolve against CRM MSISDNs; dunning cases reference billing invoices and bar inventory SIMs).

## Data Distribution

The CLI seeder generates dataset proportions of:

| Account State | Proportion | Example Content |
| --- | --- | --- |
| `CURRENT` (healthy) | ~80% | Standard invoices, fully paid / current. |
| `FIRST_NOTICE` / `WARNING` | ~15% | **Overdue invoices**, active dunning cases, issued notices. |
| `SUSPENDED` | ~5% | **Barred SIMs in Inventory**, suspended dunning cases. |

This ensures every branch of the Dunning state machine is represented in seeded data.

## Usage *(target interface)*

```bash
# Seed the database (various sizes)
python -m telco_si.seed --size demo
python -m telco_si.seed --size small
python -m telco_si.seed --size full

# Optionally control the number of subscribers generated
python -m telco_si.seed --subscribers 1000
```

### Options

| Option | Description |
| --- | --- |
| `--size` | Dataset scale: `demo` (default), `small`, `full`. |
| `--subscribers` | Override the number of generated subscribers. |
| `--seed` | Faker random seed for reproducible output. |
| `--dry-run` | Validate the plan without writing to the database. |

## Seeding Stages

1. **Catalog** — products, offers, price plans.
2. **Inventory** — SIM resources (issued `ICCID`/`MSISDN`), initial `ACTIVE` state.
3. **CRM** — subscribers and billing accounts.
4. **Usage** — CDRs resolving against subscriber MSISDNs.
5. **Billing** — invoices, receivables, balances; mark a proportion **overdue**.
6. **Dunning** — create dunning cases for overdue accounts, issue notices, and bar SIMs for the suspended proportion.