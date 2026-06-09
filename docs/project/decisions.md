# Project Decisions — BorneMap

**Last updated:** 2026-06-09

This file records small decisions that do not warrant a full ADR. Significant architecture changes go in `docs/adr/`.

## Pending Decisions

### Partner deletion behavior

**Question:** What happens when an admin deletes a partner that has stations?
**Options:**
- Cascade — delete all associated stations and chargers
- Block — prevent deletion unless partner has no stations
- Orphan — set station partner_id to null

**Outcome:** *Pending — to be decided during MVP-1 hardening (Sprint 1.6)*

## Recorded Decisions
