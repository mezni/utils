# ARCHITECTURAL DECISION RECORDS (ADR)

---

## Purpose

This file records all irreversible architectural decisions.

---

## Format

### ADR-001: External ID System

Status: Accepted

Decision:
System uses ONLY external IDs (PRT/STA/CHR) as primary identifiers.

Rationale:
- prevents internal schema leakage
- stabilizes API contract
- simplifies frontend integration

---

## Rules

- All major decisions MUST be recorded here
- No undocumented architectural changes allowed
- ADRs are immutable once accepted
