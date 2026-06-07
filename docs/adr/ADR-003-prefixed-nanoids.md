# ADR-003: Prefixed NanoIDs over UUIDs

**Status**: Accepted
**Date**: 2026-06-07

## Context

Entity identifiers need to be unique, URL-safe, and human-friendly. Standard options: auto-increment integers, UUIDv4, NanoID, or prefixed IDs.

## Decision

Use NanoID with entity-specific prefixes: PRT-..., STN-..., CHG-..., USR-..., REV-..., EVT-...

## Rationale

- Prefixes make entity types identifiable at a glance (STN-xxx is always a station)
- URL-safe alphabet (A-Z, a-z, 0-9) — no special characters
- 21-character random portion provides sufficient collision resistance
- Sequential integers expose business information (competitors can infer growth)
- UUIDs are long and unreadable in logs, URLs, and support tickets

## Consequences

- IDs are generated in application code, not the database
- The ev-core crate owns all ID generation functions
- Existing seed data uses fixed NanoIDs (e.g., PRT-alpha001) for reproducibility
- Collision probability is negligible but not zero

## Compliance

- All ID generation uses ev_core::ids module
- No service generates IDs independently
