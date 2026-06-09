# ADR-003: Prefixed NanoIDs over UUIDs (from MVP-2)

**Status:** Accepted
**Date:** 2026-06-09

## Context

Public API identifiers must be URL-safe, unique, and human-recognizable by entity type. UUIDs are opaque and long. Sequential integers expose business information and enable enumeration attacks.

## Decision

Use prefixed NanoIDs (e.g., `PRT-a3b2c1d4`, `STN-x9y8z7w6`) for all entity identifiers. NanoID prefix indicates entity type (`PRT` = partner, `STN` = station, etc.). Alphabet is alphanumeric (A-Z, a-z, 0-9). Sequential integers are never exposed in public APIs.

## Consequences

- Shorter than UUIDs while maintaining uniqueness
- Entity type recognizable from the ID prefix alone
- No enumeration risk
- Requires a shared NanoID crate (`ev-core`) from MVP-2 onward
