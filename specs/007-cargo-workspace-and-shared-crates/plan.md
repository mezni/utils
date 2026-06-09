# Implementation Plan: Cargo Workspace and Shared Crates

**Branch**: `007-cargo-workspace-and-shared-crates` | **Date**: 2026-06-09 | **Spec**: [spec.md](./spec.md)

**Input**: Sprint 2.1 — Initialize the Rust workspace, build ev-core (NanoID generation, shared enums), and build ev-db (PgPool wrapper, pagination structs). This is the first sprint of MVP-2, replacing json-server with Rust services backed by PostgreSQL + PostGIS.

## Summary

Set up the Rust workspace at `source/` with two shared library crates: `ev-core` (NanoID ID generation + canonical enums matching MVP-1 data model) and `ev-db` (PgPool initialization + generic `Paginated<T>` struct). Both crates compile with zero warnings and all unit tests pass. Downstream services (Driver Service, Admin Service) will depend on these crates in subsequent sprints.

## Technical Context

**Language/Version**: Rust 1.85+ (edition 2024)

**Primary Dependencies**: 
- `nanoid` crate (NanoID generation in ev-core)
- `serde` + `serde_json` (enum serialization in ev-core)
- `sqlx` 0.8+ with `postgres` and `runtime-tokio` features (PgPool in ev-db)
- `tokio` (async runtime for ev-db)
- `thiserror` (error types in both crates)

**Storage**: N/A (Sprint 2.1 — database schema deferred to Sprint 2.2)

**Testing**: `cargo test` (unit tests only; integration tests requiring live PostgreSQL deferred)

**Target Platform**: Linux server (x86_64)

**Project Type**: library workspace (two shared library crates)

**Performance Goals**: NanoID generation <1μs per ID; Paginated struct instantiation <1μs

**Constraints**: Zero compiler warnings (`#![deny(warnings)]`); all public items documented; 1000-ID collision test must pass

**Scale/Scope**: 2 library crates, ~15 public items total (functions + structs + enums)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

The constitution file (`.specify/memory/constitution.md`) is a template with `[PLACEHOLDER]` tokens — not ratified. No gates apply.

## Phase 0: Research

All technical decisions are documented in the spec and implementation plan. No `[NEEDS CLARIFICATION]` markers remain. Research is minimal — confirming established Rust patterns.

### Key Decisions

| Decision | Chosen Approach | Rationale |
|----------|----------------|-----------|
| NanoID crate | `nanoid` | Lightweight, well-maintained, URL-safe alphabet, allows custom length/prefix |
| Enum serialization | `serde` with `serde_repr` for integer-backed, `rename_all = "lowercase"` | Matches MVP-1 JSON convention; round-trips cleanly |
| DB pool | `sqlx::PgPool` | Standard for async Rust + PostgreSQL; used by both downstream services |
| Async runtime | `tokio` with `rt-multi-thread` | De facto standard for async Rust; required by sqlx |
| Error handling | `thiserror` derive macro | Idiomatic Rust; zero boilerplate for custom error enums |
| Workspace layout | `source/Cargo.toml` with `[workspace]` at repo root | Frontend apps are JS/TS — Rust lives in its own subdirectory tree under `source/` |

## Phase 1: Design

### Project Structure

```
source/
├── Cargo.toml              # Workspace root
├── crates/
│   ├── ev-core/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs          # Public API re-exports
│   │       ├── id.rs           # NanoID generation
│   │       └── enums.rs        # Canonical enum types
│   └── ev-db/
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs          # Public API re-exports
│           ├── pool.rs         # PgPool initialization
│           └── pagination.rs   # Paginated<T> struct
└── apps/                       # Existing JS/TS apps (unchanged)
    ├── dashboard/
    ├── driver-web/
    └── driver-mobile/
```

**Structure Decision**: Standard Cargo workspace with crates under `source/crates/`. The JS apps remain in `source/apps/` — no conflict. Two crates (ev-core, ev-db) as independently testable libraries.

### Data Model

See `data-model.md` for entity definitions, validation rules, and state transitions.

### Contracts

See `contracts/` directory for crate public API contracts (function signatures, struct definitions, enum variants).

### Quickstart

See `quickstart.md` for developer setup instructions.

### Agent Context

AGENTS.md will be updated to reference this plan file.
