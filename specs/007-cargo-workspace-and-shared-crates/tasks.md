# Tasks: Cargo Workspace and Shared Crates

**Input**: Design documents from `/specs/007-cargo-workspace-and-shared-crates/`

**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: No test tasks generated (not requested in feature specification — unit tests are inline in crate source)

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

All paths are relative to the repository root. Rust workspace lives at `source/`. Crates under `source/crates/`.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Workspace environment configuration and linting setup

- [ ] T001 Configure clippy deny-warnings in `.cargo/config.toml` at `source/.cargo/config.toml`
- [ ] T002 [P] Add `.cargo/` and `source/target/` to `.gitignore` at repo root

**Checkpoint**: Rust tooling configuration ready

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Cargo workspace with crate stubs — workspace root compiles with stub crates

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T003 Create workspace root `Cargo.toml` at `source/Cargo.toml` with members `crates/ev-core`, `crates/ev-db`, resolver 2, edition 2024
- [ ] T004 [P] Create ev-core crate scaffold at `source/crates/ev-core/` with stub `Cargo.toml` (name, version, edition) and `src/lib.rs`
- [ ] T005 [P] Create ev-db crate scaffold at `source/crates/ev-db/` with stub `Cargo.toml` (name, version, edition) and `src/lib.rs`
- [ ] T006 Verify `cargo build --all` compiles both stub crates with zero errors and zero warnings from `source/`
- [ ] T007 Verify `cargo test --all` passes from `source/`

**Checkpoint**: Foundation ready — workspace builds, crates are members, tests pass

---

## Phase 3: User Story 1 — Cargo Workspace Builds Successfully (Priority: P1) 🎯 MVP

**Goal**: Cargo workspace at `source/` compiles both ev-core and ev-db with zero errors and zero warnings

**Independent Test**: `cargo build --all` from `source/` exits with status 0 and zero warnings. `cargo test --all` passes all unit tests.

### Implementation for User Story 1

- [ ] T008 [P] [US1] Add `[workspace.dependencies]` section to `source/Cargo.toml` with shared version management for serde, thiserror, tokio, sqlx
- [ ] T009 [P] [US1] Add `#![deny(warnings)]` and `#![deny(missing_docs)]` to `source/crates/ev-core/src/lib.rs`
- [ ] T010 [P] [US1] Add `#![deny(warnings)]` and `#![deny(missing_docs)]` to `source/crates/ev-db/src/lib.rs`
- [ ] T011 [US1] Verify `cargo build --all` zero warnings from `source/`
- [ ] T012 [US1] Verify `cargo test --all` passes from `source/`
- [ ] T013 [US1] Run `cargo clippy --all-targets -- -D warnings` from `source/` — zero warnings
- [ ] T014 [US1] Commit and tag workspace milestone

**Checkpoint**: Workspace builds with zero warnings, zero clippy issues, tests pass

---

## Phase 4: User Story 2 — Shared Enums and NanoID Generation (Priority: P2)

**Goal**: ev-core crate provides NanoID generation with configurable prefix/length and four enum types with serde serialization

**Independent Test**: `cargo test -p ev-core` passes all unit tests. 1000 NanoIDs with "PRT" prefix and length 8 have zero collisions. All four enum types round-trip through serde JSON without data loss.

### Implementation for User Story 2

- [ ] T015 [P] [US2] Add dependencies to `source/crates/ev-core/Cargo.toml`: `nanoid`, `serde` with `derive` feature, `serde_json` (dev), `thiserror`
- [ ] T016 [US2] Implement NanoID generation in `source/crates/ev-core/src/id.rs` with `generate_id(prefix, length)` and `generate_id_with_alphabet(prefix, length, alphabet)` functions
- [ ] T017 [P] [US2] Implement `ConnectorType` enum in `source/crates/ev-core/src/enums.rs` with serde `rename_all = "lowercase"` and variants: Type2, Type3, CCS, CHAdeMO
- [ ] T018 [P] [US2] Implement `ChargerStatus` enum in `source/crates/ev-core/src/enums.rs` with serde `rename_all = "snake_case"` and variants: Available, InUse, Maintenance, Offline
- [ ] T019 [P] [US2] Implement `PartnerType` enum in `source/crates/ev-core/src/enums.rs` with serde `rename_all = "lowercase"` and variants: Business, Personal
- [ ] T020 [P] [US2] Implement `StationStatus` enum in `source/crates/ev-core/src/enums.rs` with serde `rename_all = "lowercase"` and variants: Available, Partial, Unavailable
- [ ] T021 [US2] Implement `EnumParseError` error type in `source/crates/ev-core/src/enums.rs` with thiserror derive and `UnknownVariant` variant
- [ ] T022 [P] [US2] Write unit tests for NanoID generation in `source/crates/ev-core/src/id.rs` — unique ID test (1000 IDs), prefix pattern test, empty prefix test, custom alphabet test
- [ ] T023 [P] [US2] Write unit tests for enum types in `source/crates/ev-core/src/enums.rs` — round-trip serialization test, unknown variant deserialization error test
- [ ] T024 [US2] Re-export all public API items in `source/crates/ev-core/src/lib.rs` with `pub use` and doc comments
- [ ] T025 [US2] Verify `cargo build -p ev-core` zero warnings from `source/`
- [ ] T026 [US2] Verify `cargo test -p ev-core` passes from `source/`
- [ ] T027 [US2] Run `cargo clippy -p ev-core` from `source/` — zero warnings

**Checkpoint**: ev-core crate fully functional — NanoID generation and shared enums with unit tests all passing

---

## Phase 5: User Story 3 — Database Pool and Pagination Utilities (Priority: P2)

**Goal**: ev-db crate provides PgPool initialization from connection string and generic `Paginated<T>` struct with correct `total_pages` computation

**Independent Test**: `cargo test -p ev-db` passes all unit tests. `Paginated` struct computes correct `total_pages` for all boundary cases. Pool initialization validates connection string without needing a live database.

### Implementation for User Story 3

- [ ] T028 [P] [US3] Add dependencies to `source/crates/ev-db/Cargo.toml`: `sqlx` with `postgres`, `runtime-tokio`, `macros` features; `tokio` with `macros`, `rt-multi-thread` features; `thiserror`
- [ ] T029 [US3] Implement `PoolError` enum and `init_pool()` function in `source/crates/ev-db/src/pool.rs` — connection string validation, PgPool creation, error handling for invalid/missing connection strings
- [ ] T030 [US3] Implement `PoolConfig` struct (connection_string, max_connections, connection_timeout) with sensible defaults in `source/crates/ev-db/src/pool.rs`
- [ ] T031 [US3] Implement `Paginated<T>` struct with `new()` constructor in `source/crates/ev-db/src/pagination.rs` — fields: data, total, page, page_size, total_pages — with correct `total_pages` = `total.div_ceil(page_size)` when total > 0, = 0 when total == 0
- [ ] T032 [US3] Re-export all public API items in `source/crates/ev-db/src/lib.rs` with `pub use` and doc comments
- [ ] T033 [P] [US3] Write unit tests for `Paginated<T>` in `source/crates/ev-db/src/pagination.rs` — zero items, exact multiple, remainder, page > total_pages
- [ ] T034 [P] [US3] Write unit tests for pool initialization in `source/crates/ev-db/src/pool.rs` — invalid connection string returns error, valid-ish connection string accepted (no live DB needed)
- [ ] T035 [US3] Verify `cargo build -p ev-db` zero warnings from `source/`
- [ ] T036 [US3] Verify `cargo test -p ev-db` passes from `source/`
- [ ] T037 [US3] Run `cargo clippy -p ev-db` from `source/` — zero warnings

**Checkpoint**: ev-db crate fully functional — PoolConfig, init_pool, Paginated<T> with unit tests all passing

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Full workspace verification and documentation

- [ ] T038 [P] Verify `cargo build --all` zero errors and zero warnings from `source/`
- [ ] T039 [P] Verify `cargo test --all` passes all unit tests from `source/`
- [ ] T040 [P] Verify `cargo clippy --all-targets -- -D warnings` passes from `source/`
- [ ] T041 [P] Run quickstart.md validation — follow setup instructions from a clean terminal

**Checkpoint**: All 3 user stories complete. Full workspace compiles, tests pass, clippy clean.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup — BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational — establishes zero-warning baseline
- **User Story 2 (Phase 4)**: Depends on Phase 3 — ev-core needs workspace buildable, but no dependency on US3
- **User Story 3 (Phase 5)**: Depends on Phase 3 — ev-db needs workspace buildable, but no dependency on US2
- **Polish (Phase 6)**: Depends on all user stories complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) — No dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational — No dependencies on other stories (ev-core is standalone)
- **User Story 3 (P2)**: Can start after Foundational — No dependencies on other stories (ev-db is standalone)

### Within Each User Story

- Core implementation before tests
- Individual enum/module tasks before crate-level verification
- Story complete (build + test + clippy) before moving to next priority

### Parallel Opportunities

- T001, T002 (Setup) — can run in parallel
- T004, T005 (Foundational crates) — can run in parallel
- T008, T009, T010 (US1 lint configs) — can run in parallel
- T015 and T028 (crate dependencies) — can run in parallel
- T017-T020 (US2 enums) — all four enums in same file but independently writable — can be parallelized
- T022, T023 (US2 tests) — can run in parallel
- T033, T034 (US3 tests) — can run in parallel
- T038, T039, T040, T041 (Polish) — can all run in parallel
- User Story 2 and User Story 3 have NO cross-dependencies — can be implemented in parallel by different developers

---

## Parallel Example: User Stories 2 and 3

```bash
# Developer A — User Story 2 (ev-core)
Task: "Implement NanoID generation in source/crates/ev-core/src/id.rs"
Task: "Implement all four enum types in source/crates/ev-core/src/enums.rs"
Task: "Write unit tests for ev-core"

# Developer B — User Story 3 (ev-db)
Task: "Implement init_pool in source/crates/ev-db/src/pool.rs"
Task: "Implement Paginated<T> in source/crates/ev-db/src/pagination.rs"
Task: "Write unit tests for ev-db"

# After both complete
Task: "Verify cargo build --all && cargo test --all && cargo clippy --all-targets"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (workspace + crate stubs)
3. Complete Phase 3: User Story 1 (lint configs, zero-warning baseline)
4. **STOP and VALIDATE**: `cargo build --all` zero warnings, `cargo test --all` passes
5. This is the MVP — workspace foundation is usable by downstream sprints

### Incremental Delivery

1. Complete Setup + Foundational → Rust workspace exists with stub crates
2. Add User Story 1 → Zero-warning lint configs → Workspace baseline (MVP!)
3. Add User Story 2 → ev-core with NanoID + enums → Test independently
4. Add User Story 3 → ev-db with pool + pagination → Test independently
5. Each story adds value without breaking previous stories

### Parallel Team Strategy

With two developers:

1. Team completes Setup + Foundational + US1 together
2. Once Foundational is done:
   - Developer A: User Story 2 (ev-core)
   - Developer B: User Story 3 (ev-db)
3. Stories complete and integrate independently — no cross-crate dependencies

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story can be independently verified with `cargo test -p <crate-name>`
- No database required for any task — SQL connection string validation is unit-testable without a live PostgreSQL instance
- Commit after each logical task group
- Stop at any checkpoint to validate independently
