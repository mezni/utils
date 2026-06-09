# Quickstart: Cargo Workspace and Shared Crates

## Prerequisites

- Rust toolchain (rustc 1.85+, cargo). Install via [rustup](https://rustup.rs/):
  ```bash
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```

## Build

From the repository root:

```bash
cd source/
cargo build --all
```

This compiles both `ev-core` and `ev-db` crates.

## Test

```bash
cd source/
cargo test --all
```

Runs all unit tests in both crates. No database required.

## Lint

```bash
cd source/
cargo clippy --all-targets -- -D warnings
```

## Workspace Structure

```
source/
├── Cargo.toml              # Workspace root
└── crates/
    ├── ev-core/             # NanoID generation + shared enums
    │   └── Cargo.toml
    └── ev-db/               # PostgreSQL pool + pagination
        └── Cargo.toml
```

## Verify

```bash
# Clean build, zero warnings
cargo build --all 2>&1 | grep -i warning || echo "PASS: zero warnings"

# All tests pass
cargo test --all 2>&1 | tail -5
```

Expected output:
```
PASS: zero warnings
test result: ok. <N> passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```
