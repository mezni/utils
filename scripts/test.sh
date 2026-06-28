#!/usr/bin/env bash
set -euo pipefail

echo "Running all tests..."

# Rust tests
cargo test --workspace

echo "All tests passed."
