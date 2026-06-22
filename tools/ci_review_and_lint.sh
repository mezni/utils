#!/bin/bash
set -e

echo "Running: Code Review and Linting"
echo "=================================="

# Run cargo clippy
echo "Running cargo clippy..."
cargo clippy --all-targets --all-features -- -D warnings

echo ""
echo "Running cargo fmt --check..."
cargo fmt --check

echo ""
echo "=================================="
echo "✓ PASS: Code review and linting completed"
exit 0
