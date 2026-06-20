#!/usr/bin/env bash
# =============================================================================
# BorneMap — Test Runner v1.0
# tools/test_runner.sh
#
# Executes all tests and reports coverage.
#
# Usage:
#   ./tools/test_runner.sh               # run all tests
#   ./tools/test_runner.sh <service>     # run tests for a specific service
#   ./tools/test_runner.sh coverage      # run tests with coverage report
# =============================================================================

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
SERVICE="${1:-}"

echo "🧪 BorneMap Test Runner"
echo ""

run_rust_tests() {
  local service="$1"
  local service_dir="$ROOT/services/$service"

  if [[ ! -d "$service_dir" ]]; then
    echo "  ⚠️  Service directory not found: $service"
    return
  fi

  echo "  Testing: $service"
  (cd "$service_dir" && cargo test 2>&1 | sed 's/^/    /')
  echo ""
}

if [[ -n "$SERVICE" ]]; then
  case "$SERVICE" in
    coverage)
      if command -v cargo-tarpaulin >/dev/null 2>&1; then
        echo "  Running coverage for all services..."
        cargo tarpaulin --workspace --out StdHtml --output-dir "$ROOT/logs/coverage" 2>&1
        echo "  ✅ Coverage report: logs/coverage/"
      elif command -v cargo-llvm-cov >/dev/null 2>&1; then
        cargo llvm-cov --workspace --lcov --output-path "$ROOT/logs/coverage.lcov" 2>&1
        echo "  ✅ Coverage report: logs/coverage.lcov"
      else
        echo "  ❌ No coverage tool found (install cargo-tarpaulin or cargo-llvm-cov)"
        exit 1
      fi
      ;;
    auth-service|driver-service|admin-service)
      run_rust_tests "$SERVICE"
      ;;
    *)
      echo "Unknown service: $SERVICE"
      echo "Valid: auth-service, driver-service, admin-service, coverage"
      exit 1
      ;;
  esac
else
  echo "  Running all Rust tests..."
  cargo test --workspace 2>&1
fi

echo ""
echo "✅ Test run complete"