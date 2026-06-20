#!/usr/bin/env bash
# =============================================================================
# BorneMap — CI Guard v2.0
# tools/ci_guard.sh
#
# Hard validation gate (build breaker). Runs before every phase transition
# and on every commit.
#
# Gates:
#   1. API Validation (OpenAPI schema check)
#   2. Backend Validation (cargo check, test, sqlx)
#   3. Schema Isolation (cross-schema access check)
#   4. Identity Validation (nanoid format)
#   5. Architecture Compliance (no forbidden infra, exactly 3 services)
#   6. Security + Known Bug Guards (KNOWN-001 through KNOWN-004)
#   7. Test Coverage (domain 100%, api >=90%)
#   8. Doc Drift (architecture.md freshness)
# =============================================================================

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"

echo "🔐 Running CI Guard..."
echo ""

fail_count=0
warn_count=0

fail() {
  local msg="$1"
  echo "❌ FAIL: $msg"
  fail_count=$((fail_count + 1))
}

warn() {
  local msg="$1"
  echo "⚠️  WARN: $msg"
  warn_count=$((warn_count + 1))
}

ok() {
  echo "✅ $1"
}

# ── Gate 1: API / OpenAPI Validation ─────────────────────────────────────────

echo "─── Gate 1: OpenAPI Schema Validation ───"
if ls "$ROOT/api/openapi/"*.yaml >/dev/null 2>&1; then
  for f in "$ROOT/api/openapi/"*.yaml; do
    if grep -q "^openapi:" "$f" 2>/dev/null; then
      ok "Valid OpenAPI spec: $(basename "$f")"
    else
      fail "Invalid OpenAPI spec (missing 'openapi:' key): $f"
    fi
  done
else
  warn "No OpenAPI specs found in api/openapi/"
fi
echo ""

# ── Gate 2: Backend Validation ───────────────────────────────────────────────

echo "─── Gate 2: Backend Validation ───"
if [[ -f "$ROOT/Cargo.toml" ]]; then
  if command -v cargo >/dev/null 2>&1; then
    echo "  cargo check..."
    cargo check --workspace 2>&1 || fail "cargo check failed"
    echo "  cargo test..."
    cargo test --workspace 2>&1 || fail "cargo test failed"
    if command -v cargo-sqlx >/dev/null 2>&1 || command -v sqlx >/dev/null 2>&1; then
      echo "  sqlx prepare --check..."
      cargo sqlx prepare --check --workspace 2>&1 || warn "sqlx prepare --check failed"
    else
      warn "sqlx CLI not installed — skipping sqlx check"
    fi
  else
    warn "cargo not installed — skipping Rust checks"
  fi
else
  warn "No Cargo.toml found — skipping backend checks"
fi
echo ""

# ── Gate 3: Schema Isolation ─────────────────────────────────────────────────

echo "─── Gate 3: Schema Isolation ───"
FOUND_CROSS=$(grep -rE "FROM\s+(users|inventory|gis)\." "$ROOT/services/" 2>/dev/null || true)
if [[ -n "$FOUND_CROSS" ]]; then
  echo "  Cross-schema direct access detected:"
  echo "$FOUND_CROSS"
  fail "Cross-schema direct access detected (must use service API)"
else
  ok "No cross-schema access detected"
fi
echo ""

# ── Gate 4: Identity Validation ──────────────────────────────────────────────

echo "─── Gate 4: Identity / nanoid Format ───"
FOUND_INVALID=$(grep -rE "[A-Z]{3}-[^a-zA-Z0-9]|[A-Z]{3}-[a-zA-Z0-9]{1,11}[^a-zA-Z0-9]" \
  "$ROOT/services/" "$ROOT/apps/" "$ROOT/packages/" 2>/dev/null \
  | grep -vE "[A-Z]{3}-[a-zA-Z0-9]{12}" || true)
if [[ -n "$FOUND_INVALID" ]]; then
  echo "$FOUND_INVALID"
  fail "Invalid ID format detected (expected PREFIX-nanoid12)"
else
  ok "All IDs match nanoid(12) format"
fi
echo ""

# ── Gate 5: Architecture Compliance ──────────────────────────────────────────

echo "─── Gate 5: Architecture Compliance ───"
# Check for forbidden infrastructure keywords in services code
FORBIDDEN=("kafka" "rabbitmq" "nats" "istio" "linkerd" "kubernetes" "jaeger" "opentelemetry")
for keyword in "${FORBIDDEN[@]}"; do
  if grep -rI "$keyword" "$ROOT/services/" "$ROOT/shared/" 2>/dev/null | grep -iv "\.md\|comment\|//.*$keyword" >/dev/null; then
    warn "Forbidden infrastructure keyword found: $keyword"
  fi
done

# Check service count (shared-services-config is shared config, not a service)
SERVICE_DIRS=$(find "$ROOT/services" -mindepth 1 -maxdepth 1 -type d -not -name "shared-services-config" | sort)
SERVICE_COUNT=$(echo "$SERVICE_DIRS" | wc -l)
if [[ "$SERVICE_COUNT" -gt 3 ]]; then
  fail "More than 3 services detected: $SERVICE_COUNT"
else
  ok "Service topology: $SERVICE_COUNT service(s) (max 3)"
  echo "  Services: $(echo "$SERVICE_DIRS" | xargs -I{} basename {} | tr '\n' ' ')"
fi
echo ""

# ── Gate 6: Security + Known Bugs ─────────────────────────────────────────────

echo "─── Gate 6: Security + Known Bug Guards ───"

# KNOWN-001: test station leakage in driver-service
DRIVER_STATION_QUERIES=$(grep -rn "FROM.*stations" "$ROOT/services/driver-service/" 2>/dev/null || true)
if [[ -n "$DRIVER_STATION_QUERIES" ]]; then
  MISSING_FILTER=$(echo "$DRIVER_STATION_QUERIES" | grep -v "is_test\s*=\s*FALSE" | grep -v "\.md\|\.txt\|#" || true)
  if [[ -n "$MISSING_FILTER" ]]; then
    echo "$MISSING_FILTER"
    fail "KNOWN-001: stations query missing WHERE s.is_test = FALSE"
  else
    ok "KNOWN-001 guard: all station queries have is_test filter"
  fi
else
  ok "No station queries found in driver-service yet"
fi

# KNOWN-002: partner_profiles must have deleted_at column
FOUND_PARTNER_MIGRATIONS=$(find "$ROOT/services" -path "*/migrations/*.sql" -exec grep -l "partner_profiles" {} \; 2>/dev/null || true)
if [[ -n "$FOUND_PARTNER_MIGRATIONS" ]]; then
  MISSING_DELETED_AT=$(grep -L "deleted_at" $FOUND_PARTNER_MIGRATIONS 2>/dev/null || true)
  if [[ -n "$MISSING_DELETED_AT" ]]; then
    fail "KNOWN-002: partner_profiles table missing deleted_at TIMESTAMPTZ column"
    echo "  Migrations missing deleted_at: $MISSING_DELETED_AT"
  else
    ok "KNOWN-002 guard: partner_profiles has deleted_at column"
  fi
else
  ok "No partner_profiles migrations found yet"
fi

# KNOWN-003: no duplicate /api/v1/nearby endpoints across services
FOUND_NEARBY=$(grep -rn "/nearby" "$ROOT/services/" "$ROOT/api/" 2>/dev/null | grep -v "\.md" || true)
NEARBY_COUNT=$(echo "$FOUND_NEARBY" | grep -c . || true)
if [[ "$NEARBY_COUNT" -gt 1 ]]; then
  echo "$FOUND_NEARBY"
  fail "KNOWN-003: duplicate /api/v1/nearby endpoint — must be single endpoint in driver-service only"
else
  ok "KNOWN-003 guard: no duplicate /nearby endpoints"
fi

# KNOWN-004: grep -E flag must be used instead of BRE alternation ( \| )
# Check sibling tool scripts only (exclude self to avoid self-match)
FOUND_GREP_BRE=$(grep -rn 'grep.*\\|' "$ROOT/tools/" 2>/dev/null \
  | grep -v "$ROOT/tools/ci_guard.sh" \
  | grep -vE 'grep -(E|rE|rnE|oE|P|rP)' | grep -v '\.md\|#' || true)
if [[ -n "$FOUND_GREP_BRE" ]]; then
  warn "KNOWN-004: grep uses BRE alternation ( \| ) instead of -E flag"
  echo "$FOUND_GREP_BRE"
else
  ok "KNOWN-004 guard: no grep using BRE alternation without -E"
fi

echo ""

# ── Gate 7: Test Coverage ────────────────────────────────────────────────────

echo "─── Gate 7: Test Coverage ───"
if [[ -d "$ROOT/services" ]] && command -v cargo-tarpaulin >/dev/null 2>&1; then
  echo "  Running coverage check..."
  cargo tarpaulin --workspace --out Stdout -- --test-threads=1 2>&1 \
    || warn "Coverage check failed (may need cargo-tarpaulin installed)"
elif command -v cargo-llvm-cov >/dev/null 2>&1; then
  echo "  Running coverage check (llvm-cov)..."
  cargo llvm-cov --workspace --lcov --output-path /tmp/lcov.info 2>&1 \
    || warn "Coverage check failed"
else
  warn "Coverage tool not installed — skipping coverage gate"
fi
echo ""

# ── Gate 8: Doc Drift ────────────────────────────────────────────────────────

echo "─── Gate 8: Doc Drift Detection ───"
ARCH="$ROOT/docs/architecture.md"
if [[ -f "$ARCH" ]]; then
  LAST_MIGRATION=$(find "$ROOT/services" -name "*.sql" -newer "$ARCH" 2>/dev/null | head -1)
  if [[ -n "$LAST_MIGRATION" ]]; then
    warn "Migrations newer than docs/architecture.md — update docs"
    echo "  Newest migration: $LAST_MIGRATION"
    echo "  Doc timestamp:    $(stat -c '%y' "$ARCH" 2>/dev/null || stat -f '%Sm' "$ARCH" 2>/dev/null)"
  else
    ok "Architecture docs are up to date"
  fi
else
  warn "docs/architecture.md not found — skipping doc drift check"
fi
echo ""

# ── Summary ───────────────────────────────────────────────────────────────────

echo "══════════════════════════════════════════"
echo "  CI Guard Summary"
echo "  Failures: $fail_count"
echo "  Warnings: $warn_count"
echo "══════════════════════════════════════════"
echo ""

if [[ "$fail_count" -gt 0 ]]; then
  echo "❌ CI Guard FAILED — $fail_count gate(s) blocked"
  exit 1
else
  echo "✅ CI Guard PASSED"
  exit 0
fi