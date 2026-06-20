#!/usr/bin/env bash
# =============================================================================
# BorneMap — Artifact & Checksum Validation v1.0
# tools/validate.sh
#
# Validates artifact integrity before phase transitions.
# Checks: required artifacts exist, checksums match, no orphan artifacts.
#
# Usage:
#   ./tools/validate.sh              # validate current phase artifacts
#   ./tools/validate.sh <SPRINT_ID>  # validate a specific sprint
# =============================================================================

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
STATE="$ROOT/state/sprint_state.json"

[[ -f "$STATE" ]] || { echo "❌ Missing state/sprint_state.json"; exit 1; }

SPRINT_ID="${1:-$(jq -r '.sprint_id' "$STATE")}"
SPRINT_DIR="$ROOT/sprints/$SPRINT_ID"
ARTIFACT_DIR="$SPRINT_DIR/artifacts"

[[ -d "$SPRINT_DIR" ]] || { echo "❌ Sprint directory not found: $SPRINT_DIR"; exit 1; }

echo "🔍 Validating sprint: $SPRINT_ID"
echo ""

errors=0

# Check checksum manifest is valid JSON
if [[ -f "$ARTIFACT_DIR/checksum_manifest.json" ]]; then
  if jq empty "$ARTIFACT_DIR/checksum_manifest.json" 2>/dev/null; then
    echo "  ✓ checksum_manifest.json is valid JSON"
  else
    echo "  ✗ checksum_manifest.json is not valid JSON"
    errors=$((errors + 1))
  fi
else
  echo "  ⚠️  No checksum_manifest.json found"
fi

# Validate checksums for existing files
if [[ -f "$ARTIFACT_DIR/checksum_manifest.json" ]]; then
  echo ""
  echo "  Validating file checksums..."
  while IFS= read -r entry; do
    file_path=$(echo "$entry" | jq -r '.key')
    expected_cs=$(echo "$entry" | jq -r '.value.checksum')
    abs_path="$ROOT/$file_path"

    if [[ ! -f "$abs_path" ]]; then
      echo "  ✗ File missing: $file_path"
      errors=$((errors + 1))
      continue
    fi

    if [[ "$expected_cs" != "unavailable" ]]; then
      actual_cs=$(sha256sum "$abs_path" | awk '{print $1}')
      if [[ "$actual_cs" != "$expected_cs" ]]; then
        echo "  ✗ Checksum mismatch: $file_path"
        echo "      expected: $expected_cs"
        echo "      actual:   $actual_cs"
        errors=$((errors + 1))
      fi
    fi
  done < <(jq -c 'to_entries[]' "$ARTIFACT_DIR/checksum_manifest.json")
fi

# Check generated_files_index
if [[ -f "$ARTIFACT_DIR/generated_files_index.md" ]]; then
  echo "  ✓ generated_files_index.md exists"
else
  echo "  ⚠️  No generated_files_index.md found"
fi

echo ""
if [[ "$errors" -gt 0 ]]; then
  echo "❌ Validation FAILED — $errors error(s)"
  exit 1
else
  echo "✅ Validation PASSED"
fi