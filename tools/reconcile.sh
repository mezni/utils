#!/usr/bin/env bash
# =============================================================================
# BorneMap — GitHub ↔ Backlog Reconciliation Tool v1.0
# tools/reconcile.sh
#
# Reads sprint ID from global state (not hardcoded) and syncs GitHub Issues
# with the canonical sprint backlog.
#
# Usage:
#   ./tools/reconcile.sh                    # default: sync issues
#   ./tools/reconcile.sh status             # show drift status
#   ./tools/reconcile.sh force              # force-create all missing issues
# =============================================================================

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
STATE_FILE="$ROOT/state/sprint_state.json"

[[ -f "$STATE_FILE" ]] || { echo "Missing state/sprint_state.json"; exit 1; }

SPRINT_ID=$(jq -r '.sprint_id' "$STATE_FILE")
REPO="${GITHUB_REPO:-}"
BACKLOG="$ROOT/sprints/$SPRINT_ID/backlog/sprint_backlog.md"
MAP="$ROOT/state/mapping.json"

command -v gh >/dev/null 2>&1 || { echo "gh CLI required"; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "jq required"; exit 1; }

if [[ -z "$REPO" ]]; then
  if gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null; then
    REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner)
  else
    echo "⚠️  No GitHub repo detected. Set GITHUB_REPO or run from a tracked repo."
    echo "   Skipping GitHub sync. Backlog remains source of truth."
    exit 0
  fi
fi

[[ -f "$BACKLOG" ]] || { echo "Missing backlog: $BACKLOG"; exit 1; }
[[ -f "$MAP" ]] || echo "{}" > "$MAP"

echo "🔎 Reconciling GitHub vs backlog for $SPRINT_ID..."

get_issue() {
  jq -r --arg id "$1" '.[$id] // empty' "$MAP"
}

set_issue() {
  jq --arg id "$1" --arg num "$2" '.[$id] = $num' "$MAP" > /tmp/map.json \
    && mv /tmp/map.json "$MAP"
}

create_if_missing() {
  local id="$1"
  local title="$2"
  local sprint="$3"

  local existing
  existing=$(get_issue "$id")

  if [[ -n "$existing" ]]; then
    echo "$existing"
    return
  fi

  local url
  url=$(gh issue create \
    --repo "$REPO" \
    --title "[$id] $title" \
    --body "Auto-synced from backlog: $SPRINT_ID" \
    --label "sprint:$sprint,sync" 2>/dev/null) || {
    echo "  ⚠️  Failed to create issue for $id (skipping)"
    return
  }

  local num
  num=$(echo "$url" | grep -oE '[0-9]+$')
  set_issue "$id" "$num"
  echo "$num"
}

# Parse sprint_backlog.md for STORY/FEAT/EPIC lines
grep -E '^\s*[-#]+\s*\[(EPIC|FEAT|STORY)-[0-9]+\]' "$BACKLOG" \
  | grep -oE '(EPIC|FEAT|STORY)-[0-9]+\] .+' \
  | while IFS='] ' read -r id title; do
      num=$(create_if_missing "$id" "$title" "$SPRINT_ID")
      if [[ -n "$num" ]]; then
        echo "  $id → #$num"
      fi
    done

echo "✅ Reconciliation complete"
