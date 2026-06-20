#!/usr/bin/env bash
# =============================================================================
# BorneMap — GitHub Label & Metadata Sync v1.0
# tools/sync.sh
#
# Syncs GitHub labels and issue metadata from sprint state.
#
# Usage:
#   ./tools/sync.sh                        # sync labels for active sprint
#   ./tools/sync.sh <SPRINT_ID>            # sync labels for specific sprint
# =============================================================================

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
STATE="$ROOT/state/sprint_state.json"

[[ -f "$STATE" ]] || { echo "❌ Missing state/sprint_state.json"; exit 1; }

SPRINT_ID="${1:-$(jq -r '.sprint_id' "$STATE")}"
MAPPING="$ROOT/state/mapping.json"
REPO="${GITHUB_REPO:-}"

command -v gh >/dev/null 2>&1 || { echo "gh CLI required"; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "jq required"; exit 1; }

if [[ -z "$REPO" ]]; then
  REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || echo "")
  [[ -z "$REPO" ]] && { echo "No GitHub repo detected"; exit 0; }
fi

echo "🔄 Syncing GitHub metadata for $SPRINT_ID..."

# Ensure labels exist
for label in "sprint:$SPRINT_ID" "phase:ingestion" "phase:contract" "phase:architecture" \
  "phase:implementation" "phase:integration" "phase:testing" "phase:review" "phase:done" \
  "status:todo" "status:in-progress" "status:done" "status:blocked" "sync"; do
  gh label create "$label" --repo "$REPO" --force 2>/dev/null || true
done

# Sync issue labels from mapping
if [[ -f "$MAPPING" ]]; then
  jq -r 'to_entries[] | "\(.key) \(.value)"' "$MAPPING" | while IFS=' ' read -r id issue_num; do
    gh issue edit "$issue_num" --repo "$REPO" --add-label "sprint:$SPRINT_ID" 2>/dev/null || true
  done
fi

echo "✅ Sync complete"