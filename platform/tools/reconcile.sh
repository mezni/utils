#!/usr/bin/env bash
set -euo pipefail

# reconcile.sh — Reverse sync: GitHub Issues → mapping.json
# Ensures mapping.json accurately reflects GitHub issue state.
# Called: after manual GitHub changes, or during recovery.

MAPPING_FILE="execution/state/mapping.json"
REPO="mezni/BorneMap"

if ! gh auth status &>/dev/null; then
  echo "[reconcile] ERROR: gh not authenticated"
  exit 1
fi

echo "[reconcile] fetching open issues from $REPO ..."
gh issue list --repo "$REPO" --state open --limit 100 --json number,title,body,labels,milestone | \
  jq '[.[] | {
    gh_id: (.number | tostring),
    title: .title,
    body: .body // "",
    labels: (.labels | map(.name) | join(",")),
    milestone: (.milestone.title // "")
  }]' > /tmp/reconciled_issues.json

CURRENT_COUNT=$(jq '.issues | length' "$MAPPING_FILE")
RECONCILED_COUNT=$(jq 'length' /tmp/reconciled_issues.json)

tmp=$(mktemp)
jq --slurpfile incoming /tmp/reconciled_issues.json \
  '.issues = $incoming[0]' "$MAPPING_FILE" > "$tmp"
mv "$tmp" "$MAPPING_FILE"

RECONCILED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
SYNC_HASH=$(sha256sum "$MAPPING_FILE" | cut -d' ' -f1)

tmp=$(mktemp)
jq --arg t "$RECONCILED_AT" --arg h "$SYNC_HASH" \
  '.last_sync = $t | .sync_hash = $h' "$MAPPING_FILE" > "$tmp"
mv "$tmp" "$MAPPING_FILE"

echo "[reconcile] complete — was: $CURRENT_COUNT issues, now: $RECONCILED_COUNT issues, hash: $SYNC_HASH"
