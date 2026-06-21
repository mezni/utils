#!/usr/bin/env bash
set -euo pipefail

# github_sync.sh — Projection sync: mapping.json → GitHub Issues
# Canonical source is mapping.json. GitHub is a projection.
# Called during GITHUB_SYNC phase and after mapping changes.

MAPPING_FILE="execution/state/mapping.json"
REPO="mezni/BorneMap"

if [ ! -f "$MAPPING_FILE" ]; then
  echo "[github_sync] ERROR: $MAPPING_FILE not found"
  exit 1
fi

if ! gh auth status &>/dev/null; then
  echo "[github_sync] ERROR: gh not authenticated. Run: gh auth login"
  exit 1
fi

mapfile -t issues < <(jq -c '.issues[] // empty' "$MAPPING_FILE")

CREATED=0
UPDATED=0

for issue in "${issues[@]}"; do
  title=$(echo "$issue" | jq -r '.title // empty')
  body=$(echo "$issue" | jq -r '.body // ""')
  labels=$(echo "$issue" | jq -r '.labels // ""')
  milestone=$(echo "$issue" | jq -r '.milestone // ""')
  gh_id=$(echo "$issue" | jq -r '.gh_id // ""')
  sprint=$(echo "$issue" | jq -r '.sprint // ""')

  if [ -z "$title" ]; then
    echo "[github_sync] WARNING: skipping issue with empty title"
    continue
  fi

  if [ -z "$gh_id" ] || [ "$gh_id" = "null" ]; then
    args=("--repo" "$REPO" "--title" "$title" "--body" "$body")
    if [ -n "$labels" ] && [ "$labels" != "null" ]; then
      args+=("--label" "$labels")
    fi
    gh issue create "${args[@]}" --output json 2>/dev/null | \
      jq -r '.number' | while read -r num; do
        echo "[github_sync] CREATED issue #$num: $title"
        tmp=$(mktemp)
        jq "( .issues[] | select(.title == \"$title\") | .gh_id ) |= \"$num\"" "$MAPPING_FILE" > "$tmp"
        mv "$tmp" "$MAPPING_FILE"
    done
    ((CREATED++))
  else
    gh issue edit "$gh_id" --repo "$REPO" --title "$title" --body "$body" 2>/dev/null || \
      echo "[github_sync] WARNING: failed to update issue #$gh_id"
    ((UPDATED++))
  fi
done

UPDATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
SYNC_HASH=$(sha256sum "$MAPPING_FILE" | cut -d' ' -f1)

tmp=$(mktemp)
jq --arg t "$UPDATED_AT" --arg h "$SYNC_HASH" \
  '.last_sync = $t | .sync_hash = $h' "$MAPPING_FILE" > "$tmp"
mv "$tmp" "$MAPPING_FILE"

echo "[github_sync] complete — created: $CREATED, updated: $UPDATED, hash: $SYNC_HASH"
