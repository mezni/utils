#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
SIZE_LIMIT="${1:-10M}" # Default size limit set to 10M if no argument is passed
REPO_DIR="$(git rev-parse --show-toplevel 2>/dev/null || true)"

if [[ -z "$REPO_DIR" ]]; then
  echo "Error: Must be run inside a Git repository." >&2
  exit 1
fi

cd "$REPO_DIR"

echo "=================================================================="
echo " Starting Git Repository Optimization"
echo " Target size limit : ${SIZE_LIMIT}"
echo " Repository path   : ${REPO_DIR}"
echo "=================================================================="

# 1. Check for uncommitted local changes
if ! git diff-index --quiet HEAD --; then
  echo "Error: Uncommitted changes detected in repository." >&2
  echo "Please commit or stash changes before proceeding." >&2
  exit 1
fi

# 2. Check for git-filter-repo installation
if ! command -v git-filter-repo &> /dev/null; then
  echo "Error: 'git-filter-repo' is not installed." >&2
  echo "Install it via pipx: 'pipx install git-filter-repo' or apt: 'sudo apt install git-filter-repo'." >&2
  exit 1
fi

# 3. Capture remote URL before filter-repo removes it
ORIGIN_URL="$(git remote get-url origin 2>/dev/null || true)"

# 4. Measure disk usage before cleanup
BEFORE_SIZE=$(du -sh .git | cut -f1)
echo "Size of .git BEFORE cleanup: $BEFORE_SIZE"

# 5. Run git-filter-repo
echo "Stripping blobs larger than ${SIZE_LIMIT}..."
git filter-repo --strip-blobs-bigger-than "$SIZE_LIMIT" --force

# 6. Purge reflogs and run aggressive garbage collection
echo "Purging reflogs and running aggressive garbage collection..."
git reflog expire --expire=now --all
git gc --prune=now --aggressive

# 7. Measure disk usage after cleanup
AFTER_SIZE=$(du -sh .git | cut -f1)
echo "Size of .git AFTER cleanup: $AFTER_SIZE"

# 8. Restore original remote origin if it existed
if [[ -n "$ORIGIN_URL" ]]; then
  echo "Restoring remote origin ($ORIGIN_URL)..."
  git remote add origin "$ORIGIN_URL" 2>/dev/null || git remote set-url origin "$ORIGIN_URL"
  echo ""
  echo "To push the cleaned history to your remote host, run:"
  echo "  git push origin --force --all"
  echo "  git push origin --force --tags"
fi

echo "=================================================================="
echo " Optimization Complete!"
echo " Space changed from $BEFORE_SIZE to $AFTER_SIZE"
echo "=================================================================="
