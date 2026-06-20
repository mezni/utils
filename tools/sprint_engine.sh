#!/usr/bin/env bash
# =============================================================================
# BorneMap — Sprint Engine v2.0
# tools/sprint_engine.sh
#
# Phase state machine + transition control for the BorneMap SDEC system.
# This is the ONLY authorized mechanism for mutating sprint phase state.
# LLMs and developers MUST NOT edit sprint_state.json directly.
#
# Usage:
#   ./tools/sprint_engine.sh status
#   ./tools/sprint_engine.sh validate
#   ./tools/sprint_engine.sh transition <TARGET_PHASE>
#   ./tools/sprint_engine.sh init <SPRINT_ID>
#   ./tools/sprint_engine.sh story-done <STORY_ID>
#   ./tools/sprint_engine.sh story-block <STORY_ID> "<reason>"
#   ./tools/sprint_engine.sh story-start <STORY_ID>
#
# Phases (in order):
#   INGESTION → CONTRACT → ARCHITECTURE → IMPLEMENTATION →
#   INTEGRATION → TESTING → REVIEW → DONE
# =============================================================================

set -euo pipefail

# ── Paths ─────────────────────────────────────────────────────────────────────

ROOT_DIR="$(git rev-parse --show-toplevel)"
GLOBAL_STATE="$ROOT_DIR/state/sprint_state.json"
GLOBAL_TRANSITION_LOG="$ROOT_DIR/state/transition_log.json"
GLOBAL_MAPPING="$ROOT_DIR/state/mapping.json"

# ── Helpers ───────────────────────────────────────────────────────────────────

log() {
  local msg="$1"
  local ts
  ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  echo "[$ts] $msg" | tee -a "$ROOT_DIR/logs/sprint.log"
}

fail() {
  log "FAIL: $1"
  echo ""
  echo "❌  sprint_engine: $1"
  echo ""
  exit 1
}

warn() {
  echo "⚠️   $1"
  log "WARN: $1"
}

ok() {
  echo "✅  $1"
  log "OK: $1"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "Required command not found: $1"
}

require_cmd jq

# ── State loading ─────────────────────────────────────────────────────────────

load_state() {
  [[ -f "$GLOBAL_STATE" ]] || fail "Global state file missing: $GLOBAL_STATE\nRun: ./tools/sprint_engine.sh init <SPRINT_ID>"

  SPRINT_ID=$(jq -r '.sprint_id' "$GLOBAL_STATE")
  [[ -n "$SPRINT_ID" && "$SPRINT_ID" != "null" ]] || fail "sprint_id is null in $GLOBAL_STATE"

  SPRINT_DIR="$ROOT_DIR/sprints/$SPRINT_ID"
  SPRINT_STATE="$SPRINT_DIR/state/sprint_state.json"
  SPRINT_TRANSITION_LOG="$SPRINT_DIR/state/transition_log.json"
  SPRINT_PHASE_HISTORY="$SPRINT_DIR/state/phase_history.json"
  ARTIFACT_DIR="$SPRINT_DIR/artifacts"

  [[ -f "$SPRINT_STATE" ]] || fail "Sprint state missing: $SPRINT_STATE"

  GLOBAL_PHASE=$(jq -r '.current_phase' "$GLOBAL_STATE")
  LOCAL_PHASE=$(jq -r '.current_phase' "$SPRINT_STATE")

  [[ "$GLOBAL_PHASE" == "$LOCAL_PHASE" ]] \
    || fail "Phase drift detected — global=$GLOBAL_PHASE, sprint=$LOCAL_PHASE\nRun reconcile manually and fix before proceeding."
}

# ── Phase transition rules ────────────────────────────────────────────────────

VALID_PHASES=(INGESTION CONTRACT ARCHITECTURE IMPLEMENTATION INTEGRATION TESTING REVIEW DONE)

is_valid_phase() {
  local phase="$1"
  for p in "${VALID_PHASES[@]}"; do
    [[ "$p" == "$phase" ]] && return 0
  done
  return 1
}

validate_transition() {
  local from="$1"
  local to="$2"

  case "$from" in
    INGESTION)      [[ "$to" == "CONTRACT" ]]       || fail "Invalid transition: $from → $to (expected CONTRACT)" ;;
    CONTRACT)       [[ "$to" == "ARCHITECTURE" ]]   || fail "Invalid transition: $from → $to (expected ARCHITECTURE)" ;;
    ARCHITECTURE)   [[ "$to" == "IMPLEMENTATION" ]] || fail "Invalid transition: $from → $to (expected IMPLEMENTATION)" ;;
    IMPLEMENTATION) [[ "$to" == "INTEGRATION" ]]    || fail "Invalid transition: $from → $to (expected INTEGRATION)" ;;
    INTEGRATION)    [[ "$to" == "TESTING" ]]        || fail "Invalid transition: $from → $to (expected TESTING)" ;;
    TESTING)        [[ "$to" == "REVIEW" ]]         || fail "Invalid transition: $from → $to (expected REVIEW)" ;;
    REVIEW)         [[ "$to" == "DONE" ]]           || fail "Invalid transition: $from → $to (expected DONE)" ;;
    DONE)           fail "Sprint is DONE. No further transitions allowed." ;;
    *)              fail "Unknown current phase: $from" ;;
  esac
}

# ── Phase artifact requirements ───────────────────────────────────────────────

required_artifacts_for_phase() {
  local phase="$1"
  case "$phase" in
    INGESTION)
      echo \
        "$SPRINT_DIR/spec/spec.md" \
        "$SPRINT_DIR/spec/scope.md" \
        "$SPRINT_DIR/spec/non_scope.md" \
        "$SPRINT_DIR/spec/assumptions.md" \
        "$SPRINT_DIR/backlog/sprint_backlog.md" \
        "$SPRINT_DIR/backlog/task_breakdown.md"
      ;;
    CONTRACT)
      echo \
        "$SPRINT_DIR/api/openapi.yaml"
      ;;
    ARCHITECTURE)
      echo \
        "$SPRINT_DIR/design/architecture.md" \
        "$SPRINT_DIR/design/data_model.md" \
        "$SPRINT_DIR/design/service_contracts.md"
      ;;
    IMPLEMENTATION)
      echo \
        "$SPRINT_DIR/implementation"
      ;;
    INTEGRATION)
      echo ""
      ;;
    TESTING)
      echo \
        "$SPRINT_DIR/testing/test_results.log" \
        "$SPRINT_DIR/testing/coverage.md"
      ;;
    REVIEW)
      echo \
        "$SPRINT_DIR/review/sprint_review.md" \
        "$SPRINT_DIR/review/validation_report.md" \
        "$SPRINT_DIR/review/retro.md" \
        "$SPRINT_DIR/backlog/follow_up.md" \
        "$ARTIFACT_DIR/generated_files_index.md" \
        "$ARTIFACT_DIR/checksum_manifest.json"
      ;;
    *)
      echo ""
      ;;
  esac
}

# ── Validate artifacts ────────────────────────────────────────────────────────

cmd_validate() {
  load_state
  echo ""
  echo "🔍 Validating artifacts for phase: $LOCAL_PHASE (sprint: $SPRINT_ID)"
  echo ""

  local missing=0
  local artifacts
  artifacts=$(required_artifacts_for_phase "$LOCAL_PHASE")

  if [[ -z "$artifacts" ]]; then
    ok "No artifact requirements for phase $LOCAL_PHASE"
    return 0
  fi

  for artifact in $artifacts; do
    if [[ -e "$artifact" ]]; then
      echo "  ✓  $artifact"
    else
      echo "  ✗  MISSING: $artifact"
      missing=$((missing + 1))
    fi
  done

  echo ""

  if [[ -f "$ARTIFACT_DIR/checksum_manifest.json" ]]; then
    jq empty "$ARTIFACT_DIR/checksum_manifest.json" 2>/dev/null \
      || fail "checksum_manifest.json is not valid JSON"
    echo "  ✓  checksum_manifest.json is valid JSON"
  fi

  if [[ "$LOCAL_PHASE" == "REVIEW" && -f "$SPRINT_DIR/bugs/active.md" ]]; then
    local active_bugs
    active_bugs=$(grep -cE "^## BUG-" "$SPRINT_DIR/bugs/active.md" 2>/dev/null || echo 0)
    if [[ "$active_bugs" -gt 0 ]]; then
      warn "$active_bugs unresolved bug(s) in bugs/active.md — resolve before DONE transition"
    fi
  fi

  if [[ "$missing" -gt 0 ]]; then
    fail "$missing required artifact(s) missing. Resolve before transitioning."
  fi

  ok "All artifacts valid for phase $LOCAL_PHASE"
}

# ── Transition ────────────────────────────────────────────────────────────────

cmd_transition() {
  local target="${1:-}"
  [[ -n "$target" ]] || fail "No target phase specified.\nUsage: sprint_engine.sh transition <PHASE>"

  load_state

  is_valid_phase "$target" || fail "Unknown target phase: $target\nValid phases: ${VALID_PHASES[*]}"
  validate_transition "$LOCAL_PHASE" "$target"

  echo ""
  echo "🔄 Transitioning: $LOCAL_PHASE → $target"
  echo ""

  cmd_validate
  echo ""

  if [[ -x "$ROOT_DIR/tools/ci_guard.sh" ]]; then
    echo "🔐 Running CI guard..."
    "$ROOT_DIR/tools/ci_guard.sh" || fail "CI guard failed. Fix issues before transitioning."
    echo ""
  else
    warn "ci_guard.sh not found or not executable — skipping CI gate"
  fi

  local ts
  ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

  local tmp_sprint
  tmp_sprint=$(mktemp)
  jq \
    --arg phase "$target" \
    --arg ts "$ts" \
    '.current_phase = $phase | .sync.last_validation = $ts' \
    "$SPRINT_STATE" > "$tmp_sprint" \
    && mv "$tmp_sprint" "$SPRINT_STATE"

  local tmp_global
  tmp_global=$(mktemp)
  jq \
    --arg phase "$target" \
    --arg ts "$ts" \
    '.current_phase = $phase | .sync.last_validation = $ts' \
    "$GLOBAL_STATE" > "$tmp_global" \
    && mv "$tmp_global" "$GLOBAL_STATE"

  local entry="{\"from\":\"$LOCAL_PHASE\",\"to\":\"$target\",\"timestamp\":\"$ts\"}"
  echo "$entry" >> "$SPRINT_TRANSITION_LOG"
  echo "$entry" >> "$GLOBAL_TRANSITION_LOG"

  local tmp_history
  tmp_history=$(mktemp)
  if [[ -f "$SPRINT_PHASE_HISTORY" ]]; then
    jq \
      --arg phase "$LOCAL_PHASE" \
      --arg ts "$ts" \
      '. + [{"phase": $phase, "completed_at": $ts}]' \
      "$SPRINT_PHASE_HISTORY" > "$tmp_history"
  else
    echo "[{\"phase\": \"$LOCAL_PHASE\", \"completed_at\": \"$ts\"}]" > "$tmp_history"
  fi
  mv "$tmp_history" "$SPRINT_PHASE_HISTORY"

  log "Transition: $LOCAL_PHASE → $target (sprint: $SPRINT_ID)"
  ok "Transition complete: $LOCAL_PHASE → $target"
  echo ""
  echo "  Next phase:    $target"
  echo "  Sprint:        $SPRINT_ID"
  echo "  Timestamp:     $ts"
  echo ""
}

# ── Init new sprint ───────────────────────────────────────────────────────────

cmd_init() {
  local sprint_id="${1:-}"
  [[ -n "$sprint_id" ]] || fail "No sprint ID specified.\nUsage: sprint_engine.sh init <SPRINT_ID>\nExample: sprint_engine.sh init sprint-001"

  echo ""
  echo "🚀 Initializing sprint: $sprint_id"
  echo ""

  local sprint_dir="$ROOT_DIR/sprints/$sprint_id"
  [[ ! -d "$sprint_dir" ]] || fail "Sprint directory already exists: $sprint_dir"

  local dirs=(
    "$sprint_dir/spec"
    "$sprint_dir/design"
    "$sprint_dir/api"
    "$sprint_dir/implementation/backend"
    "$sprint_dir/implementation/frontend"
    "$sprint_dir/implementation/shared"
    "$sprint_dir/testing/unit"
    "$sprint_dir/testing/integration"
    "$sprint_dir/bugs"
    "$sprint_dir/backlog"
    "$sprint_dir/state"
    "$sprint_dir/review"
    "$sprint_dir/artifacts"
  )

  for dir in "${dirs[@]}"; do
    mkdir -p "$dir"
    echo "  mkdir  $dir"
  done

  local ts
  ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

  cat > "$sprint_dir/state/sprint_state.json" <<EOF
{
  "sprint_id": "$sprint_id",
  "current_phase": "INGESTION",
  "entities": {
    "epics_total": 0,
    "features_total": 0,
    "stories_total": 0,
    "completed_stories": 0
  },
  "execution": {
    "active_story": null,
    "blocked": [],
    "in_progress": []
  },
  "sync": {
    "last_github_sync": null,
    "last_validation": null
  },
  "integrity": {
    "checksum": null,
    "drift_detected": false
  }
}
EOF

  echo "[]" > "$sprint_dir/state/transition_log.json"
  echo "[]" > "$sprint_dir/state/phase_history.json"

  cat > "$sprint_dir/artifacts/checksum_manifest.json" <<'EOF'
{}
EOF

  cat > "$sprint_dir/artifacts/generated_files_index.md" <<EOF
# Generated Files Index — $sprint_id
_Updated automatically. Do not edit manually._

| File | Phase | Skill | Generated At |
|---|---|---|---|
EOF

  cat > "$sprint_dir/bugs/active.md" <<EOF
# Active Bugs — $sprint_id
_No active bugs._
EOF
  cat > "$sprint_dir/bugs/resolved.md" <<EOF
# Resolved Bugs — $sprint_id
_No resolved bugs._
EOF
  cat > "$sprint_dir/bugs/regression_log.md" <<EOF
# Regression Log — $sprint_id
_No regressions recorded._
EOF

  local tmp_global
  tmp_global=$(mktemp)

  if [[ -f "$GLOBAL_STATE" ]]; then
    jq \
      --arg id "$sprint_id" \
      --arg ts "$ts" \
      '.sprint_id = $id | .current_phase = "INGESTION" | .sync.last_validation = $ts' \
      "$GLOBAL_STATE" > "$tmp_global"
  else
    cat > "$tmp_global" <<EOF
{
  "sprint_id": "$sprint_id",
  "current_phase": "INGESTION",
  "entities": {
    "epics_total": 0,
    "features_total": 0,
    "stories_total": 0,
    "completed_stories": 0
  },
  "execution": {
    "active_story": null,
    "blocked": [],
    "in_progress": []
  },
  "sync": {
    "last_github_sync": null,
    "last_validation": "$ts"
  },
  "integrity": {
    "checksum": null,
    "drift_detected": false
  }
}
EOF
  fi
  mv "$tmp_global" "$GLOBAL_STATE"

  [[ -f "$GLOBAL_MAPPING" ]] || echo "{}" > "$GLOBAL_MAPPING"
  [[ -f "$GLOBAL_TRANSITION_LOG" ]] || echo "[]" > "$GLOBAL_TRANSITION_LOG"

  mkdir -p "$ROOT_DIR/logs"
  touch "$ROOT_DIR/logs/sprint.log" "$ROOT_DIR/logs/ci.log" "$ROOT_DIR/logs/validation.log"

  echo ""
  ok "Sprint $sprint_id initialized (phase: INGESTION)"
  echo ""
  echo "  Directory:   $sprint_dir"
  echo "  Next step:   Add sprint input to $sprint_dir/spec/spec.md"
  echo ""
}

# ── Story state management ────────────────────────────────────────────────────

cmd_story_start() {
  local story_id="${1:-}"
  [[ -n "$story_id" ]] || fail "No story ID specified.\nUsage: sprint_engine.sh story-start <STORY_ID>"

  load_state

  local tmp
  tmp=$(mktemp)
  jq \
    --arg id "$story_id" \
    '
      .execution.active_story = $id |
      .execution.in_progress = (
        [.execution.in_progress[] | select(. != $id)] + [$id]
      )
    ' \
    "$SPRINT_STATE" > "$tmp" && mv "$tmp" "$SPRINT_STATE"

  jq --arg id "$story_id" '.execution.active_story = $id' \
    "$GLOBAL_STATE" > "$(mktemp)" \
    && mv "$(mktemp 2>/dev/null || echo /tmp/g_tmp.json)" "$GLOBAL_STATE" 2>/dev/null || true

  ok "Story started: $story_id"
  log "Story started: $story_id (sprint: $SPRINT_ID, phase: $LOCAL_PHASE)"
}

cmd_story_done() {
  local story_id="${1:-}"
  [[ -n "$story_id" ]] || fail "No story ID specified.\nUsage: sprint_engine.sh story-done <STORY_ID>"

  load_state

  local tmp
  tmp=$(mktemp)
  jq \
    --arg id "$story_id" \
    '
      .execution.in_progress = [.execution.in_progress[] | select(. != $id)] |
      .execution.blocked      = [.execution.blocked[]      | select(. != $id)] |
      .execution.active_story = (if .execution.active_story == $id then null else .execution.active_story end) |
      .entities.completed_stories += 1
    ' \
    "$SPRINT_STATE" > "$tmp" && mv "$tmp" "$SPRINT_STATE"

  local completed
  completed=$(jq '.entities.completed_stories' "$SPRINT_STATE")
  jq --argjson n "$completed" '.entities.completed_stories = $n' \
    "$GLOBAL_STATE" > /tmp/g_done.json && mv /tmp/g_done.json "$GLOBAL_STATE"

  ok "Story done: $story_id (completed: $completed)"
  log "Story done: $story_id (sprint: $SPRINT_ID)"
}

cmd_story_block() {
  local story_id="${1:-}"
  local reason="${2:-unspecified}"
  [[ -n "$story_id" ]] || fail "No story ID specified.\nUsage: sprint_engine.sh story-block <STORY_ID> \"<reason>\""

  load_state

  local tmp
  tmp=$(mktemp)
  jq \
    --arg id "$story_id" \
    '
      .execution.in_progress = [.execution.in_progress[] | select(. != $id)] |
      .execution.blocked = (
        [.execution.blocked[] | select(.id != $id)] + [{"id": $id}]
      ) |
      .integrity.drift_detected = true
    ' \
    "$SPRINT_STATE" > "$tmp" && mv "$tmp" "$SPRINT_STATE"

  warn "Story blocked: $story_id — $reason"
  log "Story blocked: $story_id — $reason (sprint: $SPRINT_ID)"
}

# ── Status ────────────────────────────────────────────────────────────────────

cmd_status() {
  load_state

  local completed total active drift
  completed=$(jq -r '.entities.completed_stories' "$SPRINT_STATE")
  total=$(jq -r '.entities.stories_total' "$SPRINT_STATE")
  active=$(jq -r '.execution.active_story // "none"' "$SPRINT_STATE")
  drift=$(jq -r '.integrity.drift_detected' "$SPRINT_STATE")

  local blocked_count
  blocked_count=$(jq '.execution.blocked | length' "$SPRINT_STATE")

  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  BorneMap Sprint Engine — Status"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Sprint:         $SPRINT_ID"
  echo "  Phase:          $LOCAL_PHASE"
  echo "  Progress:       $completed / $total stories done"
  echo "  Active story:   $active"
  echo "  Blocked:        $blocked_count"
  echo "  Drift detected: $drift"
  echo ""

  local next
  case "$LOCAL_PHASE" in
    INGESTION)      next="CONTRACT" ;;
    CONTRACT)       next="ARCHITECTURE" ;;
    ARCHITECTURE)   next="IMPLEMENTATION" ;;
    IMPLEMENTATION) next="INTEGRATION" ;;
    INTEGRATION)    next="TESTING" ;;
    TESTING)        next="REVIEW" ;;
    REVIEW)         next="DONE" ;;
    DONE)           next="(sprint complete)" ;;
    *)              next="unknown" ;;
  esac

  echo "  Next phase:     $next"
  echo "  Transition:     ./tools/sprint_engine.sh transition $next"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""
}

# ── Register an artifact ──────────────────────────────────────────────────────

cmd_register_artifact() {
  local file_path="${1:-}"
  local skill="${2:-unknown}"
  [[ -n "$file_path" ]] || fail "No file path specified.\nUsage: sprint_engine.sh register-artifact <relative-path> <skill>"

  load_state

  local abs_path="$ROOT_DIR/$file_path"
  [[ -f "$abs_path" ]] || fail "File not found: $abs_path"

  local checksum
  if command -v sha256sum >/dev/null 2>&1; then
    checksum=$(sha256sum "$abs_path" | awk '{print $1}')
  elif command -v shasum >/dev/null 2>&1; then
    checksum=$(shasum -a 256 "$abs_path" | awk '{print $1}')
  else
    checksum="unavailable"
    warn "sha256sum / shasum not found — checksum skipped"
  fi

  local ts
  ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

  local tmp
  tmp=$(mktemp)
  jq \
    --arg path "$file_path" \
    --arg cs "$checksum" \
    --arg ts "$ts" \
    '.[$path] = {"checksum": $cs, "registered_at": $ts}' \
    "$ARTIFACT_DIR/checksum_manifest.json" > "$tmp" \
    && mv "$tmp" "$ARTIFACT_DIR/checksum_manifest.json"

  echo "| \`$file_path\` | $LOCAL_PHASE | $skill | $ts |" \
    >> "$ARTIFACT_DIR/generated_files_index.md"

  ok "Artifact registered: $file_path (sha256: ${checksum:0:12}...)"
}

# ── CLI dispatch ──────────────────────────────────────────────────────────────

print_usage() {
  echo ""
  echo "BorneMap Sprint Engine v2.0"
  echo ""
  echo "Usage: sprint_engine.sh <command> [args]"
  echo ""
  echo "Commands:"
  echo "  init <SPRINT_ID>              Initialize a new sprint directory"
  echo "  status                        Show current sprint and phase"
  echo "  validate                      Check required artifacts for current phase"
  echo "  transition <PHASE>            Advance to next phase (runs validate + CI first)"
  echo "  story-start <STORY_ID>        Mark a story as in-progress"
  echo "  story-done <STORY_ID>         Mark a story as complete"
  echo "  story-block <STORY_ID> <why>  Mark a story as blocked"
  echo "  register-artifact <path> <skill>  Register a generated file + checksum"
  echo ""
  echo "Phase sequence:"
  echo "  INGESTION → CONTRACT → ARCHITECTURE → IMPLEMENTATION"
  echo "  → INTEGRATION → TESTING → REVIEW → DONE"
  echo ""
}

case "${1:-}" in
  init)                cmd_init "${2:-}" ;;
  status)              cmd_status ;;
  validate)            cmd_validate ;;
  transition)          cmd_transition "${2:-}" ;;
  story-start)         cmd_story_start "${2:-}" ;;
  story-done)          cmd_story_done "${2:-}" ;;
  story-block)         cmd_story_block "${2:-}" "${3:-}" ;;
  register-artifact)   cmd_register_artifact "${2:-}" "${3:-unknown}" ;;
  help|--help|-h)      print_usage ;;
  *)
    print_usage
    exit 1
  ;;
esac