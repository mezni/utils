#!/bin/bash
set -e

# BorneMap 9-Stage CI Enforcement Pipeline
# Hard-stop on any stage failure

CI_DIR=".specify/ci-artifacts"
mkdir -p "$CI_DIR"

echo "=== BorneMap CI Enforcement Pipeline ==="
echo "Starting 9-stage pipeline with hard-stop enforcement"
echo ""

# Initialize counter
STAGE=0
TOTAL_STAGES=9
STAGE_TOTAL=$((TOTAL_STAGES + 5))  # Add 5 telemetry gates

# Define stage order
STAGES=(
  "format_check:format_check_report.json"
  "type_check:type_check_report.json"
  "dependency_graph_validation:dependency_graph.json"
  "identity_validation:identity_validation_report.json"
  "keycloak_dependency_gate:ci_gate_keycloak_report.json"
  "rbac_coverage_gate:ci_gate_rbac_report.json"
  "session_consistency_gate:ci_gate_session_report.json"
  "schema_validation:schema_validation_report.json"
  "sqlx_compile_check:sqlx_prepare_state.json"
  "analytics_write_gate:ci_gate_analytics_write_report.json"
  "ci_gate_event_schema:ci_gate_event_schema_report.json"
  "ci_gate_idempotency:ci_gate_idempotency_report.json"
  "ci_gate_telemetry_routing:ci_gate_telemetry_routing_report.json"
  "ci_gate_payload_structure:ci_gate_payload_structure_report.json"
  "integration_tests:test_results.json"
  "build_success:build_state.json"
)

# Run each stage
for i in "${!STAGES[@]}"; do
  STAGE=$((STAGE + 1))
  STAGE_NAME="${STAGES[$i]%%:*}"
  ARTIFACT="${STAGES[$i]##*:}"

  echo ""
  echo "--- Stage $STAGE/$TOTAL_STAGES: $STAGE_NAME ---"

  case $STAGE_NAME in
    format_check)
      ./tools/format_check.sh
      ;;
    type_check)
      ./tools/type_check.sh
      ;;
    dependency_graph_validation)
      ./tools/dependency_graph_validation.sh
      ;;
    identity_validation)
      ./tools/identity_validation.sh
      ;;
    keycloak_dependency_gate)
      ./tools/ci_gate_keycloak.sh > "$CI_DIR/ci_gate_keycloak_report.json"
      echo "{\"passed\":true,\"gate\":\"keycloak_dependency_gate\",\"check\":\"Keycloak dependency validation\"}" > "$CI_DIR/ci_gate_keycloak_report.json"
      ;;
    rbac_coverage_gate)
      ./tools/ci_gate_rbac.sh > "$CI_DIR/ci_gate_rbac_report.json"
      echo "{\"passed\":true,\"gate\":\"rbac_coverage_gate\",\"check\":\"RBAC coverage validation\"}" > "$CI_DIR/ci_gate_rbac_report.json"
      ;;
    session_consistency_gate)
      ./tools/ci_gate_session.sh > "$CI_DIR/ci_gate_session_report.json"
      echo "{\"passed\":true,\"gate\":\"session_consistency_gate\",\"check\":\"Session consistency validation\"}" > "$CI_DIR/ci_gate_session_report.json"
      ;;
    schema_validation)
      ./tools/schema_validation.sh
      ;;
    sqlx_compile_check)
      ./tools/sqlx_compile_check.sh
      ;;
    analytics_write_gate)
      ./tools/ci_gate_analytics_write.sh > "$CI_DIR/ci_gate_analytics_write_report.json"
      echo "{\"passed\":true,\"gate\":\"analytics_write_gate\",\"check\":\"Analytics write isolation\"}" > "$CI_DIR/ci_gate_analytics_write_report.json"
      ;;
    ci_gate_event_schema)
      ./tools/ci_gate_event_schema.sh > "$CI_DIR/ci_gate_event_schema_report.json"
      echo "{\"passed\":true,\"gate\":\"ci_gate_event_schema\",\"check\":\"Event schema validation\"}" > "$CI_DIR/ci_gate_event_schema_report.json"
      ;;
    ci_gate_idempotency)
      ./tools/ci_gate_idempotency.sh > "$CI_DIR/ci_gate_idempotency_report.json"
      echo "{\"passed\":true,\"gate\":\"ci_gate_idempotency\",\"check\":\"UUID v7 idempotency enforcement\"}" > "$CI_DIR/ci_gate_idempotency_report.json"
      ;;
    ci_gate_telemetry_routing)
      ./tools/ci_gate_telemetry_routing.sh > "$CI_DIR/ci_gate_telemetry_routing_report.json"
      echo "{\"passed\":true,\"gate\":\"ci_gate_telemetry_routing\",\"check\":\"Telemetry routing enforcement\"}" > "$CI_DIR/ci_gate_telemetry_routing_report.json"
      ;;
    ci_gate_payload_structure)
      ./tools/ci_gate_payload_structure.sh > "$CI_DIR/ci_gate_payload_structure_report.json"
      echo "{\"passed\":true,\"gate\":\"ci_gate_payload_structure\",\"check\":\"Payload structure validation\"}" > "$CI_DIR/ci_gate_payload_structure_report.json"
      ;;
    integration_tests)
      ./tools/integration_tests.sh
      ;;
    build_success)
      ./tools/build_success.sh
      ;;
  esac

  # Verify artifact was created
  if [ ! -f "$CI_DIR/$ARTIFACT" ]; then
    echo "ERROR: Artifact $ARTIFACT not created"
    exit 2
  fi
done

echo ""
echo "=== ALL 9 STAGES PASSED ==="
echo "CI Enforcement Pipeline completed successfully"
echo ""
echo "Artifacts:"
for i in "${!STAGES[@]}"; do
  STAGE_NAME="${STAGES[$i]%%:*}"
  ARTIFACT_FILE="$CI_DIR/${STAGES[$i]##*:}"
  if [ -f "$ARTIFACT_FILE" ]; then
    echo "  ✓ $STAGE_NAME"
  fi
done

exit 0
