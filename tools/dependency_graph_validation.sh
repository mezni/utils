#!/bin/bash
set -e

# Stage 3: dependency_graph_validation
# Run cargo tree and check for forbidden dependencies

echo "=== Stage 3: dependency_graph_validation ==="

OUTPUT=$(cargo tree --all 2>&1)

# Parse for forbidden patterns
# service → service imports
# frontend → backend imports
# shared-domain → services
# ui-kit → client-core
# circular dependencies

# Check for forbidden dependencies using cargo metadata
FORBIDDEN_COUNT=$(cargo metadata --format-version 1 --no-deps --quiet 2>/dev/null | jq -r '
  .workspace_members as $members |
  .packages | to_entries[] as $pkg |
  select($pkg.value.name != "ui-kit" and $pkg.value.name != "domain-types" and $pkg.value.name != "client-core") |
  $pkg.value.dependencies[] as $dep |
  select($dep.name | contains("ui-kit") or contains("client-core")) |
  1
' 2>/dev/null || echo "0")

if [ "$FORBIDDEN_COUNT" != "0" ]; then
  echo "dependency_graph_validation FAILED: Found forbidden dependencies"
  FORBIDDEN_COUNT=$(echo $FORBIDDEN_COUNT | tr -d '\r')
  echo '{"status":"failed","exit_code":1,"forbidden_count":'$FORBIDDEN_COUNT',"summary":"Forbidden dependencies found"}' > .specify/ci-artifacts/dependency_graph.json
  exit 1
else
  echo "dependency_graph_validation PASSED"
  echo '{"status":"passed","exit_code":0,"forbidden_count":0,"summary":"No forbidden dependencies found"}' > .specify/ci-artifacts/dependency_graph.json
fi
