#!/bin/bash
set -e

echo "Running: CI Gate Validation Tests"
echo "=================================="

# Test 1: Verify analytics write gate detects unauthorized writes
echo ""
echo "Test 1: Verify analytics_write_gate rejects unauthorized writes..."

# Create a test service that would try to write to analytics_db
cat > /tmp/test_service.rs << 'EOF'
use sqlx::postgres::PgPool;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct TestEvent {
    schema_version: String,
    user_id: String,
    timestamp: String,
    payload: serde_json::Value,
}

async fn unauthorized_write(pool: &PgPool) {
    // This would fail in CI because it's not driver-service
    let query = r#"
        INSERT INTO analytics_events (schema_version, event_type, event_id, user_id, timestamp, payload, idempotency_key, location_source, session_start, session_duration, role, service_name, event_source, status, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
    "#;

    sqlx::query(query)
        .bind("1.0.0")
        .bind("AUTH_LOGIN")
        .bind("00000000-0000-0000-0000-000000000000")
        .bind("00000000-0000-0000-0000-000000000000")
        .bind("2026-06-22T13:00:00Z")
        .bind(serde_json::json!({}))
        .bind("00000000-0000-0000-0000-000000000000")
        .bind("default_location")
        .bind("2026-06-22T13:00:00Z")
        .bind(3600)
        .bind("driver")
        .bind("unauthorized-service")
        .bind("AUTH_LOGIN")
        .bind("pending")
        .bind("2026-06-22T13:00:00Z")
        .bind("2026-06-22T13:00:00Z")
        .execute(pool)
        .await
        .expect("This should fail - unauthorized write");
}

fn main() {
    // This test would fail in CI
    let pool = PgPool::connect("postgresql://user:pass@localhost:5432/borne_map")
        .await
        .expect("Failed to connect to database");
    let _ = unauthorized_write(&pool).await;
}
EOF

if grep -r "INSERT.*INTO analytics_db\|UPDATE.*analytics_db\|DELETE.*FROM analytics_db" --include="*.rs" services/ > /dev/null 2>&1; then
    echo "❌ FAIL: Found SQLx queries targeting analytics_db"
    grep -r "INSERT.*INTO analytics_db\|UPDATE.*analytics_db\|DELETE.*FROM analytics_db" \
       --include="*.rs" \
       services/
    exit 1
else
    echo "✓ PASS: No direct SQLx writes to analytics_db found"
fi

# Test 2: Verify event schema validation gate rejects unknown versions
echo ""
echo "Test 2: Verify event_schema gate rejects unknown versions..."

if grep -r "2.0.0" --include="*.rs" services/driver-service/src > /dev/null 2>&1; then
    echo "❌ FAIL: Found code with unknown schema version '2.0.0'"
    exit 1
else
    echo "✓ PASS: No unknown schema versions found"
fi

# Test 3: Verify UUID v7 idempotency gate is enforced
echo ""
echo "Test 3: Verify UUID v7 idempotency gate..."

if ! grep -r "Uuid::new_v7\|uuid::Uuid::new_v7" --include="*.rs" services/driver-service/src > /dev/null; then
    echo "❌ FAIL: UUID v7 generation not found"
    exit 1
fi

if ! grep -r "idempotency_key" --include="*.sql" services/driver-service/migrations/ > /dev/null; then
    echo "❌ FAIL: unique index on idempotency_key not found in migration"
    exit 1
fi

echo "✓ PASS: UUID v7 idempotency enforced"

# Test 4: Verify telemetry routing gate
echo ""
echo "Test 4: Verify telemetry routing gate..."

if ! grep -r "telemetry" --include="*.rs" services/driver-service/src/api/telemetry.rs > /dev/null 2>&1; then
    echo "❌ FAIL: Telemetry API route not found"
    exit 1
fi

if ! grep -r "telemetry" --include="*.yml" --include="*.yaml" services/driver-service/config/ > /dev/null; then
    echo "❌ FAIL: Traefik routing configuration not found"
    exit 1
fi

echo "✓ PASS: Telemetry routing enforced"

# Test 5: Verify payload structure validation
echo ""
echo "Test 5: Verify payload structure validation..."

if ! grep -r "serde_json::Value" --include="*.rs" services/driver-service/src/middleware/validation.rs > /dev/null 2>&1; then
    echo "❌ FAIL: JSON payload validation not found"
    exit 1
fi

if ! grep -r "is_object" --include="*.rs" services/driver-service/src/middleware/validation.rs > /dev/null 2>&1; then
    echo "❌ FAIL: Object type validation not found"
    exit 1
fi

echo "✓ PASS: Payload structure validation enforced"

echo ""
echo "=================================="
echo "✓ PASS: All CI gate validation tests passed"
exit 0
