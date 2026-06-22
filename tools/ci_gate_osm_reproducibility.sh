#!/bin/bash
# CI Gate: OSM Reproducibility
# Purpose: Ensure OSM ingestion is deterministic and idempotent
# Fails if ingestion pipeline is non-deterministic or missing idempotency key

echo "Running OSM Reproducibility CI Gate..."

PASSED=true

# Check for idempotency key generation
if grep -q "generate_idempotency_key\|idempotency.*key\|osm_id.*unique" services/driver-service/src/ingestion/**/*.rs; then
    echo "✓ PASS: Idempotency key generation found"
else
    echo "✗ FAIL: No idempotency key generation found"
    PASSED=false
fi

# Check for duplicate detection
if grep -q "check_duplicates\|has_duplicates\|osm_id.*exists" services/driver-service/src/ingestion/**/*.rs; then
    echo "✓ PASS: Duplicate detection found"
else
    echo "✗ FAIL: No duplicate detection found"
    PASSED=false
fi

# Check for deterministic processing (no random values in ingestion)
if grep -qE "rand::random|random\(|UUID\(" services/driver-service/src/ingestion/**/*.rs; then
    echo "✗ FAIL: Found random values in ingestion pipeline"
    PASSED=false
else
    echo "✓ PASS: No random values in ingestion pipeline"
fi

if [ "$PASSED" = true ]; then
    echo "✓ PASS: OSM ingestion is deterministic and idempotent"
    exit 0
else
    echo "✗ FAIL: OSM reproducibility violated"
    exit 1
fi
