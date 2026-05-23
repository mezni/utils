#!/bin/bash

# Test script for Core Service API

echo "=== Core Service API Test ==="
echo ""

# Base URL
BASE_URL="http://localhost:8081/api/v1"

echo "1. Testing health endpoint..."
curl -s "$BASE_URL/../health/core-service" | jq .
echo ""

echo "2. Testing metrics endpoint..."
curl -s "$BASE_URL/../metrics/core-service" | head -10
echo ""

echo "3. Creating a test company..."
COMPANY_RESPONSE=$(curl -s -X POST "$BASE_URL/companies" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Test Company",
    "description": "A test company for API validation",
    "email": "test@example.com",
    "phone": "+1234567890",
    "website": "https://testcompany.com",
    "address": "123 Test St, Test City"
  }')

echo "$COMPANY_RESPONSE" | jq .
echo ""

# Extract company ID
COMPANY_ID=$(echo "$COMPANY_RESPONSE" | jq -r '.id')
echo "Created company with ID: $COMPANY_ID"
echo ""

echo "4. Getting the created company..."
curl -s "$BASE_URL/companies/$COMPANY_ID" | jq .
echo ""

echo "5. Getting all companies..."
curl -s "$BASE_URL/companies" | jq .
echo ""

echo "6. Creating a test station..."
STATION_RESPONSE=$(curl -s -X POST "$BASE_URL/stations" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"Test Station\",
    \"description\": \"A test station for API validation\",
    \"address\": \"456 Station Ave, Test City\",
    \"latitude\": 40.7128,
    \"longitude\": -74.0060,
    \"company_id\": \"$COMPANY_ID\",
    \"phone\": \"+1234567891\",
    \"email\": \"station@testcompany.com\",
    \"access_type\": \"PUBLIC\"
  }")

echo "$STATION_RESPONSE" | jq .
echo ""

# Extract station ID
STATION_ID=$(echo "$STATION_RESPONSE" | jq -r '.id')
echo "Created station with ID: $STATION_ID"
echo ""

echo "7. Creating a test charger..."
CHARGER_RESPONSE=$(curl -s -X POST "$BASE_URL/chargers" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"Test Charger\",
    \"description\": \"A test charger for API validation\",
    \"station_id\": \"$STATION_ID\",
    \"charger_type\": \"FAST_DC\",
    \"power_output\": 50.0,
    \"voltage\": 400.0,
    \"current_type\": \"DC\",
    \"connector_types\": [\"CCS2\"],
    \"status\": \"AVAILABLE\",
    \"is_public\": true
  }")

echo "$CHARGER_RESPONSE" | jq .
echo ""

# Extract charger ID
CHARGER_ID=$(echo "$CHARGER_RESPONSE" | jq -r '.id')
echo "Created charger with ID: $CHARGER_ID"
echo ""

echo "8. Testing optimistic concurrency..."
# Get the company again to get the current version
COMPANY_V2=$(curl -s "$BASE_URL/companies/$COMPANY_ID")
VERSION=$(echo "$COMPANY_V2" | jq -r '.version')
echo "Current company version: $VERSION"

# Try to update with wrong version (should fail)
echo "Updating with wrong version (should fail)..."
curl -s -X PUT "$BASE_URL/companies/$COMPANY_ID" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"Test Company Updated\",
    \"version\": $((VERSION + 1))
  }" | jq .
echo ""

echo "9. Testing validation errors..."
echo "Creating company with invalid email (should fail)..."
curl -s -X POST "$BASE_URL/companies" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Invalid Email Company",
    "email": "invalid-email"
  }' | jq .
echo ""

echo "10. Testing soft delete..."
echo "Deleting company (soft delete)..."
curl -s -X DELETE "$BASE_URL/companies/$COMPANY_ID" | jq .
echo ""

echo "11. Trying to get deleted company (should fail)..."
curl -s "$BASE_URL/companies/$COMPANY_ID" | jq .
echo ""

echo "12. Restoring company..."
curl -s -X POST "$BASE_URL/companies/$COMPANY_ID/restore" | jq .
echo ""

echo "13. Verifying company is restored..."
curl -s "$BASE_URL/companies/$COMPANY_ID" | jq .
echo ""

echo "=== Test Complete ==="