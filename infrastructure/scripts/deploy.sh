#!/bin/bash
set -e

# Service Deployment Script for BorneMap
# Builds and deploys all three microservices

echo "=== BorneMap Service Deployment ==="
echo ""

# Check if Rust is available
if ! command -v cargo &> /dev/null; then
  echo "ERROR: Cargo is not installed"
  echo "Please install Rust: https://www.rust-lang.org/tools/install"
  exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
  echo "ERROR: docker-compose is not installed"
  exit 1
fi

# Navigate to infrastructure directory
cd infrastructure

# Start database containers
echo "Starting database containers..."
docker-compose -f docker-compose/local.yml up -d
sleep 5

# Build all services
echo "Building all services..."
cd ..
cargo build --release

echo ""
echo "=== Service Deployment Complete ==="
echo ""
echo "Services are ready to start:"
echo "  auth-service: http://localhost:3000"
echo "  driver-service: http://localhost:3001"
echo "  admin-service: http://localhost:3002"
echo ""
echo "To start services manually:"
echo "  cargo run --bin auth-service"
echo "  cargo run --bin driver-service"
echo "  cargo run --bin admin-service"
echo ""
echo "To verify health endpoints:"
echo "  curl http://localhost:3000/health"
echo "  curl http://localhost:3001/health"
echo "  curl http://localhost:3002/health"
