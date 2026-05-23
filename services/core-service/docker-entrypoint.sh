#!/bin/bash

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
until pg_isready -h postgres -p 5432 -U bornemap; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 1
done

echo "PostgreSQL is ready!"

# Run database migrations
echo "Running database migrations..."
psql -h postgres -p 5432 -U bornemap -d bornemap -f /app/migrations/001_initial_schema.sql

echo "Database migrations completed!"

# Start the application
echo "Starting core service..."
exec /usr/local/bin/core-service