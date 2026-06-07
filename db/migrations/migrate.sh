#!/bin/bash
# Migration Runner for BorneMap Database
# Applies all SQL migration files in numeric order
# Usage: DATABASE_URL="postgresql://user:pass@host:port/db" ./db/migrations/migrate.sh

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check DATABASE_URL
if [ -z "$DATABASE_URL" ]; then
    echo -e "${RED}Error: DATABASE_URL environment variable is not set${NC}"
    echo "Usage: DATABASE_URL=\"postgresql://user:pass@host:port/db\" ./db/migrations/migrate.sh"
    exit 1
fi

# Define migration and seed directories
MIGRATION_DIR="db/migrations"
SEED_DIR="db/seeds"

# Function to print success message
print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

# Function to print info message
print_info() {
    echo -e "${YELLOW}→${NC} $1"
}

# Function to check if directory exists, create if not
ensure_dir() {
    if [ ! -d "$1" ]; then
        print_info "Creating directory: $1"
        mkdir -p "$1"
    fi
}

# Check and create directories if needed
print_info "Checking directories..."
ensure_dir "$MIGRATION_DIR"
ensure_dir "$SEED_DIR"

# Find all migration files and sort them numerically
MIGRATION_FILES=$(ls -1 "$MIGRATION_DIR"/*.sql 2>/dev/null | sort -V || true)

# Check if any migration files exist
if [ -z "$MIGRATION_FILES" ]; then
    echo -e "${RED}No migration files found in $MIGRATION_DIR${NC}"
    exit 1
fi

print_success "Found migration files:"
echo "$MIGRATION_FILES" | while read -r file; do
    echo "  - $(basename "$file")"
done

# Apply each migration file
print_info "Applying migrations..."
COUNTER=0
TOTAL=$(echo "$MIGRATION_FILES" | wc -l)

echo "$MIGRATION_FILES" | while read -r file; do
    COUNTER=$((COUNTER + 1))
    BASENAME=$(basename "$file")
    echo -e "${YELLOW}[$COUNTER/$TOTAL] Applying $BASENAME...${NC}"

    # Apply migration using psql
    if psql "$DATABASE_URL" -f "$file"; then
        print_success "$BASENAME"
    else
        echo -e "${RED}Error: Failed to apply $BASENAME${NC}"
        exit 1
    fi
done

print_success "All migrations applied successfully!"

# Optionally apply seed data (uncomment to enable)
# print_info "Applying seed data..."
# SEED_FILES=$(ls -1 "$SEED_DIR"/*.sql 2>/dev/null | sort -V || true)
#
# if [ -n "$SEED_FILES" ]; then
#     echo "$SEED_FILES" | while read -r file; do
#         BASENAME=$(basename "$file")
#         print_info "Applying seed: $BASENAME"
#         psql "$DATABASE_URL" -f "$file"
#     done
#     print_success "Seed data applied"
# fi

echo ""
print_success "Database setup complete!"
