.PHONY: ci help setup deploy migrate test clean

# CI Enforcement Pipeline
ci:
	./tools/ci_guard.sh

# Setup tasks
setup:
	cargo build --release

# Deploy services
deploy:
	./infrastructure/scripts/deploy.sh

# Run database migrations
migrate:
	./infrastructure/scripts/migrate.sh

# Provision databases
provision:
	./infrastructure/scripts/provision_db.sh

# Run all tests
test:
	cargo test --workspace --all-features

# Format code
fmt:
	cargo fmt --all

# Run linter
lint:
	cargo clippy --all-targets --all-features --workspace -- -D warnings

# Run SQLx offline verification
sqlx-check:
	cargo sqlx prepare --check --all -- --database-url "$DB_URL"

# Build all packages
build:
	cargo build --release --workspace

# Clean build artifacts
clean:
	cargo clean
	rm -rf .specify/ci-artifacts
	rm -rf target

# Run specific test
test-<service>:
	cargo test --package <service> --all-features

# Show help
help:
	@echo "BorneMap Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  ci            - Run 9-stage CI enforcement pipeline"
	@echo "  setup         - Build all packages"
	@echo "  deploy        - Deploy all services"
	@echo "  migrate       - Run database migrations"
	@echo "  provision     - Provision databases"
	@echo "  test          - Run all tests"
	@echo "  fmt           - Format code"
	@echo "  lint          - Run linter"
	@echo "  sqlx-check    - Run SQLx offline verification"
	@echo "  build         - Build all packages"
	@echo "  clean         - Clean build artifacts"
	@echo "  test-<service>- Run tests for specific service"
