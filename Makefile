.PHONY: ci help setup deploy migrate test ci_gate_keycloak ci_gate_rbac ci_gate_session ci_gate_identity ci_gate_preferences ci_gate_offline ci_gate_search ci_gate_ui ci_gate_performance integration-test clean

# CI Enforcement Pipeline
ci:
	./tools/ci_guard.sh

# Individual CI gates
ci_gate_identity:
	./tools/ci_gate_identity.sh

ci_gate_keycloak:
	./tools/ci_gate_keycloak.sh

ci_gate_rbac:
	./tools/ci_gate_rbac.sh

ci_gate_session:
	./tools/ci_gate_session.sh

ci_gate_preferences:
	bash .specify/ci-gates/023-preferences-isolation.sh

ci_gate_offline:
	bash .specify/ci-gates/024-offline-storage.sh

ci_gate_search:
	bash .specify/ci-gates/025-search-safety.sh

ci_gate_ui:
	bash .specify/ci-gates/026-ui-boundary.sh

ci_gate_performance:
	bash .specify/ci-gates/027-performance-regression.sh

# Integration Tests
integration-test:
	cargo test --test integration_auth_flow_test
	cargo test --test integration_audit_flow_test

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
	@echo "  integration-test - Run integration tests"
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
