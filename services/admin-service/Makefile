.PHONY: all build test run clean sqlx-prepare fmt check lint help

# Default target
all: build

# Build the project
build:
	cargo build --release

# Build in debug mode
debug:
	cargo build

# Run tests
test:
	cargo test

# Run tests with coverage
test-coverage:
	cargo tarpaulin --out Html --output-dir target/coverage

# Run specific test
test-one:
	cargo test $(TARGET)

# Build and run the service
run: build
	./target/release/admin-service

# Build in debug mode and run
debug-run: debug
	RUST_LOG=debug cargo run

# Prepare SQLx database offline
sqlx-prepare:
	cargo sqlx prepare --check

# Check SQLx database preparation
sqlx-check:
	cargo sqlx prepare --check

# Check database connection
check-db:
	cargo run --bin check-db

# Format code
fmt:
	cargo fmt

# Check formatting
fmt-check:
	cargo fmt -- --check

# Lint code
lint:
	cargo clippy --all-targets --all-features

# Lint with specific profile
lint-release:
	cargo clippy --release --all-targets --all-features

# Run all quality checks
check: fmt-check lint test sqlx-check

# Run migration files
migrate:
	cargo sqlx migrate run

# Rollback migration
migrate-rollback:
	cargo sqlx migrate revert

# Generate migration
migrate-new:
	cargo sqlx migrate create $(NAME) --direction up

# Clean build artifacts
clean:
	cargo clean

# Clean with coverage
clean-coverage:
	cargo clean
	rm -rf target/coverage

# Update dependencies
update-deps:
	cargo update

# Check dependencies
deps-check:
	cargo outdated

# Benchmark
benchmark:
	cargo bench

# Show help
help:
	@echo "Available targets:"
	@echo "  all           - Build the project (default)"
	@echo "  build         - Build in release mode"
	@echo "  debug         - Build in debug mode"
	@echo "  test          - Run tests"
	@echo "  test-coverage - Run tests with coverage"
	@echo "  run           - Build and run the service"
	@echo "  debug-run     - Build in debug mode and run"
	@echo "  sqlx-prepare  - Prepare SQLx database offline"
	@echo "  sqlx-check    - Check SQLx database preparation"
	@echo "  fmt           - Format code"
	@echo "  fmt-check     - Check formatting"
	@echo "  lint          - Lint code"
	@echo "  lint-release  - Lint with release profile"
	@echo "  check         - Run all quality checks"
	@echo "  migrate       - Run migration files"
	@echo "  migrate-rollback - Rollback migration"
	@echo "  migrate-new   - Generate new migration"
	@echo "  clean         - Clean build artifacts"
	@echo "  clean-coverage - Clean build artifacts and coverage"
	@echo "  update-deps   - Update dependencies"
	@echo "  deps-check    - Check for outdated dependencies"
	@echo "  benchmark     - Run benchmarks"
	@echo "  help          - Show this help message"