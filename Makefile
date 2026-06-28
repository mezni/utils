.PHONY: dev build test clean migrate seed lint check-all

dev:
	cargo watch -x run

build:
	cargo build --workspace

test:
	cargo test --workspace

clean:
	cargo clean

migrate:
	./scripts/migrate.sh

seed:
	./scripts/seed.sh

lint:
	cargo clippy --workspace -- -D warnings

check-all: build test lint
	@echo "All checks passed."
