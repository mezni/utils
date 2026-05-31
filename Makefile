.PHONY: up down restart logs migrate test lint fmt clean

COMPOSE_FILE = infrastructure/compose/docker-compose.yml

up:
	docker compose -f $(COMPOSE_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE) down

restart:
	docker compose -f $(COMPOSE_FILE) down && docker compose -f $(COMPOSE_FILE) up -d

logs:
	docker compose -f $(COMPOSE_FILE) logs -f

migrate:
	cargo run --manifest-path services/driver-service/Cargo.toml --bin migrate

test:
	cargo test --workspace

lint:
	cargo clippy --workspace -- -D warnings
	cd apps/driver-web && npm run lint
	cd apps/admin-dashboard && npm run lint

fmt:
	cargo fmt --all --check
	prettier --check "**/*.{ts,tsx,js,jsx,json}"

clean:
	docker compose -f $(COMPOSE_FILE) down -v

deploy:
	@echo "Deploy via GitHub Actions — see .github/workflows/deploy.yml"
