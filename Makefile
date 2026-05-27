.PHONY: up down status test-backend dev-api

up:
	docker compose up -d
	@echo "PostGIS running on port 5432"

down:
	docker compose down

status:
	docker compose ps

test-backend:
	cd backend && cargo test --workspace

dev-api:
	cd backend && cargo run -p api-service
