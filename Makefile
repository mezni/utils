.PHONY: up down logs test lint openapi

# Start the full local development stack
up:
	docker compose up -d

# Stop and clean up the local stack
down:
	docker compose down

# View logs from all services
logs:
	docker compose logs -f

# Run all tests
test:
	docker compose run --rm --no-deps test

# Run linters across all services
lint:
	docker compose run --rm --no-deps lint

# Bundle and validate OpenAPI specifications
openapi:
	docker compose run --rm --no-deps openapi
