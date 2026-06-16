DOCKER_COMPOSE = docker compose -f infra/docker-compose.yml
PROFILES ?= infra
SERVICE ?=

.PHONY: up up-all down build rebuild logs ps stop restart clean \
        db-reset db-shell setup shell env-check

up:
	$(DOCKER_COMPOSE) --profile $(PROFILES) up -d

up-all:
	$(DOCKER_COMPOSE) --profile "infra" --profile "services" up -d

down:
	$(DOCKER_COMPOSE) down

build:
	$(DOCKER_COMPOSE) --profile services build $(SERVICE)

rebuild: down build up-all

logs:
	$(DOCKER_COMPOSE) logs -f $(SERVICE)

ps:
	$(DOCKER_COMPOSE) ps

stop:
	$(DOCKER_COMPOSE) stop $(SERVICE)

restart: stop up

clean:
	@echo "WARNING: This will remove all containers and volumes."
	@read -p "Are you sure? [y/N] " confirm; \
	if [ "$$confirm" = "y" ]; then \
		$(DOCKER_COMPOSE) down -v; \
		echo "Containers and volumes removed."; \
	fi

db-reset:
	@echo "Dropping and recreating platform_db..."
	$(DOCKER_COMPOSE) exec platform_db psql -U bornemap -d platform_db -c "\
		DROP SCHEMA IF EXISTS inventory CASCADE; \
		DROP SCHEMA IF EXISTS gis CASCADE; \
		DROP SCHEMA IF EXISTS users CASCADE; \
		DROP EXTENSION IF EXISTS postgis CASCADE;"
	$(DOCKER_COMPOSE) exec platform_db psql -U bornemap -d platform_db -f /docker-entrypoint-initdb.d/init.sql

db-shell:
	@read -p "Database (platform_db/keycloak_db/analytics_db): " db; \
	service=platform_db; \
	port=5432; \
	case $$db in \
		keycloak_db) service=keycloak_db; port=5433;; \
		analytics_db) service=analytics_db; port=5434;; \
	esac; \
	$(DOCKER_COMPOSE) exec $$service psql -U bornemap -d $$db

shell:
	@read -p "Service name: " s; \
	$(DOCKER_COMPOSE) exec $$s sh -c "exec \$$(command -v bash || command -v sh)"

setup:
	@echo "Setting up BorneMap development environment..."
	@test -f .env || cp .env.example .env
	@mkdir -p infra/db
	@echo "Done. Run 'make up' to start."

env-check:
	@echo "Checking .env..."
	@if [ ! -f .env ]; then echo "ERROR: .env not found. Run 'make setup' first."; exit 1; fi
	@echo ".env found."
	@echo "Checking required variables..."
	@while IFS='=' read -r key val || [ -n "$$key" ]; do \
		case "$$key" in \
			''|'#'*|'export '*) continue;; \
			*) \
				eval "expanded=$$val"; \
				if echo "$$expanded" | grep -q '\$${'; then \
					echo "WARNING: $$key contains unexpanded variable reference"; \
				fi;; \
		esac; \
	done < .env
	@echo "Done."
