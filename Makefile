.PHONY: up down build logs psql import-osm seed clean

up:
	docker compose -f source/infra/docker-compose.yml up -d

down:
	docker compose -f source/infra/docker-compose.yml down

build:
	docker compose -f source/infra/docker-compose.yml build

logs:
	docker compose -f source/infra/docker-compose.yml logs -f

psql:
	docker exec -it bornemap-postgres psql -U bornemap -d bornemap_platform

import-osm:
	source/scripts/import-tunisia-osm.sh

seed:
	docker exec -i bornemap-postgres psql -U bornemap -d bornemap_platform < source/scripts/seed-mvp1-data.sql

clean:
	docker compose -f source/infra/docker-compose.yml down -v
