# Makefile for NFL Analytics DB

up:
	docker-compose up -d
	sleep 5
	
install-deps:
	Rscript -e 'renv::restore()'

load-db:
	Rscript etl/R/step3_sql/step3_parquet_to_postgres.R

reset-db: down up load-db

down:
	docker-compose down

logs:
	docker-compose logs -f

ps:
	docker-compose ps
	
api-build:
	docker-compose build api

api-up:
	docker-compose up -d db api

api-rebuild:
	docker-compose build --no-cache api
	docker-compose up -d api

api-down:
	docker-compose stop api

api-logs:
	docker-compose logs -f api

api-shell:
	docker-compose exec api sh || docker-compose exec api bash

api-health:
	curl -sS http://localhost:8000/health

api-ping:
	curl -sS http://localhost:8000/api/ping
