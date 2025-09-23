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
	
start-dashboard:
	docker-compose up -d dashboard

update-dashboard:
	docker-compose down
	docker-compose up --build
	docker-compose build --no-cache api
	docker-compose build dashboard
	docker-compose up -d dashboard
	
train-pregame: up
	python3 modeling/Python/pregame_modelfit.py
	
predict-pregame:
	python3 modeling/Python/pregame_predgen.py --run-id latest --all --to-db
	
train-pregame-total: up
	python3 modeling/Python/pregame_total_modelfit.py
	
predict-total:
	python3 modeling/Python/pregame_total_predgen.py --run-id latest --all --with-pi --to-db

predict-total-week:
	python3 modeling/Python/pregame_total_predgen.py --run-id latest --season $(SEASON) --week $(WEEK) --with-pi --to-db
	
train-pregame-margin: up
	python3 modeling/Python/pregame_margin_modelfit.py	
	
predict-margin:
	python3 modeling/Python/pregame_margin_predgen.py --run-id latest --all --to-db

predict-margin-week:
	python3 modeling/Python/pregame_margin_predgen.py --run-id latest --season $(SEASON) --week $(WEEK) --with-pi --to-db

fit-models:
	python3 modeling/Python/pregame_modelfit.py
	python3 modeling/Python/pregame_total_modelfit.py
	python3 modeling/Python/pregame_margin_modelfit.py	
	
run-preds:
	python3 modeling/Python/pregame_predgen.py --run-id latest --all --to-db
	python3 modeling/Python/pregame_total_predgen.py --run-id latest --all --with-pi --to-db
	python3 modeling/Python/pregame_margin_predgen.py --run-id latest --all --to-db
	
run-data-full:
	Rscript etl/R/process_scripts/full_db_upload.R

run-data-refresh:
	Rscript etl/R/process_scripts/db_refresh.R
	
	
