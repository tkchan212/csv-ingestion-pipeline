.PHONY: setup up down clean reset-db seed ingest generate-data verify test test-unit lint psql

COMPOSE := docker compose

setup:          ## One-shot setup: .env, local venv, database up
	./scripts/setup.sh

up:             ## Start PostgreSQL
	$(COMPOSE) up -d --wait db

down:           ## Stop everything (keeps data)
	$(COMPOSE) down

clean:          ## Remove containers, database volume, built image, venv and mock data (keeps .env)
	$(COMPOSE) down -v --rmi local --remove-orphans
	rm -rf .venv src/*.egg-info data/input/*.csv data/input/manifest.json

reset-db:       ## Drop the database volume, re-run migrations, reload reference data
	$(COMPOSE) down -v
	$(COMPOSE) up -d --wait db
	$(MAKE) seed

seed:           ## Load sensor types, buildings and sensors from config/reference
	$(COMPOSE) build -q ingestor
	$(COMPOSE) run --rm --entrypoint python ingestor -m ingestor.registry /app/config/reference

ingest:         ## Build and run the ingestor against ./data/input
	$(COMPOSE) build -q ingestor
	$(COMPOSE) run --rm ingestor

generate-data:  ## Write mock CSV files into ./data/input
	.venv/bin/python scripts/generate_data.py --out data/input

verify:         ## Compare the database with data/input/manifest.json after a load
	.venv/bin/python scripts/verify.py

test:           ## Unit + integration tests (needs `make up`)
	.venv/bin/pytest

test-unit:      ## Unit tests only, no database needed
	.venv/bin/pytest -m "not integration"

lint:
	.venv/bin/ruff check src tests scripts

psql:
	$(COMPOSE) exec db psql -U $${POSTGRES_USER:-ingestor} -d $${POSTGRES_DB:-sensors}
