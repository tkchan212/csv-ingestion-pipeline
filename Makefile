.PHONY: setup up down reset-db seed ingest generate-data test test-unit lint psql

COMPOSE := docker compose

setup:          ## One-shot setup: .env, local venv, database up
	./scripts/setup.sh

up:             ## Start PostgreSQL
	$(COMPOSE) up -d --wait db

down:           ## Stop everything (keeps data)
	$(COMPOSE) down

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

test:           ## Unit + integration tests (needs `make up`)
	.venv/bin/pytest

test-unit:      ## Unit tests only, no database needed
	.venv/bin/pytest -m "not integration"

lint:
	.venv/bin/ruff check src tests scripts

psql:
	$(COMPOSE) exec db psql -U $${POSTGRES_USER:-ingestor} -d $${POSTGRES_DB:-sensors}
