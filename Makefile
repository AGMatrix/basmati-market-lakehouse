# Common tasks. `make help` lists them.
COMPOSE := docker compose

.PHONY: help setup lint fmt test build up down logs ps run-day backfill rebuild-silver dashboard

help:  ## Show this help
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

setup:  ## Install the local Python env (uv, all dependency groups)
	uv sync

lint:  ## Ruff lint + format check
	uv run ruff check .
	uv run ruff format --check .

fmt:  ## Auto-fix lint issues and format
	uv run ruff check --fix .
	uv run ruff format .

test:  ## Run unit tests
	uv run pytest

build:  ## Build the Docker images (MinIO from source, Airflow + Spark)
	$(COMPOSE) build

up:  ## Start the stack
	$(COMPOSE) up -d --build
	@echo "Airflow: http://localhost:8080   MinIO console: http://localhost:9001"

down:  ## Stop the stack (data volumes are kept)
	$(COMPOSE) down

logs:  ## Follow logs of all services
	$(COMPOSE) logs -f --tail=100

ps:  ## Service status
	$(COMPOSE) ps

AIRFLOW := $(COMPOSE) exec -T airflow-scheduler airflow

run-day:  ## Ingest one day now: make run-day [DAY=YYYY-MM-DD] (default: today + 7-day look-back)
	$(AIRFLOW) dags unpause agmarknet_daily >/dev/null
	$(AIRFLOW) dags unpause silver_daily_price >/dev/null
	$(AIRFLOW) dags trigger agmarknet_daily $(if $(DAY),--conf '{"day": "$(DAY)", "lookback_days": 0}')

backfill:  ## Backfill a date range, both ends included: make backfill FROM=2015-01-01 TO=2015-12-31
	@test -n "$(FROM)" -a -n "$(TO)" || { echo "usage: make backfill FROM=YYYY-MM-DD TO=YYYY-MM-DD"; exit 1; }
	$(AIRFLOW) dags unpause silver_daily_price >/dev/null
	$(AIRFLOW) backfill create --dag-id agmarknet_daily --from-date $(FROM) --to-date $(TO)T23:59:59 \
		--max-active-runs 2 --reprocess-behavior failed

rebuild-silver:  ## Rebuild silver from every raw partition (after a transform change)
	$(AIRFLOW) dags trigger silver_daily_price --conf '{"all": true}'

dashboard:  ## (built in a later step)
	@echo "'$@' is not implemented yet"; exit 1
