# Common tasks. `make help` lists them.
COMPOSE := docker compose

.PHONY: help setup lint fmt test build up down logs ps run-day backfill dashboard

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

run-day backfill dashboard:  ## (built in later steps)
	@echo "'$@' is not implemented yet"; exit 1
