# MasterFabric Airflow MLOps. Run `make help` for the list of targets.
SHELL := /bin/bash
COMPOSE := docker compose --env-file .env -f docker/compose.yaml
AF      := docker exec -w /opt/airflow/repo mf-mlops-airflow-scheduler
PY      ?= .venv/bin/python

.PHONY: help env up down ps logs

help: ## Show targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n",$$1,$$2}'

env: ## Create .env from .env.example (never overwrites)
	@test -f .env || cp .env.example .env

up: env ## Start PostgreSQL, S3 storage, MLflow, Airflow and the scoring API
	$(COMPOSE) up -d --build
	@echo "Airflow http://127.0.0.1:38080  MLflow http://127.0.0.1:35000  API http://127.0.0.1:38000/docs  S3 console http://127.0.0.1:39001"

down: ## Stop the stack (keeps volumes)
	$(COMPOSE) down

ps: ## Service status
	$(COMPOSE) ps -a

logs: ## Tail logs
	$(COMPOSE) logs -f --tail=50
