# MasterFabric Airflow MLOps. Run `make help` for the list of targets.
SHELL := /bin/bash
COMPOSE := docker compose --env-file .env -f docker/compose.yaml
AF      := docker exec -w /opt/airflow/repo mf-mlops-airflow-scheduler
PY      ?= .venv/bin/python

.PHONY: help env up down ps logs test backfill status report demo

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

test: ## Unit tests (data generator, quality gate, drift, model) inside the Airflow container
	$(AF) python -m pytest -q -p no:cacheprovider tests

backfill: ## Unpause churn_monthly; catchup runs the 12 months in order (about 10 minutes)
	docker exec mf-mlops-airflow-scheduler airflow dags unpause churn_monthly

status: ## Run states of churn_monthly
	docker exec mf-mlops-airflow-scheduler airflow dags list-runs -d churn_monthly -o table

report: ## Collect decisions, realised AUC, registry and task durations into docs/results/backfill.json
	$(AF) python scripts/report.py

demo: ## Time the scoring API and measure a champion rollback/restore
	$(AF) python scripts/api_demo.py
	$(AF) python scripts/hotswap_demo.py
