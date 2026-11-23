# MasterFabric Airflow MLOps. Run `make help` for the list of targets.
SHELL := /bin/bash
COMPOSE := docker compose --env-file .env -f docker/compose.yaml --profile monitoring
AF      := docker exec -w /opt/airflow/repo mf-mlops-airflow-scheduler
PY      ?= .venv/bin/python

.PHONY: help env up down ps logs test backfill status report demo monitoring candidates rollout rca drift-watch ops-assets

help: ## Show targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n",$$1,$$2}'

env: ## Create .env from .env.example (never overwrites)
	@test -f .env || cp .env.example .env

up: env ## Start PostgreSQL, S3, MLflow, Airflow, the API, Feast (Valkey, server, UI) and the Evidently UI
	$(COMPOSE) up -d --build
	@echo "Airflow http://127.0.0.1:38080  MLflow http://127.0.0.1:35000  API http://127.0.0.1:38000/docs  S3 console http://127.0.0.1:39001"
	@echo "Feast UI http://127.0.0.1:38888  Evidently UI http://127.0.0.1:38085"

down: ## Stop the stack (keeps volumes)
	$(COMPOSE) down

ps: ## Service status
	$(COMPOSE) ps -a

logs: ## Tail logs
	$(COMPOSE) logs -f --tail=50

test: ## Unit tests (generator, quality gate, drift, model, canary routing and guardrails) in the Airflow container
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

monitoring: ## Evidently and Feast tasks for months that ran before these tasks existed, then the summaries
	scripts/backfill_monitoring.sh
	docker exec mf-mlops-airflow-scheduler /home/airflow/mf-venv/bin/python -W ignore /opt/airflow/repo/scripts/evidently_summary.py
	docker exec mf-mlops-airflow-scheduler /home/airflow/mf-venv/bin/python -W ignore /opt/airflow/repo/scripts/feast_demo.py

candidates: ## Register rollout candidates: a normal one and one trained on shuffled labels
	$(AF) python -W ignore scripts/train_candidate.py good
	$(AF) python -W ignore scripts/train_candidate.py broken

rollout: ## Canary 5/25/50% with guardrails for the given versions, e.g. make rollout VERSIONS="6 5"
	$(AF) python -W ignore scripts/rollout.py $(VERSIONS)
	$(AF) python -W ignore scripts/byid_demo.py

rca: ## Root-cause report for a version against a champion, e.g. make rca VERSION=6 CHAMPION=5
	$(AF) python -W ignore scripts/rca.py $(VERSION) --champion $(CHAMPION)

drift-watch: ## Replay 2026-05/07/08 traffic on a stale champion with the drift watcher; restores the champion (about 3 minutes)
	$(AF) python -W ignore scripts/drift_experiment.py

ops-assets: ## Charts for the root-cause report and the drift watcher
	cd scripts && ../$(PY) make_ops_assets.py
