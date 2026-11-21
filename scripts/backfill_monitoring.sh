#!/usr/bin/env bash
# Run the two tasks added after the original backfill (evidently_report, publish_features) for the 12
# months that already have DAG runs, with `airflow tasks test` (same code and operator, no new run state).
# A fresh `make backfill` on empty volumes runs them inside the DAG instead.
set -euo pipefail
for d in ${MONTHS:-2025-12 2026-01 2026-02 2026-03 2026-04 2026-05 2026-06 2026-07 2026-08 2026-09 2026-10 2026-11}; do
  docker exec mf-mlops-airflow-scheduler airflow tasks test churn_monthly evidently_report "${d}-01" >/tmp/mf_ev.log 2>&1 \
    && echo "$d evidently_report ok" || { echo "$d evidently_report FAILED"; tail -20 /tmp/mf_ev.log; exit 1; }
  if [ "$d" = "2026-03" ]; then echo "$d publish_features skipped (month halted by the quality gate)"; continue; fi
  docker exec mf-mlops-airflow-scheduler airflow tasks test churn_monthly publish_features "${d}-01" >/tmp/mf_pf.log 2>&1 \
    && echo "$d publish_features ok" || { echo "$d publish_features FAILED"; tail -20 /tmp/mf_pf.log; exit 1; }
done
