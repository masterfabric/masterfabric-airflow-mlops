"""Monthly churn model lifecycle.

    land ─┬─ dq_gate ─┬─ checks_passed ─ check_drift ─┐
          │           └─ halt_month                  ├─ decide ─┬─ train_and_promote ─┬─ score
          └─ realise_last_month ─────────────────────┘          └─ keep_champion ─────┘

One run per month (2025-12 .. 2026-11), backfilled with catchup and max_active_runs=1 so months are
processed strictly in order: each decision depends on the registry state the previous month left.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from mlops import pipeline

MONTH = "{{ logical_date.strftime('%Y-%m') }}"

with DAG(
    dag_id="churn_monthly",
    description="DQ gate, drift, champion/challenger retraining and batch scoring for the churn model",
    start_date=datetime(2025, 12, 1),
    end_date=datetime(2026, 11, 1),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["mlops", "churn"],
    doc_md=__doc__,
) as dag:
    land = PythonOperator(task_id="land", python_callable=pipeline.land, op_args=[MONTH])
    gate = BranchPythonOperator(
        task_id="dq_gate", op_args=[MONTH],
        python_callable=lambda m: "checks_passed" if pipeline.dq_gate(m) else "halt_month")
    passed = EmptyOperator(task_id="checks_passed")
    halt = PythonOperator(task_id="halt_month", python_callable=pipeline.halt, op_args=[MONTH])
    realise = PythonOperator(task_id="realise_last_month", python_callable=pipeline.realise, op_args=[MONTH])
    drift = PythonOperator(task_id="check_drift", python_callable=pipeline.check_drift, op_args=[MONTH])
    decide = BranchPythonOperator(
        task_id="decide", op_args=[MONTH],
        python_callable=lambda m: "train_and_promote" if pipeline.decide(m) == "retrain" else "keep_champion")
    train = PythonOperator(task_id="train_and_promote", python_callable=pipeline.train_and_promote, op_args=[MONTH])
    keep = EmptyOperator(task_id="keep_champion")
    score = PythonOperator(task_id="score", python_callable=pipeline.score, op_args=[MONTH],
                           trigger_rule="none_failed_min_one_success")

    land >> gate >> [passed, halt]
    land >> realise
    passed >> drift >> decide
    realise >> decide
    decide >> [train, keep] >> score
