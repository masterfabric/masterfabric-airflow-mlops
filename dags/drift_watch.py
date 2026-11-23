"""Drift watcher: PSI on the features the API actually served, every 10 minutes; retrain on sustained drift
when new labels exist (mlops/drift_watch.py, ADR 0010).

    check ─┬─ retrain        (sustained breach and a labelled month the last attempt did not have)
           └─ no_action      (ok, insufficient rows, pending, or suppressed: no new labels)

`as_of` is the business month whose labels count as available; it defaults to the newest landed month and
can be set per run (`-c '{"as_of": "2026-08"}'`), which the replay experiment does.
Created paused: unpause it to run on the schedule.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from mlops import drift_watch


def _check(params, ti):
    row = drift_watch.check(params["as_of"])
    ti.xcom_push("check_id", row["id"])
    return "retrain" if row["action"] == "retrain" else "no_action"


def _retrain(params, ti):
    return drift_watch.retrain(params["as_of"], ti.xcom_pull(task_ids="check", key="check_id"))


with DAG(
    dag_id="drift_watch",
    description="PSI on served traffic; drift-triggered retraining with a new-labels guard",
    start_date=datetime(2026, 11, 1),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    params={"as_of": Param("2026-11", type="string")},
    tags=["mlops", "churn", "drift"],
    doc_md=__doc__,
) as dag:
    check = BranchPythonOperator(task_id="check", python_callable=_check)
    retrain = PythonOperator(task_id="retrain", python_callable=_retrain)
    check >> [retrain, EmptyOperator(task_id="no_action")]
