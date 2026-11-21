"""Collect the backfill's measured outcome into docs/results/backfill.json.

Reads the monitoring tables, the MLflow registry, Airflow task durations, and computes the
"oracle" AUC of the generator's true churn probabilities per month as an upper bound.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg2
from mlflow import MlflowClient
from sklearn.metrics import roc_auc_score

from mlops import data, db


def airflow_durations() -> list[dict]:
    url = os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"].replace("postgresql+psycopg2", "postgresql")
    with psycopg2.connect(url) as c, c.cursor() as cur:
        cur.execute("""SELECT to_char(dr.execution_date, 'YYYY-MM') AS month, ti.task_id, ti.state,
                              round(ti.duration::numeric, 2)
                       FROM task_instance ti JOIN dag_run dr ON dr.run_id = ti.run_id AND dr.dag_id = ti.dag_id
                       WHERE ti.dag_id = 'churn_monthly' ORDER BY 1, ti.start_date""")
        return [{"month": m, "task": t, "state": s, "seconds": float(d) if d is not None else None}
                for m, t, s, d in cur.fetchall()]


def main() -> None:
    gen = data.generate(int(os.environ.get("MLOPS_SEED", "42")))
    oracle = {m: round(float(roc_auc_score(df[data.LABEL], df.true_churn_prob)), 4) for m, df in gen.items()}
    client = MlflowClient()
    versions = []
    for mv in sorted(client.search_model_versions("name='churn'"), key=lambda v: int(v.version)):
        versions.append({"version": mv.version, "aliases": mv.aliases, "tags": mv.tags, "run_id": mv.run_id})
    tasks = airflow_durations()
    run_seconds = {}
    for t in tasks:
        run_seconds[t["month"]] = round(run_seconds.get(t["month"], 0) + (t["seconds"] or 0), 2)
    out = {
        "months": data.MONTHS,
        "dq": db.query("SELECT month, passed, rules, critical_failures, warnings, rows FROM dq_results ORDER BY month"),
        "drift": db.query("SELECT month, champion_version, max_psi, max_feature, psi FROM drift ORDER BY month"),
        "decisions": db.query("SELECT month, action, reasons, champion_before, champion_after, challenger_version, details "
                              "FROM decisions ORDER BY month"),
        "realised": db.query("SELECT month, policy, model_version, auc, pr_auc, top_decile_lift, base_rate, n "
                             "FROM realised ORDER BY month, policy"),
        "oracle_auc": oracle,
        "registry": versions,
        "task_seconds": tasks,
        "task_seconds_per_month": run_seconds,
    }
    Path("docs/results/backfill.json").write_text(json.dumps(out, indent=1, default=str))
    print(json.dumps({k: out[k] for k in ("oracle_auc", "task_seconds_per_month")}, indent=1))


if __name__ == "__main__":
    main()
