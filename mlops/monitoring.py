"""Evidently monitoring for the monthly run: data drift against the champion's training sample and the
realised quality of last month's scores, as HTML reports and as snapshots in an Evidently workspace.

Runs in the Feast/Evidently virtualenv (NumPy 2), not in Airflow's own environment:

    /home/airflow/mf-venv/bin/python -m mlops.monitoring --month 2026-07

It only observes. The retrain decision stays with the in-house PSI rule (ADR 0004), and this module
records where Evidently agrees or disagrees with it.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import time
import warnings

warnings.filterwarnings("ignore")
os.environ.setdefault("DO_NOT_TRACK", "1")
os.environ.setdefault("EVIDENTLY_DISABLE_TELEMETRY", "1")

import pandas as pd  # noqa: E402
from evidently import BinaryClassification, DataDefinition, Dataset, Report  # noqa: E402
from evidently.presets import ClassificationPreset, DataDriftPreset  # noqa: E402

from mlops import data, db, drift, store  # noqa: E402

WORKSPACE = os.environ.get("EVIDENTLY_WORKSPACE", "/opt/evidently-ws")
PROJECT = "churn-monitoring"
DDL = """
CREATE TABLE IF NOT EXISTS evidently_drift (month TEXT PRIMARY KEY, champion_version TEXT, columns INT,
    drifted_columns INT, drifted_share DOUBLE PRECISION, per_column JSONB, psi_method JSONB,
    in_house_psi JSONB, seconds DOUBLE PRECISION, report_key TEXT, computed_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS evidently_quality (month TEXT PRIMARY KEY, model_version TEXT, roc_auc DOUBLE PRECISION,
    in_house_auc DOUBLE PRECISION, metrics JSONB, report_key TEXT, computed_at TIMESTAMPTZ DEFAULT now());
"""


def _definition() -> DataDefinition:
    return DataDefinition(numerical_columns=list(data.FEATURES_NUM), categorical_columns=list(data.FEATURES_CAT))


def _project(ws):
    for p in ws.list_projects():
        if p.name == PROJECT:
            return p
    p = ws.create_project(PROJECT)
    p.description = "Monthly feature drift vs the champion's training sample, and realised quality of last month's scores"
    p.save()
    return p


def _value_drift(snapshot_dict: dict) -> tuple[dict, dict]:
    """Split a DataDriftPreset result into the overall count and one entry per column."""
    overall, cols = {}, {}
    for m in snapshot_dict["metrics"]:
        cfg = m["config"]
        if cfg["type"].endswith("DriftedColumnsCount"):
            overall = m["value"]
        elif cfg["type"].endswith("ValueDrift"):
            cols[cfg["column"]] = {"method": cfg["method"], "threshold": cfg["threshold"],
                                   "score": round(float(m["value"]), 4)}
    return overall, cols


def _drifted(c: dict) -> bool:
    # Distances (Wasserstein, Jensen-Shannon, PSI) drift above the threshold; p-value tests drift below it.
    return c["score"] < c["threshold"] if "p_value" in c["method"].lower() else c["score"] >= c["threshold"]


def _put_html(snapshot, key: str) -> str:
    path = f"/tmp/{key.replace('/', '_')}"
    snapshot.save_html(path)
    with open(path, "rb") as f:
        store._s3().put_object(Bucket=store.BUCKET, Key=key, Body=f.read(), ContentType="text/html")
    return key


def drift_report(month: str, ws=None) -> dict | None:
    row = db.query("SELECT champion_version FROM drift WHERE month=%s", (month,))
    if not row:
        return None                                   # no champion yet: nothing to compare against
    v = row[0]["champion_version"]
    X = data.FEATURES_NUM + data.FEATURES_CAT
    ref = store.get(f"reference/churn/v{v}.parquet")[X]
    cur = store.get(store.snapshot_key(month))[X]
    dd = _definition()
    t0 = time.perf_counter()
    snap = Report([DataDriftPreset()]).run(Dataset.from_pandas(cur, data_definition=dd),
                                           Dataset.from_pandas(ref, data_definition=dd))
    seconds = time.perf_counter() - t0
    overall, cols = _value_drift(snap.dict())
    for c in cols.values():
        c["drifted"] = _drifted(c)
    # Same data with Evidently's PSI implementation, to cross-check the in-house PSI the DAG acts on.
    psi_snap = Report([DataDriftPreset(method="psi", threshold=0.25)]).run(
        Dataset.from_pandas(cur, data_definition=dd), Dataset.from_pandas(ref, data_definition=dd))
    _, psi_cols = _value_drift(psi_snap.dict())
    key = _put_html(snap, f"reports/evidently/drift/month={month}.html")
    if ws is not None:
        ws.add_run(_project(ws).id, snap, include_data=False, name=f"drift {month} vs v{v}")
    out = {"month": month, "champion_version": v, "columns": len(cols),
           "drifted_columns": sum(c["drifted"] for c in cols.values()),
           "drifted_share": round(sum(c["drifted"] for c in cols.values()) / len(cols), 4),
           "per_column": cols, "psi_method": {k: c["score"] for k, c in psi_cols.items()},
           "in_house_psi": drift.psi_report(ref, cur), "seconds": round(seconds, 2), "report_key": key}
    db.upsert("evidently_drift", out, ("month",))
    return out


def quality_report(month: str, ws=None) -> dict | None:
    """Realised quality of the auto policy's scores for `month`, once its labels have arrived."""
    key = store.scores_key(month).replace("scores.parquet", "auto.parquet")
    if not (store.exists(key) and store.exists(store.labels_key(month))):
        return None
    s = store.get(key).merge(store.get(store.labels_key(month)), on="subscriber_id")
    frame = pd.DataFrame({"target": s[data.LABEL].astype(int), "prediction": s.churn_probability})
    dd = DataDefinition(classification=[BinaryClassification(target="target", prediction_probas="prediction")])
    snap = Report([ClassificationPreset()]).run(Dataset.from_pandas(frame, data_definition=dd), None)
    metrics = {}
    for m in snap.dict()["metrics"]:
        name = m["config"]["type"].split(":")[-1]
        if isinstance(m["value"], (int, float)):
            metrics[name] = round(float(m["value"]), 4)
    rkey = _put_html(snap, f"reports/evidently/quality/month={month}.html")
    if ws is not None:
        ws.add_run(_project(ws).id, snap, include_data=False, name=f"quality {month} v{s.model_version.iloc[0]}")
    ih = db.query("SELECT auc FROM realised WHERE month=%s AND policy='auto'", (month,))
    out = {"month": month, "model_version": str(s.model_version.iloc[0]), "roc_auc": metrics.get("RocAuc"),
           "in_house_auc": ih[0]["auc"] if ih else None, "metrics": metrics, "report_key": rkey}
    db.upsert("evidently_quality", out, ("month",))
    return out


def run(month: str) -> dict:
    with db.conn() as c, c.cursor() as cur:
        cur.execute(DDL)
    try:
        from evidently.ui.workspace import Workspace
        ws = Workspace.create(WORKSPACE)
    except Exception:                                   # noqa: BLE001  reports still go to S3 and the DB
        ws = None
    prev = str(pd.Period(month, "M") - 1)
    return {"drift": drift_report(month, ws), "quality_of_last_month": quality_report(prev, ws)}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--month", required=True)
    r = run(p.parse_args().month)
    d, q = r["drift"], r["quality_of_last_month"]
    print(json.dumps({"drift": d and {k: d[k] for k in ("month", "champion_version", "drifted_columns", "columns", "seconds")},
                      "quality": q and {k: q[k] for k in ("month", "model_version", "roc_auc", "in_house_auc")}}))


if __name__ == "__main__":
    main()
