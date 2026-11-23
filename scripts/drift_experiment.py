"""Replay three months of traffic through the API with the drift watcher on, starting from a stale champion.

Scenario: v1 (trained on 2025-12..2026-02) is put back as champion, and the API serves the subscribers of
2026-05 (its own era), then 2026-07 (the 5G tariff has spread, labels only up to 2026-06), then 2026-08
(labels up to 2026-07). After every 2,500 subscribers the `drift_watch` DAG runs once (`airflow dags test`)
with `as_of` set to the month being served. At the end the champion alias is restored.

    python /opt/airflow/repo/scripts/drift_experiment.py     # in the Airflow container; about 10 minutes
Results -> docs/results/drift_watch.json
"""
from __future__ import annotations

import json
import math
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

import mlflow
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
from mlops import data, db, model, pipeline, store  # noqa: E402
from rollout import INT, get, post, wait_for  # noqa: E402

PHASES = [("2026-05", 4), ("2026-07", 6), ("2026-08", 6)]     # (month served = as_of, chunks)
CHUNK, BATCH, WORKERS = 2500, 50, 4
STALE = "1"
X = data.FEATURES_NUM + data.FEATURES_CAT


def subscribers(month: str) -> list[dict]:
    df = store.get(store.snapshot_key(month))[["subscriber_id"] + X].sample(frac=1, random_state=5)
    out = []
    for r in df.to_dict("records"):
        for k, v in r.items():
            if isinstance(v, float) and math.isnan(v):
                r[k] = None
            elif k in INT:
                r[k] = int(v)
        out.append(r)
    return out


def send(recs: list[dict]) -> None:
    with ThreadPoolExecutor(WORKERS) as ex:
        list(ex.map(lambda i: post("/predict", recs[i:i + BATCH]), range(0, len(recs), BATCH)))
    while get("/model")["log"]["queued"]:
        time.sleep(0.2)
    time.sleep(1.5)                                   # let the feature writer flush


def run_dag(as_of: str, n: int) -> tuple[dict, float]:
    t0 = time.perf_counter()
    date = (datetime(2026, 11, 23) + timedelta(minutes=10 * n)).isoformat()
    p = subprocess.run(["airflow", "dags", "test", "drift_watch", date, "-c", json.dumps({"as_of": as_of})],
                       capture_output=True, text=True)
    if p.returncode:
        raise RuntimeError(p.stderr[-2000:])
    row = db.query("SELECT id, champion, rows, max_psi, feature, breach, action, newest_label, challenger, details "
                   "FROM drift_watch ORDER BY id DESC LIMIT 1")[0]
    return row, round(time.perf_counter() - t0, 1)


def main() -> None:
    c = pipeline._client()
    original = c.get_model_version_by_alias(pipeline.MODEL, "champion").version
    with db.conn() as con, con.cursor() as cur:            # a clean watcher history for this experiment
        cur.execute("DROP TABLE IF EXISTS drift_watch")
        cur.execute("CREATE TABLE IF NOT EXISTS request_features (id BIGSERIAL PRIMARY KEY, ts TIMESTAMPTZ NOT NULL, "
                    "subscriber_id TEXT, model_version TEXT, features JSONB)")
        cur.execute("DELETE FROM request_features")
    c.set_registered_model_alias(pipeline.MODEL, "champion", STALE)
    wait_for(lambda m: m["version"] == STALE)
    out = {"stale_champion": STALE, "restored_champion": original, "chunk": CHUNK, "phases": PHASES,
           "window": 2000, "min_rows": 1000, "consecutive": 2, "checks": []}
    n, sent, t_drift = 0, 0, None
    try:
        for month, chunks in PHASES:
            recs = subscribers(month)
            for k in range(chunks):
                send(recs[k * CHUNK:(k + 1) * CHUNK])
                sent += CHUNK
                if month != "2026-05" and t_drift is None:
                    t_drift = True                 # first drifted traffic is being served
                    out["drift_onset_sent"] = sent - CHUNK
                    t_onset = time.time()
                row, secs = run_dag(month, n)
                n += 1
                rec = {"check": n, "month": month, "sent": sent, "dag_seconds": secs, "since_onset_s":
                       round(time.time() - t_onset, 1) if t_drift else None} | {k: row[k] for k in row if k != "id"}
                if row["action"] == "retrain_promoted":
                    swap, _ = wait_for(lambda m: m["version"] == row["challenger"], timeout=90)
                    rec["api_swap_s"] = swap
                    rec["serving_since_onset_s"] = round(time.time() - t_onset, 1)
                out["checks"].append(rec)
                print(json.dumps(rec, default=str), flush=True)
        promoted = [r for r in out["checks"] if r["action"] == "retrain_promoted"]
        if promoted:                                       # what the retrain bought on the month it was served in
            new = promoted[-1]["challenger"]
            df = pipeline._frame(["2026-08"])
            out["auc_2026_08"] = {}
            for v in (STALE, new):                    # the restored champion trained on 2026-08 itself
                m = mlflow.sklearn.load_model(f"models:/{pipeline.MODEL}/{v}")
                out["auc_2026_08"][f"v{v}"] = model.evaluate(m.predict_proba(df[X])[:, 1], df[data.LABEL].to_numpy())["auc"]
    finally:
        c.set_registered_model_alias(pipeline.MODEL, "champion", original)
        wait_for(lambda m: m["version"] == original, timeout=90)
        for r in out["checks"]:
            if r.get("challenger"):
                c.set_model_version_tag(pipeline.MODEL, r["challenger"], "role", "drift_watch_experiment")
    (ROOT / "docs/results/drift_watch.json").write_text(json.dumps(out, indent=2, default=str) + "\n")


if __name__ == "__main__":
    main()
