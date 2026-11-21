"""Call the scoring API with real subscribers from the latest snapshot and time it.

Writes docs/results/api_demo.json: which model version answered, per-request latency for single and
batch requests, and agreement with batch scoring of the same subscribers.
"""
from __future__ import annotations

import json
import statistics
import time
import urllib.request
from pathlib import Path

import numpy as np

from mlops import store

API = "http://api:8000"
MONTH = "2026-11"


def call(path: str, body=None):
    req = urllib.request.Request(API + path, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def main() -> None:
    snap = store.get(store.snapshot_key(MONTH)).sample(n=1000, random_state=5)
    batch_scores = store.get(store.scores_key(MONTH).replace("scores.parquet", "auto.parquet")).set_index("subscriber_id")
    recs = json.loads(snap.drop(columns=["month"]).to_json(orient="records"))
    info = call("/model")
    single = []
    for r in recs[:200]:
        t = time.perf_counter()
        call("/predict", [r])
        single.append((time.perf_counter() - t) * 1000)
    t = time.perf_counter()
    res = call("/predict", recs)
    batch_ms = (time.perf_counter() - t) * 1000
    api = {s["subscriber_id"]: s["churn_probability"] for s in res["scores"]}
    diff = np.abs(np.array([api[i] - batch_scores.loc[i, "churn_probability"] for i in api]))
    out = {"model": info, "answered_by_version": res["model_version"],
           "single_request_ms": {"p50": round(statistics.median(single), 2),
                                 "p95": round(float(np.percentile(single, 95)), 2), "n": len(single)},
           "batch_1000_ms": round(batch_ms, 1),
           "max_abs_diff_vs_batch_scoring": float(diff.max())}
    Path("docs/results/api_demo.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
