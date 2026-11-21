"""Latency of /predict/by-id (Feast online fetch + champion) vs /predict with features in the body.

Run in the Airflow container (monitoring profile up):  python /opt/airflow/repo/scripts/byid_demo.py
"""
from __future__ import annotations

import json
import math
import sys
import time
import urllib.request
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mlops import data, store  # noqa: E402

API = "http://api:8000"
OUT = Path(__file__).resolve().parents[1] / "docs/results/api_byid.json"
INT = ["tenure_months", "complaints_90d", "support_calls_30d", "payment_delay_days", "contract_months_left",
       "device_age_months", "family_lines", "discount_pct", "device_5g"]


def post(path, body):
    req = urllib.request.Request(API + path, json.dumps(body).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def main() -> None:
    snap = store.get(store.snapshot_key("2026-11"))[["subscriber_id"] + data.FEATURES_NUM + data.FEATURES_CAT]
    snap = snap.sample(2000, random_state=5)
    recs = []
    for r in snap.to_dict("records"):
        recs.append({k: (None if isinstance(v, float) and math.isnan(v) else int(v) if k in INT else v) for k, v in r.items()})
    res, same = {}, []
    for n in (1, 100):
        for name in ("predict_body", "predict_by_id"):
            ts = []
            for i in range(220):
                chunk = [recs[(i * n + j) % len(recs)] for j in range(n)]
                t0 = time.perf_counter()
                out = post("/predict", chunk) if name == "predict_body" else \
                    post("/predict/by-id", {"subscriber_ids": [c["subscriber_id"] for c in chunk]})
                ts.append((time.perf_counter() - t0) * 1000)
                if name == "predict_by_id" and n == 100 and i < 20:
                    same.append(out)
            a = np.array(ts[20:])
            res[f"{name}_{n}"] = {"p50_ms": round(float(np.percentile(a, 50)), 2),
                                  "p95_ms": round(float(np.percentile(a, 95)), 2), "n": len(a)}
    # by-id scores must equal body scores for the same subscribers
    diffs = []
    for i, out in enumerate(same):
        chunk = [recs[(i * 100 + j) % len(recs)] for j in range(100)]
        ref = {s["subscriber_id"]: s["churn_probability"] for s in post("/predict", chunk)["scores"]}
        diffs += [abs(s["churn_probability"] - ref[s["subscriber_id"]]) for s in out["scores"]]
    res["max_abs_score_diff_byid_vs_body"] = max(diffs)
    res["compared_scores"] = len(diffs)
    res["model_version"] = out["model_version"]
    OUT.write_text(json.dumps(res, indent=2) + "\n")
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
