"""Measure the Feast integration -> docs/results/feast.json.

1. Point-in-time training frame from Feast vs the pipeline's direct snapshot join (must be identical).
2. What a naive "latest features" join would have done to the same labels (wrong-time / missing rows).
3. The halted month 2026-03: Feast falls back to the last gated snapshot inside the 31-day TTL.
4. Online store: values for 1,000 sampled subscribers equal the offline 2026-11 snapshot;
   latency of the HTTP feature server and of the SDK, for 1 and 100 entities.

Run inside the Airflow container (monitoring profile up):
    docker exec mf-mlops-airflow-scheduler /home/airflow/mf-venv/bin/python -W ignore /opt/airflow/repo/scripts/feast_demo.py
"""
from __future__ import annotations

import json
import warnings
import sys
import time
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mlops import data, features, store  # noqa: E402

OUT = Path(__file__).resolve().parents[1] / "docs/results/feast.json"
URL = "http://feast-server:6566/get-online-features"
F = features.FEATURES
TRAIN = ["2026-07", "2026-08", "2026-09"]
ONLINE_MONTH = "2026-11"
rng = np.random.default_rng(7)


def direct(months):
    parts = []
    for m in months:
        f = store.get(store.snapshot_key(m))
        lab = store.get(store.labels_key(m))
        p = f.merge(lab[["subscriber_id", data.LABEL]], on="subscriber_id")
        p["label_month"] = m
        parts.append(p)
    return pd.concat(parts, ignore_index=True)


def same(a: pd.Series, b: pd.Series) -> np.ndarray:
    if a.dtype.kind in "fi" and b.dtype.kind in "fi":
        return np.isclose(a.to_numpy(float), b.to_numpy(float), equal_nan=True)
    return (a.astype(str) == b.astype(str)).to_numpy()


def compare(left, right, key=("subscriber_id", "label_month")):
    m = left.merge(right, on=list(key), suffixes=("_l", "_r"), how="left", indicator=True)
    missing = int((m["_merge"] == "left_only").sum())
    diff = np.zeros(len(m), bool)
    for f in F:
        diff |= ~same(m[f + "_l"], m[f + "_r"])
    return {"rows": len(m), "rows_missing": missing, "rows_with_any_diff": int(diff.sum()),
            "share_with_any_diff": round(float(diff.mean()), 4)}


def post(ids):
    body = json.dumps({"feature_service": "churn_model", "entities": {"subscriber_id": list(ids)}}).encode()
    req = urllib.request.Request(URL, body, {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def pct(xs):
    a = np.array(xs) * 1000
    return {"p50_ms": round(float(np.percentile(a, 50)), 2), "p95_ms": round(float(np.percentile(a, 95)), 2),
            "n": len(xs)}


def main() -> None:
    out = {}
    # 1. point-in-time frame vs direct join
    t0 = time.perf_counter(); pit = features.training_frame(TRAIN); t_pit = time.perf_counter() - t0
    t0 = time.perf_counter(); dj = direct(TRAIN); t_dj = time.perf_counter() - t0
    out["point_in_time_vs_direct"] = {"months": TRAIN, "feast_seconds": round(t_pit, 2),
                                      "direct_seconds": round(t_dj, 2),
                                      **compare(dj[["subscriber_id", "label_month"] + F],
                                                pit[["subscriber_id", "label_month"] + F])}
    # 2. naive join: same labels, features from the latest snapshot
    latest = store.get(store.snapshot_key(ONLINE_MONTH))[["subscriber_id"] + F]
    naive = dj[["subscriber_id", "label_month"]].merge(latest, on="subscriber_id", how="left")
    r = compare(dj[["subscriber_id", "label_month"] + F], naive)
    churned = dj[dj[data.LABEL] == 1].subscriber_id
    r["churners_missing_in_latest"] = int((~churned.isin(latest.subscriber_id)).sum())
    r["churners"] = int(len(churned))
    r["rows_all_features_null"] = int(naive[F].isna().all(axis=1).sum())
    j = dj.merge(naive, on=["subscriber_id", "label_month"], suffixes=("", "_n"))
    r["share_diff_by_feature"] = {f: round(float((~same(j[f], j[f + "_n"])).mean()), 4) for f in F}
    out["naive_latest_join"] = r
    # 3. halted month
    halted = features.training_frame(["2026-03"])
    feb = store.get(store.snapshot_key("2026-02"))[["subscriber_id"] + F]
    feb["label_month"] = "2026-03"
    h = compare(halted[["subscriber_id", "label_month"] + F], feb)
    h["null_feature_rows"] = int(halted[F].isna().any(axis=1).sum())
    out["halted_month_2026_03"] = h
    # 4. online store
    snap = store.get(store.snapshot_key(ONLINE_MONTH))[["subscriber_id"] + F]
    ids = list(rng.choice(snap.subscriber_id.to_numpy(), 1000, replace=False))
    res = post(ids)
    names = res["metadata"]["feature_names"]
    online = pd.DataFrame({n: r["values"] for n, r in zip(names, res["results"])})
    online["label_month"] = "x"
    ref = snap[snap.subscriber_id.isin(ids)].copy(); ref["label_month"] = "x"
    out["online_vs_offline"] = {"month": ONLINE_MONTH, **compare(ref, online)}
    fs = features.store_()
    lat = {}
    for n in (1, 100):
        for name, fn in (("http", lambda b: post(b)),
                         ("sdk", lambda b: fs.get_online_features(
                             features=fs.get_feature_service("churn_model"),
                             entity_rows=[{"subscriber_id": i} for i in b]).to_dict())):
            for _ in range(20):
                fn(ids[:n])                                     # warm-up
            ts = []
            for _ in range(300):
                b = list(rng.choice(ids, n, replace=False))
                t0 = time.perf_counter(); fn(b); ts.append(time.perf_counter() - t0)
            lat[f"{name}_{n}"] = pct(ts)
    out["online_latency"] = lat
    out["note"] = "Latency measured from the Airflow container over the compose network (Colima, M4 Pro)."
    OUT.write_text(json.dumps(out, indent=2, default=str) + "\n")
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
