"""Staged canary rollout with guardrails, rollback, and a paired A/B promotion decision (ADR 0007).

Replays the 2026-10 snapshot (the latest month with realised labels) through the live API at each stage,
reads the guardrails from `predictions_log`, and either rolls the candidate back or, after the last
stage, decides promotion on the realised labels. Results -> docs/results/rollout.json.

Run in the Airflow container (stack up):
    python /opt/airflow/repo/scripts/rollout.py 6 5      # candidate versions, in order
"""
from __future__ import annotations

import json
import math
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.metrics import roc_auc_score

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mlops import data, db, pipeline, rca, store  # noqa: E402

API = "http://api:8000"
MONTH = "2026-10"
STAGES = [5, 25, 50]
BATCH, WORKERS = 50, 4
LIMITS = {"error_rate": 0.005, "latency_factor": 1.5, "latency_slack_ms": 5.0, "spearman_min": 0.80,
          "mean_rel_diff_max": 0.25, "share_tolerance_pts": 1.5, "auc_ni_margin": -0.005}
ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / os.environ.get("ROLLOUT_OUT", "docs/results/rollout.json")
INT = ["tenure_months", "complaints_90d", "support_calls_30d", "payment_delay_days", "contract_months_left",
       "device_age_months", "family_lines", "discount_pct", "device_5g"]


def get(path):
    with urllib.request.urlopen(API + path, timeout=10) as r:
        return json.loads(r.read())


def post(path, body):
    req = urllib.request.Request(API + path, json.dumps(body).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def wait_for(cond, timeout=60):
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < timeout:
        m = get("/model")
        if cond(m):
            return round(time.perf_counter() - t0, 2), m
        time.sleep(0.25)
    raise TimeoutError("API did not pick up the registry change")


def payloads() -> list[list[dict]]:
    df = store.get(store.snapshot_key(MONTH))[["subscriber_id"] + data.FEATURES_NUM + data.FEATURES_CAT]
    recs = []
    for r in df.to_dict("records"):
        for k, v in r.items():
            if isinstance(v, float) and math.isnan(v):
                r[k] = None
            elif k in INT:
                r[k] = int(v)
        recs.append(r)
    return [recs[i:i + BATCH] for i in range(0, len(recs), BATCH)]


def replay(batches) -> tuple[datetime, datetime, float]:
    start = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    with ThreadPoolExecutor(WORKERS) as ex:
        list(ex.map(lambda b: post("/predict", b), batches))
    secs = time.perf_counter() - t0
    logged = get("/model")["log"]
    while True:                                   # wait for the async log to drain
        time.sleep(1)
        now = get("/model")["log"]
        if now["queued"] == 0 and now["logged"] == logged["logged"]:
            break
        logged = now
    return start, datetime.now(timezone.utc), secs


def rows(start, end) -> pd.DataFrame:
    return pd.DataFrame(db.query("SELECT subscriber_id, variant, model_version, probability, shadow_probability, "
                                 "latency_ms, error FROM predictions_log WHERE ts BETWEEN %s AND %s", (start, end)))


def guardrails(df: pd.DataFrame, target: float) -> dict:
    can = df[df.variant.isin(["canary", "canary_fallback"])]
    ch = df[df.variant == "champion"]
    ok = can[can.variant == "canary"]
    g = {"rows": len(df), "canary_rows": len(can), "routed_share_pct": round(100 * len(can) / len(df), 2),
         "error_rate": round(float((can.variant == "canary_fallback").mean()) if len(can) else 0.0, 5),
         "canary_p95_ms": round(float(ok.latency_ms.quantile(.95)), 2) if len(ok) else None,
         "champion_p95_ms": round(float(ch.latency_ms.quantile(.95)), 2),
         "spearman": round(float(spearmanr(ok.probability, ok.shadow_probability).statistic), 4) if len(ok) > 2 else None,
         "canary_mean": round(float(ok.probability.mean()), 4), "shadow_mean": round(float(ok.shadow_probability.mean()), 4)}
    g["mean_rel_diff"] = round(abs(g["canary_mean"] - g["shadow_mean"]) / g["shadow_mean"], 4)
    lat_lim = LIMITS["latency_factor"] * g["champion_p95_ms"] + LIMITS["latency_slack_ms"]
    g["checks"] = {
        "errors": g["error_rate"] <= LIMITS["error_rate"],
        "latency": g["canary_p95_ms"] is not None and g["canary_p95_ms"] <= lat_lim,
        "rank_agreement": g["spearman"] is not None and g["spearman"] >= LIMITS["spearman_min"],
        "mean_prediction": g["mean_rel_diff"] <= LIMITS["mean_rel_diff_max"],
        "routed_share": abs(g["routed_share_pct"] - target) <= LIMITS["share_tolerance_pts"]}
    g["passed"] = all(g["checks"].values())
    return g


def paired_auc(df: pd.DataFrame, labels: pd.DataFrame, n_boot=1000) -> dict:
    ok = df[df.variant == "canary"].merge(labels, on="subscriber_id")
    y, a, b = ok[data.LABEL].to_numpy(), ok.probability.to_numpy(), ok.shadow_probability.to_numpy()
    rng = np.random.default_rng(11)
    diffs = []
    for _ in range(n_boot):
        i = rng.integers(0, len(y), len(y))
        if y[i].min() == y[i].max():
            continue
        diffs.append(roc_auc_score(y[i], a[i]) - roc_auc_score(y[i], b[i]))
    lo, hi = np.percentile(diffs, [2.5, 97.5])
    return {"n": len(y), "positives": int(y.sum()), "auc_canary": round(roc_auc_score(y, a), 4),
            "auc_champion_same_rows": round(roc_auc_score(y, b), 4),
            "delta": round(roc_auc_score(y, a) - roc_auc_score(y, b), 4),
            "ci95": [round(float(lo), 4), round(float(hi), 4)], "bootstrap": len(diffs)}


def unpaired_ab(df: pd.DataFrame, labels: pd.DataFrame) -> dict:
    m = df.merge(labels, on="subscriber_id")
    return {arm: {"n": int((m.variant == arm).sum()),
                  "auc": round(roc_auc_score(m[m.variant == arm][data.LABEL], m[m.variant == arm].probability), 4)}
            for arm in ("champion", "canary")}


def run_candidate(v: str, batches, labels) -> dict:
    c = pipeline._client()
    champ_before = c.get_model_version_by_alias(pipeline.MODEL, "champion").version
    c.set_registered_model_tag(pipeline.MODEL, "canary_percent", "0")
    c.set_registered_model_alias(pipeline.MODEL, "canary", v)
    load_s, _ = wait_for(lambda m: m["canary_version"] == v)
    res = {"version": v, "role": c.get_model_version(pipeline.MODEL, v).tags.get("role"),
           "champion_before": champ_before, "canary_load_s": load_s, "stages": []}
    prev_canary: set[str] = set()
    for pct in STAGES:
        c.set_registered_model_tag(pipeline.MODEL, "canary_percent", str(pct))
        pick_s, _ = wait_for(lambda m: m["canary_percent"] == pct)
        start, end, secs = replay(batches)
        df = rows(start, end)
        g = guardrails(df, pct)
        now_canary = set(df[df.variant != "champion"].subscriber_id)
        g |= {"target_pct": pct, "picked_up_s": pick_s, "replay_s": round(secs, 2),
              "requests": len(batches), "sticky": prev_canary <= now_canary,
              "unpaired_ab": unpaired_ab(df, labels)}
        g["checks"]["sticky"] = g["sticky"]
        g["passed"] = all(g["checks"].values())
        prev_canary = now_canary
        res["stages"].append(g)
        print(json.dumps({k: g[k] for k in ("target_pct", "routed_share_pct", "spearman", "mean_rel_diff",
                                            "canary_p95_ms", "champion_p95_ms", "passed")}), flush=True)
        if not g["passed"]:
            failed = [k for k, ok in g["checks"].items() if not ok]
            c.set_registered_model_tag(pipeline.MODEL, "canary_percent", "0")
            c.delete_registered_model_alias(pipeline.MODEL, "canary")
            res["rollback_s"], _ = wait_for(lambda m: m["canary_version"] is None)
            c.set_model_version_tag(pipeline.MODEL, v, "rollback_reason", f"stage {pct}%: {','.join(failed)}")
            res |= {"outcome": "rolled_back", "failed_at_pct": pct, "failed_checks": failed}
            # automatic root-cause report, attached to the version and saved next to the results
            r = rca.investigate(v, MONTH, stage=g, champion=champ_before)
            (ROOT / f"docs/results/rca_v{v}.json").write_text(json.dumps(r, indent=2, default=str) + "\n")
            (ROOT / f"docs/results/rca_v{v}.md").write_text(rca.to_markdown(r))
            c.set_model_version_tag(pipeline.MODEL, v, "rca_root_cause", r["root_cause"] or "none")
            res["rca"] = {"root_cause": r["root_cause"], "verdict": r["verdict"], "seconds": r["seconds"]}
            return res
    res["paired_auc"] = pa = paired_auc(df, labels)
    promote = pa["ci95"][0] > LIMITS["auc_ni_margin"]
    c.set_registered_model_tag(pipeline.MODEL, "canary_percent", "0")
    c.delete_registered_model_alias(pipeline.MODEL, "canary")
    if promote:
        c.set_registered_model_alias(pipeline.MODEL, "champion", v)
        c.set_model_version_tag(pipeline.MODEL, v, "promoted_by", "canary rollout ADR 0007")
        res["champion_swap_s"], _ = wait_for(lambda m: m["version"] == v and m["canary_version"] is None)
    else:
        res["rollback_s"], _ = wait_for(lambda m: m["canary_version"] is None)
    res["outcome"] = "promoted" if promote else "not_promoted"
    res["champion_after"] = c.get_model_version_by_alias(pipeline.MODEL, "champion").version
    return res


def main() -> None:
    batches = payloads()
    labels = store.get(store.labels_key(MONTH))[["subscriber_id", data.LABEL]]
    out = {"month_replayed": MONTH, "subscribers": sum(map(len, batches)), "batch": BATCH, "workers": WORKERS,
           "stages_pct": STAGES, "limits": LIMITS, "candidates": []}
    for v in sys.argv[1:]:
        r = run_candidate(v, batches, labels)
        print(json.dumps({k: r.get(k) for k in ("version", "outcome", "failed_at_pct", "failed_checks",
                                                "rollback_s", "champion_swap_s", "paired_auc")}), flush=True)
        out["candidates"].append(r)
    OUT.write_text(json.dumps(out, indent=2, default=str) + "\n")


if __name__ == "__main__":
    main()
