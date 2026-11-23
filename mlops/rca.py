"""Automatic root-cause report for a canary that was rolled back (or, as a control, one that passed).

A rollback says *that* a candidate disagreed with the champion; the on-call engineer needs *why*. This
module runs one check per hypothesis, in the order an engineer would rule them out, using only what the
platform already records: the guardrail numbers, the registry (signature, lineage tags, holdout AUC),
the lake (snapshots and labels) and the replayed month. Nothing reads the candidate's `role` tag or how
it was produced; the verdict has to follow from evidence.

    infrastructure    errors and latency of the canary arm
    population        are canary-routed subscribers different from the rest? (PSI on the routed split)
    contract          same input signature as the champion?
    training_inputs   did the candidate train on a different population? (PSI of its training months
                      against the champion's)
    model_signal      does the candidate rank anything on its own holdout? (registered holdout AUC)
    training_labels   retrain from the candidate's own lineage tags on the lake's labels: does the
                      reproduction match the registered artefact? Does the artefact predict the labels of
                      the months it says it was trained on?

Each check returns status `ruled_out`, `supported` or `inconclusive` with the numbers behind it.
"""
from __future__ import annotations

import json
import time

import mlflow
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from mlops import data, drift, model, pipeline, store
from mlops.routing import bucket

X = data.FEATURES_NUM + data.FEATURES_CAT
LIMITS = {"population_psi_max": 0.10, "training_psi_max": 0.25, "signal_auc_min": 0.55,
          "reproduction_gap_max": 0.02, "train_fit_auc_min": 0.60}


def _check(name: str, status: str, finding: str, **evidence) -> dict:
    return {"hypothesis": name, "status": status, "finding": finding, "evidence": evidence}


def infrastructure(stage: dict | None) -> dict:
    if not stage:
        return _check("infrastructure", "inconclusive", "no guardrail numbers given")
    bad = stage["error_rate"] > 0.005 or (stage["canary_p95_ms"] or 1e9) > 1.5 * stage["champion_p95_ms"] + 5
    return _check("infrastructure", "supported" if bad else "ruled_out",
                  f"canary errors {stage['error_rate']:.2%}, p95 {stage['canary_p95_ms']} ms vs champion "
                  f"{stage['champion_p95_ms']} ms", error_rate=stage["error_rate"],
                  canary_p95_ms=stage["canary_p95_ms"], champion_p95_ms=stage["champion_p95_ms"])


def population(snap: pd.DataFrame, pct: float) -> dict:
    routed = snap.subscriber_id.map(lambda s: bucket(s) < pct)
    psi = drift.psi_report(snap[~routed], snap[routed])
    worst = max(psi, key=psi.get)
    ok = psi[worst] <= LIMITS["population_psi_max"]
    return _check("population", "ruled_out" if ok else "supported",
                  f"routed {int(routed.sum()):,} vs {int((~routed).sum()):,} subscribers: max PSI {psi[worst]:.4f} ({worst})",
                  max_psi=psi[worst], feature=worst, routed=int(routed.sum()))


def contract(c, version: str, champion: str) -> dict:
    def sig(v):
        info = mlflow.models.get_model_info(f"models:/{pipeline.MODEL}/{v}")
        return [(i["name"], i["type"]) for i in json.loads(info.signature.inputs.to_json())]
    a, b = sig(version), sig(champion)
    return _check("contract", "ruled_out" if a == b else "supported",
                  "identical input signature" if a == b else "input signature differs from the champion's",
                  inputs=len(a), differences=sorted(set(map(tuple, a)) ^ set(map(tuple, b))))


def _months(tags) -> list[str]:
    return tags.get("train_months", "").split(",") if tags.get("train_months") else []


def training_inputs(tags: dict, champ_tags: dict) -> dict:
    a, b = _months(tags), _months(champ_tags)
    if not a or not b:
        return _check("training_inputs", "inconclusive", "no train_months lineage tag")
    fa = pd.concat([store.get(store.snapshot_key(m)) for m in a])
    fb = pd.concat([store.get(store.snapshot_key(m)) for m in b])
    psi = drift.psi_report(fb.sample(n=min(20000, len(fb)), random_state=3), fa.sample(n=min(20000, len(fa)), random_state=3))
    worst = max(psi, key=psi.get)
    ok = psi[worst] <= LIMITS["training_psi_max"]
    return _check("training_inputs", "ruled_out" if ok else "supported",
                  f"training months {','.join(a)} vs champion's {','.join(b)}: max PSI {psi[worst]:.4f} ({worst})",
                  max_psi=psi[worst], feature=worst, candidate_months=a, champion_months=b)


def model_signal(tags: dict) -> dict:
    auc = float(tags.get("holdout_auc", "nan"))
    weak = auc < LIMITS["signal_auc_min"]
    return _check("model_signal", "supported" if weak else "ruled_out",
                  f"registered holdout AUC {auc:.4f} on {tags.get('holdout_month')}"
                  + (" - no better than chance" if weak else ""), holdout_auc=auc)


def training_labels(cand, tags: dict) -> dict:
    """Rebuild the candidate from its own lineage tags with the lake's labels, and score the artefact on the
    months it claims to have learnt from."""
    months, hold = _months(tags), tags.get("holdout_month")
    if not months or not hold:
        return _check("training_labels", "inconclusive", "no lineage tags to reproduce from")
    t0 = time.perf_counter()
    tr, ho = pipeline._frame(months), pipeline._frame([hold])
    repro = model.train(tr)
    rep_auc = model.evaluate(repro.predict_proba(ho[X])[:, 1], ho[data.LABEL].to_numpy())["auc"]
    fit_auc = model.evaluate(cand.predict_proba(tr[X])[:, 1], tr[data.LABEL].to_numpy())["auc"]
    reg_auc = float(tags.get("holdout_auc", "nan"))
    gap = rep_auc - reg_auc
    bad = gap > LIMITS["reproduction_gap_max"] and fit_auc < LIMITS["train_fit_auc_min"]
    finding = (f"reproduction from lineage scores {rep_auc:.4f} on {hold}, the registered artefact {reg_auc:.4f} "
               f"(gap {gap:+.4f}); the artefact scores {fit_auc:.4f} AUC on its own training months' labels")
    return _check("training_labels", "supported" if bad else "ruled_out", finding, reproduction_auc=rep_auc,
                  registered_auc=reg_auc, gap=round(gap, 4), artefact_auc_on_training_labels=fit_auc,
                  train_rows=len(tr), seconds=round(time.perf_counter() - t0, 1))


def disagreement(cand, champ, snap: pd.DataFrame) -> dict:
    """Which features each model's ranking follows (Spearman of prediction vs feature), on the replayed month."""
    a, b = cand.predict_proba(snap[X])[:, 1], champ.predict_proba(snap[X])[:, 1]
    rho = {}
    for f in data.FEATURES_NUM:
        v = snap[f].fillna(snap[f].median())
        rho[f] = (round(float(spearmanr(v, a).statistic), 3), round(float(spearmanr(v, b).statistic), 3))
    top = sorted(rho, key=lambda f: -abs(rho[f][1]))[:5]
    return {"spearman_candidate_vs_champion": round(float(spearmanr(a, b).statistic), 4),
            "feature_rank_correlation_top5_of_champion": {f: {"candidate": rho[f][0], "champion": rho[f][1]} for f in top}}


ORDER = ["infrastructure", "population", "contract", "training_inputs", "model_signal", "training_labels"]
VERDICTS = {
    "infrastructure": "serving problem: the canary errored or was slow",
    "population": "routing problem: the canary saw a different population",
    "contract": "contract problem: the candidate expects different inputs",
    "training_inputs": "data problem: the candidate trained on a different population",
    "model_signal": "the candidate has no predictive signal",
    "training_labels": "the candidate was trained on labels that do not match the lake: label corruption "
                       "in the training job (shuffled, misjoined or leaked from another table)",
}


def investigate(version: str, month: str = "2026-10", stage: dict | None = None,
                champion: str | None = None) -> dict:
    t0 = time.perf_counter()
    c = pipeline._client()
    champion = champion or c.get_model_version_by_alias(pipeline.MODEL, "champion").version
    tags = c.get_model_version(pipeline.MODEL, version).tags
    ctags = c.get_model_version(pipeline.MODEL, champion).tags
    cand = mlflow.sklearn.load_model(f"models:/{pipeline.MODEL}/{version}")
    champ = mlflow.sklearn.load_model(f"models:/{pipeline.MODEL}/{champion}")
    snap = store.get(store.snapshot_key(month))
    pct = stage["target_pct"] if stage else 5
    checks = [infrastructure(stage), population(snap, pct), contract(c, version, champion),
              training_inputs(tags, ctags), model_signal(tags), training_labels(cand, tags)]
    supported = [k["hypothesis"] for k in checks if k["status"] == "supported"]
    # the most specific supported hypothesis explains the others (no signal is a symptom of bad labels)
    root = supported[-1] if supported else None
    return {"version": version, "champion": champion, "month": month, "stage_pct": pct,
            "rollback_reason": tags.get("rollback_reason"), "checks": checks, "supported": supported,
            "root_cause": root, "verdict": VERDICTS[root] if root else "no anomaly found: the candidate looks sound",
            "disagreement": disagreement(cand, champ, snap), "seconds": round(time.perf_counter() - t0, 1)}


def to_markdown(r: dict) -> str:
    icon = {"ruled_out": "ruled out", "supported": "**SUPPORTED**", "inconclusive": "inconclusive"}
    lines = [f"# Root-cause report: churn v{r['version']} vs champion v{r['champion']}", "",
             f"- Rollback reason: {r['rollback_reason'] or 'none (not rolled back)'}",
             f"- Replayed month: {r['month']}, stage {r['stage_pct']} %",
             f"- **Verdict: {r['verdict']}**", f"- Generated in {r['seconds']} s", "",
             "| # | Hypothesis | Status | Finding |", "|---|---|---|---|"]
    for i, k in enumerate(r["checks"], 1):
        lines.append(f"| {i} | {k['hypothesis']} | {icon[k['status']]} | {k['finding']} |")
    d = r["disagreement"]
    lines += ["", f"Rank agreement with the champion on all {r['month']} subscribers: Spearman "
                  f"{d['spearman_candidate_vs_champion']}.", "",
              "| Feature (champion's top 5) | Candidate follows it (Spearman) | Champion follows it |", "|---|---:|---:|"]
    for f, v in d["feature_rank_correlation_top5_of_champion"].items():
        lines.append(f"| {f} | {v['candidate']} | {v['champion']} |")
    return "\n".join(lines) + "\n"
