"""Task logic for the monthly DAG. Each function is plain Python (testable without Airflow) and writes
what it did to the monitoring tables, so every decision can be audited afterwards."""
from __future__ import annotations

import os

import mlflow
import pandas as pd
from mlflow import MlflowClient
from mlflow.models import infer_signature

from mlops import data, db, drift, model, quality, store

MODEL = "churn"
EXPERIMENT = "churn-training"
TRAIN_MONTHS = 3                 # sliding window of labelled months
MIN_GAIN = 0.002                 # challenger must beat the champion by this much AUC on the same holdout
DECAY = 0.03                     # realised AUC this far below validation AUC triggers retraining
PSI_LIMIT = 0.25                 # any feature above this triggers retraining
MAX_AGE = 6                      # months; retrain at least this often
SEED = int(os.environ.get("MLOPS_SEED", "42"))


def prev_month(m: str) -> str:
    return str(pd.Period(m, "M") - 1)


def _client() -> MlflowClient:
    return MlflowClient()


def champion() -> tuple[str | None, dict]:
    try:
        mv = _client().get_model_version_by_alias(MODEL, "champion")
        return mv.version, mv.tags
    except Exception:
        return None, {}


# -- 1. landing -------------------------------------------------------------------------------
def land(month: str) -> dict:
    """The source delivers this month's features, and last month's churn outcomes (labels lag a month)."""
    db.init()
    feats, _ = data.snapshot(month, SEED)
    store.put(feats, store.snapshot_key(month))
    out = {"month": month, "rows": len(feats)}
    if month != data.MONTHS[0]:
        p = prev_month(month)
        _, labels = data.snapshot(p, SEED)
        store.put(labels[["month", "subscriber_id", data.LABEL]], store.labels_key(p))
        out["labels_for"] = p
    return out


# -- 2. data quality gate ---------------------------------------------------------------------
def dq_gate(month: str) -> bool:
    df = store.get(store.snapshot_key(month))
    prev = db.query("SELECT rows FROM dq_results WHERE month = %s", (prev_month(month),))
    s = quality.summarise(quality.run_checks(df, prev[0]["rows"] if prev else None))
    db.upsert("dq_results", {"month": month, "passed": s["passed"], "rules": s["rules"],
                             "critical_failures": s["critical_failures"], "warnings": s["warnings"],
                             "checks": s["checks"], "rows": len(df)}, ("month",))
    return s["passed"]


def halt(month: str) -> None:
    v, _ = champion()
    failures = db.query("SELECT critical_failures FROM dq_results WHERE month=%s", (month,))[0]["critical_failures"]
    db.upsert("decisions", {"month": month, "action": "halted", "reasons": ["data_quality"] + failures,
                            "champion_before": v, "champion_after": v, "challenger_version": None,
                            "details": {"note": "no training and no scoring on a snapshot that failed the gate; "
                                                "last month's scores stay in use"}}, ("month",))


# -- 3. realised performance of last month's scores -------------------------------------------
def realise(month: str) -> list[dict]:
    p = prev_month(month)
    if not (store.exists(store.labels_key(p))):
        return []
    labels = store.get(store.labels_key(p))
    rows = []
    for policy in ("auto", "static"):
        key = store.scores_key(p).replace("scores.parquet", f"{policy}.parquet")
        if not store.exists(key):
            continue
        s = store.get(key).merge(labels, on="subscriber_id")
        m = model.evaluate(s.churn_probability.to_numpy(), s[data.LABEL].to_numpy())
        row = {"month": p, "model_version": str(s.model_version.iloc[0]), "policy": policy, "auc": m["auc"],
               "pr_auc": m["pr_auc"], "top_decile_lift": m["top_decile_lift"], "base_rate": m["base_rate"], "n": m["n"]}
        db.upsert("realised", row, ("month", "policy"))
        rows.append(row)
    return rows


# -- 4. drift + decision ----------------------------------------------------------------------
def check_drift(month: str) -> dict:
    v, _ = champion()
    if v is None:
        return {}
    ref = store.get(f"reference/churn/v{v}.parquet")
    cur = store.get(store.snapshot_key(month))
    psi = drift.psi_report(ref, cur)
    worst = max(psi, key=psi.get)
    db.upsert("drift", {"month": month, "champion_version": v, "psi": psi, "max_psi": psi[worst],
                        "max_feature": worst}, ("month",))
    return {"max_psi": psi[worst], "feature": worst}


def labelled_months(month: str) -> list[str]:
    """Months before `month` whose snapshot passed the gate and whose labels have arrived."""
    ok = {r["month"] for r in db.query("SELECT month FROM dq_results WHERE passed")}
    return [m for m in data.MONTHS if m < month and m in ok and store.exists(store.labels_key(m))]


def decide(month: str) -> str:
    v, tags = champion()
    reasons, details = [], {}
    avail = labelled_months(month)
    if v is None:
        if len(avail) >= TRAIN_MONTHS + 1:
            reasons.append("no_champion")
        else:
            details["waiting_for_labels"] = f"{len(avail)}/{TRAIN_MONTHS + 1} labelled months"
    else:
        val_auc = float(tags.get("holdout_auc", 0))
        last = db.query("SELECT month, auc FROM realised WHERE policy='auto' ORDER BY month DESC LIMIT 1")
        if last and last[0]["auc"] < val_auc - DECAY:
            reasons.append("performance_decay")
            details["realised_auc"] = last[0]["auc"]
            details["validation_auc"] = val_auc
        d = db.query("SELECT max_psi, max_feature FROM drift WHERE month=%s", (month,))
        if d and d[0]["max_psi"] > PSI_LIMIT:
            reasons.append("data_drift")
            details["max_psi"] = d[0]["max_psi"]
            details["psi_feature"] = d[0]["max_feature"]
        trained_on = tags.get("trained_in_month")
        if trained_on and (pd.Period(month, "M") - pd.Period(trained_on, "M")).n >= MAX_AGE:
            reasons.append("scheduled")
    action = "retrain" if reasons else "keep"
    db.upsert("decisions", {"month": month, "action": action, "reasons": reasons, "champion_before": v,
                            "champion_after": v, "challenger_version": None, "details": details}, ("month",))
    return action


# -- 5. train challenger, compare, promote ----------------------------------------------------
def _frame(months: list[str]) -> pd.DataFrame:
    parts = []
    for m in months:
        f = store.get(store.snapshot_key(m))
        lab = store.get(store.labels_key(m))
        parts.append(f.merge(lab[["subscriber_id", data.LABEL]], on="subscriber_id"))
    return pd.concat(parts, ignore_index=True)


def challenger(month: str, run_name: str | None = None, extra_tags: dict | None = None) -> dict:
    """Train on the newest labelled months before `month`, compare with the champion on the newest one and
    promote if the gain is at least MIN_GAIN. Shared by the monthly DAG and the drift watcher."""
    avail = labelled_months(month)
    holdout_m, train_ms = avail[-1], avail[-1 - TRAIN_MONTHS:-1]
    train_df, hold = _frame(train_ms), _frame([holdout_m])
    X = data.FEATURES_NUM + data.FEATURES_CAT
    mlflow.set_experiment(EXPERIMENT)
    v_before, tags_before = champion()
    with mlflow.start_run(run_name=run_name or f"challenger-{month}") as run:
        pipe = model.train(train_df)
        ch = model.evaluate(pipe.predict_proba(hold[X])[:, 1], hold[data.LABEL].to_numpy())
        mlflow.log_params(model.PARAMS | {"train_months": ",".join(train_ms), "holdout_month": holdout_m,
                                          "train_rows": len(train_df), "decided_in": month})
        mlflow.log_metrics({f"holdout_{k}": v for k, v in ch.items() if v is not None})
        champ_auc = None
        if v_before is not None:
            champ = mlflow.sklearn.load_model(f"models:/{MODEL}/{v_before}")
            cm = model.evaluate(champ.predict_proba(hold[X])[:, 1], hold[data.LABEL].to_numpy())
            champ_auc = cm["auc"]
            mlflow.log_metric("champion_holdout_auc", champ_auc)
        sample = train_df[X].sample(n=min(5, len(train_df)), random_state=1)
        info = mlflow.sklearn.log_model(pipe, "model", registered_model_name=MODEL,
                                        signature=infer_signature(sample, pipe.predict_proba(sample)[:, 1]),
                                        input_example=sample.head(2), pyfunc_predict_fn="predict_proba")
        version = str(info.registered_model_version)
    client = _client()
    tags = {"holdout_auc": ch["auc"], "holdout_month": holdout_m, "train_months": ",".join(train_ms),
            "trained_in_month": month} | (extra_tags or {})
    for k, v in tags.items():
        client.set_model_version_tag(MODEL, version, k, str(v))
    store.put(train_df[X].sample(n=min(20000, len(train_df)), random_state=3), f"reference/churn/v{version}.parquet")

    promoted = champ_auc is None or ch["auc"] >= champ_auc + MIN_GAIN
    if promoted:
        client.set_registered_model_alias(MODEL, "champion", version)
        if v_before is None:
            client.set_registered_model_alias(MODEL, "static_baseline", version)   # the "never retrain" policy
    else:
        client.set_model_version_tag(MODEL, version, "rejected_reason", f"auc {ch['auc']} < champion {champ_auc} + {MIN_GAIN}")
    return {"version": version, "promoted": promoted, "challenger_auc": ch["auc"], "champion_auc": champ_auc,
            "champion_before": v_before, "holdout_month": holdout_m, "train_months": train_ms,
            "run_id": run.info.run_id}


def train_and_promote(month: str) -> dict:
    r = challenger(month)
    version, promoted, v_before = r["version"], r["promoted"], r["champion_before"]
    row = db.query("SELECT reasons, details FROM decisions WHERE month=%s", (month,))[0]
    db.upsert("decisions", {"month": month, "action": "promoted" if promoted else "rejected",
                            "reasons": row["reasons"], "champion_before": v_before,
                            "champion_after": version if promoted else v_before, "challenger_version": version,
                            "details": row["details"] | {"challenger_auc": r["challenger_auc"],
                                                         "champion_auc": r["champion_auc"],
                                                         "holdout_month": r["holdout_month"],
                                                         "train_months": r["train_months"],
                                                         "run_id": r["run_id"]}}, ("month",))
    return {k: r[k] for k in ("version", "promoted", "challenger_auc", "champion_auc")}


# -- 6. batch scoring -------------------------------------------------------------------------
def score(month: str) -> dict:
    df = store.get(store.snapshot_key(month))
    X = data.FEATURES_NUM + data.FEATURES_CAT
    out = {}
    for policy, alias in (("auto", "champion"), ("static", "static_baseline")):
        try:
            mv = _client().get_model_version_by_alias(MODEL, alias)
        except Exception:
            continue
        m = mlflow.sklearn.load_model(f"models:/{MODEL}/{mv.version}")
        s = pd.DataFrame({"subscriber_id": df.subscriber_id, "churn_probability": m.predict_proba(df[X])[:, 1],
                          "model_version": mv.version, "month": month})
        store.put(s, store.scores_key(month).replace("scores.parquet", f"{policy}.parquet"))
        out[policy] = mv.version
    return out
