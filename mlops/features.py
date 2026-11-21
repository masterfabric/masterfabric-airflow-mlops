"""Feast integration: publish gated monthly snapshots to the offline store, materialise the latest month
to the online store, and build point-in-time correct training frames.

Runs in the Feast/Evidently virtualenv:

    /home/airflow/mf-venv/bin/python -m mlops.features publish --month 2026-11
"""
from __future__ import annotations

import argparse
import json
import os
import time
import warnings
from datetime import timedelta
from pathlib import Path

warnings.filterwarnings("ignore")
os.environ.setdefault("FEAST_USAGE", "False")

import pandas as pd  # noqa: E402

from mlops import data, db, store  # noqa: E402

REPO = Path(os.environ.get("FEAST_REPO", Path(__file__).resolve().parent.parent / "feature_repo"))
OFFLINE = REPO / "data" / "subscriber_features.parquet"
FEATURES = data.FEATURES_NUM + data.FEATURES_CAT


def month_ts(month: str) -> pd.Timestamp:
    return pd.Timestamp(f"{month}-01", tz="UTC")


def gated_months(upto: str) -> list[str]:
    ok = {r["month"] for r in db.query("SELECT month FROM dq_results WHERE passed")}
    return [m for m in data.MONTHS if m <= upto and m in ok]


def store_():
    from feast import FeatureStore
    return FeatureStore(repo_path=str(REPO))


def export_offline(upto: str) -> dict:
    """Rewrite the offline Parquet from every gated snapshot up to `upto` (idempotent)."""
    parts = []
    for m in gated_months(upto):
        f = store.get(store.snapshot_key(m))[["subscriber_id"] + FEATURES].copy()
        f["event_timestamp"] = month_ts(m)
        parts.append(f)
    df = pd.concat(parts, ignore_index=True)
    OFFLINE.parent.mkdir(parents=True, exist_ok=True)
    tmp = OFFLINE.with_suffix(".tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(OFFLINE)
    return {"rows": len(df), "months": len(parts)}


def apply() -> float:
    import importlib.util
    from feast.repo_operations import apply_total  # noqa: F401  (import check only)
    spec = importlib.util.spec_from_file_location("features", REPO / "features.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    t0 = time.perf_counter()
    store_().apply([mod.subscriber, mod.subscriber_monthly, mod.churn_model])
    return time.perf_counter() - t0


def materialize(month: str) -> dict:
    """Load the month's values into the online store (the window covers exactly that month's rows)."""
    t0 = time.perf_counter()
    start = month_ts(month) - timedelta(seconds=1)
    store_().materialize(start_date=start.to_pydatetime(), end_date=(month_ts(month) + timedelta(hours=1)).to_pydatetime())
    return {"month": month, "seconds": round(time.perf_counter() - t0, 2)}


def training_frame(months: list[str]) -> pd.DataFrame:
    """Labels of `months` joined point-in-time to the features valid at each label's month."""
    ents = []
    for m in months:
        lab = store.get(store.labels_key(m))[["subscriber_id", data.LABEL]].copy()
        lab["event_timestamp"] = month_ts(m) + timedelta(hours=1)
        lab["label_month"] = m
        ents.append(lab)
    fs = store_()
    return fs.get_historical_features(entity_df=pd.concat(ents, ignore_index=True),
                                      features=fs.get_feature_service("churn_model")).to_df()


def publish(month: str) -> dict:
    out = {"offline": export_offline(month), "apply_s": round(apply(), 2)}
    out["materialize"] = materialize(month)
    return out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("cmd", choices=["publish"])
    p.add_argument("--month", required=True)
    a = p.parse_args()
    print(json.dumps(publish(a.month)))


if __name__ == "__main__":
    main()
