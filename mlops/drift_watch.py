"""Drift watcher: retrain when the traffic the API serves has drifted, not only once a month.

Every check takes the newest WINDOW feature rows the API sampled for the current champion
(`request_features`, FEATURE_SAMPLE_PCT of subscribers) and computes PSI per feature against the
champion's training reference, the same reference and limit (0.25) as the monthly DAG. It retrains only
when all of these hold:

1. at least MIN_ROWS rows were sampled since the champion took over (no verdict on thin data),
2. max PSI > PSI_LIMIT in CONSECUTIVE checks in a row (one noisy window does not retrain),
3. a labelled month exists that the last drift-triggered attempt for this champion did not have.
   Without new labels a challenger can only learn the old regime again (the 2026-07 rejection in the
   monthly run), so the watcher records `suppressed_no_new_labels` and keeps alerting instead.

The retrain itself is `pipeline.challenger`: the same window, holdout and MIN_GAIN rule as the monthly DAG.
Every check is a row in `drift_watch`.
"""
from __future__ import annotations

import time

import pandas as pd

from mlops import data, db, drift, pipeline, store

WINDOW, MIN_ROWS, CONSECUTIVE = 2000, 1000, 2
DDL = """CREATE TABLE IF NOT EXISTS drift_watch (id BIGSERIAL PRIMARY KEY, checked_at TIMESTAMPTZ DEFAULT now(),
    as_of TEXT, champion TEXT, rows INT, max_psi DOUBLE PRECISION, feature TEXT, psi JSONB, breach BOOLEAN,
    action TEXT, newest_label TEXT, challenger TEXT, details JSONB)"""


def _init() -> None:
    with db.conn() as c, c.cursor() as cur:
        cur.execute(DDL)


def _insert(row: dict) -> int:
    import json
    cols = list(row)
    vals = [json.dumps(v) if isinstance(v, (dict, list)) else v for v in row.values()]
    with db.conn() as c, c.cursor() as cur:
        cur.execute(f"INSERT INTO drift_watch ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))}) RETURNING id", vals)
        return cur.fetchone()[0]


def _window(champion: str) -> pd.DataFrame:
    rows = db.query("SELECT features FROM request_features WHERE model_version = %s ORDER BY id DESC LIMIT %s",
                    (champion, WINDOW))
    df = pd.DataFrame([r["features"] for r in rows])
    for f in data.FEATURES_NUM:
        if f in df:
            df[f] = pd.to_numeric(df[f], errors="coerce")
    return df


def check(as_of: str) -> dict:
    """One watcher run. Returns the row it recorded; row['action'] == 'retrain' means: retrain now."""
    _init()
    v, _ = pipeline.champion()
    cur = _window(v)
    row = {"as_of": as_of, "champion": v, "rows": len(cur)}
    if len(cur) < MIN_ROWS:
        row |= {"action": "insufficient_rows", "breach": False}
    else:
        psi = drift.psi_report(store.get(f"reference/churn/v{v}.parquet"), cur)
        worst = max(psi, key=psi.get)
        breach = psi[worst] > pipeline.PSI_LIMIT
        row |= {"max_psi": psi[worst], "feature": worst, "psi": psi, "breach": breach}
        prev = db.query("SELECT breach FROM drift_watch WHERE champion = %s AND action <> 'insufficient_rows' "
                        "ORDER BY id DESC LIMIT %s", (v, CONSECUTIVE - 1))
        sustained = breach and len(prev) == CONSECUTIVE - 1 and all(p["breach"] for p in prev)
        labelled = pipeline.labelled_months(as_of)
        newest = labelled[-1] if labelled else None
        row["newest_label"] = newest
        tried = db.query("SELECT newest_label FROM drift_watch WHERE champion = %s AND action LIKE 'retrain%%' "
                         "ORDER BY id DESC LIMIT 1", (v,))
        if not breach:
            row["action"] = "ok"
        elif not sustained:
            row["action"] = "breach_pending"
        elif tried and tried[0]["newest_label"] == newest:
            row["action"] = "suppressed_no_new_labels"
        else:
            row["action"] = "retrain"
    row["id"] = _insert(row)
    return row


def retrain(as_of: str, check_id: int) -> dict:
    t0 = time.perf_counter()
    r = pipeline.challenger(as_of, run_name=f"drift-watch-{as_of}-{check_id}",
                            extra_tags={"trigger": "drift_watch", "drift_check_id": check_id})
    secs = round(time.perf_counter() - t0, 1)
    action = "retrain_promoted" if r["promoted"] else "retrain_rejected"
    with db.conn() as c, c.cursor() as cur:
        import json
        cur.execute("UPDATE drift_watch SET action = %s, challenger = %s, details = %s WHERE id = %s",
                    (action, r["version"], json.dumps({k: r[k] for k in ("challenger_auc", "champion_auc",
                                                       "holdout_month", "train_months")} | {"seconds": secs}), check_id))
    return r | {"action": action, "seconds": secs}
