"""Churn scoring API that follows the model registry, with a sticky canary and shadow scoring.

Every REFRESH_SECONDS the service resolves two aliases in the MLflow registry:

* `champion` (MODEL_ALIAS) answers by default. When Airflow promotes a new champion, the API swaps
  models in place without a restart, and `/model` shows which version is answering.
* `canary` (CANARY_ALIAS) is optional. Its traffic share is the registered-model tag `canary_percent`
  (0-100, default 0). A subscriber is routed by a hash of its id, so the same subscriber always gets
  the same variant while the percentage is unchanged, and raising the percentage only moves new
  buckets to the canary. Canary requests are also scored by the champion ("shadow"), so the rollout
  guardrails can compare the two models on identical inputs. A canary that raises falls back to the
  champion and the request is logged as a canary error.

`latency_ms` in the log is the inference time of the model that answered (champion or canary), so the
latency guardrail compares the two models rather than the canary path's extra shadow call.

Each scored subscriber is written asynchronously to the Postgres table `predictions_log` (variant,
model version, probability, shadow probability, latency); logging never blocks or fails a request.
`/predict/by-id` fetches features from the Feast feature server instead of taking them in the body.
Until a champion exists, `/predict` returns 503.
"""
from __future__ import annotations

import json
import os
import queue
import threading
import time
import urllib.request
import uuid
from datetime import datetime, timezone

import mlflow
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from mlflow import MlflowClient
from pydantic import BaseModel, Field

from mlops.routing import bucket

NAME = os.environ.get("MODEL_NAME", "churn")
ALIAS = os.environ.get("MODEL_ALIAS", "champion")
CANARY = os.environ.get("CANARY_ALIAS", "canary")
REFRESH = int(os.environ.get("REFRESH_SECONDS", "15"))
# Share of subscribers whose request features are kept for the drift watcher (stable per subscriber,
# salted so it is independent of canary routing).
FEATURE_SAMPLE_PCT = float(os.environ.get("FEATURE_SAMPLE_PCT", "20"))
DB = os.environ.get("MLOPS_DB")
FEAST_URL = os.environ.get("FEAST_URL")

app = FastAPI(title="MasterFabric churn scoring", version="2.0")
_state: dict = {"model": None, "version": None, "run_id": None, "loaded_at": None, "swaps": 0, "last_error": None,
                "canary_model": None, "canary_version": None, "canary_percent": 0.0}
_lock = threading.Lock()
_log: queue.Queue = queue.Queue(maxsize=200_000)
_flog: queue.Queue = queue.Queue(maxsize=200_000)
_stats = {"logged": 0, "log_dropped": 0, "log_errors": 0}

DDL = """
CREATE TABLE IF NOT EXISTS predictions_log (id BIGSERIAL PRIMARY KEY, ts TIMESTAMPTZ NOT NULL, request_id TEXT,
    subscriber_id TEXT, variant TEXT, model_version TEXT, probability DOUBLE PRECISION,
    shadow_version TEXT, shadow_probability DOUBLE PRECISION, latency_ms DOUBLE PRECISION, error TEXT);
CREATE INDEX IF NOT EXISTS predictions_log_ts ON predictions_log (ts);
CREATE TABLE IF NOT EXISTS request_features (id BIGSERIAL PRIMARY KEY, ts TIMESTAMPTZ NOT NULL,
    subscriber_id TEXT, model_version TEXT, features JSONB);
CREATE INDEX IF NOT EXISTS request_features_ts ON request_features (ts);
"""


def _load(client: MlflowClient, alias: str, key: str) -> None:
    mv = client.get_model_version_by_alias(NAME, alias)
    if mv.version != _state[key]:
        m = mlflow.pyfunc.load_model(f"models:/{NAME}@{alias}")
        with _lock:
            if key == "version":
                _state.update(model=m, version=mv.version, run_id=mv.run_id, swaps=_state["swaps"] + 1,
                              loaded_at=datetime.now(timezone.utc).isoformat())
            else:
                _state.update(canary_model=m, canary_version=mv.version)


def refresh_once(client: MlflowClient | None = None) -> None:
    client = client or MlflowClient()
    try:
        _load(client, ALIAS, "version")
        _state["last_error"] = None
    except Exception as e:  # no champion yet, registry restarting, ...
        _state["last_error"] = type(e).__name__
    try:
        _load(client, CANARY, "canary_version")
        pct = float(client.get_registered_model(NAME).tags.get("canary_percent", 0))
        _state["canary_percent"] = min(max(pct, 0.0), 100.0)
    except Exception:  # no canary alias: all traffic to the champion
        with _lock:
            _state.update(canary_model=None, canary_version=None, canary_percent=0.0)


def _refresh_loop() -> None:
    client = MlflowClient()
    while True:
        refresh_once(client)
        time.sleep(REFRESH)


def _writer() -> None:
    import psycopg2
    from psycopg2.extras import execute_values
    con = None
    while True:
        rows = [_log.get()]
        while len(rows) < 5000:
            try:
                rows.append(_log.get_nowait())
            except queue.Empty:
                break
        try:
            if con is None or con.closed:
                con = psycopg2.connect(DB)
                with con, con.cursor() as cur:
                    cur.execute(DDL)
            with con, con.cursor() as cur:
                execute_values(cur, "INSERT INTO predictions_log (ts, request_id, subscriber_id, variant, model_version, "
                                    "probability, shadow_version, shadow_probability, latency_ms, error) VALUES %s", rows)
            _stats["logged"] += len(rows)
        except Exception:
            _stats["log_errors"] += 1
            con = None
            time.sleep(1)


def _feature_writer() -> None:
    import psycopg2
    from psycopg2.extras import Json, execute_values
    con = None
    while True:
        rows = [_flog.get()]
        while len(rows) < 5000:
            try:
                rows.append(_flog.get_nowait())
            except queue.Empty:
                break
        try:
            if con is None or con.closed:
                con = psycopg2.connect(DB)
                with con, con.cursor() as cur:
                    cur.execute(DDL)
            with con, con.cursor() as cur:
                execute_values(cur, "INSERT INTO request_features (ts, subscriber_id, model_version, features) VALUES %s",
                               [(t, s, v, Json(f)) for t, s, v, f in rows])
            _stats["features_logged"] = _stats.get("features_logged", 0) + len(rows)
        except Exception:
            _stats["log_errors"] += 1
            con = None
            time.sleep(1)


def _sampled(subscriber_id: str) -> bool:
    return bucket("features:" + subscriber_id) < FEATURE_SAMPLE_PCT


@app.on_event("startup")
def _start() -> None:
    threading.Thread(target=_refresh_loop, daemon=True).start()
    if DB:
        threading.Thread(target=_writer, daemon=True).start()
        threading.Thread(target=_feature_writer, daemon=True).start()


class Subscriber(BaseModel):
    subscriber_id: str
    tenure_months: int = Field(ge=0)
    monthly_fee: float
    data_gb: float | None = None
    voice_min: float
    dropped_call_rate: float = Field(ge=0, le=1)
    complaints_90d: int = Field(ge=0)
    support_calls_30d: int = Field(ge=0)
    payment_delay_days: int = Field(ge=0)
    contract_months_left: int = Field(ge=0)
    device_age_months: int = Field(ge=0)
    family_lines: int = Field(ge=0)
    discount_pct: int = Field(ge=0, le=100)   # the model signature is integer; MLflow enforces it
    nps: float = Field(ge=-100, le=100)
    plan: str
    region: str
    device_5g: int = Field(ge=0, le=1)


class Ids(BaseModel):
    subscriber_ids: list[str]


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "model_loaded": _state["model"] is not None}


@app.get("/model")
def model() -> dict:
    hide = ("model", "canary_model")
    return {k: v for k, v in _state.items() if k not in hide} | {"name": NAME, "alias": ALIAS, "canary_alias": CANARY,
                                                                 "log": dict(_stats, queued=_log.qsize())}


def _proba(m, df: pd.DataFrame) -> np.ndarray:
    p = np.asarray(m.predict(df))
    return p[:, 1] if p.ndim == 2 else p        # pyfunc wraps predict_proba: column 1 is P(churn)


def _score(df: pd.DataFrame) -> dict:
    t0 = time.perf_counter()
    with _lock:
        champ, v = _state["model"], _state["version"]
        can, cv, pct = _state["canary_model"], _state["canary_version"], _state["canary_percent"]
    if champ is None:
        raise HTTPException(503, "no champion model registered yet")
    X = df.drop(columns=["subscriber_id"])
    ids = df.subscriber_id.to_numpy()
    to_canary = np.array([bucket(i) < pct for i in ids]) if can is not None and pct > 0 else np.zeros(len(df), bool)
    champ_p = _proba(champ, X)                   # champion scores everyone: answer or shadow
    champ_ms = (time.perf_counter() - t0) * 1000
    lat = np.full(len(df), champ_ms)
    out_p, variant, version, err = champ_p.copy(), np.full(len(df), "champion", object), np.full(len(df), v, object), None
    if to_canary.any():
        t1 = time.perf_counter()
        try:
            out_p[to_canary] = _proba(can, X[to_canary])
            variant[to_canary], version[to_canary] = "canary", cv
        except Exception as e:               # fall back to the champion, but count it against the canary
            err = type(e).__name__
            variant[to_canary] = "canary_fallback"
        lat[to_canary] = (time.perf_counter() - t1) * 1000    # the canary model's own inference time
    ms = (time.perf_counter() - t0) * 1000
    if DB:
        rid, now = uuid.uuid4().hex[:12], datetime.now(timezone.utc)
        for i in range(len(df)):
            shadow = to_canary[i]
            row = (now, rid, ids[i], variant[i], version[i], float(out_p[i]), v if shadow else None,
                   float(champ_p[i]) if shadow else None, float(lat[i]), err if shadow else None)
            try:
                _log.put_nowait(row)
            except queue.Full:
                _stats["log_dropped"] += 1
        recs = X.to_dict("records")
        for i in range(len(df)):
            if FEATURE_SAMPLE_PCT > 0 and _sampled(ids[i]):
                f = {k: (None if isinstance(val, float) and np.isnan(val) else
                         val.item() if hasattr(val, "item") else val) for k, val in recs[i].items()}
                try:
                    _flog.put_nowait((now, ids[i], v, f))
                except queue.Full:
                    _stats["log_dropped"] += 1
    return {"model_version": v, "canary_version": cv, "canary_percent": pct, "latency_ms": round(ms, 2),
            "scores": [{"subscriber_id": s, "churn_probability": round(float(p), 5), "variant": var, "model_version": ver}
                       for s, p, var, ver in zip(ids, out_p, variant, version)]}


@app.post("/predict")
def predict(subs: list[Subscriber]) -> dict:
    return _score(pd.DataFrame([s.model_dump() for s in subs]))


FEATURES = list(Subscriber.model_fields)[1:]


@app.post("/predict/by-id")
def predict_by_id(body: Ids) -> dict:
    if not FEAST_URL:
        raise HTTPException(501, "FEAST_URL not configured")
    t0 = time.perf_counter()
    req = urllib.request.Request(f"{FEAST_URL}/get-online-features", json.dumps(
        {"feature_service": "churn_model", "entities": {"subscriber_id": body.subscriber_ids}}).encode(),
        {"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            res = json.loads(r.read())
    except Exception as e:
        raise HTTPException(503, f"feature server unavailable: {type(e).__name__}")
    names = res["metadata"]["feature_names"]
    df = pd.DataFrame({n: c["values"] for n, c in zip(names, res["results"])})
    fetch_ms = (time.perf_counter() - t0) * 1000
    missing = df[FEATURES].isna().all(axis=1)
    if missing.all():
        raise HTTPException(404, "no online features for these subscribers")
    df = df[~missing][["subscriber_id"] + FEATURES]
    for f, info in Subscriber.model_fields.items():
        if info.annotation is int:
            df[f] = df[f].astype("int64")
    out = _score(df.reset_index(drop=True))
    return out | {"feature_fetch_ms": round(fetch_ms, 2), "not_found": [i for i, m in zip(body.subscriber_ids, missing) if m]}
