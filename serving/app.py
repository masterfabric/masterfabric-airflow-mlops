"""Churn scoring API that follows the model registry.

The service resolves `models:/<MODEL_NAME>@<MODEL_ALIAS>` every REFRESH_SECONDS. When Airflow promotes
a new champion, the API swaps models in place without a restart or redeploy, and `/model` shows which
version is answering. Until a champion exists, `/predict` returns 503.
"""
from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone

import mlflow
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from mlflow import MlflowClient
from pydantic import BaseModel, Field

NAME = os.environ.get("MODEL_NAME", "churn")
ALIAS = os.environ.get("MODEL_ALIAS", "champion")
REFRESH = int(os.environ.get("REFRESH_SECONDS", "15"))

app = FastAPI(title="MasterFabric churn scoring", version="1.0")
_state: dict = {"model": None, "version": None, "run_id": None, "loaded_at": None, "swaps": 0, "last_error": None}
_lock = threading.Lock()


def _refresh() -> None:
    client = MlflowClient()
    while True:
        try:
            mv = client.get_model_version_by_alias(NAME, ALIAS)
            if mv.version != _state["version"]:
                model = mlflow.pyfunc.load_model(f"models:/{NAME}@{ALIAS}")
                with _lock:
                    _state.update(model=model, version=mv.version, run_id=mv.run_id,
                                  loaded_at=datetime.now(timezone.utc).isoformat(), swaps=_state["swaps"] + 1,
                                  last_error=None)
        except Exception as e:  # no champion yet, registry restarting, ...
            _state["last_error"] = type(e).__name__
        time.sleep(REFRESH)


@app.on_event("startup")
def _start() -> None:
    threading.Thread(target=_refresh, daemon=True).start()


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


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "model_loaded": _state["model"] is not None}


@app.get("/model")
def model() -> dict:
    return {k: v for k, v in _state.items() if k != "model"} | {"name": NAME, "alias": ALIAS}


@app.post("/predict")
def predict(subs: list[Subscriber]) -> dict:
    with _lock:
        m, version = _state["model"], _state["version"]
    if m is None:
        raise HTTPException(503, "no champion model registered yet")
    df = pd.DataFrame([s.model_dump() for s in subs])
    proba = np.asarray(m.predict(df.drop(columns=["subscriber_id"])))
    if proba.ndim == 2:                      # pyfunc wraps predict_proba: column 1 is P(churn)
        proba = proba[:, 1]
    return {"model_version": version,
            "scores": [{"subscriber_id": s, "churn_probability": round(float(p), 5)} for s, p in zip(df.subscriber_id, proba)]}
