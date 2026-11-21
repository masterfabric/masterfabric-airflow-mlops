"""Feature pipeline, training and evaluation for the churn model (scikit-learn)."""
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from mlops.data import FEATURES_CAT, FEATURES_NUM, LABEL

PARAMS = {"learning_rate": 0.06, "max_iter": 300, "max_leaf_nodes": 31, "min_samples_leaf": 80,
          "l2_regularization": 1.0, "early_stopping": True, "validation_fraction": 0.15, "random_state": 7}


def build_pipeline(params: dict | None = None) -> Pipeline:
    pre = ColumnTransformer([
        ("num", SimpleImputer(strategy="median"), FEATURES_NUM),
        # handle_unknown="ignore": a tariff launched after training becomes all-zeros, not a crash.
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), FEATURES_CAT),
    ])
    return Pipeline([("pre", pre), ("clf", HistGradientBoostingClassifier(**(params or PARAMS)))])


def train(df: pd.DataFrame, params: dict | None = None) -> Pipeline:
    pipe = build_pipeline(params)
    pipe.fit(df[FEATURES_NUM + FEATURES_CAT], df[LABEL])
    return pipe


def evaluate(proba: np.ndarray, y: np.ndarray) -> dict:
    order = np.argsort(-proba)
    top = order[: max(1, len(order) // 10)]
    base = y.mean()
    return {"auc": round(float(roc_auc_score(y, proba)), 4),
            "pr_auc": round(float(average_precision_score(y, proba)), 4),
            "top_decile_lift": round(float(y[top].mean() / base), 3) if base > 0 else None,
            "top_decile_capture": round(float(y[top].sum() / max(y.sum(), 1)), 4),
            "base_rate": round(float(base), 4), "n": int(len(y))}
