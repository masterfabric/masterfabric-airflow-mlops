"""Population Stability Index per feature: how far this month's inputs moved from the training data."""
from __future__ import annotations

import numpy as np
import pandas as pd

from mlops.data import FEATURES_CAT, FEATURES_NUM

EPS = 1e-4


def psi_numeric(ref: pd.Series, cur: pd.Series, bins: int = 10) -> float:
    ref, cur = ref.dropna(), cur.dropna()
    edges = np.unique(np.quantile(ref, np.linspace(0, 1, bins + 1)))
    if len(edges) < 3:
        return 0.0
    edges[0], edges[-1] = -np.inf, np.inf
    r = np.histogram(ref, edges)[0] / len(ref) + EPS
    c = np.histogram(cur, edges)[0] / len(cur) + EPS
    return float(np.sum((c - r) * np.log(c / r)))


def psi_categorical(ref: pd.Series, cur: pd.Series) -> float:
    cats = sorted(set(ref.astype(str)) | set(cur.astype(str)))
    r = ref.astype(str).value_counts(normalize=True).reindex(cats, fill_value=0).to_numpy() + EPS
    c = cur.astype(str).value_counts(normalize=True).reindex(cats, fill_value=0).to_numpy() + EPS
    return float(np.sum((c - r) * np.log(c / r)))


def psi_report(ref: pd.DataFrame, cur: pd.DataFrame) -> dict[str, float]:
    out = {f: round(psi_numeric(ref[f], cur[f]), 4) for f in FEATURES_NUM}
    out |= {f: round(psi_categorical(ref[f], cur[f]), 4) for f in FEATURES_CAT}
    return out
