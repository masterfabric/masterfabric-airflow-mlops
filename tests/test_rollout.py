"""Canary routing, guardrails and the paired AUC decision (no stack needed)."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

from mlops.routing import bucket, is_canary

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import rollout  # noqa: E402

IDS = [f"S{i:08d}" for i in range(20_000)]


def test_bucket_is_stable_and_in_range():
    assert all(0 <= bucket(i) < 100 for i in IDS[:1000])
    assert [bucket(i) for i in IDS[:50]] == [bucket(i) for i in IDS[:50]]


def test_share_close_to_target():
    for pct in (5, 25, 50):
        share = 100 * np.mean([is_canary(i, pct) for i in IDS])
        assert abs(share - pct) < 1.0


def test_raising_percentage_keeps_existing_canaries():
    s5 = {i for i in IDS if is_canary(i, 5)}
    s25 = {i for i in IDS if is_canary(i, 25)}
    assert s5 <= s25 and not any(is_canary(i, 0) for i in IDS[:100])


def _log(n, corr, err=0, seed=0):
    rng = np.random.default_rng(seed)
    shadow = rng.uniform(0.01, 0.2, n)
    canary = shadow if corr else rng.permutation(shadow)
    can = pd.DataFrame({"subscriber_id": [f"c{i}" for i in range(n)], "variant": "canary", "probability": canary,
                        "shadow_probability": shadow, "latency_ms": 5.0})
    if err:
        can.loc[:err - 1, "variant"] = "canary_fallback"
    ch = pd.DataFrame({"subscriber_id": [f"h{i}" for i in range(n * 19)], "variant": "champion",
                       "probability": 0.1, "shadow_probability": np.nan, "latency_ms": 5.0})
    return pd.concat([can, ch], ignore_index=True)


def test_guardrails_pass_identical_model():
    assert rollout.guardrails(_log(500, True), 5)["passed"]


def test_guardrails_catch_rank_scramble_and_errors():
    g = rollout.guardrails(_log(500, False), 5)
    assert not g["checks"]["rank_agreement"] and g["checks"]["mean_prediction"]   # same mean, wrong order
    assert not rollout.guardrails(_log(500, True, err=10), 5)["checks"]["errors"]


def test_paired_auc_ci_contains_zero_for_same_model():
    rng = np.random.default_rng(1)
    y = rng.integers(0, 2, 2000)
    p = np.clip(y * 0.3 + rng.uniform(0, 0.7, 2000), 0, 1)
    df = pd.DataFrame({"subscriber_id": range(2000), "variant": "canary", "probability": p, "shadow_probability": p})
    r = rollout.paired_auc(df, pd.DataFrame({"subscriber_id": range(2000), "churned_next_month": y}), n_boot=200)
    assert r["delta"] == 0 and r["ci95"][0] <= 0 <= r["ci95"][1]
