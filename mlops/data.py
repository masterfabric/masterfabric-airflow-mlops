"""Synthetic subscriber base that evolves month by month, with churn, drift and a broken feed.

The generator is the "source system" (CRM + billing + network KPIs). It is deterministic for a seed,
so every month can be regenerated identically, and it knows the true churn probability of every
subscriber, which the analysis uses to explain what the models get right and wrong.

What changes over the 12 months (2025-12 .. 2026-11):

- covariate drift: data usage grows ~2.5% a month and 5G devices spread through the base;
- a new tariff, ``postpaid_5g``, appears in 2026-07 (a category no earlier model has seen);
- **concept drift from 2026-06**: a competitor's 5G offer makes heavy data users on 4G devices and
  price-sensitive subscribers far more likely to leave. The features do not change, their meaning does;
- **2026-03 arrives broken**: 9% of ``data_gb`` is null and some fees are negative (an ETL bug upstream).
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

MONTHS = [str(p) for p in pd.period_range("2025-12", "2026-11", freq="M")]
DRIFT_MONTH = "2026-06"
NEW_PLAN_MONTH = "2026-07"
BROKEN_MONTH = "2026-03"
PLANS = ["prepaid", "postpaid_s", "postpaid_m", "postpaid_l"]
PLAN_FEE = {"prepaid": 12.0, "postpaid_s": 22.0, "postpaid_m": 32.0, "postpaid_l": 45.0, "postpaid_5g": 55.0}
REGIONS = ["IST", "ANK", "IZM", "OTHER"]

FEATURES_NUM = ["tenure_months", "monthly_fee", "data_gb", "voice_min", "dropped_call_rate", "complaints_90d",
                "support_calls_30d", "payment_delay_days", "contract_months_left", "device_age_months",
                "family_lines", "discount_pct", "nps"]
FEATURES_CAT = ["plan", "region", "device_5g"]
LABEL = "churned_next_month"


@dataclass
class Base:
    df: pd.DataFrame
    next_id: int


def _new_subscribers(n: int, start_id: int, month_idx: int, rng: np.random.Generator) -> pd.DataFrame:
    plan = rng.choice(PLANS, size=n, p=[0.35, 0.25, 0.25, 0.15])
    p5g = min(0.15 + 0.04 * month_idx, 0.60)
    df = pd.DataFrame({
        "subscriber_id": [f"S{start_id + i:08d}" for i in range(n)],
        "tenure_months": rng.integers(0, 3, n) if month_idx > 0 else rng.integers(0, 96, n),
        "plan": plan,
        "region": rng.choice(REGIONS, size=n, p=[0.45, 0.2, 0.15, 0.2]),
        "device_5g": (rng.random(n) < p5g).astype(int),
        "device_age_months": rng.integers(0, 48, n),
        "family_lines": rng.poisson(0.6, n),
        "discount_pct": rng.choice([0, 0, 0, 5, 10, 15, 20], size=n),
        "contract_months_left": np.where(plan == "prepaid", 0, rng.integers(0, 25, n)),
        "usage_level": rng.lognormal(0.0, 0.6, n),   # latent appetite for data, not a feature
        "price_sensitivity": rng.beta(2, 5, n),       # latent, not a feature
    })
    return df


def _churn_logit(df: pd.DataFrame, month: str) -> np.ndarray:
    z = (-4.2
         + 0.08 * df.payment_delay_days
         + 0.55 * df.complaints_90d
         + 0.25 * df.support_calls_30d
         + 0.7 * (df.contract_months_left == 0)
         - 0.015 * np.minimum(df.tenure_months, 60)
         + 18.0 * df.dropped_call_rate
         + 0.35 * (df.plan == "prepaid")
         - 0.25 * df.family_lines
         - 0.02 * (df.nps - 30) / 10
         - 0.02 * df.discount_pct)
    if month >= DRIFT_MONTH:
        heavy_4g = (df.device_5g == 0) & (df.data_gb > df.data_gb.quantile(0.6))
        z = z + 1.6 * heavy_4g + 2.2 * df.price_sensitivity * (df.monthly_fee > 30)
        z = z - 0.4 * (df.contract_months_left > 6)          # contracts protect a little
    return z.to_numpy()


def _evolve(df: pd.DataFrame, month_idx: int, rng: np.random.Generator) -> pd.DataFrame:
    n = len(df)
    df = df.copy()
    df["tenure_months"] = df.tenure_months + 1
    df["device_age_months"] = df.device_age_months + 1
    upgrade = (df.device_5g == 0) & (rng.random(n) < 0.025 + 0.01 * (df.device_age_months > 30))
    df.loc[upgrade, "device_5g"] = 1
    df.loc[upgrade, "device_age_months"] = 0
    df["contract_months_left"] = np.where(df.plan == "prepaid", 0,
                                          np.where(df.contract_months_left > 0, df.contract_months_left - 1,
                                                   np.where(rng.random(n) < 0.3, 12, 0)))
    trend = 1.025 ** month_idx
    df["data_gb"] = np.round(df.usage_level * 9.0 * trend * (1 + 0.6 * df.device_5g) * rng.lognormal(0, 0.15, n), 2)
    df["voice_min"] = np.round(rng.gamma(3.0, 70.0, n) * (1.1 - 0.02 * month_idx / 12), 0)
    df["dropped_call_rate"] = np.round(np.clip(rng.gamma(2.0, 0.006, n) + 0.01 * (df.region == "OTHER"), 0, 0.2), 4)
    df["complaints_90d"] = rng.poisson(0.25 + 1.2 * df.dropped_call_rate * 10, n)
    df["support_calls_30d"] = rng.poisson(0.3 + 0.3 * df.complaints_90d, n)
    df["payment_delay_days"] = np.where(rng.random(n) < 0.12, rng.integers(1, 45, n), 0)
    df["nps"] = np.clip(np.round(45 - 25 * df.complaints_90d - 200 * df.dropped_call_rate + rng.normal(0, 18, n)), -100, 100)
    df["monthly_fee"] = np.round(df.plan.map(PLAN_FEE) * (1 - df.discount_pct / 100) * rng.normal(1.0, 0.03, n), 2)
    return df


def generate(seed: int = 42, n0: int = 60000) -> dict[str, pd.DataFrame]:
    """All monthly snapshots with labels and the true churn probability."""
    rng = np.random.default_rng(seed)
    base = Base(_new_subscribers(n0, 0, 0, rng), n0)
    out: dict[str, pd.DataFrame] = {}
    for i, month in enumerate(MONTHS):
        if month == NEW_PLAN_MONTH:   # migrate some heavy postpaid users to the new 5G tariff
            m = (base.df.plan.isin(["postpaid_m", "postpaid_l"])) & (base.df.device_5g == 1) & (rng.random(len(base.df)) < 0.35)
            base.df.loc[m, "plan"] = "postpaid_5g"
        snap = _evolve(base.df, i, rng)
        p = 1 / (1 + np.exp(-_churn_logit(snap, month)))
        churn = rng.random(len(snap)) < p
        snap["true_churn_prob"] = np.round(p, 5)
        snap[LABEL] = churn.astype(int)
        snap.insert(0, "month", month)
        out[month] = snap.drop(columns=["usage_level", "price_sensitivity"]).reset_index(drop=True)
        # Next month's base: churners leave, joiners arrive (~1% net growth).
        stay = snap.loc[~churn, base.df.columns.intersection(snap.columns).tolist()]
        stay = stay.assign(usage_level=base.df.loc[~churn, "usage_level"].values,
                           price_sensitivity=base.df.loc[~churn, "price_sensitivity"].values)
        joiners = _new_subscribers(int(churn.sum() + 0.01 * len(snap)), base.next_id, i + 1, rng)
        base = Base(pd.concat([stay, joiners], ignore_index=True), base.next_id + len(joiners))
    return out


def snapshot(month: str, seed: int = 42) -> tuple[pd.DataFrame, pd.DataFrame]:
    """What the source system delivers for `month`: features (no label), and the labels of that month,
    which in reality only become known one month later."""
    snap = generate(seed)[month]
    feats = snap[["month", "subscriber_id"] + FEATURES_NUM + FEATURES_CAT].copy()
    if month == BROKEN_MONTH:
        rng = np.random.default_rng(seed + 303)
        feats.loc[rng.random(len(feats)) < 0.09, "data_gb"] = np.nan
        feats.loc[rng.random(len(feats)) < 0.004, "monthly_fee"] *= -1
    labels = snap[["month", "subscriber_id", LABEL, "true_churn_prob"]].copy()
    return feats, labels
