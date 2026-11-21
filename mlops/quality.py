"""Data-quality gate for a monthly snapshot. Critical failures stop the month; warnings are recorded."""
from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd

from mlops.data import FEATURES_CAT, FEATURES_NUM, PLANS

KNOWN_PLANS = set(PLANS) | {"postpaid_5g"}


@dataclass
class Check:
    rule: str
    severity: str          # critical | warning
    passed: bool
    observed: float
    threshold: str


def run_checks(df: pd.DataFrame, prev_rows: int | None = None) -> list[Check]:
    n = len(df)
    c: list[Check] = []

    def add(rule, sev, ok, obs, thr):
        c.append(Check(rule, sev, bool(ok), float(obs), thr))

    missing_cols = [x for x in ["subscriber_id"] + FEATURES_NUM + FEATURES_CAT if x not in df.columns]
    add("schema_complete", "critical", not missing_cols, len(missing_cols), "0 missing columns")
    if missing_cols:
        return c
    add("row_count_min", "critical", n >= 10000, n, ">= 10000")
    if prev_rows:
        change = abs(n - prev_rows) / prev_rows
        add("row_count_change", "warning", change <= 0.2, round(change, 4), "<= 20% vs previous month")
    add("subscriber_id_unique", "critical", df.subscriber_id.is_unique, int(df.subscriber_id.duplicated().sum()), "0 duplicates")
    worst_null = df[FEATURES_NUM + FEATURES_CAT].isna().mean().max()
    add("null_rate_max", "critical", worst_null <= 0.01, round(worst_null, 4), "<= 1% per column")
    add("monthly_fee_non_negative", "critical", (df.monthly_fee.dropna() >= 0).all(), int((df.monthly_fee < 0).sum()), "0 negative")
    add("dropped_call_rate_range", "critical", df.dropped_call_rate.between(0, 1).all(), float(df.dropped_call_rate.max()), "0..1")
    add("nps_range", "critical", df.nps.between(-100, 100).all(), float(df.nps.abs().max()), "-100..100")
    add("tenure_non_negative", "critical", (df.tenure_months >= 0).all(), int((df.tenure_months < 0).sum()), "0 negative")
    unknown = set(df.plan.unique()) - KNOWN_PLANS
    add("plan_known", "warning", not unknown, len(unknown), "only catalogued tariffs")
    add("device_5g_binary", "critical", df.device_5g.isin([0, 1]).all(), int((~df.device_5g.isin([0, 1])).sum()), "0 or 1")
    return c


def summarise(checks: list[Check]) -> dict:
    crit = [x for x in checks if x.severity == "critical" and not x.passed]
    warn = [x for x in checks if x.severity == "warning" and not x.passed]
    return {"passed": not crit, "rules": len(checks), "critical_failures": [x.rule for x in crit],
            "warnings": [x.rule for x in warn], "checks": [asdict(x) for x in checks]}
