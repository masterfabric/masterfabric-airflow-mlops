import numpy as np
import pandas as pd
import pytest

from mlops import data, drift, model, quality


@pytest.fixture(scope="module")
def months():
    return data.generate(seed=42, n0=20000)


def test_generator_is_deterministic():
    a = data.generate(seed=1, n0=3000)
    b = data.generate(seed=1, n0=3000)
    for m in data.MONTHS:
        pd.testing.assert_frame_equal(a[m], b[m])


def test_base_evolves_and_churn_rises_after_the_competitor_offer(months):
    sizes = [len(months[m]) for m in data.MONTHS]
    assert sizes == sorted(sizes)                               # ~1% net growth every month
    before = np.mean([months[m][data.LABEL].mean() for m in data.MONTHS if m < data.DRIFT_MONTH])
    after = np.mean([months[m][data.LABEL].mean() for m in data.MONTHS if m >= data.DRIFT_MONTH])
    assert 0.02 < before < 0.05 and after > before * 1.3


def test_new_tariff_appears_in_july(months):
    assert "postpaid_5g" not in set(months["2026-06"].plan)
    assert "postpaid_5g" in set(months[data.NEW_PLAN_MONTH].plan)


def test_snapshot_hides_labels_and_breaks_march():
    feats, labels = data.snapshot("2026-02", seed=42)
    assert data.LABEL not in feats.columns and data.LABEL in labels.columns
    bad, _ = data.snapshot(data.BROKEN_MONTH, seed=42)
    assert bad.data_gb.isna().mean() > 0.05 and (bad.monthly_fee < 0).any()


def test_quality_gate_passes_good_and_fails_broken_month():
    good, _ = data.snapshot("2026-02", seed=42)
    s = quality.summarise(quality.run_checks(good, prev_rows=len(good)))
    assert s["passed"], s["critical_failures"]
    bad, _ = data.snapshot(data.BROKEN_MONTH, seed=42)
    s = quality.summarise(quality.run_checks(bad))
    assert not s["passed"]
    assert set(s["critical_failures"]) == {"null_rate_max", "monthly_fee_non_negative"}


def test_quality_gate_catches_duplicates_and_schema_loss():
    good, _ = data.snapshot("2026-02", seed=42)
    dup = pd.concat([good, good.head(50)])
    assert "subscriber_id_unique" in quality.summarise(quality.run_checks(dup))["critical_failures"]
    assert "schema_complete" in quality.summarise(quality.run_checks(good.drop(columns=["nps"])))["critical_failures"]


def test_psi_is_zero_for_same_data_and_flags_a_shift():
    rng = np.random.default_rng(0)
    a = pd.Series(rng.normal(0, 1, 20000))
    assert drift.psi_numeric(a, a) < 1e-6
    assert drift.psi_numeric(a, a + 1.0) > 0.25
    c = pd.Series(["x"] * 900 + ["y"] * 100)
    assert drift.psi_categorical(c, pd.Series(["x"] * 500 + ["y"] * 300 + ["z"] * 200)) > 0.25


def test_model_learns_and_tolerates_unseen_tariff(months):
    train = pd.concat([months[m] for m in data.MONTHS[:3]])
    pipe = model.train(train)
    test = months["2026-08"]                                   # contains postpaid_5g, unseen in training
    p = pipe.predict_proba(test[data.FEATURES_NUM + data.FEATURES_CAT])[:, 1]
    m = model.evaluate(p, test[data.LABEL].to_numpy())
    assert m["auc"] > 0.6 and m["top_decile_lift"] > 1.5
