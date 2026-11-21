# 0008. Evidently reports drift and quality; the in-house PSI still decides

- Status: Accepted
- Date: 2026-11-21

## Context

The DAG retrains on in-house PSI > 0.25 (ADR 0004). Reviewers and on-call engineers need a richer
view: per-column distributions, the realised classification metrics, a history across months. Evidently
gives that out of the box, but its defaults are not the DAG's rule, and Feast 0.68 (ADR 0009) needs
NumPy 2 while the Airflow 2.10.5 image pins NumPy 1.26.

## Options

| Option | For | Against |
|---|---|---|
| Replace the PSI rule with Evidently's verdict | One tool | Changes the decision rule after the fact; the defaults flag every month here |
| **Evidently as a parallel, observe-only task** | Reports and history without touching the decision | Two drift numbers to explain |
| Hand-written HTML reports | No dependency | Re-implementing what Evidently already does |

## Decision

`evidently_report` runs after `check_drift` and `realise_last_month` with `trigger_rule=none_failed`
and is not upstream of `decide`. It writes per-column drift (Evidently default methods), Evidently's
PSI on the same data, and the realised quality of last month's scores to `evidently_drift` /
`evidently_quality`, an HTML report per month to `s3://lake/reports/evidently/`, and a run stamped with
the business month to the Evidently workspace (UI on 127.0.0.1:38085). Evidently and Feast live in a
separate virtualenv in the Airflow image (`/home/airflow/mf-venv`) and run as `BashOperator`s.

## Consequences

- Measured over 2026-06..11: the default detector (Wasserstein / Jensen-Shannon > 0.1) flagged 3–7 of
  16 columns in **every** month, including the two quiet months the DAG kept the champion. At 58k–66k
  rows a 0.1 distance is easy to cross; as a retrain trigger it would retrain monthly.
- Evidently's PSI agrees with the in-house trigger in 5 of 6 months. Categorical PSI matches to 0.0008;
  numeric PSI differs by up to 0.33 because Evidently bins on equal widths and the in-house PSI on
  reference deciles (long-tailed integer features such as `device_age_months` move most).
- Realised ROC AUC from Evidently equals the in-house value in all 6 months (difference 0.0).
- One report takes about 1 s. The task never blocks the decision; if it fails, the month still runs.
