# Architecture decision records

Each record states the context, the options that were weighed, the decision and its measured
consequences. They are written for a reader who knows the Cloudera world (CDE Airflow, CML) and wants
to see where this lab deliberately differs and why.

| # | Decision |
|---|---|
| [0001](0001-airflow-monthly-catchup.md) | One Airflow run per month, backfilled in order with catchup |
| [0002](0002-data-quality-gate-halts.md) | A blocking data-quality gate that halts the month |
| [0003](0003-mlflow-registry-aliases.md) | MLflow registry aliases as the deployment contract |
| [0004](0004-retrain-triggers.md) | Retrain on evidence: decay, drift or age |
| [0005](0005-sliding-window-holdout.md) | Sliding 3-month window, newest labelled month as holdout, minimum gain |
| [0006](0006-serving-hot-swap.md) | Online API resolves @champion and hot-swaps without a restart |
| [0007](0007-canary-rollout-guardrails.md) | Champion changes go through a sticky canary with pre-declared guardrails |
| [0008](0008-evidently-observes-psi-decides.md) | Evidently reports drift and quality; the in-house PSI still decides |
| [0009](0009-feast-point-in-time-features.md) | Feast for point-in-time training frames and an online store |
| [0010](0010-automatic-root-cause-report.md) | A rolled-back canary gets an automatic root-cause report |
| [0011](0011-drift-triggered-retraining.md) | Drift on served traffic triggers retraining, guarded by new labels |
