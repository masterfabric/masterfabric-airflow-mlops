# 0010. A rolled-back canary gets an automatic root-cause report

- Status: Accepted
- Date: 2026-11-23

## Context

ADR 0007 rolls a candidate back when a guardrail fails. The guardrail says *that* the candidate
disagreed with the champion (v6: Spearman −0.088 at 5 %), not *why*. The on-call engineer then
repeats the same checks by hand: was it the serving path, the routed population, the input
contract, the training data, the model, or its labels?

## Decision

- `mlops/rca.py` runs one check per hypothesis, in the order they are usually ruled out:
  infrastructure (guardrail errors and latency), population (PSI between routed and non-routed
  subscribers), contract (MLflow input signatures), training inputs (PSI between the candidate's and
  the champion's training months), model signal (registered holdout AUC < 0.55) and training labels
  (retrain from the version's own lineage tags on the lake's labels; score the artefact on the labels
  of the months it claims to have learnt from).
- Each check returns `ruled_out`, `supported` or `inconclusive` with its numbers. The most specific
  supported hypothesis is the root cause.
- It only uses what the platform records anyway; it never reads how the candidate was produced
  (the experiment's `role` tag is ignored).
- `scripts/rollout.py` runs it on every rollback, saves `docs/results/rca_v<version>.{json,md}` and
  tags the version `rca_root_cause`.

## Evidence

v6 re-rolled at 5 % against champion v5 (`docs/results/rca_v6.md`): rolled back on rank agreement
again (Spearman −0.073); report generated in 2.1 s. Infrastructure, population (max PSI 0.0053),
contract and training inputs ruled out; model signal supported (holdout AUC 0.4414); training labels
supported: a reproduction from v6's lineage tags scores 0.8297 on 2026-09, the artefact 0.4414, and
the artefact scores 0.4484 on its own training months' labels. Verdict: label corruption in the
training job, which is what was injected (shuffled labels). Control: the same report on v5 rules out
every hypothesis (reproduction 0.8297 = registered 0.8297).

## Consequences

- The deciding check retrains a model (2 s here, minutes to hours at production scale). It needs
  complete lineage tags (`train_months`, `holdout_month`); without them it says `inconclusive`.
- Six hypotheses cover the failures this lab can produce. A candidate that is wrong only on a segment,
  or a feature computed differently online than offline, needs further checks (per-segment rank
  agreement, the Feast online/offline comparison of ADR 0009).
