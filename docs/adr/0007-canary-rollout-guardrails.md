# 0007. Champion changes go through a sticky canary with pre-declared guardrails

- Status: Accepted
- Date: 2026-11-21

## Context

ADR 0003/0006 promote a challenger by moving `@champion` once it beats the champion on one offline
holdout month. That catches a weaker model, but not a model that is fine offline and wrong online
(a broken feature pipeline, a mislabelled training set, a slow artefact). The rule below was written
and committed **before** the rollout experiment was run, so the thresholds were not tuned to the result.

## Options

| Option | For | Against |
|---|---|---|
| Direct alias move (current) | Instant, simple | Every subscriber hits a bad model at once |
| Random per-request split | Even traffic | A subscriber flips between models; no clean comparison |
| **Sticky hash split + shadow champion** | Same subscriber, same variant; paired comparison on identical inputs | Champion runs twice on canary traffic |
| Separate canary deployment behind the gateway | Isolation | A second service to run; no paired scores |

## Decision

- `@canary` alias + registered-model tag `canary_percent`; the API routes `sha256(subscriber_id)`
  buckets below the percentage to the canary and also scores them with the champion (shadow).
- Stages 5 % → 25 % → 50 %. After each stage the guardrails are read from `predictions_log`:

| Guardrail | Limit |
|---|---|
| Canary errors (fallbacks) | ≤ 0.5 % of canary rows |
| Canary p95 inference latency | ≤ 1.5 × champion p95 + 5 ms |
| Spearman rank correlation, canary vs shadow champion | ≥ 0.80 |
| Mean predicted churn, canary vs shadow champion | relative difference ≤ 25 % |
| Routed share | within ±1.5 points of the target; no subscriber changes variant |

- Any failure: `canary_percent` → 0 and the alias is removed (rollback); the candidate gets a
  `rollback_reason` tag.
- Promotion after 50 % only if, on the realised labels of the replayed month, the paired AUC difference
  (canary − shadow champion on the same canary subscribers, 1,000 bootstrap resamples) has a 95 % CI
  lower bound > −0.005 (non-inferiority).

## Consequences

- Measured (replay of 66,274 subscribers of 2026-10 per stage, thresholds unchanged from this ADR):
  - v6, trained on shuffled labels: rolled back at 5 % on rank agreement (Spearman −0.088). Its mean
    prediction was within 11 % of the champion's, so the mean guardrail alone would have let it through.
    The API reported the canary gone 2.9 s after the alias was removed.
  - v5, the normal recipe one month newer: passed 5/25/50 % (Spearman 0.969, routed share 5.10 /
    25.20 / 50.05 %, no subscriber changed variant, 0 errors). Paired ΔAUC on 33,173 canary subscribers
    −0.0007, 95 % CI [−0.0031, +0.0016] → non-inferior → promoted; v5 answered 7.8 s later.
  - Unpaired arms at 50 % differ by 0.0105 AUC (0.8347 vs 0.8242) only because the two halves of the
    population differ; on the same rows the models are within 0.001.
- The replay uses the latest month with labels; in production the labels arrive a month after the
  canary, so the AUC step would be a second, delayed gate while the guardrails run live.
