# 0011. Drift on served traffic triggers retraining, guarded by new labels

- Status: Accepted
- Date: 2026-11-23

## Context

The monthly DAG (ADR 0004) checks drift once a month on the landed snapshot. Served traffic can drift
in between, and the only signal the API has is the inputs it scores. But retraining needs labels, and
labels arrive a month late: in the monthly run of 2026-07, a drift-triggered challenger trained
before the new tariff lost to the champion (0.7816 vs 0.7819).

## Decision

- The API keeps the features of a stable 20 % sample of subscribers (`request_features`).
- `drift_watch` DAG (every 10 minutes, created paused) computes PSI on the newest 2,000 sampled rows
  served by the current champion against that champion's training reference, with the monthly
  limit 0.25.
- It retrains (same `pipeline.challenger` as the monthly DAG: 3-month window, newest labelled
  holdout, gain ≥ 0.002) only if the breach holds for 2 consecutive checks, ≥ 1,000 rows exist, and a
  labelled month exists that the last drift-triggered attempt did not have. Otherwise it records
  `suppressed_no_new_labels` and keeps alerting.

## Evidence

`make drift-watch` (`docs/results/drift_watch.json`): stale champion v1, 2,500 subscribers per check.
2026-05 traffic: PSI 0.19, no action. 2026-07 traffic from subscriber 10,000: breach at 20,000 (PSI
0.266), retrain at 22,500 rejected (v10 0.7816 vs 0.7819, the same model as the monthly v2), then
suppressed. 2026-08 traffic with 2026-07 labels available: retrain promoted v11 (0.819 vs 0.7796, the
same as the monthly v3); the API served v11 4.1 s later. On 2026-08 labels: v1 0.7818, v11 0.8188.
After the swap PSI against v11's reference stayed above 0.25 (the tariff kept spreading) and the
guard suppressed every further retrain. In the first run the guard was per champion, and the watcher
retrained v11's recipe again at once (identical AUC, rejected); the guard is now global.

## Consequences

- With monthly labels the watcher cannot promote earlier than the monthly run; it reaches the same
  models. It pays off when labels arrive more often than the schedule, and as a drift alarm.
- Sustained drift with no new labels keeps alerting without retraining: a human or a labelling
  process has to act.
- Sampled request features add one insert per sampled subscriber to Postgres.
