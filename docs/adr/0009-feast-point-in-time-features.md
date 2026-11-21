# 0009. Feast for point-in-time training frames and an online store

- Status: Accepted
- Date: 2026-11-21

## Context

Training joins each month's labels to that month's snapshot by hand (`pipeline._frame`). That is
correct only because the code always passes the right month. Online channels also want to score a
subscriber by id, without sending sixteen features in the request.

## Options

| Option | For | Against |
|---|---|---|
| Keep the hand-written join | No new service | Correctness depends on every caller; no online store |
| **Feast: file offline store + Valkey online store** | Point-in-time joins by timestamp; same definitions online and offline; small | One more service and a registry file |
| A managed feature store (Tecton, Databricks, CDP) | Operated for you | Not runnable on a laptop |

## Decision

`feature_repo/` defines entity `subscriber`, feature view `subscriber_monthly` (16 features, TTL 31
days) on a parquet export of the **gated** snapshots, and feature service `churn_model`. The DAG task
`publish_features` (after `checks_passed`) rewrites the offline export, applies the definitions and
materialises the month into Valkey 8.1. The API's `/predict/by-id` reads from the Feast feature server.

## Consequences

- Measured: Feast's point-in-time frame for 2026-07..09 equals the hand-written join on all 194,913
  rows; it takes 3.7–4.0 s against 0.09 s for the direct join.
- A naive join to the latest snapshot would give every row at least one wrong-time value and leave
  29,878 rows (15.3 %) with no features at all, including all 11,179 churners: the label leaks.
- The halted month 2026-03 is not exported; within the 31-day TTL Feast serves 2026-02 values for it,
  and 2,858 subscribers new in March get no features. The DAG never trains on 2026-03 anyway.
- Online equals offline for 1,000 sampled subscribers. Fetch latency over the compose network: HTTP
  p50 1.1 ms for one id, 6.1–6.5 ms for 100; the SDK 1.7 ms and 4.4–4.6 ms.
- Training still uses `_frame`; switching it to Feast is a one-line change, kept out of this step so the
  published decisions stay reproducible.
