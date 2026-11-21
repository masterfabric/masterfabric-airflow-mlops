# Build timeline

How this repo was built: infrastructure first, then the generator, quality gate, drift and model, then the monthly DAG, the 12-month backfill, the scoring API demos, images, ADRs and documentation. Fixes found while running the system (numpy pin, API signature types) are separate commits.

Every step below is one commit, in order. Times are Europe/Istanbul (UTC+3).

| # | Commit | Time | Change |
|---|---|---|---|
| 1 | `a7bc140` | 2026-11-21 03:16 | chore: scaffold repository with AGPL-3.0 license and community files |
| 2 | `820dc4d` | 2026-11-21 03:21 | feat(infra): add Airflow, MLflow, PostgreSQL, S3 storage and a scoring API |
| 3 | `103d358` | 2026-11-21 03:23 | fix(infra): pin numpy 1.26 so Airflow's pandas build keeps working |
| 4 | `153949b` | 2026-11-21 03:23 | feat(ml): add evolving subscriber data, DQ gate, PSI drift and churn model |
| 5 | `45d8afb` | 2026-11-21 03:23 | feat(pipeline): add the monthly Airflow DAG with champion/challenger retraining |
| 6 | `6661360` | 2026-11-21 03:29 | fix(serving): match model signature types and take P(churn) column |
| 7 | `dcf3952` | 2026-11-21 03:29 | feat(report): export backfill decisions, metrics and registry to JSON |
| 8 | `4fa3368` | 2026-11-21 03:29 | docs(results): add measured 12-month backfill results |
| 9 | `18c875b` | 2026-11-21 03:29 | feat(serving): add API demo that times requests and checks parity |
| 10 | `56ede8a` | 2026-11-21 03:30 | docs(results): add measured API latency and parity results |
| 11 | `d78ddba` | 2026-11-21 03:30 | feat(serving): measure alias-driven rollback and restore |
| 12 | `fbc9fb9` | 2026-11-21 03:30 | build(make): add demo target and complete .PHONY list |
| 13 | `c84d035` | 2026-11-21 03:32 | chore(viz): add shared MasterFabric visual helpers |
| 14 | `67ccdf1` | 2026-11-21 03:32 | docs(images): add charts for realised AUC, decisions, PSI and registry |
| 15 | `fc6a8b9` | 2026-11-21 03:35 | feat(docs): add Playwright screenshot capture for the running lab |
| 16 | `b0315e6` | 2026-11-21 03:35 | docs(screenshots): add Airflow, MLflow, API and S3 console captures |
| 17 | `4a44be8` | 2026-11-21 03:36 | docs(images): add architecture, DAG flow and Cloudera mapping diagrams |
| 18 | `5f1d2f7` | 2026-11-21 03:37 | docs(hero): add 1600x900 hero from real screenshots and results |
| 19 | `ae5e663` | 2026-11-21 03:39 | docs(adr): 0001 monthly Airflow runs with ordered catchup |
| 20 | `20b7d9b` | 2026-11-21 03:39 | docs(adr): 0002 blocking data-quality gate |
| 21 | `24287a8` | 2026-11-21 03:39 | docs(adr): 0003 registry aliases as deployment contract |
| 22 | `7c5d0e6` | 2026-11-21 03:39 | docs(adr): 0004 retrain on decay, drift or age |
| 23 | `4963483` | 2026-11-21 03:39 | docs(adr): 0005 sliding window, newest-month holdout, minimum gain |
| 24 | `9ba2fcb` | 2026-11-21 03:39 | docs(adr): 0006 alias-polling API with hot swap |
| 25 | `d89eb9c` | 2026-11-21 03:39 | docs(adr): add ADR index |
| 26 | `87eb9ed` | 2026-11-21 03:41 | docs(readme): full README with results, figures, ADRs and limitations |
| 27 | `20c6544` | 2026-11-21 03:41 | docs(contributing): add contribution guide |

The final commit adds this file; see `git log` for the full messages, which explain
the why of each step (including the fixes found by running the system).
