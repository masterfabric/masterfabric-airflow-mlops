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

## Addition: Evidently, Feast and a canary rollout (2026-11-21)

Started from `798e173`. Infrastructure first (a separate virtualenv for Feast and Evidently, the
`monitoring` compose profile), then the two DAG tasks and their measurements, then the serving changes.
ADR 0007 with the guardrail limits was committed **before** the rollout run. Times are Europe/Istanbul (UTC+3).

| # | Commit | Time | Change |
|---|---|---|---|
| 1 | `156b84e` | 2026-11-21 10:56 | Feast and Evidently in a separate virtualenv in the Airflow image (Feast needs NumPy 2) and a monitor image |
| 2 | `6f80551` | 2026-11-21 10:56 | Compose profile monitoring: Valkey online store, Feast feature server and UI, Evidently UI |
| 3 | `afe0e14` | 2026-11-21 10:56 | Evidently drift and realised-quality reports per month, cross-checked against the in-house PSI and AUC |
| 4 | `2c4bd41` | 2026-11-21 10:56 | Feast feature repo and publishing: gated snapshots to the offline store, latest month to Valkey |
| 5 | `20ab895` | 2026-11-21 11:01 | Evidently UI runs as the Airflow uid so both can write the shared workspace |
| 6 | `e6282c6` | 2026-11-21 11:01 | DAG: evidently_report (observe only) and publish_features tasks in the monitoring virtualenv |
| 7 | `9940c91` | 2026-11-21 11:01 | Run the two new tasks for the 12 existing months with airflow tasks test |
| 8 | `9cdcdd4` | 2026-11-21 11:01 | Evidently vs in-house PSI and AUC: 5/6 trigger agreement, default detector flags every month |
| 9 | `88afdfa` | 2026-11-21 11:02 | Feast measurements: point-in-time frame equals the direct join (0 of 194,913 rows differ), naive latest join loses every churner, online = offline |
| 10 | `b82bb41` | 2026-11-21 11:08 | API: sticky hash-routed canary with shadow champion scoring, async prediction log, /predict/by-id via Feast |
| 11 | `e7c3306` | 2026-11-21 11:08 | ADR 0007: canary stages and guardrail limits, declared before the rollout run; log per-model inference time |
| 12 | `8c2e940` | 2026-11-21 11:09 | Register rollout candidates: v5 (normal recipe, one month newer) and v6 (shuffled training labels) |
| 13 | `3cc9622` | 2026-11-21 11:13 | Canary rollout run: v6 rolled back at 5% (rank correlation -0.09), v5 passed 5/25/50% and was promoted (paired dAUC -0.0007, CI [-0.0031, 0.0016]) |
| 14 | `d0e8def` | 2026-11-21 11:13 | Measure /predict/by-id (Feast fetch + model) against features in the body; scores identical |
| 15 | `f44b6e9` | 2026-11-21 11:14 | Move sticky routing to mlops/routing.py; tests for routing, guardrails and the paired AUC (14 pass) |
| 16 | `2647d70` | 2026-11-21 11:16 | Charts and diagram for drift detectors, Feast point-in-time and latency, canary rollout |
| 17 | `2d4be6a` | 2026-11-21 11:21 | Evidently runs stamped with their business month, tagged, idempotent per month; dashboard panels for drift count and AUC |
| 18 | `90a2758` | 2026-11-21 11:21 | Screenshots: Evidently dashboard, report list and 2026-07 drift report, Feast UI, registry after the rollout, Airflow graph |
| 19 | `27e8df2` | 2026-11-21 11:21 | Evidently summary after the workspace rebuild (timings only change) |
| 20 | `1b99bee` | 2026-11-21 11:22 | Makefile: monitoring profile is part of the stack; monitoring, candidates and rollout targets |
| 21 | `775b495` | 2026-11-21 11:22 | ADRs 0008 (Evidently observes, PSI decides) and 0009 (Feast point-in-time), 0007 results, NOTICE for the new dependencies |
| 22 | `7728b3c` | 2026-11-21 11:24 | README: Evidently, Feast and canary results with Figures 7-10 and 17-23, quickstart, walkthrough, limitations |
| 23 | (this commit) | | docs(timeline): this section |

Verification at the end of this addition:

| Check | Result |
|---|---|
| Unit tests (`make test`) | 14 passed |
| DAG import errors | none; 12 tasks including `evidently_report` and `publish_features` |
| New tasks for the 12 existing months | all succeeded (`scripts/backfill_monitoring.sh`); `publish_features` skipped for the halted 2026-03 |
| Mermaid in README | renders (mermaid-cli) |
| License check of the new virtualenv (121 packages) | permissive except psycopg2-binary (LGPL-3.0) and certifi / tqdm / pathspec (MPL-2.0), all unmodified; listed in NOTICE |
| Stack after the run | stopped with `make down` (volumes kept) |

Found while running it:

- The Evidently UI container first ran as root and created the workspace, so the Airflow task (uid
  50000) could not write to it. The UI now runs as uid 50000.
- Feast UI needs `grpcio`; the `feast[redis,grpcio]` extra is installed.
- Running `docker compose` without `--env-file .env` recreated the services with empty credentials;
  the stack was brought back with the Makefile's command, and volumes were not affected.
- The first latency guardrail would have compared a canary request (canary plus shadow champion) with a
  champion-only request; the log now stores each model's own inference time.
