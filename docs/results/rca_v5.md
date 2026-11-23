# Root-cause report: churn v5 vs champion v4

- Rollback reason: none (not rolled back)
- Replayed month: 2026-10, stage 5 %
- **Verdict: no anomaly found: the candidate looks sound**
- Generated in 2.3 s

| # | Hypothesis | Status | Finding |
|---|---|---|---|
| 1 | infrastructure | inconclusive | no guardrail numbers given |
| 2 | population | ruled out | routed 3,381 vs 62,893 subscribers: max PSI 0.0053 (voice_min) |
| 3 | contract | ruled out | identical input signature |
| 4 | training_inputs | ruled out | training months 2026-06,2026-07,2026-08 vs champion's 2026-05,2026-06,2026-07: max PSI 0.0108 (plan) |
| 5 | model_signal | ruled out | registered holdout AUC 0.8297 on 2026-09 |
| 6 | training_labels | ruled out | reproduction from lineage scores 0.8297 on 2026-09, the registered artefact 0.8297 (gap +0.0000); the artefact scores 0.8511 AUC on its own training months' labels |

Rank agreement with the champion on all 2026-10 subscribers: Spearman 0.9692.

| Feature (champion's top 5) | Candidate follows it (Spearman) | Champion follows it |
|---|---:|---:|
| payment_delay_days | 0.406 | 0.426 |
| complaints_90d | 0.369 | 0.394 |
| contract_months_left | -0.377 | -0.387 |
| nps | -0.281 | -0.309 |
| tenure_months | -0.265 | -0.276 |
