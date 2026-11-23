# Root-cause report: churn v6 vs champion v5

- Rollback reason: stage 5%: rank_agreement
- Replayed month: 2026-10, stage 5 %
- **Verdict: the candidate was trained on labels that do not match the lake: label corruption in the training job (shuffled, misjoined or leaked from another table)**
- Generated in 2.1 s

| # | Hypothesis | Status | Finding |
|---|---|---|---|
| 1 | infrastructure | ruled out | canary errors 0.00%, p95 6.0 ms vs champion 27.45 ms |
| 2 | population | ruled out | routed 3,381 vs 62,893 subscribers: max PSI 0.0053 (voice_min) |
| 3 | contract | ruled out | identical input signature |
| 4 | training_inputs | ruled out | training months 2026-06,2026-07,2026-08 vs champion's 2026-06,2026-07,2026-08: max PSI 0.0000 (tenure_months) |
| 5 | model_signal | **SUPPORTED** | registered holdout AUC 0.4414 on 2026-09 - no better than chance |
| 6 | training_labels | **SUPPORTED** | reproduction from lineage scores 0.8297 on 2026-09, the registered artefact 0.4414 (gap +0.3883); the artefact scores 0.4484 AUC on its own training months' labels |

Rank agreement with the champion on all 2026-10 subscribers: Spearman -0.0715.

| Feature (champion's top 5) | Candidate follows it (Spearman) | Champion follows it |
|---|---:|---:|
| payment_delay_days | -0.151 | 0.406 |
| contract_months_left | 0.115 | -0.377 |
| complaints_90d | -0.022 | 0.369 |
| nps | -0.009 | -0.281 |
| tenure_months | -0.026 | -0.265 |
