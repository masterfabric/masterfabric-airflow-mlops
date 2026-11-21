"""Summarise the Evidently tables next to the in-house drift/quality tables -> docs/results/evidently.json.

Run inside the Airflow container's monitoring venv:
    docker exec mf-mlops-airflow-scheduler /home/airflow/mf-venv/bin/python /opt/airflow/repo/scripts/evidently_summary.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mlops import data, db  # noqa: E402

THRESHOLD = 0.25          # the DAG's PSI retrain trigger (ADR 0004)
OUT = Path(__file__).resolve().parents[1] / "docs/results/evidently.json"


def main() -> None:
    ev = {r["month"]: r for r in db.query("SELECT * FROM evidently_drift ORDER BY month")}
    ih = {r["month"]: r for r in db.query("SELECT * FROM drift ORDER BY month")}
    dec = {r["month"]: r["action"] for r in db.query("SELECT month, action FROM decisions")}
    months, diffs_cat, diffs_num = [], [], []
    for m, e in ev.items():
        psi_ev, psi_ih = e["psi_method"], ih[m]["psi"]
        flag_ev = sorted(k for k, v in psi_ev.items() if v > THRESHOLD)
        flag_ih = sorted(k for k, v in psi_ih.items() if v > THRESHOLD)
        for k in psi_ev:
            (diffs_cat if k in data.FEATURES_CAT else diffs_num).append(abs(psi_ev[k] - psi_ih[k]))
        months.append({
            "month": m, "champion_version": e["champion_version"], "decision": dec.get(m),
            "evidently_default_drifted": sorted(k for k, c in e["per_column"].items() if c["drifted"]),
            "evidently_default_methods": sorted({c["method"] for c in e["per_column"].values()}),
            "evidently_psi_max": max(psi_ev.items(), key=lambda kv: kv[1]),
            "in_house_psi_max": [ih[m]["max_feature"], round(ih[m]["max_psi"], 4)],
            "evidently_psi_over_025": flag_ev, "in_house_psi_over_025": flag_ih,
            "trigger_agrees": bool(flag_ev) == bool(flag_ih), "seconds": e["seconds"]})
    q = db.query("SELECT month, model_version, roc_auc, in_house_auc FROM evidently_quality ORDER BY month")
    out = {
        "threshold_psi": THRESHOLD,
        "drift": months,
        "summary": {
            "months": len(months),
            "psi_trigger_agreement": f"{sum(x['trigger_agrees'] for x in months)}/{len(months)}",
            "months_with_default_drift_flags": sum(bool(x["evidently_default_drifted"]) for x in months),
            "months_in_house_trigger": sum(bool(x["in_house_psi_over_025"]) for x in months),
            "max_abs_psi_diff_categorical": round(max(diffs_cat), 4),
            "max_abs_psi_diff_numeric": round(max(diffs_num), 4),
            "max_abs_auc_diff": round(max(abs(r["roc_auc"] - r["in_house_auc"]) for r in q), 6),
            "mean_report_seconds": round(sum(x["seconds"] for x in months) / len(months), 2)},
        "quality": [{k: (round(v, 4) if isinstance(v, float) else v) for k, v in r.items()} for r in q],
    }
    OUT.write_text(json.dumps(out, indent=2) + "\n")
    print(json.dumps(out["summary"], indent=2))
    for x in months:
        print(x["month"], x["decision"], "default:", x["evidently_default_drifted"], "| psi>0.25 ev/ih:",
              x["evidently_psi_over_025"], x["in_house_psi_over_025"])


if __name__ == "__main__":
    main()
