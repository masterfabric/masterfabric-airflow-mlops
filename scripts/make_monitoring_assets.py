"""Charts and the diagram for the Evidently / Feast / canary addition, from docs/results/*.json."""
from __future__ import annotations

import json

import numpy as np

from mfviz import (GREEN, GREY, HEX, ICE, IMG, ORANGE, PURPLE, RED, RESULTS, WHITE, YELLOW, arrow, box, canvas,
                   font, mpl_setup)

plt = mpl_setup()
EV = json.loads((RESULTS / "evidently.json").read_text())
FE = json.loads((RESULTS / "feast.json").read_text())
RO = json.loads((RESULTS / "rollout.json").read_text())
BY = json.loads((RESULTS / "api_byid.json").read_text())


def evidently_vs_psi():
    rows = EV["drift"]
    ms = [r["month"] for r in rows]
    x = np.arange(len(ms))
    a = [len(r["evidently_default_drifted"]) for r in rows]
    b = [len(r["evidently_psi_over_025"]) for r in rows]
    c = [len(r["in_house_psi_over_025"]) for r in rows]
    fig, ax = plt.subplots(figsize=(13, 5.2))
    w = 0.26
    ax.bar(x - w, a, w, color=HEX["GREY"], label="Evidently default (Wasserstein / Jensen-Shannon > 0.1)")
    ax.bar(x, b, w, color=HEX["ICE"], label="Evidently PSI > 0.25 (equal-width bins)")
    ax.bar(x + w, c, w, color=HEX["GREEN"], label="in-house PSI > 0.25 (decile bins), the DAG's trigger")
    for i, r in enumerate(rows):
        ax.text(i, max(a[i], b[i], c[i]) + 0.35, f"{r['decision']}\nchampion v{r['champion_version']}", ha="center",
                fontsize=10, color=HEX["WHITE"])
        for dx, v in ((-w, a[i]), (0, b[i]), (w, c[i])):
            ax.text(i + dx, v + 0.08, str(v), ha="center", fontsize=9, color=HEX["GREY"])
    ax.set_xticks(x, ms)
    ax.set_ylim(0, 10)
    ax.set_ylabel("columns flagged as drifted (of 16)")
    ax.set_title("Same months, three drift detectors: the default flags every month; only PSI > 0.25 matches "
                 "the retrain decisions", loc="left")
    ax.legend(loc="upper right")
    fig.tight_layout()
    fig.savefig(IMG / "evidently_vs_psi.png")
    plt.close(fig)


def feast_results():
    fig, (ax, ax2) = plt.subplots(1, 2, figsize=(13, 5.4), gridspec_kw={"width_ratios": [1.25, 1]})
    sh = FE["naive_latest_join"]["share_diff_by_feature"]
    items = sorted(sh.items(), key=lambda kv: kv[1])
    ax.barh([k for k, _ in items], [100 * v for _, v in items], color=HEX["ORANGE"])
    for i, (_, v) in enumerate(items):
        ax.text(100 * v + 1, i, f"{100 * v:.1f}%", va="center", fontsize=9, color=HEX["GREY"])
    ax.set_xlim(0, 115)
    ax.set_xlabel("% of 194,913 training rows with a different value")
    n = FE["naive_latest_join"]
    ax.set_title(f"Naive join to the latest snapshot vs point-in-time\n"
                 f"{n['rows_all_features_null']:,} rows all-null (all {n['churners']:,} churners); Feast: 0 differ",
                 loc="left", fontsize=12)
    lat = FE["online_latency"]
    labels = ["Feast HTTP\n1 id", "Feast SDK\n1 id", "API by-id\n1 id", "Feast HTTP\n100 ids", "Feast SDK\n100 ids",
              "API by-id\n100 ids"]
    p50 = [lat["http_1"]["p50_ms"], lat["sdk_1"]["p50_ms"], BY["predict_by_id_1"]["p50_ms"],
           lat["http_100"]["p50_ms"], lat["sdk_100"]["p50_ms"], BY["predict_by_id_100"]["p50_ms"]]
    p95 = [lat["http_1"]["p95_ms"], lat["sdk_1"]["p95_ms"], BY["predict_by_id_1"]["p95_ms"],
           lat["http_100"]["p95_ms"], lat["sdk_100"]["p95_ms"], BY["predict_by_id_100"]["p95_ms"]]
    x = np.arange(len(labels))
    ax2.bar(x - 0.2, p50, 0.4, color=HEX["ICE"], label="p50")
    ax2.bar(x + 0.2, p95, 0.4, color=HEX["PURPLE"], label="p95")
    for i in range(len(x)):
        ax2.text(i + 0.2, p95[i] + 0.2, f"{p95[i]:.1f}", ha="center", fontsize=9, color=HEX["GREY"])
    ax2.set_xticks(x, labels, fontsize=9)
    ax2.set_ylabel("ms (Valkey online store, compose network)")
    ax2.set_title("Online feature fetch and fetch + score", loc="left", fontsize=12)
    ax2.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(IMG / "feast_results.png")
    plt.close(fig)


def canary_rollout():
    fig, (ax, ax2) = plt.subplots(1, 2, figsize=(13, 5.2))
    for c, col in zip(RO["candidates"], (HEX["RED"], HEX["GREEN"])):
        st = c["stages"]
        lab = f"v{c['version']} ({'shuffled labels' if 'broken' in c['role'] else 'normal recipe'}): {c['outcome'].replace('_', ' ')}"
        ax.plot([s["target_pct"] for s in st], [s["spearman"] for s in st], marker="o", ms=10, lw=2.5, color=col,
                label=lab)
        for s in st:
            ax.annotate(f"{s['spearman']:.3f}", (s["target_pct"], s["spearman"]), textcoords="offset points",
                        xytext=(0, 10), ha="center", fontsize=10, color=col)
    ax.axhline(RO["limits"]["spearman_min"], color=HEX["YELLOW"], ls="--", lw=1.5, label="guardrail ≥ 0.80")
    ax.set_xticks(RO["stages_pct"], [f"{p}%" for p in RO["stages_pct"]])
    ax.set_xlim(0, 55)
    ax.set_ylim(-0.3, 1.15)
    ax.set_xlabel("canary stage (share of subscribers)")
    ax.set_ylabel("Spearman: canary vs shadow champion")
    ax.set_title("Guardrail: rank agreement on identical requests", loc="left", fontsize=12)
    ax.legend(loc="lower right", fontsize=9)
    good = [c for c in RO["candidates"] if c["outcome"] == "promoted"][0]
    pa = good["paired_auc"]
    s50 = good["stages"][-1]["unpaired_ab"]
    names = ["unpaired:\nchampion arm", "unpaired:\ncanary arm", "paired: v4\n(shadow)", "paired: v5\n(canary)"]
    vals = [s50["champion"]["auc"], s50["canary"]["auc"], pa["auc_champion_same_rows"], pa["auc_canary"]]
    cols = [HEX["GREY"], HEX["GREY"], HEX["ICE"], HEX["GREEN"]]
    ax2.bar(range(4), vals, color=cols)
    for i, v in enumerate(vals):
        ax2.text(i, v + 0.001, f"{v:.4f}", ha="center", fontsize=10, color=HEX["WHITE"])
    ax2.set_xticks(range(4), names, fontsize=9)
    ax2.set_ylim(0.81, 0.845)
    ax2.set_ylabel("AUC on 2026-10 realised labels")
    ax2.set_title(f"50% stage: paired ΔAUC {pa['delta']:+.4f}, 95% CI [{pa['ci95'][0]:+.4f}, {pa['ci95'][1]:+.4f}]"
                  f"\n> −0.005 → promoted. Unpaired arms differ by population, not model", loc="left", fontsize=12)
    fig.tight_layout()
    fig.savefig(IMG / "canary_rollout.png")
    plt.close(fig)


def monitoring_diagram():
    im, d = canvas(1600, 900)
    d.text((60, 40), "Monitoring, features and rollout (profile: monitoring)", font=font(36, 800), fill=WHITE)
    d.text((60, 90), "Evidently and Feast run in a separate virtualenv in the Airflow image; the API routes a sticky canary.",
           font=font(20, 450), fill=GREY)
    box(d, (50, 170, 400, 560), "Airflow churn_monthly", ["dq_gate → publish_features", "check_drift + realise",
                                                         "  → evidently_report", "  (observe only, never gates)",
                                                         "decide · train · score", "", "Evidently and Feast:",
                                                         "/home/airflow/mf-venv", "(Feast needs NumPy 2)"], ICE, line_size=18)
    box(d, (500, 150, 880, 400), "Feast 0.68", ["offline: gated snapshots", "  (parquet, point-in-time)",
                                               "online: Valkey 8.1", "feature server :36566", "UI :38888"], ORANGE, line_size=18)
    box(d, (500, 450, 880, 660), "Evidently 0.7", ["drift vs champion sample", "quality of last month's scores",
                                                  "HTML → S3 reports/", "workspace → UI :38085"], PURPLE, line_size=18)
    box(d, (980, 150, 1550, 470), "FastAPI scoring", ["@champion answers by default", "@canary + tag canary_percent",
                                                     "sha256(subscriber_id) bucket → sticky", "canary rows also scored by champion",
                                                     "/predict/by-id fetches from Feast"], GREEN, line_size=18)
    box(d, (1010, 560, 1550, 860), "scripts/rollout.py", ["stages 5 → 25 → 50 %", "guardrails from predictions_log:",
                                                        "  errors · p95 · Spearman · mean · share",
                                                        "fail → rollback (remove alias)",
                                                        "pass → paired ΔAUC CI > −0.005 → promote"], YELLOW, line_size=18)
    box(d, (500, 720, 880, 870), "PostgreSQL mlops", ["evidently_drift · evidently_quality", "predictions_log"], RED,
        line_size=18)
    arrow(d, (400, 270), (500, 270), ORANGE, "publish")
    arrow(d, (400, 520), (500, 550), PURPLE, "report")
    arrow(d, (880, 300), (980, 300), ORANGE, "features")
    arrow(d, (690, 660), (690, 720), PURPLE)
    arrow(d, (1280, 560), (1280, 470), YELLOW, "registry tags")
    d.line([(1000, 470), (930, 470), (930, 760)], fill=GREEN, width=3)
    arrow(d, (930, 760), (880, 760), GREEN)
    d.text((938, 600), "async", font=font(15, 600), fill=GREEN)
    d.text((938, 620), "log", font=font(15, 600), fill=GREEN)
    arrow(d, (1010, 830), (880, 830), YELLOW)
    im.save(IMG / "monitoring_rollout.png", optimize=True)


if __name__ == "__main__":
    evidently_vs_psi()
    feast_results()
    canary_rollout()
    monitoring_diagram()
