"""Charts from the measured backfill results in docs/results (matplotlib, dark theme)."""
from __future__ import annotations

import json

import numpy as np

from mfviz import HEX, IMG, RESULTS, mpl_setup

plt = mpl_setup()
R = json.loads((RESULTS / "backfill.json").read_text())
MONTHS = R["months"]
ACT = {"keep": HEX["GREY"], "halted": HEX["RED"], "promoted": HEX["GREEN"], "rejected": HEX["ORANGE"]}


def realised_auc():
    rows = R["realised"]
    ms = sorted({r["month"] for r in rows})
    auto = {r["month"]: r for r in rows if r["policy"] == "auto"}
    static = {r["month"]: r for r in rows if r["policy"] == "static"}
    fig, ax = plt.subplots(figsize=(13, 5.2))
    ax.plot(MONTHS, [R["oracle_auc"][m] for m in MONTHS], color=HEX["PURPLE"], ls=":", lw=2, marker="o", ms=4,
            label="oracle: generator's true probabilities (upper bound)")
    ax.plot(ms, [static[m]["auc"] for m in ms], color=HEX["GREY"], lw=2.2, marker="s",
            label="static: first champion v1, never retrained")
    ax.plot(ms, [auto[m]["auc"] for m in ms], color=HEX["GREEN"], lw=2.6, marker="o",
            label="auto: champion chosen by the DAG")
    for m in ms:
        ax.annotate(f"v{auto[m]['model_version']}\n{auto[m]['auc']:.3f}", (m, auto[m]["auc"]), textcoords="offset points",
                    xytext=(0, -30), ha="center", fontsize=9, color=HEX["GREEN"])
        if static[m]["model_version"] != auto[m]["model_version"]:
            ax.annotate(f"{static[m]['auc']:.3f}", (m, static[m]["auc"]), textcoords="offset points", xytext=(0, -16),
                        ha="center", fontsize=9, color=HEX["GREY"])
    for d in R["decisions"]:
        if d["action"] in ("promoted", "halted", "rejected"):
            ax.axvline(d["month"], color=ACT[d["action"]], alpha=0.35, lw=6)
    ax.set_ylim(0.74, 0.87)
    ax.set_ylabel("AUC on next month's realised churn labels")
    ax.set_title("Realised AUC per month (labels arrive one month later). Bands: halted (red), "
                 "promoted (green), rejected (orange)", loc="left")
    ax.legend(loc="lower left", ncols=1)
    fig.tight_layout()
    fig.savefig(IMG / "realised_auc.png")
    plt.close(fig)


def decision_timeline():
    dec = {d["month"]: d for d in R["decisions"]}
    secs = R["task_seconds_per_month"]
    fig, (ax, ax2) = plt.subplots(2, 1, figsize=(13, 4.8), gridspec_kw={"height_ratios": [1.3, 1]}, sharex=True)
    for i, m in enumerate(MONTHS):
        d = dec[m]
        ax.scatter(i, 0, s=900, color=ACT[d["action"]], zorder=3)
        ax.text(i, 0.0, d["action"][0].upper(), ha="center", va="center", fontsize=13, color=HEX["BG"], weight="bold")
        why = d["reasons"]
        if d["action"] == "keep" and "waiting_for_labels" in d["details"]:
            why = ["waiting for", "labels"]
        elif d["action"] == "keep":
            why = [f"v{d['champion_after']} kept", "no trigger"]
        elif d["action"] == "promoted":
            why = [f"v{d['champion_after']}"] + [r.replace("_", " ") for r in why]
        elif d["action"] == "rejected":
            why = [f"v{d['challenger_version']} rejected"] + [r.replace("_", " ") for r in why]
        elif d["action"] == "halted":
            why = ["DQ gate"] + why[1:]
        ax.text(i, -0.35, "\n".join(w.replace("_", " ") for w in why[:4]), ha="center", va="top", fontsize=8.5,
                color=HEX["WHITE"])
    ax.set_ylim(-0.75, 0.3)
    ax.set_yticks([])
    ax.grid(False)
    ax.set_title("What the monthly DAG decided (K keep, H halted, P promoted, R rejected)", loc="left")
    ax2.bar(range(len(MONTHS)), [secs[m] for m in MONTHS], color=HEX["ICE"])
    for i, m in enumerate(MONTHS):
        ax2.text(i, secs[m] + 0.2, f"{secs[m]:.1f}", ha="center", fontsize=9)
    ax2.set_ylabel("task seconds")
    ax2.set_ylim(0, max(secs.values()) * 1.35)
    ax2.set_xticks(range(len(MONTHS)), MONTHS, rotation=0, fontsize=9)
    fig.tight_layout()
    fig.savefig(IMG / "decision_timeline.png")
    plt.close(fig)


def psi_heatmap():
    rows = R["drift"]
    feats = sorted(rows[-1]["psi"], key=lambda f: -max(r["psi"].get(f, 0) for r in rows))
    ms = [r["month"] for r in rows]
    z = np.array([[r["psi"].get(f, np.nan) for r in rows] for f in feats])
    fig, ax = plt.subplots(figsize=(13, 6.2))
    im = ax.imshow(z, cmap="magma", aspect="auto", vmin=0, vmax=max(0.4, np.nanmax(z)))
    for i in range(len(feats)):
        for j in range(len(ms)):
            v = z[i, j]
            ax.text(j, i, f"{v:.2f}", ha="center", va="center", fontsize=8.5,
                    color=HEX["BG"] if v > 0.2 else HEX["WHITE"], weight="bold" if v >= 0.25 else "normal")
    ax.set_xticks(range(len(ms)), [f"{m}\n(vs v{r['champion_version']})" for m, r in zip(ms, rows)], fontsize=9)
    ax.set_yticks(range(len(feats)), feats)
    ax.set_title("PSI of each feature: current month vs the champion's training data. A value ≥ 0.25 "
                 "triggers a retrain", loc="left")
    ax.grid(False)
    fig.colorbar(im, ax=ax, fraction=0.025, pad=0.01)
    fig.tight_layout()
    fig.savefig(IMG / "psi_heatmap.png")
    plt.close(fig)


def registry():
    reg = R["registry"]
    fig, ax = plt.subplots(figsize=(13, 4.2))
    for r in reg:
        t = r["tags"]
        rejected = "rejected_reason" in t
        ax.bar(f"v{r['version']}\ntrained {t['trained_in_month']}\nholdout {t['holdout_month']}",
               float(t["holdout_auc"]), color=HEX["ORANGE"] if rejected else HEX["GREEN"])
        ax.text(len(ax.patches) - 1, float(t["holdout_auc"]) + 0.002, t["holdout_auc"] + (" (rejected)" if rejected else ""),
                ha="center", fontsize=10)
    ax.set_ylim(0.76, 0.84)
    ax.set_ylabel("holdout AUC")
    ax.set_title("MLflow model versions of 'churn': holdout = the most recent labelled month, never seen in training",
                 loc="left")
    fig.tight_layout()
    fig.savefig(IMG / "registry_versions.png")
    plt.close(fig)


if __name__ == "__main__":
    IMG.mkdir(parents=True, exist_ok=True)
    realised_auc()
    decision_timeline()
    psi_heatmap()
    registry()
    print("charts written to", IMG)
