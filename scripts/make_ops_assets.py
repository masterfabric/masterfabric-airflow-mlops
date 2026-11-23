"""Charts for the root-cause report and the drift watcher.

    .venv/bin/python scripts/make_ops_assets.py
Reads docs/results/rca_v6.json, rca_v5.json and drift_watch.json.
"""
from __future__ import annotations

import json

from mfviz import HEX, IMG, RESULTS, mpl_setup


def load(name):
    return json.loads((RESULTS / f"{name}.json").read_text())


def rca() -> None:
    plt = mpl_setup()
    bad, good = load("rca_v6"), load("rca_v5")
    fig, ax = plt.subplots(1, 2, figsize=(16, 5.6), gridspec_kw={"width_ratios": [1.15, 1]})
    names = [k["hypothesis"] for k in bad["checks"]]
    col = {"ruled_out": HEX["GREEN"], "supported": HEX["RED"], "inconclusive": HEX["GREY"]}
    for j, (r, label) in enumerate(((bad, f"v{bad['version']} (rolled back)"), (good, f"v{good['version']} (control)"))):
        for i, k in enumerate(r["checks"]):
            ax[0].add_patch(plt.Rectangle((j - 0.42, len(names) - 1 - i - 0.38), 0.84, 0.76, color=col[k["status"]]))
            ax[0].text(j, len(names) - 1 - i, k["status"].replace("_", " "), ha="center", va="center", fontsize=11,
                       color="black", weight="bold")
    ax[0].set_yticks(range(len(names)), names[::-1])
    ax[0].set_xticks([0, 1], [f"v{bad['version']} (rolled back)", f"v{good['version']} (passed, control)"])
    ax[0].set_xlim(-0.6, 1.6)
    ax[0].set_ylim(-0.6, len(names) - 0.4)
    ax[0].grid(False)
    ax[0].set_title(f"Hypotheses checked in {bad['seconds']} s: root cause = {bad['root_cause']}", loc="left")
    tl = bad["checks"][-1]["evidence"]
    gl = good["checks"][-1]["evidence"]
    groups = ["reproduction\nfrom lineage", "registered\nartefact", "artefact on its own\ntraining labels"]
    xs = range(3)
    for off, e, c, lbl in ((-0.18, tl, HEX["RED"], f"v{bad['version']}"), (0.18, gl, HEX["GREEN"], f"v{good['version']}")):
        vals = [e["reproduction_auc"], e["registered_auc"], e["artefact_auc_on_training_labels"]]
        b = ax[1].bar([x + off for x in xs], vals, width=0.34, color=c, label=lbl)
        for bb, v in zip(b, vals):
            ax[1].text(bb.get_x() + bb.get_width() / 2, v, f"{v:.3f}", ha="center", va="bottom", fontsize=9)
    ax[1].axhline(0.5, color=HEX["GREY"], ls="--", lw=1, label="chance")
    ax[1].set_xticks(list(xs), groups)
    ax[1].set_ylim(0, 1)
    ax[1].set_ylabel("AUC")
    ax[1].set_title("The deciding check: retrain from the version's own lineage tags", loc="left")
    ax[1].legend(fontsize=9)
    fig.suptitle("Automatic root-cause report after the v6 canary rollback", x=0.01, ha="left", fontsize=13)
    fig.tight_layout()
    fig.savefig(IMG / "rca_report.png")
    plt.close(fig)


def drift_watch() -> None:
    plt = mpl_setup()
    d = load("drift_watch")
    ch = d["checks"]
    fig, ax = plt.subplots(figsize=(15, 5.6))
    x = [c["sent"] / 1000 for c in ch]
    y = [c["max_psi"] if c["max_psi"] is not None else float("nan") for c in ch]
    ax.plot(x, y, "o-", color=HEX["ICE"], label="max feature PSI, served traffic vs champion's training data")
    ax.axhline(0.25, color=HEX["RED"], ls="--", lw=1, label="PSI limit 0.25 (same as the monthly DAG)")
    start = 0
    for month, n in d["phases"]:
        end = start + n * d["chunk"] / 1000
        ax.axvspan(start, end, color=HEX["GREY"], alpha=0.06 if month == "2026-07" else 0.0)
        ax.text((start + end) / 2, 0.02, f"traffic {month}", ha="center", color=HEX["GREY"])
        start = end
    mark = {"breach_pending": ("breach 1/2", HEX["YELLOW"]), "retrain_rejected": ("retrain: rejected", HEX["ORANGE"]),
            "retrain_promoted": ("retrain: promoted", HEX["GREEN"]),
            "suppressed_no_new_labels": ("suppressed: no new labels", HEX["PURPLE"]),
            "insufficient_rows": ("< 1,000 rows", HEX["GREY"])}
    seen = set()
    for c in ch:
        if c["action"] in mark:
            lbl, col = mark[c["action"]]
            yy = c["max_psi"] if c["max_psi"] is not None else 0.08
            ax.scatter(c["sent"] / 1000, yy, s=160, color=col, zorder=3, label=None if lbl in seen else lbl)
            seen.add(lbl)
            if c["action"].startswith("retrain"):
                det = c["details"]
                ax.annotate(f"v{c['challenger']}: {det['challenger_auc']} vs {det['champion_auc']}\n"
                            f"train {','.join(m[2:] for m in det['train_months'])}, holdout {det['holdout_month'][2:]}",
                            (c["sent"] / 1000, yy), xytext=(40, -62 if c["action"] == "retrain_rejected" else 26),
                            textcoords="offset points", ha="center", fontsize=9)
    for c in ch:
        if c.get("api_swap_s"):
            ax.annotate(f"API serves v{c['challenger']} {c['api_swap_s']} s later;\nwindow restarts for the new champion",
                        (c["sent"] / 1000 + 1.2, 0.12), fontsize=9, color=HEX["GREEN"])
    ax.axvline(d["drift_onset_sent"] / 1000, color=HEX["GREY"], lw=0.8, ls=":")
    ax.set_xlabel("subscribers scored by the API (thousands); a watcher run after every 2,500")
    ax.set_ylabel("max PSI")
    ax.set_ylim(0, 0.45)
    ax.legend(fontsize=9, loc="upper left", ncol=2)
    ax.set_title(f"Drift watcher on a stale champion (v{d['stale_champion']}): AUC on 2026-08 labels "
                 f"v{d['stale_champion']} {d['auc_2026_08'][f'v' + d['stale_champion']]} → "
                 + " ".join(f"{k} {v}" for k, v in d["auc_2026_08"].items() if k != 'v' + d['stale_champion']), loc="left")
    fig.tight_layout()
    fig.savefig(IMG / "drift_watch.png")
    plt.close(fig)


if __name__ == "__main__":
    rca()
    drift_watch()
    print("ok")
