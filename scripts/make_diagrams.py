"""Render the architecture, DAG/decision and Cloudera-mapping diagrams (Pillow)."""
from __future__ import annotations

from mfviz import GREEN, GREY, ICE, IMG, ORANGE, PANEL, PURPLE, RED, WHITE, YELLOW, arrow, box, canvas, font


def architecture():
    im, d = canvas(1600, 900)
    d.text((60, 40), "MasterFabric Airflow MLOps · architecture", font=font(36, 800), fill=WHITE)
    d.text((60, 90), "One Airflow run per month decides whether the churn model is retrained; the API follows the registry alias.",
           font=font(20, 450), fill=GREY)
    box(d, (50, 170, 360, 450), "Monthly feeds", ["subscriber snapshot", "churn labels (1 month late)", "60k → 66k rows",
                                                  "drift from 2026-06", "new tariff 2026-07", "broken feed 2026-03"], YELLOW, line_size=17)
    box(d, (440, 150, 860, 560), "Airflow 2.10 (LocalExecutor)", ["churn_monthly · @monthly", "catchup, max_active_runs=1",
                                                                   "land → dq_gate (11 rules)", "realise last month's AUC",
                                                                   "PSI drift vs champion data", "decide: decay · drift · age",
                                                                   "train challenger · compare", "promote via alias · score"],
        ICE, line_size=17)
    box(d, (940, 150, 1290, 420), "MLflow 2.22", ["experiment churn-training", "registry: churn", "@champion",
                                                 "@static_baseline", "artifacts proxied to S3"], PURPLE, line_size=17)
    box(d, (1340, 150, 1560, 420), "FastAPI", ["/predict", "/model", "polls alias", "every 15 s", "hot-swap"], GREEN, line_size=17)
    box(d, (440, 620, 860, 860), "RustFS S3", ["lake/landing/…/month=YYYY-MM", "lake/scores/churn/…", "lake/reference/churn/vN",
                                              "mlflow/ (model artifacts)"], ORANGE, line_size=17)
    box(d, (940, 520, 1290, 860), "PostgreSQL 16", ["airflow (metadata)", "mlflow (tracking + registry)",
                                                   "mlops: dq_results", "  drift · realised", "  decisions"], RED, line_size=17)
    box(d, (50, 560, 360, 860), "Consumers", ["CRM retention list", "  (monthly batch scores)", "online channels",
                                              "  (API, p50 2.7 ms)", "reports · README charts"], GREY, line_size=17)
    arrow(d, (360, 310), (440, 310), YELLOW, "land")
    arrow(d, (860, 280), (940, 280), PURPLE, "log · register")
    arrow(d, (1290, 285), (1340, 285), GREEN)
    arrow(d, (650, 560), (650, 620), ORANGE, "parquet")
    arrow(d, (830, 560), (940, 600), RED, "monitoring", at=0.55)
    arrow(d, (1115, 420), (1115, 520), RED, "backend")
    arrow(d, (1040, 420), (800, 620), ORANGE, "artifacts", at=0.35)
    arrow(d, (440, 760), (360, 760), ORANGE, "scores")
    d.line([(1450, 420), (1450, 888), (200, 888)], fill=GREEN, width=3)
    arrow(d, (200, 888), (200, 860), GREEN)
    d.text((1180, 862), "online scoring (POST /predict)", font=font(16, 600), fill=GREEN)
    im.save(IMG / "architecture.png", optimize=True)


def dag_flow():
    im, d = canvas(1600, 900)
    d.text((60, 40), "churn_monthly: one month, one decision", font=font(36, 800), fill=WHITE)
    d.text((60, 90), "dags/churn_monthly.py · logic in mlops/pipeline.py · every decision is written to mlops.decisions",
           font=font(20, 450), fill=GREY)
    box(d, (50, 330, 290, 520), "land", ["snapshot + labels", "→ S3 parquet"], YELLOW, line_size=18)
    box(d, (370, 170, 690, 400), "dq_gate", ["11 rules (9 critical)", "schema · nulls ≤ 1%", "ranges · uniqueness",
                                            "row count · tariffs"], ICE, line_size=18)
    box(d, (370, 470, 690, 640), "halt_month", ["no drift, no training,", "no scoring this month"], RED, line_size=18)
    box(d, (370, 700, 690, 860), "realise_last_month", ["AUC of last month's", "scores, auto & static"], PURPLE, line_size=18)
    box(d, (770, 170, 1070, 400), "check_drift", ["PSI per feature", "vs the champion's", "training sample"], ORANGE, line_size=18)
    box(d, (1150, 170, 1550, 470), "decide", ["no champion + 4 labelled", "realised < holdout − 0.03",
                                             "max PSI > 0.25", "champion ≥ 6 months old", "any → retrain, else keep"],
        YELLOW, line_size=18)
    box(d, (1150, 530, 1550, 760), "train_and_promote", ["HGB on last 3 labelled months", "holdout: newest labelled month",
                                                        "promote if AUC ≥ champion", "  + 0.002 → @champion"], GREEN, line_size=18)
    box(d, (770, 530, 1070, 760), "score", ["auto (@champion)", "static (@static_baseline)", "→ lake/scores/…"], ICE, line_size=18)
    arrow(d, (290, 400), (370, 300), GREY)
    arrow(d, (290, 470), (370, 780), GREY)
    arrow(d, (530, 400), (530, 470), RED, "fail")
    arrow(d, (690, 285), (770, 285), GREEN, "pass")
    arrow(d, (1070, 285), (1150, 285), GREY)
    d.line([(690, 780), (730, 780), (730, 450)], fill=PURPLE, width=3)
    arrow(d, (730, 450), (1150, 450), PURPLE, "realised AUC", at=0.5)
    arrow(d, (1350, 470), (1350, 530), GREEN, "retrain")
    arrow(d, (1150, 640), (1070, 640), GREY)
    arrow(d, (1150, 380), (1000, 530), GREY, "keep", at=0.25)
    im.save(IMG / "dag_flow.png", optimize=True)


def cloudera_mapping():
    im, d = canvas(1600, 760)
    d.text((60, 40), "How the lab maps to Cloudera CDP (CDE + CML)", font=font(36, 800), fill=WHITE)
    rows = [("Orchestration", "Airflow 2.10, LocalExecutor", "Cloudera Data Engineering (managed Airflow)"),
            ("Feature data", "parquet on RustFS S3", "Iceberg tables on Ozone/HDFS via SDX"),
            ("Data quality gate", "mlops/quality.py (11 rules)", "Great Expectations / Deequ jobs in CDE"),
            ("Training", "sklearn HGB in the worker", "CML Jobs or CDE Spark (Spark MLlib, XGBoost)"),
            ("Tracking + registry", "MLflow 2.22 + aliases", "CML Experiments + Model Registry"),
            ("Online serving", "FastAPI polling @champion", "CML Model Endpoints / Cloudera AI Inference"),
            ("Monitoring", "PSI + realised AUC in Postgres", "CML model metrics · Data Visualization dashboards"),
            ("Security", "loopback-only, lab creds", "Ranger, Kerberos, Knox, Atlas lineage")]
    y = 120
    d.text((80, y), "Concern", font=font(20, 700), fill=GREY)
    d.text((420, y), "This lab", font=font(20, 700), fill=ICE)
    d.text((900, y), "Cloudera CDP", font=font(20, 700), fill=ORANGE)
    y += 44
    for i, (a, b, c) in enumerate(rows):
        if i % 2 == 0:
            d.rounded_rectangle((60, y - 8, 1540, y + 62), radius=12, fill=PANEL)
        d.text((80, y + 12), a, font=font(21, 700), fill=WHITE)
        d.text((420, y + 12), b, font=font(20, 450), fill=WHITE)
        d.text((900, y + 12), c, font=font(20, 450), fill=WHITE)
        y += 72
    im.save(IMG / "cloudera_mapping.png", optimize=True)


if __name__ == "__main__":
    architecture()
    dag_flow()
    cloudera_mapping()
    print("diagrams written to", IMG)
