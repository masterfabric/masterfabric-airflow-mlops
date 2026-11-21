"""Register a rollout candidate without touching @champion.

    good:   the normal recipe on 2026-06..08, holdout 2026-09 (one month newer than champion v4)
    broken: the same months with the training labels shuffled (a mislabelled-training-set incident)

The replay month (2026-10) is outside both training windows.
Run in the Airflow container:  python /opt/airflow/repo/scripts/train_candidate.py good|broken
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import mlflow
import numpy as np
from mlflow.models import infer_signature

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mlops import data, model, pipeline  # noqa: E402

TRAIN, HOLD = ["2026-06", "2026-07", "2026-08"], "2026-09"


def main(kind: str) -> None:
    X = data.FEATURES_NUM + data.FEATURES_CAT
    tr, ho = pipeline._frame(TRAIN), pipeline._frame([HOLD])
    if kind == "broken":
        tr[data.LABEL] = np.random.default_rng(13).permutation(tr[data.LABEL].to_numpy())
    mlflow.set_experiment("churn-rollout-candidates")
    with mlflow.start_run(run_name=f"candidate-{kind}"):
        pipe = model.train(tr)
        ev = model.evaluate(pipe.predict_proba(ho[X])[:, 1], ho[data.LABEL].to_numpy())
        mlflow.log_params(model.PARAMS | {"train_months": ",".join(TRAIN), "holdout_month": HOLD, "kind": kind})
        mlflow.log_metrics({f"holdout_{k}": v for k, v in ev.items() if v is not None})
        s = tr[X].sample(n=5, random_state=1)
        info = mlflow.sklearn.log_model(pipe, "model", registered_model_name=pipeline.MODEL,
                                        signature=infer_signature(s, pipe.predict_proba(s)[:, 1]),
                                        input_example=s.head(2), pyfunc_predict_fn="predict_proba")
    v = str(info.registered_model_version)
    c = pipeline._client()
    for k, val in {"role": f"rollout_candidate_{kind}", "holdout_auc": ev["auc"], "holdout_month": HOLD,
                   "train_months": ",".join(TRAIN)}.items():
        c.set_model_version_tag(pipeline.MODEL, v, k, str(val))
    print(json.dumps({"kind": kind, "version": v, "holdout_auc": ev["auc"]}))


if __name__ == "__main__":
    main(sys.argv[1])
