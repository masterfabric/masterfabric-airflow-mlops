"""Measure how long the API takes to pick up a champion alias change, then restore it.

Points `churn@champion` at the previous version (a rollback), polls /model until the API answers with it,
then points the alias back and measures again. Writes docs/results/hotswap_demo.json.
"""
from __future__ import annotations

import json
import time
import urllib.request
from pathlib import Path

from mlflow import MlflowClient

API = "http://api:8000"
NAME = "churn"


def model_version() -> str:
    with urllib.request.urlopen(API + "/model", timeout=10) as r:
        return str(json.loads(r.read())["version"])


def swap_to(client: MlflowClient, version: str, timeout: float = 60) -> float:
    client.set_registered_model_alias(NAME, "champion", version)
    t = time.perf_counter()
    while time.perf_counter() - t < timeout:
        if model_version() == version:
            return round(time.perf_counter() - t, 2)
        time.sleep(0.25)
    raise TimeoutError(f"API did not load version {version} within {timeout}s")


def main() -> None:
    client = MlflowClient()
    current = client.get_model_version_by_alias(NAME, "champion").version
    versions = sorted(int(v.version) for v in client.search_model_versions(f"name='{NAME}'"))
    previous = str(max(v for v in versions if v < int(current)))
    out = {"refresh_interval_s": 15, "start_version": current,
           "rollback": {"to": previous, "seconds_until_served": swap_to(client, previous)},
           "restore": {"to": current, "seconds_until_served": swap_to(client, current)},
           "final_version_served": model_version()}
    Path("docs/results/hotswap_demo.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
