"""Root-cause report for any registered version against a champion (the rollout runs it on rollback).

    python /opt/airflow/repo/scripts/rca.py 5 --champion 4      # control: a candidate that passed
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from mlops import rca  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("version")
p.add_argument("--champion")
p.add_argument("--month", default="2026-10")
a = p.parse_args()
r = rca.investigate(a.version, a.month, champion=a.champion)
(ROOT / f"docs/results/rca_v{a.version}.json").write_text(json.dumps(r, indent=2, default=str) + "\n")
(ROOT / f"docs/results/rca_v{a.version}.md").write_text(rca.to_markdown(r))
print(rca.to_markdown(r))
