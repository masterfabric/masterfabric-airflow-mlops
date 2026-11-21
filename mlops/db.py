"""Monitoring tables in PostgreSQL (database `mlops`). Every task writes what it decided and why."""
from __future__ import annotations

import json
import os

import psycopg2

DDL = """
CREATE TABLE IF NOT EXISTS dq_results (month TEXT PRIMARY KEY, passed BOOLEAN, rules INT,
    critical_failures JSONB, warnings JSONB, checks JSONB, rows INT, checked_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS drift (month TEXT PRIMARY KEY, champion_version TEXT, psi JSONB, max_psi DOUBLE PRECISION,
    max_feature TEXT, computed_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS realised (month TEXT, model_version TEXT, policy TEXT, auc DOUBLE PRECISION,
    pr_auc DOUBLE PRECISION, top_decile_lift DOUBLE PRECISION, base_rate DOUBLE PRECISION, n INT,
    PRIMARY KEY (month, policy));
CREATE TABLE IF NOT EXISTS decisions (month TEXT PRIMARY KEY, action TEXT, reasons JSONB,
    champion_before TEXT, champion_after TEXT, challenger_version TEXT, details JSONB,
    decided_at TIMESTAMPTZ DEFAULT now());
"""


def conn():
    return psycopg2.connect(os.environ["MLOPS_DB"])


def init() -> None:
    with conn() as c, c.cursor() as cur:
        cur.execute(DDL)


def upsert(table: str, row: dict, key: tuple[str, ...]) -> None:
    cols = list(row)
    vals = [json.dumps(v) if isinstance(v, (dict, list)) else v for v in row.values()]
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in key)
    sql = (f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))}) "
           f"ON CONFLICT ({', '.join(key)}) DO UPDATE SET {sets}")
    with conn() as c, c.cursor() as cur:
        cur.execute(sql, vals)


def query(sql: str, params: tuple = ()) -> list[dict]:
    with conn() as c, c.cursor() as cur:
        cur.execute(sql, params)
        names = [d[0] for d in cur.description]
        return [dict(zip(names, r)) for r in cur.fetchall()]
