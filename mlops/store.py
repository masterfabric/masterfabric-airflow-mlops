"""Parquet on S3-compatible storage (bucket `lake`): landing snapshots, labels, scores."""
from __future__ import annotations

import io
import os

import boto3
import pandas as pd

BUCKET = "lake"


def _s3():
    return boto3.client("s3", endpoint_url=os.environ.get("S3_ENDPOINT", "http://s3:9000"))


def put(df: pd.DataFrame, key: str) -> str:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    _s3().put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    return f"s3://{BUCKET}/{key}"


def get(key: str) -> pd.DataFrame:
    body = _s3().get_object(Bucket=BUCKET, Key=key)["Body"].read()
    return pd.read_parquet(io.BytesIO(body))


def exists(key: str) -> bool:
    try:
        _s3().head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False


def snapshot_key(month: str) -> str:
    return f"landing/subscribers/month={month}/snapshot.parquet"


def labels_key(month: str) -> str:
    return f"landing/churn_labels/month={month}/labels.parquet"


def scores_key(month: str) -> str:
    return f"scores/churn/month={month}/scores.parquet"
