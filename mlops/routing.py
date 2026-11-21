"""Sticky canary routing shared by the API and the tests (standard library only)."""
from __future__ import annotations

import hashlib


def bucket(subscriber_id: str) -> float:
    """Stable position in [0, 100) for a subscriber id: the same id always lands in the same bucket, so a
    subscriber keeps its variant, and raising the canary percentage only adds buckets to the canary."""
    return int(hashlib.sha256(subscriber_id.encode()).hexdigest()[:8], 16) % 10_000 / 100


def is_canary(subscriber_id: str, percent: float) -> bool:
    return bucket(subscriber_id) < percent
