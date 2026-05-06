"""Shared types for vigil.

Discipline: types here are dependency-free (only stdlib + numpy). Anything
that imports psycopg, fastapi, or click belongs elsewhere — these types
flow through every module so they need to stay portable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True, slots=True)
class AnomalyKey:
    """Identity of a (component, op, tenant) baseline bucket.

    tenant_id is None for unattributed events (cron, system ops). The
    keys (component, op, None) and (component, op, '<some-uuid>') are
    distinct buckets.
    """

    component: str
    op: str
    tenant_id: str | None

    def stable_str(self) -> str:
        """Stable string form for hashing into pattern_ids and persistence."""
        return f"{self.component}|{self.op}|{self.tenant_id or '_'}"


@dataclass(frozen=True, slots=True)
class MetricBucket:
    """A single time-bucketed event count for one AnomalyKey.

    V1 metric is "count of events in the bucket". A future ADR may add
    latency-p95-per-bucket and error-rate-per-bucket; the bucket's `value`
    field is metric-agnostic.
    """

    key: AnomalyKey
    bucket_start: datetime  # inclusive
    bucket_end: datetime  # exclusive
    value: float
    # Source event ids that contributed. Used for forensic dereference,
    # not for arithmetic.
    sample_event_ids: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class BaselineSnapshot:
    """Per-key rolling baseline statistics.

    p95 is the 95th-percentile of the per-bucket values over the last
    `window_seconds`. n is the number of buckets used (sanity check —
    vigil refuses to evaluate keys with too-few samples; see baseline.py
    MIN_BUCKETS_FOR_BASELINE).
    """

    key: AnomalyKey
    p95: float
    p50: float  # not used for detection in V1; kept for forensic visibility
    n: int
    window_seconds: int
    computed_at: datetime


@dataclass(frozen=True, slots=True)
class Anomaly:
    """A detected anomaly. Maps 1:1 to a row in `anomalies` table."""

    pattern_id: str
    component: str
    op: str
    tenant_id: str | None
    baseline_window: str  # '7d'
    baseline_value: float
    observed_window: str  # '5m'
    observed_value: float
    multiplier: float
    sample_event_ids: tuple[str, ...]
    detected_at: datetime

    # Optional, only populated on read from DB.
    id: str | None = None
    dismissed_at: datetime | None = None
    dismiss_reason: str | None = None
