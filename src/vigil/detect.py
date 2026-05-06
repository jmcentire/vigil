"""Detection — apply the multiplicative threshold against baseline_p95.

ADR-001 V1 algorithm:

    For each (component, op, tenant):
      baseline_p95 = 95th percentile of bucket-values over last 7 days
      observed = bucket-value over last 5 minutes
      if observed > VIGIL_MULTIPLIER * baseline_p95:
        emit Anomaly(...)

Sim's argument (locked in ADR-001): z-score is wrong here because
request rates are log-normal/multimodal/bursty. Quantile + multiplicative
is distribution-free and self-tuning per tenant. Do not bring back z.

`pattern_id` is hashed from (component, op, tenant_id, day-bucket of
detected_at). Same anomaly within the same UTC day collapses to one
pattern_id; cross-day re-firings get a fresh id (operators see "this
came back today").
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from datetime import UTC, datetime

from vigil.baseline import BaselineStore
from vigil.types import Anomaly, AnomalyKey, MetricBucket


def evaluate_observation(
    *,
    key: AnomalyKey,
    observed_value: float,
    sample_event_ids: Iterable[str],
    baseline_store: BaselineStore,
    multiplier: float,
    baseline_window: str = "7d",
    observed_window: str = "5m",
    now: datetime | None = None,
) -> Anomaly | None:
    """Compare a single observed value to the baseline; emit Anomaly or None.

    Returns None when:
      - The baseline is cold (too few buckets).
      - observed_value <= multiplier * baseline_p95 (no anomaly).
      - baseline_p95 is 0 (degenerate — divide-by-zero protection).

    Returns Anomaly when observed_value > multiplier * baseline_p95.
    """
    if multiplier <= 0:
        raise ValueError("multiplier must be > 0")
    if observed_value < 0:
        raise ValueError("observed_value must be >= 0")
    if now is None:
        now = datetime.now(tz=UTC)

    snapshot = baseline_store.compute_p95(key, now=now)
    if snapshot is None:
        return None
    if snapshot.p95 <= 0:
        # Degenerate baseline — every bucket is zero. We refuse to emit
        # because "1 event vs 0 baseline" is infinite-multiplier and
        # almost certainly noise from a brand-new (component, op).
        return None

    threshold = multiplier * snapshot.p95
    if observed_value <= threshold:
        return None

    actual_multiplier = observed_value / snapshot.p95
    pattern_id = compute_pattern_id(key, now)
    return Anomaly(
        pattern_id=pattern_id,
        component=key.component,
        op=key.op,
        tenant_id=key.tenant_id,
        baseline_window=baseline_window,
        baseline_value=snapshot.p95,
        observed_window=observed_window,
        observed_value=float(observed_value),
        multiplier=actual_multiplier,
        sample_event_ids=tuple(sample_event_ids),
        detected_at=now,
    )


def evaluate_buckets(
    buckets: Iterable[MetricBucket],
    *,
    baseline_store: BaselineStore,
    multiplier: float,
    now: datetime | None = None,
) -> list[Anomaly]:
    """Evaluate a batch of buckets in one pass. Returns all firings."""
    out: list[Anomaly] = []
    for bucket in buckets:
        anomaly = evaluate_observation(
            key=bucket.key,
            observed_value=bucket.value,
            sample_event_ids=bucket.sample_event_ids,
            baseline_store=baseline_store,
            multiplier=multiplier,
            now=now,
        )
        if anomaly is not None:
            out.append(anomaly)
    return out


def compute_pattern_id(key: AnomalyKey, when: datetime) -> str:
    """Stable id for the same (component, op, tenant) on the same UTC day.

    Used for pattern_id in the anomalies table. Two firings on the same
    day collapse to one pattern_id (operator sees "this happened
    repeatedly today"); a firing tomorrow gets a fresh id (operator
    sees "this came back").
    """
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    day = when.astimezone(UTC).date().isoformat()
    payload = f"{key.stable_str()}|{day}".encode()
    return "vigil-" + hashlib.sha256(payload).hexdigest()[:16]
