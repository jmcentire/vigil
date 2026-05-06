"""Tests for detection — the multiplicative threshold against baseline_p95."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from vigil.baseline import BaselineStore
from vigil.detect import (
    compute_pattern_id,
    evaluate_buckets,
    evaluate_observation,
)
from vigil.types import AnomalyKey, MetricBucket

KEY = AnomalyKey(component="reeve.adapters.llm", op="messages.create", tenant_id="t1")


def _populate_warm(
    store: BaselineStore,
    *,
    key: AnomalyKey = KEY,
    value: float = 10.0,
    n_buckets: int = 50,
    now: datetime | None = None,
) -> datetime:
    if now is None:
        now = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)
    for i in range(n_buckets):
        ts = now - timedelta(seconds=(i + 1) * store.bucket_seconds)
        store.record_bucket(
            MetricBucket(
                key=key,
                bucket_start=store.align_bucket(ts),
                bucket_end=store.align_bucket(ts) + timedelta(seconds=store.bucket_seconds),
                value=value,
            ),
            now=now,
        )
    return now


def test_below_threshold_no_anomaly() -> None:
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=10
    )
    now = _populate_warm(store, value=10.0)
    a = evaluate_observation(
        key=KEY,
        observed_value=29.0,  # 2.9x — under 3x threshold
        sample_event_ids=("e1",),
        baseline_store=store,
        multiplier=3.0,
        now=now,
    )
    assert a is None


def test_at_threshold_no_anomaly() -> None:
    """observed == 3 * baseline must NOT fire (strict-greater-than)."""
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=10
    )
    now = _populate_warm(store, value=10.0)
    a = evaluate_observation(
        key=KEY,
        observed_value=30.0,
        sample_event_ids=(),
        baseline_store=store,
        multiplier=3.0,
        now=now,
    )
    assert a is None


def test_above_threshold_emits_anomaly() -> None:
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=10
    )
    now = _populate_warm(store, value=10.0)
    a = evaluate_observation(
        key=KEY,
        observed_value=47.0,
        sample_event_ids=("e1", "e2", "e3"),
        baseline_store=store,
        multiplier=3.0,
        now=now,
    )
    assert a is not None
    assert a.component == KEY.component
    assert a.op == KEY.op
    assert a.tenant_id == KEY.tenant_id
    assert a.baseline_value == pytest.approx(10.0)
    assert a.observed_value == pytest.approx(47.0)
    assert a.multiplier == pytest.approx(4.7)
    assert a.sample_event_ids == ("e1", "e2", "e3")
    assert a.detected_at == now


def test_cold_baseline_no_emit() -> None:
    """Anomaly never fires when baseline is cold (insufficient data)."""
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=20
    )
    # Only 5 buckets warmed; need 20.
    now = _populate_warm(store, value=10.0, n_buckets=5)
    a = evaluate_observation(
        key=KEY,
        observed_value=10000.0,
        sample_event_ids=(),
        baseline_store=store,
        multiplier=3.0,
        now=now,
    )
    assert a is None


def test_zero_baseline_no_emit() -> None:
    """All-zero baseline + any observed -> no emit (degenerate)."""
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=10
    )
    now = _populate_warm(store, value=0.0)
    a = evaluate_observation(
        key=KEY,
        observed_value=100.0,
        sample_event_ids=(),
        baseline_store=store,
        multiplier=3.0,
        now=now,
    )
    assert a is None


def test_per_tenant_scaling() -> None:
    """High-volume tenant baseline >> low-volume; same observed_value differs."""
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=10
    )
    now = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)
    big = AnomalyKey(component="reeve.adapters.llm", op="op", tenant_id="big")
    small = AnomalyKey(component="reeve.adapters.llm", op="op", tenant_id="small")
    _populate_warm(store, key=big, value=100.0, now=now)
    _populate_warm(store, key=small, value=1.0, now=now)
    # 50 events in 5 minutes = noise for big tenant, anomaly for small.
    big_anom = evaluate_observation(
        key=big,
        observed_value=50.0,
        sample_event_ids=(),
        baseline_store=store,
        multiplier=3.0,
        now=now,
    )
    small_anom = evaluate_observation(
        key=small,
        observed_value=50.0,
        sample_event_ids=(),
        baseline_store=store,
        multiplier=3.0,
        now=now,
    )
    assert big_anom is None
    assert small_anom is not None
    assert small_anom.multiplier == pytest.approx(50.0)


def test_threshold_tuning_changes_emit() -> None:
    """Lowering multiplier surfaces more; raising it suppresses."""
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=10
    )
    now = _populate_warm(store, value=10.0)
    # observed = 25 → 2.5x. With multiplier=3 → no fire. With multiplier=2 → fire.
    a3 = evaluate_observation(
        key=KEY,
        observed_value=25.0,
        sample_event_ids=(),
        baseline_store=store,
        multiplier=3.0,
        now=now,
    )
    a2 = evaluate_observation(
        key=KEY,
        observed_value=25.0,
        sample_event_ids=(),
        baseline_store=store,
        multiplier=2.0,
        now=now,
    )
    assert a3 is None
    assert a2 is not None
    assert a2.multiplier == pytest.approx(2.5)


def test_negative_observed_rejected() -> None:
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=10
    )
    now = _populate_warm(store, value=10.0)
    with pytest.raises(ValueError):
        evaluate_observation(
            key=KEY,
            observed_value=-1.0,
            sample_event_ids=(),
            baseline_store=store,
            multiplier=3.0,
            now=now,
        )


def test_invalid_multiplier_rejected() -> None:
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=10
    )
    now = _populate_warm(store, value=10.0)
    with pytest.raises(ValueError):
        evaluate_observation(
            key=KEY,
            observed_value=10.0,
            sample_event_ids=(),
            baseline_store=store,
            multiplier=0.0,
            now=now,
        )


def test_evaluate_buckets_batch() -> None:
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=10
    )
    now = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)
    k1 = AnomalyKey(component="a", op="o1", tenant_id="t")
    k2 = AnomalyKey(component="a", op="o2", tenant_id="t")
    _populate_warm(store, key=k1, value=10.0, now=now)
    _populate_warm(store, key=k2, value=10.0, now=now)
    buckets = [
        MetricBucket(
            key=k1,
            bucket_start=now,
            bucket_end=now + timedelta(seconds=300),
            value=5.0,  # below threshold
        ),
        MetricBucket(
            key=k2,
            bucket_start=now,
            bucket_end=now + timedelta(seconds=300),
            value=100.0,  # 10x — fires
            sample_event_ids=("x",),
        ),
    ]
    out = evaluate_buckets(buckets, baseline_store=store, multiplier=3.0, now=now)
    assert len(out) == 1
    assert out[0].op == "o2"


def test_pattern_id_stable_within_day() -> None:
    """Same key + same UTC day -> same pattern_id."""
    a = compute_pattern_id(KEY, datetime(2026, 5, 5, 1, 0, 0, tzinfo=UTC))
    b = compute_pattern_id(KEY, datetime(2026, 5, 5, 23, 59, 59, tzinfo=UTC))
    assert a == b


def test_pattern_id_changes_across_days() -> None:
    a = compute_pattern_id(KEY, datetime(2026, 5, 5, 23, 59, 59, tzinfo=UTC))
    b = compute_pattern_id(KEY, datetime(2026, 5, 6, 0, 0, 1, tzinfo=UTC))
    assert a != b


def test_pattern_id_changes_across_keys() -> None:
    when = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)
    other = AnomalyKey(component=KEY.component, op="other-op", tenant_id=KEY.tenant_id)
    assert compute_pattern_id(KEY, when) != compute_pattern_id(other, when)
