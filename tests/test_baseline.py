"""Tests for the rolling-baseline store.

Targets:
    - quantile math (numpy-correct, no off-by-one)
    - multi-day data shape (7d window = 2016 5-min buckets)
    - new-tenant cold start (returns None until min buckets)
    - rolling-window trim (old buckets drop)
    - persistence round-trip (snapshot/load preserves state)
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pytest

from vigil.baseline import BaselineStore
from vigil.types import AnomalyKey, MetricBucket

KEY = AnomalyKey(component="reeve.adapters.llm", op="messages.create", tenant_id="t1")


def _record(
    store: BaselineStore,
    *,
    bucket_start: datetime,
    value: float,
    now: datetime | None = None,
    key: AnomalyKey = KEY,
) -> None:
    store.record_bucket(
        MetricBucket(
            key=key,
            bucket_start=bucket_start,
            bucket_end=bucket_start + timedelta(seconds=store.bucket_seconds),
            value=value,
        ),
        now=now,
    )


def test_align_bucket_is_idempotent() -> None:
    store = BaselineStore(bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=10)
    ts = datetime(2026, 5, 5, 12, 7, 33, tzinfo=UTC)
    aligned = store.align_bucket(ts)
    assert aligned == datetime(2026, 5, 5, 12, 5, 0, tzinfo=UTC)
    assert store.align_bucket(aligned) == aligned


def test_naive_timestamp_treated_as_utc() -> None:
    store = BaselineStore(bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=10)
    naive = datetime(2026, 5, 5, 12, 7, 33)
    aligned = store.align_bucket(naive)
    assert aligned.tzinfo is not None
    assert aligned == datetime(2026, 5, 5, 12, 5, 0, tzinfo=UTC)


def test_cold_start_returns_none() -> None:
    store = BaselineStore(bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=24)
    # Add a few buckets — fewer than min.
    now = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)
    for i in range(5):
        ts = now - timedelta(seconds=(i + 1) * 300)
        _record(store, bucket_start=store.align_bucket(ts), value=1.0, now=now)
    snapshot = store.compute_p95(KEY, now=now)
    assert snapshot is None


def test_warm_baseline_p95_constant_data(warm_store: BaselineStore) -> None:
    snapshot = warm_store.compute_p95(
        AnomalyKey(component="test", op="probe", tenant_id=None),
        now=datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC),
    )
    assert snapshot is not None
    # All values are 1.0; p95 is 1.0 regardless of how many.
    assert snapshot.p95 == pytest.approx(1.0)
    assert snapshot.p50 == pytest.approx(1.0)
    assert snapshot.n >= 12


def test_warm_baseline_quantile_skewed() -> None:
    """p95 of [0,0,...,0,100] (95% zeros, 5% hundreds) should land near 100."""
    # window must be wide enough to hold all buckets.
    store = BaselineStore(
        bucket_seconds=300, window_seconds=7 * 86400, min_buckets_for_baseline=10
    )
    now = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)
    n_zero = 95
    n_hundred = 5
    for i in range(n_zero):
        ts = now - timedelta(seconds=(i + 1) * 300)
        _record(store, bucket_start=store.align_bucket(ts), value=0.0, now=now)
    for i in range(n_hundred):
        ts = now - timedelta(seconds=(n_zero + i + 1) * 300)
        _record(store, bucket_start=store.align_bucket(ts), value=100.0, now=now)
    snapshot = store.compute_p95(KEY, now=now)
    assert snapshot is not None
    # Verify against numpy directly.
    expected = float(np.quantile([0.0] * n_zero + [100.0] * n_hundred, 0.95))
    assert snapshot.p95 == pytest.approx(expected)


def test_seven_day_window_holds_2016_buckets() -> None:
    """ADR-001 V1 = 7d window of 5-minute buckets = 2016 entries per key."""
    store = BaselineStore(
        bucket_seconds=300, window_seconds=7 * 24 * 60 * 60, min_buckets_for_baseline=10
    )
    now = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)
    target = (7 * 24 * 60 * 60) // 300  # 2016
    for i in range(target):
        ts = now - timedelta(seconds=(target - i) * 300)
        _record(store, bucket_start=store.align_bucket(ts), value=float(i % 7), now=now)
    snapshot = store.compute_p95(KEY, now=now)
    assert snapshot is not None
    assert snapshot.n == target


def test_old_buckets_are_trimmed() -> None:
    store = BaselineStore(bucket_seconds=300, window_seconds=3600, min_buckets_for_baseline=2)
    now = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)
    # Bucket from 2 hours ago — should be dropped at next record/compute.
    too_old = store.align_bucket(now - timedelta(hours=2))
    _record(store, bucket_start=too_old, value=999.0, now=now)
    in_window = store.align_bucket(now - timedelta(minutes=10))
    _record(store, bucket_start=in_window, value=1.0, now=now)
    in_window_2 = store.align_bucket(now - timedelta(minutes=20))
    _record(store, bucket_start=in_window_2, value=2.0, now=now)
    snapshot = store.compute_p95(KEY, now=now)
    assert snapshot is not None
    # The 999 bucket would have dragged p95 to ~999 if not trimmed; it's gone.
    assert snapshot.p95 < 50


def test_multiple_keys_independent() -> None:
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=5
    )
    now = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)
    k1 = AnomalyKey(component="a", op="b", tenant_id="t1")
    k2 = AnomalyKey(component="a", op="b", tenant_id="t2")
    for i in range(10):
        ts = now - timedelta(seconds=(i + 1) * 300)
        _record(store, bucket_start=store.align_bucket(ts), value=10.0, key=k1, now=now)
        _record(store, bucket_start=store.align_bucket(ts), value=1000.0, key=k2, now=now)
    s1 = store.compute_p95(k1, now=now)
    s2 = store.compute_p95(k2, now=now)
    assert s1 is not None and s2 is not None
    assert s1.p95 == pytest.approx(10.0)
    assert s2.p95 == pytest.approx(1000.0)


def test_record_replaces_same_bucket() -> None:
    """Re-recording the same bucket_start should overwrite, not duplicate."""
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=2
    )
    now = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)
    bucket_start = store.align_bucket(now - timedelta(minutes=10))
    _record(store, bucket_start=bucket_start, value=10.0, now=now)
    _record(store, bucket_start=bucket_start, value=20.0, now=now)
    bucket_start_2 = store.align_bucket(now - timedelta(minutes=20))
    _record(store, bucket_start=bucket_start_2, value=10.0, now=now)
    snapshot = store.compute_p95(KEY, now=now)
    assert snapshot is not None
    assert snapshot.n == 2  # not 3 — same bucket replaced


def test_out_of_order_buckets_sorted_chronologically() -> None:
    """Late-arriving older buckets should be inserted in chronological order."""
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=3
    )
    now = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)
    # Insert latest first, then earlier.
    _record(store, bucket_start=store.align_bucket(now - timedelta(minutes=5)), value=3.0, now=now)
    _record(store, bucket_start=store.align_bucket(now - timedelta(minutes=15)), value=1.0, now=now)
    _record(store, bucket_start=store.align_bucket(now - timedelta(minutes=10)), value=2.0, now=now)
    # Internal series should still produce correct p95.
    snapshot = store.compute_p95(KEY, now=now)
    assert snapshot is not None
    assert snapshot.n == 3


def test_snapshot_round_trip(tmp_path: Path) -> None:
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=5
    )
    now = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)
    for i in range(20):
        ts = now - timedelta(seconds=(i + 1) * 300)
        _record(store, bucket_start=store.align_bucket(ts), value=float(i), now=now)
    snap_path = tmp_path / "baseline.json"
    store.snapshot_to_path(snap_path)
    assert snap_path.exists()
    # Re-load and verify p95 matches.
    loaded = BaselineStore.load_from_path(
        snap_path,
        bucket_seconds=300,
        window_seconds=86400,
        min_buckets_for_baseline=5,
    )
    s_orig = store.compute_p95(KEY, now=now)
    s_load = loaded.compute_p95(KEY, now=now)
    assert s_orig is not None and s_load is not None
    assert s_orig.p95 == pytest.approx(s_load.p95)
    assert s_orig.n == s_load.n


def test_load_from_missing_path_returns_empty_store(tmp_path: Path) -> None:
    snap_path = tmp_path / "does-not-exist.json"
    loaded = BaselineStore.load_from_path(
        snap_path,
        bucket_seconds=300,
        window_seconds=86400,
        min_buckets_for_baseline=5,
    )
    assert loaded.all_keys() == []


def test_unsupported_schema_version_rejected(tmp_path: Path) -> None:
    snap_path = tmp_path / "bad.json"
    snap_path.write_text(json.dumps({"schema_version": 999}))
    with pytest.raises(ValueError):
        BaselineStore.load_from_path(
            snap_path,
            bucket_seconds=300,
            window_seconds=86400,
            min_buckets_for_baseline=5,
        )


def test_invalid_constructor_args() -> None:
    with pytest.raises(ValueError):
        BaselineStore(bucket_seconds=0, window_seconds=86400, min_buckets_for_baseline=5)
    with pytest.raises(ValueError):
        BaselineStore(bucket_seconds=300, window_seconds=0, min_buckets_for_baseline=5)
    with pytest.raises(ValueError):
        BaselineStore(bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=0)


def test_all_snapshots_skips_cold_keys() -> None:
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=10
    )
    now = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)
    k_warm = AnomalyKey(component="c", op="o", tenant_id="warm")
    k_cold = AnomalyKey(component="c", op="o", tenant_id="cold")
    for i in range(15):
        ts = now - timedelta(seconds=(i + 1) * 300)
        _record(store, bucket_start=store.align_bucket(ts), value=1.0, key=k_warm, now=now)
    for i in range(3):
        ts = now - timedelta(seconds=(i + 1) * 300)
        _record(store, bucket_start=store.align_bucket(ts), value=1.0, key=k_cold, now=now)
    snaps = store.all_snapshots(now=now)
    assert len(snaps) == 1
    assert snaps[0].key == k_warm
