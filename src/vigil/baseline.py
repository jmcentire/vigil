"""Rolling baseline computation per (component, op, tenant) bucket.

ADR-001 V1: 7-day rolling 95th-percentile, in-memory ring buffer per
(component, op, tenant). Persisted to disk via JSON snapshot every N
minutes; recomputed from event source on cold start.

Discipline:
    - Buckets are *fixed-grid* time buckets (5-minute aligned by default).
      Two events in the same physical minute go in the same bucket.
    - We hold raw bucket values, not raw events. The p95 is over bucket
      values; this is intentional — bursty traffic spikes the bucket
      value, which shows up at the right resolution for "is this 5
      minutes weird?".
    - Cold start: if fewer than MIN_BUCKETS_FOR_BASELINE buckets exist,
      `compute_p95` returns None. Detection skips the key. No false
      positives during warmup.
    - We never UPDATE buckets — the ingest layer is responsible for
      idempotency (cursor table). If `record_bucket` is called twice for
      the same bucket_start, the second call OVERWRITES (replace
      semantics). This is explicit: ingest is "process this 5m of
      events, here's the count", and reprocessing should give the same
      count.

Persistence is intentionally simple JSON — not pickle (cross-version
incompat) and not Postgres (a separate baselines table adds load with
no clear benefit when V1 cold-recomputes from trace_samples in <60s).
"""

from __future__ import annotations

import json
import os
import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np

from vigil.types import AnomalyKey, BaselineSnapshot, MetricBucket


@dataclass
class _BucketSeries:
    """Per-key time series of bucket values.

    Stored as an OrderedDict keyed by the bucket_start datetime so the
    ordering is insertion-order = chronological (we always append at the
    head and trim from the tail). Total memory per key bounded by
    (window_seconds / bucket_seconds) entries; for 7d/5m that's 2016
    floats — 16KB per key, comfortable.
    """

    bucket_seconds: int
    window_seconds: int
    # Insertion-ordered: oldest first.
    buckets: OrderedDict[datetime, float]

    def trim(self, now: datetime) -> None:
        """Drop buckets older than the rolling window."""
        cutoff = now - timedelta(seconds=self.window_seconds)
        # Walk from the front (oldest) and pop until we hit a fresh one.
        while self.buckets:
            oldest = next(iter(self.buckets))
            if oldest < cutoff:
                self.buckets.popitem(last=False)
            else:
                break


class BaselineStore:
    """In-memory store of rolling baselines per AnomalyKey.

    Thread-safe: a single threading.Lock guards the dict. All write paths
    are O(log n) on the per-key time series; reads (compute_p95) are O(n)
    over a ~2k-entry vector — sub-millisecond with numpy.
    """

    def __init__(
        self,
        *,
        bucket_seconds: int,
        window_seconds: int,
        min_buckets_for_baseline: int,
    ) -> None:
        if bucket_seconds <= 0:
            raise ValueError("bucket_seconds must be > 0")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be > 0")
        if min_buckets_for_baseline <= 0:
            raise ValueError("min_buckets_for_baseline must be > 0")
        self._bucket_seconds = bucket_seconds
        self._window_seconds = window_seconds
        self._min_buckets = min_buckets_for_baseline
        self._series: dict[AnomalyKey, _BucketSeries] = {}
        self._lock = threading.Lock()

    @property
    def bucket_seconds(self) -> int:
        return self._bucket_seconds

    @property
    def window_seconds(self) -> int:
        return self._window_seconds

    def align_bucket(self, ts: datetime) -> datetime:
        """Snap a timestamp to the start of its bucket (UTC, fixed grid)."""
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        epoch = ts.timestamp()
        floored = epoch - (epoch % self._bucket_seconds)
        return datetime.fromtimestamp(floored, tz=UTC)

    def record_bucket(
        self,
        bucket: MetricBucket,
        *,
        now: datetime | None = None,
    ) -> None:
        """Record (or replace) a single bucket value.

        Caller must align bucket.bucket_start to the grid (use
        align_bucket). Out-of-window buckets are dropped silently — the
        ingest layer should not be feeding us week-old data.
        """
        if now is None:
            now = datetime.now(tz=UTC)
        if bucket.bucket_start.tzinfo is None:
            bucket_start = bucket.bucket_start.replace(tzinfo=UTC)
        else:
            bucket_start = bucket.bucket_start
        cutoff = now - timedelta(seconds=self._window_seconds)
        if bucket_start < cutoff:
            return
        with self._lock:
            series = self._series.get(bucket.key)
            if series is None:
                series = _BucketSeries(
                    bucket_seconds=self._bucket_seconds,
                    window_seconds=self._window_seconds,
                    buckets=OrderedDict(),
                )
                self._series[bucket.key] = series
            # Replace-semantics: if the bucket already exists, overwrite.
            # OrderedDict's insertion order is preserved on overwrite (good
            # — we don't want to disturb chronology), but we must move
            # newly-arrived later-buckets to the end.
            existing = bucket_start in series.buckets
            series.buckets[bucket_start] = float(bucket.value)
            if not existing:
                # Sort only when a new key landed out-of-order (rare —
                # ingest is usually monotonic).
                if not _is_last_key(series.buckets, bucket_start):
                    items = sorted(series.buckets.items(), key=lambda kv: kv[0])
                    series.buckets = OrderedDict(items)
            series.trim(now)

    def compute_p95(
        self,
        key: AnomalyKey,
        *,
        now: datetime | None = None,
    ) -> BaselineSnapshot | None:
        """Return BaselineSnapshot for `key`, or None if too few buckets.

        Cold-start protection: returns None until min_buckets_for_baseline
        non-empty buckets exist within the rolling window. This is the
        single most important false-positive guard in V1.
        """
        if now is None:
            now = datetime.now(tz=UTC)
        with self._lock:
            series = self._series.get(key)
            if series is None:
                return None
            series.trim(now)
            values = list(series.buckets.values())
        if len(values) < self._min_buckets:
            return None
        arr = np.asarray(values, dtype=np.float64)
        # numpy quantile with default linear interpolation. p95.
        p95 = float(np.quantile(arr, 0.95))
        p50 = float(np.quantile(arr, 0.50))
        return BaselineSnapshot(
            key=key,
            p95=p95,
            p50=p50,
            n=len(values),
            window_seconds=self._window_seconds,
            computed_at=now,
        )

    def all_keys(self) -> list[AnomalyKey]:
        with self._lock:
            return list(self._series.keys())

    def all_snapshots(self, *, now: datetime | None = None) -> list[BaselineSnapshot]:
        """Compute p95 for all keys; skip keys without enough samples.

        Used by `vigil baseline` (the inspection CLI) and by /v1/health
        as a quick sanity check that ingest is producing data.
        """
        out: list[BaselineSnapshot] = []
        for key in self.all_keys():
            snap = self.compute_p95(key, now=now)
            if snap is not None:
                out.append(snap)
        return out

    # --- Persistence -------------------------------------------------------

    def to_dict(self) -> dict:
        """Serialize to a JSON-safe dict. Buckets become {iso: value}."""
        with self._lock:
            return {
                "schema_version": 1,
                "bucket_seconds": self._bucket_seconds,
                "window_seconds": self._window_seconds,
                "min_buckets_for_baseline": self._min_buckets,
                "series": [
                    {
                        "component": k.component,
                        "op": k.op,
                        "tenant_id": k.tenant_id,
                        "buckets": [
                            [bucket_start.isoformat(), value]
                            for bucket_start, value in s.buckets.items()
                        ],
                    }
                    for k, s in self._series.items()
                ],
            }

    @staticmethod
    def from_dict(data: dict) -> BaselineStore:
        if data.get("schema_version") != 1:
            raise ValueError(
                f"Unsupported baseline snapshot schema_version: {data.get('schema_version')}"
            )
        store = BaselineStore(
            bucket_seconds=int(data["bucket_seconds"]),
            window_seconds=int(data["window_seconds"]),
            min_buckets_for_baseline=int(data["min_buckets_for_baseline"]),
        )
        for series_dict in data.get("series", []):
            key = AnomalyKey(
                component=series_dict["component"],
                op=series_dict["op"],
                tenant_id=series_dict.get("tenant_id"),
            )
            buckets: OrderedDict[datetime, float] = OrderedDict()
            for ts_iso, value in series_dict.get("buckets", []):
                buckets[datetime.fromisoformat(ts_iso)] = float(value)
            store._series[key] = _BucketSeries(
                bucket_seconds=store._bucket_seconds,
                window_seconds=store._window_seconds,
                buckets=buckets,
            )
        return store

    def snapshot_to_path(self, path: str | os.PathLike[str]) -> None:
        """Write JSON snapshot atomically (write to .tmp, rename)."""
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f)
        tmp.replace(p)

    @staticmethod
    def load_from_path(
        path: str | os.PathLike[str],
        *,
        bucket_seconds: int,
        window_seconds: int,
        min_buckets_for_baseline: int,
    ) -> BaselineStore:
        """Load snapshot if present, else fresh empty store with given params."""
        p = Path(path)
        if not p.exists():
            return BaselineStore(
                bucket_seconds=bucket_seconds,
                window_seconds=window_seconds,
                min_buckets_for_baseline=min_buckets_for_baseline,
            )
        with p.open("r", encoding="utf-8") as f:
            return BaselineStore.from_dict(json.load(f))


def _is_last_key(d: OrderedDict[datetime, float], k: datetime) -> bool:
    """True iff k is the most-recently-inserted (last) key."""
    if not d:
        return False
    return next(reversed(d)) == k
