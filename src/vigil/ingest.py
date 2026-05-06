"""Ingest — pull events from Reeve's `trace_samples` and aggregate into buckets.

V1 model (ADR-001): pull from Reeve's `trace_samples` table on a 1-minute
cron (the `vigil run` loop calls `ingest_cycle` once per loop interval).

Each `trace_samples` row is one sampled request; `entries` is a JSONB
array of `{component, op, elapsedMs, outcome, tenantId?, detail?}`. We
flatten that into per-(component, op, tenant) event counts bucketed by
the trace's `ended_at` timestamp.

Idempotency
-----------

We use `ingest_cursors` to remember the highest `ended_at` timestamp
consumed per (source, component, op). Re-running ingest:

1. Reads the cursor for source='reeve.trace_samples'.
2. Queries Reeve only for rows with `ended_at > cursor` (strict >).
3. Aggregates the new rows into MetricBuckets, replace-semantics in
   BaselineStore.
4. Updates the cursor to max(ended_at) seen.

Race window: if two `trace_samples` rows have the exact same `ended_at`
and ingest is interrupted between them, the second is missed. In
practice ended_at comes from Postgres's `now()` and has microsecond
resolution; collision is rare. We accept this — V1 is forensic, not
audit-grade.

Cold start (no source DB)
-------------------------

If `REEVE_DATABASE_URL` is unset or the DB is unreachable, `ingest_cycle`
raises `IngestError`. The `vigil run` loop catches this and logs a
warning; the process keeps running so the API stays available.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import psycopg
from psycopg.rows import dict_row

from vigil.baseline import BaselineStore
from vigil.persist import get_cursor, update_cursor
from vigil.types import AnomalyKey, MetricBucket

logger = logging.getLogger(__name__)

REEVE_TRACE_SOURCE = "reeve.trace_samples"


class IngestError(Exception):
    """Raised when ingest fails (typically: source DB unreachable)."""


@dataclass(frozen=True, slots=True)
class IngestStats:
    rows_read: int
    buckets_recorded: int
    new_cursor: datetime | None
    keys_touched: int


def ingest_cycle(
    *,
    reeve_conninfo: str,
    vigil_conninfo: str,
    baseline_store: BaselineStore,
    batch_size: int = 5000,
    now: datetime | None = None,
) -> IngestStats:
    """One pass: read new trace_samples, aggregate, record into baseline.

    Returns IngestStats describing what happened. Caller is responsible for
    deciding what to do next (typically: feed `recent buckets` into detect()).
    """
    if not reeve_conninfo:
        raise IngestError(
            "REEVE_DATABASE_URL is not set; cannot pull trace_samples."
        )
    if not vigil_conninfo:
        raise IngestError(
            "VIGIL_DATABASE_URL is not set; cannot read/write ingest_cursors."
        )
    if now is None:
        now = datetime.now(tz=UTC)

    # We use a single cursor row keyed by (source, component='*', op='*') in
    # V1 — all flattened entries advance together. ADR-002 may split per
    # (component, op) when the read becomes a hot spot.
    cursor = get_cursor(
        vigil_conninfo, source=REEVE_TRACE_SOURCE, component="*", op="*"
    )
    if cursor is None:
        # First run: bootstrap from the start of the rolling baseline window.
        cursor = now - timedelta(seconds=baseline_store.window_seconds)

    rows = _read_trace_samples(
        reeve_conninfo,
        since_exclusive=cursor,
        until_inclusive=now,
        limit=batch_size,
    )
    if not rows:
        return IngestStats(
            rows_read=0, buckets_recorded=0, new_cursor=None, keys_touched=0
        )

    buckets = _aggregate_to_buckets(
        rows,
        bucket_seconds=baseline_store.bucket_seconds,
    )
    keys_touched: set[AnomalyKey] = set()
    for bucket in buckets:
        baseline_store.record_bucket(bucket, now=now)
        keys_touched.add(bucket.key)

    max_ended_at = max(r["ended_at"] for r in rows)
    update_cursor(
        vigil_conninfo,
        source=REEVE_TRACE_SOURCE,
        component="*",
        op="*",
        last_event_at=max_ended_at,
    )
    logger.info(
        "ingest_cycle: rows=%d buckets=%d keys=%d new_cursor=%s",
        len(rows),
        len(buckets),
        len(keys_touched),
        max_ended_at.isoformat(),
    )
    return IngestStats(
        rows_read=len(rows),
        buckets_recorded=len(buckets),
        new_cursor=max_ended_at,
        keys_touched=len(keys_touched),
    )


def buckets_for_observation(
    baseline_store: BaselineStore,
    *,
    now: datetime | None = None,
    observed_window_seconds: int,
) -> list[MetricBucket]:
    """Return current-bucket values for every key, suitable for detect().

    "Current bucket" = the most recent fully-or-partially-formed bucket
    for each key, intersected with the observed_window. We use the most
    recent bucket only (V1 — observed_window is one bucket period). When
    observed_window > bucket_seconds, ADR-002 covers aggregation here.
    """
    if now is None:
        now = datetime.now(tz=UTC)
    if observed_window_seconds < baseline_store.bucket_seconds:
        # Observed window must be at least one bucket; otherwise we have
        # nothing to compare against.
        return []
    out: list[MetricBucket] = []
    cutoff = now - timedelta(seconds=observed_window_seconds)
    for key in baseline_store.all_keys():
        # Use the highest-bucket-value within the observed window — if a
        # 5-minute traffic spike happens in any one bucket of the window,
        # that's the signal we want to fire on.
        with baseline_store._lock:  # type: ignore[attr-defined]
            series = baseline_store._series.get(key)  # type: ignore[attr-defined]
            if series is None or not series.buckets:
                continue
            recent_values: list[tuple[datetime, float]] = []
            for ts, val in reversed(series.buckets.items()):
                if ts < cutoff:
                    break
                recent_values.append((ts, val))
        if not recent_values:
            continue
        # Pick the bucket with the maximum value within the observed window.
        ts_max, val_max = max(recent_values, key=lambda tv: tv[1])
        out.append(
            MetricBucket(
                key=key,
                bucket_start=ts_max,
                bucket_end=ts_max + timedelta(seconds=baseline_store.bucket_seconds),
                value=val_max,
                sample_event_ids=(),  # populated in detect path below if needed
            )
        )
    return out


# --- Internal helpers -----------------------------------------------------


def _read_trace_samples(
    conninfo: str,
    *,
    since_exclusive: datetime,
    until_inclusive: datetime,
    limit: int,
) -> list[dict]:
    """Read rows from Reeve's trace_samples table in (since, until] window.

    Read-only. We never write to Reeve's DB.
    """
    sql = """
        SELECT id, tenant_id, op, outcome, elapsed_ms, entries, ended_at
          FROM trace_samples
         WHERE ended_at > %s AND ended_at <= %s
         ORDER BY ended_at ASC
         LIMIT %s
    """
    try:
        with psycopg.connect(conninfo) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql, (since_exclusive, until_inclusive, limit))
                return list(cur.fetchall())
    except psycopg.Error as err:
        raise IngestError(
            f"Failed to read trace_samples from REEVE_DATABASE_URL: {err}"
        ) from err


def _aggregate_to_buckets(
    rows: Iterable[dict],
    *,
    bucket_seconds: int,
) -> list[MetricBucket]:
    """Flatten trace_samples rows into per-(component,op,tenant) buckets.

    For each row, we expand `entries` (the JSONB array of TraceEntry) and
    count one event per entry. The (component, op) come from the entry;
    the tenant_id from the row (nested-entry tenant overrides if present).
    """
    counts: dict[tuple[AnomalyKey, datetime], int] = defaultdict(int)
    sample_ids: dict[tuple[AnomalyKey, datetime], list[str]] = defaultdict(list)

    for row in rows:
        ended_at: datetime = row["ended_at"]
        if ended_at.tzinfo is None:
            ended_at = ended_at.replace(tzinfo=UTC)
        bucket_start = _align_bucket(ended_at, bucket_seconds)
        row_id = str(row["id"])
        row_tenant = (
            str(row["tenant_id"]) if row.get("tenant_id") is not None else None
        )
        entries = row.get("entries") or []
        if not entries:
            # Even a no-entries trace is one event for the top-level op.
            key = AnomalyKey(
                component="reeve.tracing", op=row["op"], tenant_id=row_tenant
            )
            bk = (key, bucket_start)
            counts[bk] += 1
            if len(sample_ids[bk]) < 32:
                sample_ids[bk].append(row_id)
            continue
        for entry in entries:
            component = str(entry.get("component", "unknown"))
            op = str(entry.get("op", "unknown"))
            tenant_id = entry.get("tenantId") or row_tenant
            key = AnomalyKey(component=component, op=op, tenant_id=tenant_id)
            bk = (key, bucket_start)
            counts[bk] += 1
            if len(sample_ids[bk]) < 32:
                sample_ids[bk].append(row_id)

    out: list[MetricBucket] = []
    for (key, bucket_start), count in counts.items():
        out.append(
            MetricBucket(
                key=key,
                bucket_start=bucket_start,
                bucket_end=bucket_start + timedelta(seconds=bucket_seconds),
                value=float(count),
                sample_event_ids=tuple(sample_ids[(key, bucket_start)]),
            )
        )
    return out


def _align_bucket(ts: datetime, bucket_seconds: int) -> datetime:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    epoch = ts.timestamp()
    floored = epoch - (epoch % bucket_seconds)
    return datetime.fromtimestamp(floored, tz=UTC)


__all__ = [
    "IngestError",
    "IngestStats",
    "REEVE_TRACE_SOURCE",
    "buckets_for_observation",
    "ingest_cycle",
]
