"""Tests for ingest — the trace_samples → MetricBuckets pipeline.

Most tests run against in-memory data via the internal helpers
(`_aggregate_to_buckets`, `_align_bucket`); the DB-backed pull cycle is
gated on VIGIL_TEST_DATABASE_URL via the `require_db` marker.

The cursor + idempotency contract is the most important behavior here:
re-running ingest on the same data must produce the same baseline state.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from tests.conftest import require_db
from vigil.baseline import BaselineStore
from vigil.ingest import (
    REEVE_TRACE_SOURCE,
    IngestError,
    _aggregate_to_buckets,
    _align_bucket,
    buckets_for_observation,
    ingest_cycle,
)
from vigil.persist import connect, get_cursor
from vigil.types import AnomalyKey, MetricBucket


def _row(
    *,
    id: str,
    ended_at: datetime,
    op: str = "GET /test",
    tenant_id: str | None = None,
    entries: list[dict] | None = None,
) -> dict:
    return {
        "id": id,
        "tenant_id": tenant_id,
        "op": op,
        "outcome": "ok",
        "elapsed_ms": 42,
        "entries": entries or [],
        "ended_at": ended_at,
    }


def test_align_bucket_to_5min_grid() -> None:
    ts = datetime(2026, 5, 5, 12, 7, 33, 444, tzinfo=UTC)
    aligned = _align_bucket(ts, 300)
    assert aligned == datetime(2026, 5, 5, 12, 5, 0, tzinfo=UTC)


def test_aggregate_empty() -> None:
    assert _aggregate_to_buckets([], bucket_seconds=300) == []


def test_aggregate_single_row_with_entries() -> None:
    ts = datetime(2026, 5, 5, 12, 7, 33, tzinfo=UTC)
    row = _row(
        id="r1",
        ended_at=ts,
        tenant_id="t1",
        entries=[
            {"component": "reeve.adapters.llm", "op": "messages.create", "elapsedMs": 10, "outcome": "ok"},
            {"component": "reeve.adapters.llm", "op": "messages.create", "elapsedMs": 12, "outcome": "ok"},
            {"component": "reeve.adapters.email", "op": "send", "elapsedMs": 20, "outcome": "ok"},
        ],
    )
    buckets = _aggregate_to_buckets([row], bucket_seconds=300)
    by_key = {b.key: b for b in buckets}
    llm = AnomalyKey(component="reeve.adapters.llm", op="messages.create", tenant_id="t1")
    email = AnomalyKey(component="reeve.adapters.email", op="send", tenant_id="t1")
    assert llm in by_key
    assert email in by_key
    assert by_key[llm].value == 2.0
    assert by_key[email].value == 1.0
    # Both buckets aligned to 12:05
    assert by_key[llm].bucket_start == datetime(2026, 5, 5, 12, 5, 0, tzinfo=UTC)


def test_aggregate_no_entries_uses_top_level_op() -> None:
    """A trace with empty entries still counts as one event for its top op."""
    ts = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)
    row = _row(id="r1", ended_at=ts, op="GET /webhook", tenant_id=None, entries=[])
    buckets = _aggregate_to_buckets([row], bucket_seconds=300)
    assert len(buckets) == 1
    b = buckets[0]
    assert b.key.op == "GET /webhook"
    assert b.key.component == "reeve.tracing"
    assert b.key.tenant_id is None
    assert b.value == 1.0


def test_aggregate_entry_tenant_overrides_row_tenant() -> None:
    ts = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)
    row = _row(
        id="r1",
        ended_at=ts,
        tenant_id="row-tenant",
        entries=[
            {"component": "c", "op": "o", "elapsedMs": 1, "outcome": "ok", "tenantId": "entry-tenant"},
        ],
    )
    buckets = _aggregate_to_buckets([row], bucket_seconds=300)
    assert len(buckets) == 1
    assert buckets[0].key.tenant_id == "entry-tenant"


def test_aggregate_multi_bucket() -> None:
    """Two rows in different 5-min buckets stay separate."""
    t1 = datetime(2026, 5, 5, 12, 3, 0, tzinfo=UTC)
    t2 = datetime(2026, 5, 5, 12, 8, 0, tzinfo=UTC)
    rows = [
        _row(
            id="a", ended_at=t1, tenant_id="t",
            entries=[{"component": "c", "op": "o", "elapsedMs": 1, "outcome": "ok"}],
        ),
        _row(
            id="b", ended_at=t2, tenant_id="t",
            entries=[{"component": "c", "op": "o", "elapsedMs": 1, "outcome": "ok"}],
        ),
    ]
    buckets = _aggregate_to_buckets(rows, bucket_seconds=300)
    assert len(buckets) == 2
    starts = {b.bucket_start for b in buckets}
    assert datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC) in starts
    assert datetime(2026, 5, 5, 12, 5, 0, tzinfo=UTC) in starts


def test_aggregate_sample_ids_capped() -> None:
    """sample_event_ids per bucket is capped (we only need ~32 for forensic context)."""
    ts = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)
    rows = [
        _row(
            id=f"r{i}", ended_at=ts, tenant_id="t",
            entries=[{"component": "c", "op": "o", "elapsedMs": 1, "outcome": "ok"}],
        )
        for i in range(100)
    ]
    buckets = _aggregate_to_buckets(rows, bucket_seconds=300)
    assert len(buckets) == 1
    assert buckets[0].value == 100.0
    assert len(buckets[0].sample_event_ids) <= 32


def test_buckets_for_observation_picks_max_in_window() -> None:
    """observation = max bucket value within the observed_window for each key."""
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=2
    )
    now = datetime(2026, 5, 5, 12, 7, 0, tzinfo=UTC)
    key = AnomalyKey(component="c", op="o", tenant_id="t")
    # Three buckets in the last 30m; the 12:00 bucket has 50 events.
    for ts, value in [
        (now - timedelta(minutes=20), 5.0),
        (now - timedelta(minutes=10), 50.0),
        (now - timedelta(minutes=5), 7.0),
    ]:
        store.record_bucket(
            MetricBucket(
                key=key,
                bucket_start=store.align_bucket(ts),
                bucket_end=store.align_bucket(ts) + timedelta(seconds=300),
                value=value,
            ),
            now=now,
        )
    obs = buckets_for_observation(store, observed_window_seconds=30 * 60, now=now)
    assert len(obs) == 1
    assert obs[0].value == 50.0


def test_buckets_for_observation_window_smaller_than_bucket_returns_empty() -> None:
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=2
    )
    now = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)
    obs = buckets_for_observation(store, observed_window_seconds=60, now=now)
    assert obs == []


def test_ingest_cycle_missing_reeve_url_raises() -> None:
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=2
    )
    with pytest.raises(IngestError):
        ingest_cycle(
            reeve_conninfo="",
            vigil_conninfo="postgresql://localhost/x",
            baseline_store=store,
        )


def test_ingest_cycle_missing_vigil_url_raises() -> None:
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=2
    )
    with pytest.raises(IngestError):
        ingest_cycle(
            reeve_conninfo="postgresql://localhost/x",
            vigil_conninfo="",
            baseline_store=store,
        )


# ---- DB-backed: pull cycle + idempotency --------------------------------


def _create_trace_samples_table(conninfo: str) -> None:
    with connect(conninfo) as conn, conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trace_samples (
                id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                tenant_id     uuid,
                op            text NOT NULL,
                correlation_id text,
                outcome       text NOT NULL,
                elapsed_ms    integer NOT NULL,
                entries       jsonb NOT NULL DEFAULT '[]'::jsonb,
                started_at    timestamptz NOT NULL,
                ended_at      timestamptz NOT NULL DEFAULT now()
            )
            """
        )


def _insert_trace_sample(
    conninfo: str,
    *,
    op: str,
    tenant_id: str | None,
    entries: list[dict],
    ended_at: datetime,
) -> str:
    import json as _json
    new_id = str(uuid.uuid4())
    with connect(conninfo) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO trace_samples
                (id, tenant_id, op, outcome, elapsed_ms, entries, started_at, ended_at)
            VALUES (%s, %s, %s, 'ok', 5, %s::jsonb, %s, %s)
            """,
            (
                new_id,
                uuid.UUID(tenant_id) if tenant_id else None,
                op,
                _json.dumps(entries),
                ended_at,
                ended_at,
            ),
        )
    return new_id


@require_db
def test_ingest_cycle_pull_and_idempotency(fresh_db: str) -> None:
    """Running ingest twice should produce identical baseline state."""
    _create_trace_samples_table(fresh_db)
    now = datetime.now(tz=UTC).replace(microsecond=0)
    tenant = str(uuid.uuid4())
    # Insert 5 trace_samples spanning ~25 minutes.
    for i in range(5):
        ts = now - timedelta(minutes=5 + i * 5)
        _insert_trace_sample(
            fresh_db,
            op="GET /probe",
            tenant_id=tenant,
            entries=[
                {"component": "reeve.adapters.llm", "op": "messages.create", "elapsedMs": 10, "outcome": "ok"},
            ],
            ended_at=ts,
        )

    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=2
    )
    stats1 = ingest_cycle(
        reeve_conninfo=fresh_db,
        vigil_conninfo=fresh_db,
        baseline_store=store,
        now=now,
    )
    assert stats1.rows_read == 5
    assert stats1.buckets_recorded >= 1
    cursor1 = get_cursor(fresh_db, source=REEVE_TRACE_SOURCE, component="*", op="*")
    assert cursor1 is not None

    # Snapshot baseline state for comparison.
    store_snapshot_1 = store.to_dict()

    # Re-run: should see no new rows (cursor advanced) → no change.
    stats2 = ingest_cycle(
        reeve_conninfo=fresh_db,
        vigil_conninfo=fresh_db,
        baseline_store=store,
        now=now,
    )
    assert stats2.rows_read == 0
    cursor2 = get_cursor(fresh_db, source=REEVE_TRACE_SOURCE, component="*", op="*")
    assert cursor2 == cursor1
    store_snapshot_2 = store.to_dict()

    # Strip computed_at-like fields and compare the meaningful state.
    assert store_snapshot_1["series"] == store_snapshot_2["series"]


@require_db
def test_ingest_cycle_advances_cursor_on_new_data(fresh_db: str) -> None:
    _create_trace_samples_table(fresh_db)
    now = datetime.now(tz=UTC).replace(microsecond=0)
    tenant = str(uuid.uuid4())
    _insert_trace_sample(
        fresh_db, op="op1", tenant_id=tenant,
        entries=[{"component": "c", "op": "o", "elapsedMs": 1, "outcome": "ok"}],
        ended_at=now - timedelta(minutes=10),
    )
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=2
    )
    ingest_cycle(reeve_conninfo=fresh_db, vigil_conninfo=fresh_db, baseline_store=store, now=now)
    cursor1 = get_cursor(fresh_db, source=REEVE_TRACE_SOURCE, component="*", op="*")

    # Add one more row; advance time forward by 1 second on cycle so new
    # row is within "now" window.
    later = now + timedelta(seconds=1)
    _insert_trace_sample(
        fresh_db, op="op1", tenant_id=tenant,
        entries=[{"component": "c", "op": "o", "elapsedMs": 1, "outcome": "ok"}],
        ended_at=later,
    )
    ingest_cycle(reeve_conninfo=fresh_db, vigil_conninfo=fresh_db, baseline_store=store, now=later + timedelta(seconds=1))
    cursor2 = get_cursor(fresh_db, source=REEVE_TRACE_SOURCE, component="*", op="*")
    assert cursor2 is not None and cursor1 is not None
    assert cursor2 > cursor1


@require_db
def test_ingest_cycle_handles_unreachable_source(fresh_db: str) -> None:
    """When REEVE_DATABASE_URL points at a dead DB, IngestError surfaces."""
    store = BaselineStore(
        bucket_seconds=300, window_seconds=86400, min_buckets_for_baseline=2
    )
    bad = "postgresql://nobody@127.0.0.1:1/nonexistent"
    with pytest.raises(IngestError):
        ingest_cycle(reeve_conninfo=bad, vigil_conninfo=fresh_db, baseline_store=store)


# Suppress unused warning for `os` (explicitly imported for env interaction
# in further tests).
_ = os
