"""Pytest fixtures for vigil tests.

DB-touching tests are gated on VIGIL_TEST_DATABASE_URL. When unset, those
tests are skipped (we still want the unit suite green on a laptop without
postgres).
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from vigil.baseline import BaselineStore
from vigil.config import Config
from vigil.persist import connect, run_migrations
from vigil.types import AnomalyKey, MetricBucket


def _has_test_db() -> bool:
    return bool(os.environ.get("VIGIL_TEST_DATABASE_URL", "").strip())


require_db = pytest.mark.skipif(
    not _has_test_db(),
    reason="VIGIL_TEST_DATABASE_URL not set; skipping DB-backed tests.",
)


@pytest.fixture(scope="session")
def test_db_url() -> str:
    url = os.environ.get("VIGIL_TEST_DATABASE_URL", "").strip()
    if not url:
        pytest.skip("VIGIL_TEST_DATABASE_URL not set")
    return url


@pytest.fixture()
def fresh_db(test_db_url: str) -> Iterator[str]:
    """Drop+recreate vigil's tables before each DB test."""
    with connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "DROP TABLE IF EXISTS anomalies, ingest_cursors, "
            "vigil_migrations, trace_samples CASCADE"
        )
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    run_migrations(test_db_url, str(migrations_dir))
    # Tests that need trace_samples create it themselves; the bare vigil
    # migrations don't include it (that's reeve's table).
    yield test_db_url
    with connect(test_db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "DROP TABLE IF EXISTS anomalies, ingest_cursors, "
            "vigil_migrations, trace_samples CASCADE"
        )


@pytest.fixture()
def baseline_store_5m_7d() -> BaselineStore:
    return BaselineStore(
        bucket_seconds=5 * 60,
        window_seconds=7 * 24 * 60 * 60,
        min_buckets_for_baseline=12,
    )


@pytest.fixture()
def warm_store(baseline_store_5m_7d: BaselineStore) -> BaselineStore:
    """A baseline_store pre-loaded with 7d of synthetic data, value=1.0/bucket."""
    store = baseline_store_5m_7d
    now = datetime(2026, 5, 5, 12, 0, 0, tzinfo=UTC)
    bucket_seconds = store.bucket_seconds
    n_buckets = store.window_seconds // bucket_seconds
    key = AnomalyKey(component="test", op="probe", tenant_id=None)
    # Walk backwards from now in 5-minute buckets.
    for i in range(n_buckets):
        ts = now - timedelta(seconds=(n_buckets - i) * bucket_seconds)
        bucket_start = store.align_bucket(ts)
        store.record_bucket(
            MetricBucket(
                key=key,
                bucket_start=bucket_start,
                bucket_end=bucket_start + timedelta(seconds=bucket_seconds),
                value=1.0,
            ),
            now=now,
        )
    return store


@pytest.fixture()
def example_config(tmp_path: Path) -> Config:
    return Config(
        multiplier=3.0,
        baseline_window_seconds=7 * 24 * 60 * 60,
        observed_window_seconds=5 * 60,
        bucket_seconds=5 * 60,
        min_buckets_for_baseline=12,
        loop_interval_seconds=60,
        baseline_snapshot_path=str(tmp_path / "baselines.json"),
        baseline_snapshot_interval_seconds=300,
        reeve_database_url=os.environ.get("VIGIL_TEST_DATABASE_URL", ""),
        vigil_database_url=os.environ.get("VIGIL_TEST_DATABASE_URL", ""),
        api_host="127.0.0.1",
        api_port=0,
        default_page_size=100,
        max_page_size=500,
    )
