"""Configuration — env-var-driven, no config file.

ADR-001 keeps vigil simple: env vars, no YAML, no per-tenant overrides.
When per-tenant tuning is needed, that's a separate ADR (and probably a
DB-backed override table; see `dismiss_reason` flow in detect.py).
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Config:
    # Multiplicative threshold against baseline_p95. Default 3x — anything
    # above this fires an Anomaly. Tunable per deployment via env.
    multiplier: float
    # Baseline window in seconds. ADR-001 V1: 7 days.
    baseline_window_seconds: int
    # Observed window in seconds. ADR-001 V1: 5 minutes.
    observed_window_seconds: int
    # Bucket size in seconds for baseline aggregation. The baseline_p95
    # is the 95th-percentile of *bucket values*, not raw events. We use
    # 5-minute buckets so the baseline window has 7d/5m = 2016 samples.
    bucket_seconds: int
    # Minimum buckets required before a baseline is "warm" enough to detect
    # against. Cold buckets are skipped (no false-positives during cold start).
    min_buckets_for_baseline: int
    # How often the ingest+detect loop runs (seconds).
    loop_interval_seconds: int
    # Where the baseline snapshot is persisted between restarts.
    baseline_snapshot_path: str
    # How often the baseline snapshot is written to disk (seconds).
    baseline_snapshot_interval_seconds: int
    # Source DB (Reeve's trace_samples). Read-only access.
    reeve_database_url: str
    # Vigil's own DB (anomalies, ingest_cursors). Read-write.
    vigil_database_url: str
    # API host/port for `vigil run`'s HTTP server.
    api_host: str
    api_port: int
    # Default page size for /v1/anomalies.
    default_page_size: int
    max_page_size: int

    @staticmethod
    def from_env() -> Config:
        return Config(
            multiplier=_float_env("VIGIL_MULTIPLIER", 3.0),
            baseline_window_seconds=_int_env(
                "VIGIL_BASELINE_WINDOW_SECONDS", 7 * 24 * 60 * 60
            ),
            observed_window_seconds=_int_env("VIGIL_OBSERVED_WINDOW_SECONDS", 5 * 60),
            bucket_seconds=_int_env("VIGIL_BUCKET_SECONDS", 5 * 60),
            min_buckets_for_baseline=_int_env("VIGIL_MIN_BUCKETS", 24),
            loop_interval_seconds=_int_env("VIGIL_LOOP_INTERVAL_SECONDS", 60),
            baseline_snapshot_path=os.environ.get(
                "VIGIL_BASELINE_SNAPSHOT_PATH", "/var/lib/vigil/baselines.json"
            ),
            baseline_snapshot_interval_seconds=_int_env(
                "VIGIL_BASELINE_SNAPSHOT_INTERVAL_SECONDS", 5 * 60
            ),
            reeve_database_url=os.environ.get(
                "REEVE_DATABASE_URL",
                os.environ.get("DATABASE_URL", ""),
            ),
            vigil_database_url=os.environ.get(
                "VIGIL_DATABASE_URL",
                os.environ.get("DATABASE_URL", ""),
            ),
            api_host=os.environ.get("VIGIL_API_HOST", "0.0.0.0"),
            api_port=_int_env("VIGIL_API_PORT", 8080),
            default_page_size=_int_env("VIGIL_DEFAULT_PAGE_SIZE", 100),
            max_page_size=_int_env("VIGIL_MAX_PAGE_SIZE", 500),
        )


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default
