"""Persistence for vigil's anomalies + ingest_cursors tables.

Uses psycopg 3 with simple connection-per-call (no pool — vigil is
single-instance, low-volume; a pool is premature). All public functions
take a `conninfo` (string DSN) so tests can point at a different DB.

Discipline:
    - All timestamp columns are `timestamptz`. Python side passes
      timezone-aware datetimes; psycopg adapts directly.
    - sample_event_ids is `text[]` — psycopg adapts a Python list/tuple
      of str directly.
    - Reads always order by `detected_at DESC` (newest first; matches
      operator workflow).
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from vigil.types import Anomaly


@contextmanager
def connect(conninfo: str) -> Iterator[psycopg.Connection]:
    """Yield a psycopg connection; commit on success, rollback on exception."""
    if not conninfo:
        raise RuntimeError(
            "connect() called with empty conninfo. Set VIGIL_DATABASE_URL."
        )
    conn = psycopg.connect(conninfo, autocommit=False)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def insert_anomalies(conninfo: str, anomalies: Sequence[Anomaly]) -> list[str]:
    """Insert anomalies; return their IDs in input order.

    Note: this is fire-and-forget INSERT. We do not de-duplicate against
    existing rows here — the detect layer is responsible for not
    re-evaluating the same observation twice. (Pattern_id collapses
    same-day re-firings on the read side.)
    """
    if not anomalies:
        return []
    ids: list[str] = []
    with connect(conninfo) as conn, conn.cursor() as cur:
        for a in anomalies:
            cur.execute(
                """
                INSERT INTO anomalies (
                    pattern_id, component, op, tenant_id,
                    baseline_window, baseline_value,
                    observed_window, observed_value,
                    multiplier, sample_event_ids, detected_at
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s
                )
                RETURNING id
                """,
                (
                    a.pattern_id,
                    a.component,
                    a.op,
                    _maybe_uuid(a.tenant_id),
                    a.baseline_window,
                    a.baseline_value,
                    a.observed_window,
                    a.observed_value,
                    a.multiplier,
                    list(a.sample_event_ids),
                    a.detected_at,
                ),
            )
            row = cur.fetchone()
            assert row is not None
            ids.append(str(row[0]))
    return ids


def query_anomalies(
    conninfo: str,
    *,
    component: str | None = None,
    op: str | None = None,
    tenant_id: str | None = None,
    since: datetime | None = None,
    include_dismissed: bool = False,
    limit: int = 100,
    offset: int = 0,
) -> list[Anomaly]:
    """Forensic query — returns anomalies matching filters, newest first."""
    conditions: list[str] = []
    params: list[object] = []
    if component:
        conditions.append("component = %s")
        params.append(component)
    if op:
        conditions.append("op = %s")
        params.append(op)
    if tenant_id:
        conditions.append("tenant_id = %s")
        params.append(_maybe_uuid(tenant_id))
    if since is not None:
        conditions.append("detected_at >= %s")
        params.append(since)
    if not include_dismissed:
        conditions.append("dismissed_at IS NULL")
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = f"""
        SELECT id, pattern_id, component, op, tenant_id,
               baseline_window, baseline_value,
               observed_window, observed_value,
               multiplier, sample_event_ids, detected_at,
               dismissed_at, dismiss_reason
          FROM anomalies
          {where}
         ORDER BY detected_at DESC
         LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])
    with connect(conninfo) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return [_row_to_anomaly(row) for row in rows]


def get_anomaly(conninfo: str, anomaly_id: str) -> Anomaly | None:
    """Lookup by id. Returns None if not found."""
    with connect(conninfo) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT id, pattern_id, component, op, tenant_id,
                   baseline_window, baseline_value,
                   observed_window, observed_value,
                   multiplier, sample_event_ids, detected_at,
                   dismissed_at, dismiss_reason
              FROM anomalies
             WHERE id = %s
            """,
            (_maybe_uuid(anomaly_id),),
        )
        row = cur.fetchone()
    return _row_to_anomaly(row) if row is not None else None


def dismiss_anomaly(conninfo: str, anomaly_id: str, reason: str) -> bool:
    """Mark an anomaly dismissed. Returns True if a row was updated.

    Idempotent: dismissing an already-dismissed anomaly returns True
    without changing the existing dismiss_reason (operators may be
    repeating themselves; we don't surprise them).
    """
    if not reason or not reason.strip():
        raise ValueError("dismiss reason must be non-empty")
    with connect(conninfo) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE anomalies
               SET dismissed_at = COALESCE(dismissed_at, now()),
                   dismiss_reason = COALESCE(dismiss_reason, %s)
             WHERE id = %s
            """,
            (reason, _maybe_uuid(anomaly_id)),
        )
        return cur.rowcount > 0


# --- Ingest cursors -------------------------------------------------------


def get_cursor(
    conninfo: str, *, source: str, component: str, op: str
) -> datetime | None:
    with connect(conninfo) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_event_at FROM ingest_cursors
             WHERE source = %s AND component = %s AND op = %s
            """,
            (source, component, op),
        )
        row = cur.fetchone()
    return row[0] if row else None


def update_cursor(
    conninfo: str,
    *,
    source: str,
    component: str,
    op: str,
    last_event_at: datetime,
) -> None:
    """Upsert the ingest cursor for a (source, component, op) tuple."""
    with connect(conninfo) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingest_cursors (source, component, op, last_event_at, updated_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (source, component, op) DO UPDATE
               SET last_event_at = GREATEST(ingest_cursors.last_event_at, EXCLUDED.last_event_at),
                   updated_at = now()
            """,
            (source, component, op, last_event_at),
        )


# --- Migrations -----------------------------------------------------------


def run_migrations(conninfo: str, migrations_dir: str) -> list[str]:
    """Apply every .sql in migrations_dir in lexical order.

    Tracks applied migrations in `vigil_migrations` table. Returns the
    list of newly-applied migration filenames.
    """
    import os

    files = sorted(f for f in os.listdir(migrations_dir) if f.endswith(".sql"))
    applied: list[str] = []
    with connect(conninfo) as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS vigil_migrations (
                filename text PRIMARY KEY,
                applied_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        cur.execute("SELECT filename FROM vigil_migrations")
        already = {row[0] for row in cur.fetchall()}
        for fname in files:
            if fname in already:
                continue
            path = os.path.join(migrations_dir, fname)
            with open(path, encoding="utf-8") as f:
                sql = f.read()
            cur.execute(sql)
            cur.execute(
                "INSERT INTO vigil_migrations (filename) VALUES (%s)", (fname,)
            )
            applied.append(fname)
    return applied


# --- Internal helpers -----------------------------------------------------


def _maybe_uuid(value: str | None) -> UUID | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def _row_to_anomaly(row: dict) -> Anomaly:
    return Anomaly(
        id=str(row["id"]) if row.get("id") is not None else None,
        pattern_id=row["pattern_id"],
        component=row["component"],
        op=row["op"],
        tenant_id=str(row["tenant_id"]) if row.get("tenant_id") is not None else None,
        baseline_window=row["baseline_window"],
        baseline_value=float(row["baseline_value"]),
        observed_window=row["observed_window"],
        observed_value=float(row["observed_value"]),
        multiplier=float(row["multiplier"]),
        sample_event_ids=tuple(row["sample_event_ids"] or ()),
        detected_at=row["detected_at"],
        dismissed_at=row.get("dismissed_at"),
        dismiss_reason=row.get("dismiss_reason"),
    )


__all__ = [
    "connect",
    "insert_anomalies",
    "query_anomalies",
    "get_anomaly",
    "dismiss_anomaly",
    "get_cursor",
    "update_cursor",
    "run_migrations",
]


# Suppress unused warnings for asdict / timedelta which are imported for
# possible future test/diagnostic use.
_ = (asdict, timedelta, timezone)
