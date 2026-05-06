"""vigil CLI — entry points for `vigil run | query | migrate | baseline`.

Discipline: this is the ONE place that wires Config → BaselineStore →
ingest_cycle → detect → persist. Everything below is library code.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import click

from vigil import __version__
from vigil.api import create_app
from vigil.baseline import BaselineStore
from vigil.config import Config
from vigil.detect import evaluate_buckets
from vigil.ingest import IngestError, buckets_for_observation, ingest_cycle
from vigil.persist import insert_anomalies, query_anomalies, run_migrations

logger = logging.getLogger("vigil")

# Where the bundled migrations live — installed alongside the package.
_PACKAGE_DIR = Path(__file__).resolve().parent
_DEFAULT_MIGRATIONS_DIR = _PACKAGE_DIR.parent.parent / "migrations"


def _find_migrations_dir() -> Path:
    """Locate migrations dir whether running from source tree or installed."""
    candidates = [
        _DEFAULT_MIGRATIONS_DIR,
        Path.cwd() / "migrations",
        _PACKAGE_DIR / "migrations",
    ]
    for c in candidates:
        if c.is_dir() and any(p.suffix == ".sql" for p in c.iterdir()):
            return c
    raise click.ClickException(
        "Could not locate migrations/ directory. "
        f"Tried: {[str(c) for c in candidates]}"
    )


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@click.group()
@click.version_option(__version__)
@click.option(
    "--log-level",
    default=os.environ.get("VIGIL_LOG_LEVEL", "info"),
    help="Log level (debug, info, warning, error).",
)
def cli(log_level: str) -> None:
    """vigil — sub-threshold anomaly detection."""
    _setup_logging(log_level)


@cli.command()
def migrate() -> None:
    """Apply pending DB migrations against VIGIL_DATABASE_URL."""
    cfg = Config.from_env()
    if not cfg.vigil_database_url:
        raise click.ClickException("VIGIL_DATABASE_URL is not set.")
    migrations_dir = _find_migrations_dir()
    applied = run_migrations(cfg.vigil_database_url, str(migrations_dir))
    if not applied:
        click.echo("No pending migrations.")
    else:
        click.echo(f"Applied: {', '.join(applied)}")


@cli.command()
@click.option("--component")
@click.option("--op")
@click.option("--tenant", "tenant_id")
@click.option(
    "--since",
    default="24h",
    help="Window — '24h' / '7d' / '30m' / ISO-8601. Default 24h.",
)
@click.option("--include-dismissed", is_flag=True, default=False)
@click.option("--limit", type=int, default=100)
@click.option("--json", "as_json", is_flag=True, default=False)
def query(
    component: str | None,
    op: str | None,
    tenant_id: str | None,
    since: str,
    include_dismissed: bool,
    limit: int,
    as_json: bool,
) -> None:
    """Ad-hoc anomaly query."""
    from vigil.api import _parse_since  # reuse parser

    cfg = Config.from_env()
    if not cfg.vigil_database_url:
        raise click.ClickException("VIGIL_DATABASE_URL is not set.")
    since_dt = _parse_since(since)
    anomalies = query_anomalies(
        cfg.vigil_database_url,
        component=component,
        op=op,
        tenant_id=tenant_id,
        since=since_dt,
        include_dismissed=include_dismissed,
        limit=limit,
    )
    if as_json:
        click.echo(
            json.dumps(
                [
                    {
                        "id": a.id,
                        "pattern_id": a.pattern_id,
                        "component": a.component,
                        "op": a.op,
                        "tenant_id": a.tenant_id,
                        "baseline_value": a.baseline_value,
                        "observed_value": a.observed_value,
                        "multiplier": round(a.multiplier, 3),
                        "detected_at": a.detected_at.isoformat(),
                        "dismissed_at": a.dismissed_at.isoformat()
                        if a.dismissed_at
                        else None,
                    }
                    for a in anomalies
                ],
                default=str,
            )
        )
        return
    if not anomalies:
        click.echo("No anomalies match.")
        return
    for a in anomalies:
        marker = "X" if a.dismissed_at else "*"
        click.echo(
            f"{marker} {a.detected_at.isoformat()}  "
            f"{a.component}:{a.op}  tenant={a.tenant_id or '-'}  "
            f"observed={a.observed_value:.1f}  baseline_p95={a.baseline_value:.1f}  "
            f"x{a.multiplier:.2f}  id={a.id}"
        )


@cli.command()
@click.option("--json", "as_json", is_flag=True, default=False)
def baseline(as_json: bool) -> None:
    """Print current in-memory baselines from the on-disk snapshot."""
    cfg = Config.from_env()
    snapshot_path = cfg.baseline_snapshot_path
    if not Path(snapshot_path).exists():
        raise click.ClickException(
            f"No baseline snapshot at {snapshot_path}. "
            "Run `vigil run` for at least one cycle first."
        )
    store = BaselineStore.load_from_path(
        snapshot_path,
        bucket_seconds=cfg.bucket_seconds,
        window_seconds=cfg.baseline_window_seconds,
        min_buckets_for_baseline=cfg.min_buckets_for_baseline,
    )
    snapshots = store.all_snapshots()
    if as_json:
        click.echo(
            json.dumps(
                [
                    {
                        "component": s.key.component,
                        "op": s.key.op,
                        "tenant_id": s.key.tenant_id,
                        "p95": s.p95,
                        "p50": s.p50,
                        "n_buckets": s.n,
                        "window_seconds": s.window_seconds,
                    }
                    for s in snapshots
                ]
            )
        )
        return
    if not snapshots:
        click.echo("No warm baselines yet (cold start; need more data).")
        return
    for s in snapshots:
        click.echo(
            f"{s.key.component}:{s.key.op}  tenant={s.key.tenant_id or '-'}  "
            f"p95={s.p95:.2f}  p50={s.p50:.2f}  n={s.n}"
        )


@cli.command()
@click.option(
    "--no-api",
    is_flag=True,
    default=False,
    help="Run the ingest+detect loop without serving the HTTP API.",
)
@click.option(
    "--no-loop",
    is_flag=True,
    default=False,
    help="Serve the HTTP API only (no ingest/detect loop).",
)
def run(no_api: bool, no_loop: bool) -> None:
    """Start the ingest+detect loop (and optionally serve the HTTP API)."""
    if no_api and no_loop:
        raise click.ClickException("Refuse to start with both --no-api and --no-loop.")
    cfg = Config.from_env()
    if not cfg.vigil_database_url:
        raise click.ClickException("VIGIL_DATABASE_URL is not set.")
    store = BaselineStore.load_from_path(
        cfg.baseline_snapshot_path,
        bucket_seconds=cfg.bucket_seconds,
        window_seconds=cfg.baseline_window_seconds,
        min_buckets_for_baseline=cfg.min_buckets_for_baseline,
    )

    stop = threading.Event()

    def _handle_signal(signum, frame):  # noqa: ARG001
        logger.info("received signal %s; shutting down", signum)
        stop.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    loop_thread: threading.Thread | None = None
    if not no_loop:
        loop_thread = threading.Thread(
            target=_run_loop,
            args=(cfg, store, stop),
            name="vigil-loop",
            daemon=True,
        )
        loop_thread.start()

    if no_api:
        # Just wait for the loop to be told to stop.
        try:
            while not stop.is_set():
                stop.wait(1.0)
        except KeyboardInterrupt:
            stop.set()
        if loop_thread is not None:
            loop_thread.join(timeout=10)
        return

    # API server (blocks).
    import uvicorn

    app = create_app(cfg)
    config = uvicorn.Config(
        app,
        host=cfg.api_host,
        port=cfg.api_port,
        log_level="info",
        access_log=False,
    )
    server = uvicorn.Server(config)

    # uvicorn installs its own signal handlers which conflict with ours.
    # Run uvicorn's serve loop in the main thread; on shutdown, set stop.
    try:
        asyncio.run(server.serve())
    finally:
        stop.set()
        if loop_thread is not None:
            loop_thread.join(timeout=10)


def _run_loop(cfg: Config, store: BaselineStore, stop: threading.Event) -> None:
    """Background ingest + detect loop. Runs until `stop` is set."""
    last_snapshot = datetime.now(tz=UTC) - timedelta(days=1)
    while not stop.is_set():
        cycle_start = datetime.now(tz=UTC)
        try:
            stats = ingest_cycle(
                reeve_conninfo=cfg.reeve_database_url,
                vigil_conninfo=cfg.vigil_database_url,
                baseline_store=store,
            )
            logger.info(
                "ingest: rows=%d buckets=%d keys=%d",
                stats.rows_read,
                stats.buckets_recorded,
                stats.keys_touched,
            )
            obs_buckets = buckets_for_observation(
                store,
                observed_window_seconds=cfg.observed_window_seconds,
            )
            anomalies = evaluate_buckets(
                obs_buckets, baseline_store=store, multiplier=cfg.multiplier
            )
            if anomalies:
                ids = insert_anomalies(cfg.vigil_database_url, anomalies)
                logger.warning(
                    "DETECTED %d anomalies (ids=%s)",
                    len(anomalies),
                    ids[:5],
                )
            else:
                logger.debug("no anomalies this cycle")
        except IngestError as err:
            logger.warning("ingest skipped: %s", err)
        except Exception:
            logger.exception("cycle failed")

        # Snapshot the baseline periodically.
        if (
            (cycle_start - last_snapshot).total_seconds()
            >= cfg.baseline_snapshot_interval_seconds
        ):
            try:
                store.snapshot_to_path(cfg.baseline_snapshot_path)
                last_snapshot = cycle_start
                logger.debug("baseline snapshot written to %s", cfg.baseline_snapshot_path)
            except OSError:
                logger.exception("failed to snapshot baseline")

        # Wait until next cycle (interruptible).
        elapsed = (datetime.now(tz=UTC) - cycle_start).total_seconds()
        sleep_for = max(0.0, cfg.loop_interval_seconds - elapsed)
        stop.wait(sleep_for)


def main() -> int:
    cli()  # type: ignore[no-untyped-call]
    return 0


if __name__ == "__main__":
    sys.exit(main())
