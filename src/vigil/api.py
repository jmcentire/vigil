"""FastAPI HTTP layer — forensic queries over the anomalies table.

V1 endpoints (ADR-001):

    GET  /v1/health                              — liveness, for Baton's adapter
    GET  /v1/anomalies?component=&op=&tenant=
              &since=24h&include_dismissed=&limit=&offset=
    GET  /v1/anomalies/{id}
    POST /v1/anomalies/{id}/dismiss   { "reason": "..." }

Discipline: this module imports `persist` and `config`, never `baseline`
or `detect`. Detection is a different process; the API is read-mostly
(plus dismiss). When the API and the run-loop share a process (the
default `vigil run`), they share a Config but never a BaselineStore —
the API does not need it.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Path, Query, status
from pydantic import BaseModel, Field

from vigil import __version__
from vigil.config import Config
from vigil.persist import (
    dismiss_anomaly,
    get_anomaly,
    query_anomalies,
)
from vigil.types import Anomaly


class AnomalyOut(BaseModel):
    id: str
    pattern_id: str
    component: str
    op: str
    tenant_id: str | None
    baseline_window: str
    baseline_value: float
    observed_window: str
    observed_value: float
    multiplier: float
    sample_event_ids: list[str]
    detected_at: datetime
    dismissed_at: datetime | None = None
    dismiss_reason: str | None = None

    @staticmethod
    def from_anomaly(a: Anomaly) -> AnomalyOut:
        return AnomalyOut(
            id=a.id or "",
            pattern_id=a.pattern_id,
            component=a.component,
            op=a.op,
            tenant_id=a.tenant_id,
            baseline_window=a.baseline_window,
            baseline_value=a.baseline_value,
            observed_window=a.observed_window,
            observed_value=a.observed_value,
            multiplier=a.multiplier,
            sample_event_ids=list(a.sample_event_ids),
            detected_at=a.detected_at,
            dismissed_at=a.dismissed_at,
            dismiss_reason=a.dismiss_reason,
        )


class AnomalyList(BaseModel):
    anomalies: list[AnomalyOut]
    count: int
    limit: int
    offset: int


class DismissRequest(BaseModel):
    reason: str = Field(min_length=1, max_length=2000)


class HealthResponse(BaseModel):
    status: str
    version: str


_DURATION_RE = re.compile(r"^\s*(\d+)\s*([smhd])\s*$", re.IGNORECASE)


def _parse_since(value: str | None) -> datetime | None:
    """Accept '24h', '7d', '30m', '60s' or an ISO-8601 timestamp.

    Returns a UTC datetime or None.
    """
    if not value:
        return None
    m = _DURATION_RE.match(value)
    if m:
        n = int(m.group(1))
        unit = m.group(2).lower()
        seconds = {
            "s": n,
            "m": n * 60,
            "h": n * 60 * 60,
            "d": n * 60 * 60 * 24,
        }[unit]
        return datetime.now(tz=UTC) - timedelta(seconds=seconds)
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as err:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid `since`: {value!r} — use '24h' / '7d' / ISO-8601.",
        ) from err
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def _validate_uuid(value: str) -> str:
    try:
        UUID(value)
    except ValueError as err:
        raise HTTPException(
            status_code=400, detail=f"Invalid id: {value!r}"
        ) from err
    return value


def create_app(config: Config) -> FastAPI:
    """Build the FastAPI app with config baked in (closure over conninfo)."""
    app = FastAPI(
        title="vigil",
        version=__version__,
        description=(
            "Sub-threshold anomaly detection for the Exemplar stack. "
            "Read-mostly forensic API; detection runs in `vigil run`."
        ),
    )

    def get_config() -> Config:
        return config

    @app.get("/v1/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(status="ok", version=__version__)

    @app.get("/v1/anomalies", response_model=AnomalyList)
    def list_anomalies(
        cfg: Config = Depends(get_config),
        component: str | None = Query(None),
        op: str | None = Query(None),
        tenant: str | None = Query(None, alias="tenant"),
        since: str | None = Query(
            None,
            description=(
                "Window for filtering. Accepts '24h' / '7d' / '30m' / '60s' "
                "or ISO-8601 timestamp."
            ),
        ),
        include_dismissed: bool = Query(False),
        limit: int = Query(100, ge=1),
        offset: int = Query(0, ge=0),
    ) -> AnomalyList:
        if limit > cfg.max_page_size:
            raise HTTPException(
                status_code=400,
                detail=f"limit must be <= {cfg.max_page_size}",
            )
        since_dt = _parse_since(since)
        anomalies = query_anomalies(
            cfg.vigil_database_url,
            component=component,
            op=op,
            tenant_id=tenant,
            since=since_dt,
            include_dismissed=include_dismissed,
            limit=limit,
            offset=offset,
        )
        return AnomalyList(
            anomalies=[AnomalyOut.from_anomaly(a) for a in anomalies],
            count=len(anomalies),
            limit=limit,
            offset=offset,
        )

    @app.get("/v1/anomalies/{anomaly_id}", response_model=AnomalyOut)
    def get_one(
        anomaly_id: str = Path(...),
        cfg: Config = Depends(get_config),
    ) -> AnomalyOut:
        _validate_uuid(anomaly_id)
        a = get_anomaly(cfg.vigil_database_url, anomaly_id)
        if a is None:
            raise HTTPException(status_code=404, detail="anomaly not found")
        return AnomalyOut.from_anomaly(a)

    @app.post(
        "/v1/anomalies/{anomaly_id}/dismiss",
        status_code=status.HTTP_200_OK,
        response_model=AnomalyOut,
    )
    def dismiss(
        body: DismissRequest,
        anomaly_id: str = Path(...),
        cfg: Config = Depends(get_config),
    ) -> AnomalyOut:
        _validate_uuid(anomaly_id)
        ok = dismiss_anomaly(cfg.vigil_database_url, anomaly_id, body.reason)
        if not ok:
            raise HTTPException(status_code=404, detail="anomaly not found")
        a = get_anomaly(cfg.vigil_database_url, anomaly_id)
        if a is None:
            # extremely-narrow race: someone deleted between the UPDATE
            # and the SELECT. Surface as 404.
            raise HTTPException(status_code=404, detail="anomaly not found")
        return AnomalyOut.from_anomaly(a)

    return app


__all__ = ["create_app", "AnomalyOut", "AnomalyList", "DismissRequest"]
