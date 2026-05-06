"""Tests for the FastAPI surface — routing, parsing, dismiss flow.

Pure-route tests (since-parsing, validation) run anywhere. Tests that
exercise the DB path are gated on VIGIL_TEST_DATABASE_URL.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tests.conftest import require_db
from vigil.api import _parse_since, create_app
from vigil.config import Config
from vigil.persist import insert_anomalies
from vigil.types import Anomaly


def _make_config(db_url: str, tmp_path: Path) -> Config:
    return Config(
        multiplier=3.0,
        baseline_window_seconds=7 * 24 * 60 * 60,
        observed_window_seconds=5 * 60,
        bucket_seconds=5 * 60,
        min_buckets_for_baseline=12,
        loop_interval_seconds=60,
        baseline_snapshot_path=str(tmp_path / "baselines.json"),
        baseline_snapshot_interval_seconds=300,
        reeve_database_url=db_url,
        vigil_database_url=db_url,
        api_host="127.0.0.1",
        api_port=0,
        default_page_size=100,
        max_page_size=500,
    )


def _seed_anomaly(db_url: str, **overrides) -> str:
    base = dict(
        pattern_id="vigil-test-1",
        component="reeve.adapters.llm",
        op="messages.create",
        tenant_id=str(uuid.uuid4()),
        baseline_window="7d",
        baseline_value=10.0,
        observed_window="5m",
        observed_value=47.0,
        multiplier=4.7,
        sample_event_ids=("e1", "e2"),
        detected_at=datetime.now(tz=UTC),
    )
    base.update(overrides)
    a = Anomaly(**base)
    ids = insert_anomalies(db_url, [a])
    return ids[0]


def test_parse_since_relative_durations() -> None:
    now = datetime.now(tz=UTC)
    h24 = _parse_since("24h")
    assert h24 is not None
    assert (now - h24) - timedelta(hours=24) < timedelta(seconds=2)
    h7d = _parse_since("7d")
    assert h7d is not None
    assert (now - h7d) - timedelta(days=7) < timedelta(seconds=2)
    m30 = _parse_since("30m")
    assert m30 is not None
    assert (now - m30) - timedelta(minutes=30) < timedelta(seconds=2)
    s60 = _parse_since("60s")
    assert s60 is not None
    assert (now - s60) - timedelta(seconds=60) < timedelta(seconds=2)


def test_parse_since_iso() -> None:
    iso = "2026-05-04T12:00:00+00:00"
    out = _parse_since(iso)
    assert out == datetime(2026, 5, 4, 12, 0, 0, tzinfo=UTC)


def test_parse_since_invalid() -> None:
    from fastapi import HTTPException

    with pytest.raises(HTTPException):
        _parse_since("not-a-duration")


def test_parse_since_none() -> None:
    assert _parse_since(None) is None
    assert _parse_since("") is None


def test_health_endpoint_no_db(tmp_path: Path) -> None:
    """/v1/health works without DB (it must — Baton uses it for liveness)."""
    cfg = _make_config("postgresql://nobody@127.0.0.1:1/x", tmp_path)
    app = create_app(cfg)
    client = TestClient(app)
    resp = client.get("/v1/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert "version" in body


def test_invalid_uuid_400(tmp_path: Path) -> None:
    cfg = _make_config("postgresql://nobody@127.0.0.1:1/x", tmp_path)
    app = create_app(cfg)
    client = TestClient(app)
    resp = client.get("/v1/anomalies/not-a-uuid")
    assert resp.status_code == 400


def test_invalid_uuid_dismiss_400(tmp_path: Path) -> None:
    cfg = _make_config("postgresql://nobody@127.0.0.1:1/x", tmp_path)
    app = create_app(cfg)
    client = TestClient(app)
    resp = client.post(
        "/v1/anomalies/not-a-uuid/dismiss",
        json={"reason": "noise"},
    )
    assert resp.status_code == 400


def test_dismiss_validation_rejects_empty_reason(tmp_path: Path) -> None:
    cfg = _make_config("postgresql://nobody@127.0.0.1:1/x", tmp_path)
    app = create_app(cfg)
    client = TestClient(app)
    fake_id = str(uuid.uuid4())
    resp = client.post(f"/v1/anomalies/{fake_id}/dismiss", json={"reason": ""})
    assert resp.status_code == 422  # pydantic min_length=1


@require_db
def test_list_returns_seeded_anomaly(fresh_db: str, tmp_path: Path) -> None:
    aid = _seed_anomaly(fresh_db)
    cfg = _make_config(fresh_db, tmp_path)
    client = TestClient(create_app(cfg))
    resp = client.get("/v1/anomalies")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["anomalies"][0]["id"] == aid


@require_db
def test_list_filters_by_component(fresh_db: str, tmp_path: Path) -> None:
    _seed_anomaly(fresh_db, component="comp-a")
    _seed_anomaly(fresh_db, component="comp-b", pattern_id="vigil-test-2")
    cfg = _make_config(fresh_db, tmp_path)
    client = TestClient(create_app(cfg))
    resp = client.get("/v1/anomalies?component=comp-a")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["anomalies"][0]["component"] == "comp-a"


@require_db
def test_list_filters_by_op_and_tenant(fresh_db: str, tmp_path: Path) -> None:
    t1 = str(uuid.uuid4())
    t2 = str(uuid.uuid4())
    _seed_anomaly(fresh_db, op="op1", tenant_id=t1)
    _seed_anomaly(fresh_db, op="op2", tenant_id=t2, pattern_id="p2")
    cfg = _make_config(fresh_db, tmp_path)
    client = TestClient(create_app(cfg))

    resp_op = client.get("/v1/anomalies?op=op1")
    assert resp_op.status_code == 200
    assert resp_op.json()["count"] == 1
    assert resp_op.json()["anomalies"][0]["op"] == "op1"

    resp_t = client.get(f"/v1/anomalies?tenant={t2}")
    assert resp_t.status_code == 200
    assert resp_t.json()["count"] == 1
    assert resp_t.json()["anomalies"][0]["tenant_id"] == t2


@require_db
def test_list_excludes_dismissed_by_default(fresh_db: str, tmp_path: Path) -> None:
    aid = _seed_anomaly(fresh_db, op="op-dismissed")
    cfg = _make_config(fresh_db, tmp_path)
    client = TestClient(create_app(cfg))
    # Dismiss it.
    r = client.post(f"/v1/anomalies/{aid}/dismiss", json={"reason": "noise"})
    assert r.status_code == 200
    # Default list — excluded.
    body = client.get("/v1/anomalies?op=op-dismissed").json()
    assert body["count"] == 0
    # With include_dismissed — included.
    body2 = client.get("/v1/anomalies?op=op-dismissed&include_dismissed=true").json()
    assert body2["count"] == 1
    assert body2["anomalies"][0]["dismissed_at"] is not None


@require_db
def test_get_one_404(fresh_db: str, tmp_path: Path) -> None:
    cfg = _make_config(fresh_db, tmp_path)
    client = TestClient(create_app(cfg))
    fake_id = str(uuid.uuid4())
    resp = client.get(f"/v1/anomalies/{fake_id}")
    assert resp.status_code == 404


@require_db
def test_get_one_returns_seeded(fresh_db: str, tmp_path: Path) -> None:
    aid = _seed_anomaly(fresh_db)
    cfg = _make_config(fresh_db, tmp_path)
    client = TestClient(create_app(cfg))
    resp = client.get(f"/v1/anomalies/{aid}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == aid
    assert body["multiplier"] == pytest.approx(4.7)
    assert body["sample_event_ids"] == ["e1", "e2"]


@require_db
def test_dismiss_endpoint(fresh_db: str, tmp_path: Path) -> None:
    aid = _seed_anomaly(fresh_db)
    cfg = _make_config(fresh_db, tmp_path)
    client = TestClient(create_app(cfg))
    resp = client.post(f"/v1/anomalies/{aid}/dismiss", json={"reason": "known issue"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["dismissed_at"] is not None
    assert body["dismiss_reason"] == "known issue"


@require_db
def test_dismiss_404_for_unknown_id(fresh_db: str, tmp_path: Path) -> None:
    cfg = _make_config(fresh_db, tmp_path)
    client = TestClient(create_app(cfg))
    fake_id = str(uuid.uuid4())
    resp = client.post(f"/v1/anomalies/{fake_id}/dismiss", json={"reason": "x"})
    assert resp.status_code == 404


@require_db
def test_dismiss_idempotent(fresh_db: str, tmp_path: Path) -> None:
    aid = _seed_anomaly(fresh_db)
    cfg = _make_config(fresh_db, tmp_path)
    client = TestClient(create_app(cfg))
    r1 = client.post(f"/v1/anomalies/{aid}/dismiss", json={"reason": "first"})
    assert r1.status_code == 200
    first_dismissed_at = r1.json()["dismissed_at"]
    r2 = client.post(f"/v1/anomalies/{aid}/dismiss", json={"reason": "second"})
    assert r2.status_code == 200
    # The first dismissal wins (we COALESCE so the original reason and
    # timestamp are preserved).
    assert r2.json()["dismissed_at"] == first_dismissed_at
    assert r2.json()["dismiss_reason"] == "first"


@require_db
def test_pagination(fresh_db: str, tmp_path: Path) -> None:
    for i in range(7):
        _seed_anomaly(fresh_db, pattern_id=f"p{i}", op=f"op{i}")
    cfg = _make_config(fresh_db, tmp_path)
    client = TestClient(create_app(cfg))
    page_1 = client.get("/v1/anomalies?limit=3&offset=0").json()
    page_2 = client.get("/v1/anomalies?limit=3&offset=3").json()
    page_3 = client.get("/v1/anomalies?limit=3&offset=6").json()
    assert page_1["count"] == 3
    assert page_2["count"] == 3
    assert page_3["count"] == 1
    ids_1 = {a["id"] for a in page_1["anomalies"]}
    ids_2 = {a["id"] for a in page_2["anomalies"]}
    assert ids_1.isdisjoint(ids_2)


@require_db
def test_limit_validation_above_max(fresh_db: str, tmp_path: Path) -> None:
    cfg = _make_config(fresh_db, tmp_path)
    client = TestClient(create_app(cfg))
    resp = client.get(f"/v1/anomalies?limit={cfg.max_page_size + 1}")
    assert resp.status_code == 400
