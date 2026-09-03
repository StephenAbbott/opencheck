"""``GET /source-health`` and the shaping behind it (Phase 161).

The sweep's report is read, never re-derived; what is served is a shaping of
it; and a missing report is reported as missing rather than as healthy.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck import source_health
from opencheck.app import app
from opencheck.config import get_settings

REPORT = {
    "generated_at": "2026-08-31T07:31:04Z",
    "registry_size": 40,
    "probed": 40,
    "counts": {"ok": 2, "degraded": 1, "fail": 0, "skipped": 1},
    "compared_against": "2026-08-24T07:30:58Z",
    "statement_collapses": {"gleif": {"relationship": {"was": 12, "now": 0}}},
    "sources": {
        "gleif": {
            "source_id": "gleif",
            "tier": "broad",
            "status": "ok",
            "reason": "",
            "liveness": "live",
            "retrieved_at": "2026-08-31T07:31:02Z",
            "latency_ms": 412,
            "result_size": 9001,
            "observed_fields": ["legal_name", "lei"],
            "attempts": 1,
            "known_gap": "",
            "statement_counts": {"entity": 3, "person": 0, "relationship": 9, "interest:shareholding": 9},
        },
        "jar_lithuania": {
            "source_id": "jar_lithuania",
            "tier": "broad",
            "status": "degraded",
            "reason": "register unreachable from CI (HTTP 403)",
            "liveness": None,
            "retrieved_at": None,
            "latency_ms": 1180,
            "result_size": 0,
            "observed_fields": [],
            "attempts": 2,
            "known_gap": "the register refuses datacentre IPs",
            "statement_counts": None,
        },
        "bolagsverket": {
            "source_id": "bolagsverket",
            "tier": "broad",
            "status": "skipped",
            "reason": "not configured: BOLAGSVERKET_API_KEY",
            "liveness": None,
            "retrieved_at": None,
            "latency_ms": None,
            "result_size": None,
            "observed_fields": [],
            "attempts": 0,
            "known_gap": "",
            "statement_counts": None,
        },
        "climate_trace": {
            "source_id": "climate_trace",
            "tier": "broad",
            "status": "ok",
            "reason": "",
            "liveness": "snapshot",
            "retrieved_at": "2026-08-01T00:00:00Z",
            "latency_ms": 1900,
            "result_size": 200,
            "observed_fields": ["emissions"],
            "attempts": 1,
            "known_gap": "",
            "statement_counts": {"entity": 1, "person": 0, "relationship": 0},
        },
    },
}

HISTORY = {
    "runs": [
        {"generated_at": "2026-08-17T07:30:00Z", "statuses": {"gleif": "ok", "jar_lithuania": "ok"}},
        {"generated_at": "2026-08-24T07:30:58Z", "statuses": {"gleif": "ok", "jar_lithuania": "degraded"}},
    ]
}


def test_shape_serves_a_shaping_of_the_report_not_the_report() -> None:
    out = source_health.shape(REPORT, HISTORY)
    assert out["available"] is True
    assert out["generated_at"] == "2026-08-31T07:31:04Z"
    assert out["counts"] == {"ok": 2, "degraded": 1, "fail": 0, "skipped": 1}
    gleif = out["sources"]["gleif"]
    assert gleif["status"] == "ok"
    assert gleif["liveness"] == "live"
    assert gleif["latency_ms"] == 412
    # Entity + person + relationship; the interest histogram is inside relationship.
    assert gleif["statement_total"] == 12
    assert gleif["statement_collapse"] == {"relationship": {"was": 12, "now": 0}}
    # Engineer's fields stay in the artifact.
    assert "observed_fields" not in gleif
    assert "result_size" not in gleif
    assert "tier" not in gleif
    jar = out["sources"]["jar_lithuania"]
    assert jar["known_gap"] == "the register refuses datacentre IPs"
    assert jar["attempts"] == 2
    assert jar["statement_total"] is None
    assert out["sources"]["bolagsverket"]["status"] == "skipped"


def test_shape_history_is_oldest_first_and_ends_with_the_current_run() -> None:
    out = source_health.shape(REPORT, HISTORY)
    assert out["sweeps"] == ["2026-08-17T07:30:00Z", "2026-08-24T07:30:58Z", "2026-08-31T07:31:04Z"]
    assert out["sources"]["jar_lithuania"]["history"] == ["ok", "degraded", "degraded"]
    # A source the older sweeps did not know carries only the runs it was in.
    assert out["sources"]["climate_trace"]["history"] == ["ok"]


def test_shape_without_history_and_with_the_run_already_appended() -> None:
    assert source_health.shape(REPORT, None)["sources"]["gleif"]["history"] == ["ok"]
    appended = {"runs": HISTORY["runs"] + [{"generated_at": REPORT["generated_at"], "statuses": {"gleif": "ok"}}]}
    out = source_health.shape(REPORT, appended)
    assert len(out["sweeps"]) == 3, "the current run is not appended twice"


def test_shape_caps_history_at_eight_sweeps() -> None:
    runs = [{"generated_at": f"2026-0{m}-0{d}T07:30:00Z", "statuses": {"gleif": "ok"}} for m in (5, 6, 7) for d in (1, 2, 3, 4)]
    out = source_health.shape(REPORT, {"runs": runs})
    assert len(out["sweeps"]) == source_health.HISTORY_SHOWN == 8
    assert out["sweeps"][-1] == REPORT["generated_at"]


def test_shape_reports_a_missing_report_as_missing() -> None:
    assert source_health.shape(None, None) == {"available": False, "reason": "no sweep report"}
    assert source_health.shape({"generated_at": "x"}, None)["available"] is False


@pytest.fixture
def _report_on_disk(monkeypatch, tmp_path: Path):
    (tmp_path / "source-health.json").write_text(json.dumps(REPORT))
    (tmp_path / source_health.HISTORY_FILENAME).write_text(json.dumps(HISTORY))
    monkeypatch.setenv("OPENCHECK_SOURCE_HEALTH_FILE", str(tmp_path / "source-health.json"))
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    source_health.reset_for_tests()
    yield tmp_path
    get_settings.cache_clear()
    source_health.reset_for_tests()


def test_endpoint_reads_the_file_beside_its_history(_report_on_disk: Path) -> None:
    client = TestClient(app)
    r = client.get("/source-health")
    assert r.status_code == 200
    assert r.headers["cache-control"] == "public, max-age=3600"
    body = r.json()
    assert body["available"] is True
    assert body["sources"]["jar_lithuania"]["history"] == ["ok", "degraded", "degraded"]


def test_endpoint_serves_the_cached_payload_within_the_hour(_report_on_disk: Path) -> None:
    client = TestClient(app)
    assert client.get("/source-health").json()["counts"]["ok"] == 2
    changed = dict(REPORT, counts={"ok": 99, "degraded": 0, "fail": 0, "skipped": 0})
    (_report_on_disk / "source-health.json").write_text(json.dumps(changed))
    assert client.get("/source-health").json()["counts"]["ok"] == 2, "not re-read inside the refresh window"


def test_endpoint_reports_no_report_when_the_file_is_absent(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("OPENCHECK_SOURCE_HEALTH_FILE", str(tmp_path / "missing.json"))
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    source_health.reset_for_tests()
    try:
        body = TestClient(app).get("/source-health").json()
        assert body == {"available": False, "reason": "no sweep has published a report yet"}
    finally:
        get_settings.cache_clear()
        source_health.reset_for_tests()


def test_endpoint_can_be_switched_off(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("OPENCHECK_SOURCE_HEALTH_URL", "")
    monkeypatch.delenv("OPENCHECK_SOURCE_HEALTH_FILE", raising=False)
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    source_health.reset_for_tests()
    try:
        body = TestClient(app).get("/source-health").json()
        assert body == {"available": False, "reason": "source health not configured"}
    finally:
        get_settings.cache_clear()
        source_health.reset_for_tests()


@pytest.mark.asyncio
async def test_load_serves_stale_and_says_so_when_the_refresh_fails(monkeypatch, tmp_path: Path) -> None:
    path = tmp_path / "source-health.json"
    path.write_text(json.dumps(REPORT))
    monkeypatch.setenv("OPENCHECK_SOURCE_HEALTH_FILE", str(path))
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    source_health.reset_for_tests()
    try:
        first = await source_health.load(now=0.0)
        assert first["available"] is True and "stale" not in first

        async def boom(_location: str):
            raise OSError("release asset unreachable")

        monkeypatch.setattr(source_health, "_read", boom)
        later = await source_health.load(now=source_health.REFRESH_AFTER_S + 1)
        assert later["available"] is True
        assert later["stale"] is True
        assert later["generated_at"] == REPORT["generated_at"]
    finally:
        get_settings.cache_clear()
        source_health.reset_for_tests()


def test_default_location_is_the_release_asset_and_history_sits_beside_it() -> None:
    url = get_settings().source_health_url
    assert url.endswith("/releases/download/source-health-latest/source-health.json")
    assert source_health._history_location(url).endswith(
        "/releases/download/source-health-latest/source-health-history.json"
    )
    assert source_health._history_location("/tmp/x/source-health.json") == "/tmp/x/source-health-history.json"
