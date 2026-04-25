"""Integration tests for the /report endpoint.

Exercises the full /report pipeline (search → reconcile → deepen-top-N
→ assess risk) end-to-end via mocked HTTP. The Phase 0 stub adapters
in REGISTRY do most of the work for the no-live path; one test mocks
GLEIF live so we can confirm the AMLA layer signal lights up when the
deepened bundle has real BODS shape.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from pytest_httpx import HTTPXMock

from opencheck.app import app
from opencheck.config import get_settings


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_report_endpoint_returns_full_shape() -> None:
    """Even from the stub path, /report returns the documented shape."""
    client = TestClient(app)
    r = client.get("/report", params={"q": "Rosneft", "kind": "entity"})
    assert r.status_code == 200
    body = r.json()
    for key in (
        "query",
        "kind",
        "hits",
        "errors",
        "cross_source_links",
        "risk_signals",
        "bods",
        "bods_issues",
        "license_notices",
    ):
        assert key in body, f"missing {key}"
    assert body["query"] == "Rosneft"
    # No live → no BODS (stub adapters return is_stub=True).
    assert body["bods"] == []
    assert body["risk_signals"] == []


def test_report_endpoint_caps_deepen_at_zero() -> None:
    """deepen_top=0 lets a caller skip the per-hit fetch round-trip."""
    client = TestClient(app)
    r = client.get(
        "/report",
        params={"q": "anything", "kind": "entity", "deepen_top": 0},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["bods"] == []


def test_deepen_emits_amla_non_eu_signal_for_offshore_gleif_record(
    monkeypatch, httpx_mock: HTTPXMock
) -> None:
    """End-to-end: a GLEIF record incorporated in BVI fires the AMLA
    NON_EU_JURISDICTION signal via /deepen.risk_signals.

    (We don't assert on COMPLEX_OWNERSHIP_LAYERS here because GLEIF's
    Level 2 parent model is a star — both parents point to the subject —
    so the longest chain is 2, not 3. Layer counting is exercised
    rigorously in test_risk_amla.)
    """
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()

    api = "https://api.gleif.org/api/v1"
    lei_a = "AAAA00000000000000A1"

    httpx_mock.add_response(
        url=f"{api}/lei-records/{lei_a}",
        json={
            "data": {
                "id": lei_a,
                "attributes": {
                    "lei": lei_a,
                    "entity": {
                        "legalName": {"name": "Offshore Holdings Ltd"},
                        "jurisdiction": "VG",
                    },
                },
            }
        },
    )
    # No parents — return 404 for both, then exception 404s, so
    # the GLEIF mapper just emits the subject entity statement.
    for path in (
        "direct-parent",
        "direct-parent-reporting-exception",
        "ultimate-parent",
        "ultimate-parent-reporting-exception",
    ):
        httpx_mock.add_response(
            url=f"{api}/lei-records/{lei_a}/{path}", status_code=404
        )

    client = TestClient(app)
    r = client.get("/deepen", params={"source": "gleif", "hit_id": lei_a})
    assert r.status_code == 200
    body = r.json()
    assert "risk_signals" in body
    codes = {s["code"] for s in body["risk_signals"]}
    assert "NON_EU_JURISDICTION" in codes
    sig = next(
        s for s in body["risk_signals"] if s["code"] == "NON_EU_JURISDICTION"
    )
    assert "VG" in sig["summary"]
    assert sig["evidence"]["jurisdictions"][0]["code"] == "VG"
