"""Integration tests for POST /export/markdown.

Mirrors ``test_export_pdf_endpoint.py``: the lookup pipeline is mocked and the
endpoint wiring is asserted (rebuild from lookup, forward the narrative, the
stored-dispositions fallback, a Markdown attachment with a filename). Unlike
the PDF route there is no 503 path — the Markdown renderer has no optional
toolchain and runs for real here.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.config import get_settings
from opencheck.routers.lookup import LookupResponse
from opencheck.sources.base import SearchKind


@pytest.fixture(autouse=True)
def _clear_settings():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _fake_lookup_response() -> LookupResponse:
    return LookupResponse(
        query="2138000000000000A001",
        kind=SearchKind.ENTITY,
        hits=[],
        errors={},
        cross_source_links=[],
        risk_signals=[],
        bods=[],
        bods_issues=[],
        license_notices=[],
        lei="2138000000000000A001",
        legal_name="Northwind Logistics Ltd",
        jurisdiction="GB",
        derived_identifiers={},
    )


async def _fake_lookup(lei, deepen_top=5, refresh=False):
    return _fake_lookup_response()


def test_export_markdown_streams_markdown(monkeypatch):
    monkeypatch.setattr("opencheck.routers.export._lookup_impl", _fake_lookup)

    client = TestClient(app)
    r = client.post("/export/markdown", json={"lei": "2138000000000000A001"})
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/markdown")
    assert "opencheck-northwind-logistics-ltd-" in r.headers["content-disposition"]
    assert r.headers["content-disposition"].endswith('.md"')
    body = r.text
    assert body.startswith("# OpenCheck due-diligence report — Northwind Logistics Ltd")
    assert "https://opencheck.world/?lei=2138000000000000A001" in body
    # No narrative supplied → no summary section.
    assert "## Summary" not in body


def test_export_markdown_forwards_narrative(monkeypatch):
    monkeypatch.setattr("opencheck.routers.export._lookup_impl", _fake_lookup)

    client = TestClient(app)
    narrative = {"summary": "An entity.", "overall_confidence": "high", "packet": {"facts": []}}
    r = client.post(
        "/export/markdown", json={"lei": "2138000000000000A001", "narrative": narrative}
    )
    assert r.status_code == 200
    assert "## Summary (high confidence)" in r.text
    assert "An entity." in r.text


def test_export_markdown_falls_back_to_stored_dispositions(monkeypatch, tmp_path):
    """No dispositions in the request → the stored sheet for run_id is used."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr("opencheck.routers.export._lookup_impl", _fake_lookup)

    from opencheck.dispositions import ClaimDisposition, DispositionRecord, save_dispositions

    run_id = "0123456789abcdef"
    save_dispositions(
        DispositionRecord(
            lei="2138000000000000A001",
            run_id=run_id,
            dispositions=[ClaimDisposition(claim_id="c1", status="accepted")],
        )
    )

    client = TestClient(app)
    narrative = {
        "summary": "An entity.",
        "run_id": run_id,
        "packet": {"facts": []},
        "claims": [{"id": "c1", "text": "The entity is active.", "fact_ids": []}],
    }
    r = client.post(
        "/export/markdown", json={"lei": "2138000000000000A001", "narrative": narrative}
    )
    assert r.status_code == 200
    assert "**Analyst review:** 1 accepted" in r.text
    assert "**[Accepted" in r.text


def test_export_markdown_requires_lei():
    client = TestClient(app)
    r = client.post("/export/markdown", json={})
    assert r.status_code == 422
