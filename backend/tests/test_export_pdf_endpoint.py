"""Integration tests for POST /export/pdf.

The lookup pipeline and the WeasyPrint render are both mocked: we assert the
endpoint wiring (rebuild from lookup, forward the narrative, stream a PDF with a
filename, 503 when the toolchain is missing), not the PDF bytes themselves.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.config import get_settings
from opencheck.reporting import PdfUnavailable
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


def test_export_pdf_streams_a_pdf(monkeypatch):
    monkeypatch.setattr("opencheck.routers.export._lookup_impl", _fake_lookup)
    captured = {}

    def _fake_build(report, *, narrative=None):
        captured["report"] = report
        captured["narrative"] = narrative
        return b"%PDF-1.7\nfake"

    monkeypatch.setattr("opencheck.routers.export.build_report_pdf", _fake_build)

    client = TestClient(app)
    r = client.post("/export/pdf", json={"lei": "2138000000000000A001"})
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/pdf"
    assert "opencheck-northwind-logistics-ltd-" in r.headers["content-disposition"]
    assert r.headers["content-disposition"].endswith('.pdf"')
    assert r.content == b"%PDF-1.7\nfake"
    # The lookup result was handed to the builder; no narrative was supplied.
    assert captured["report"]["lei"] == "2138000000000000A001"
    assert captured["narrative"] is None


def test_export_pdf_forwards_narrative(monkeypatch):
    monkeypatch.setattr("opencheck.routers.export._lookup_impl", _fake_lookup)
    seen = {}

    def _fake_build(report, *, narrative=None):
        seen["narrative"] = narrative
        return b"%PDF-1.7\n"

    monkeypatch.setattr("opencheck.routers.export.build_report_pdf", _fake_build)

    client = TestClient(app)
    narrative = {"summary": "An entity.", "overall_confidence": "high", "packet": {"facts": []}}
    r = client.post("/export/pdf", json={"lei": "2138000000000000A001", "narrative": narrative})
    assert r.status_code == 200
    assert seen["narrative"]["summary"] == "An entity."


def test_export_pdf_503_when_toolchain_missing(monkeypatch):
    monkeypatch.setattr("opencheck.routers.export._lookup_impl", _fake_lookup)

    def _raise(report, *, narrative=None):
        raise PdfUnavailable("WeasyPrint not installed")

    monkeypatch.setattr("opencheck.routers.export.build_report_pdf", _raise)

    client = TestClient(app)
    r = client.post("/export/pdf", json={"lei": "2138000000000000A001"})
    assert r.status_code == 503
    assert "WeasyPrint" in r.json()["detail"]
