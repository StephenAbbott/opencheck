"""Integration tests for /lookup — the LEI-anchored entry point."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pytest_httpx import HTTPXMock

from opencheck.app import app
from opencheck.config import get_settings


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path: Path):
    """Tmp data root + offline by default. Individual tests opt in to
    live mocking by setting OPENCHECK_ALLOW_LIVE."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    monkeypatch.delenv("OPENSANCTIONS_API_KEY", raising=False)
    monkeypatch.delenv("COMPANIES_HOUSE_API_KEY", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_lookup_rejects_invalid_lei_shape(client: TestClient) -> None:
    r = client.get("/lookup", params={"lei": "not-an-lei"})
    assert r.status_code == 400
    assert "20-character" in r.json()["detail"]


def test_lookup_rejects_lowercase_too_short(client: TestClient) -> None:
    r = client.get("/lookup", params={"lei": "abc"})
    assert r.status_code == 400


def test_lookup_returns_404_when_gleif_has_no_record(client: TestClient) -> None:
    """Offline + no fixture for the LEI → GLEIF fetch returns a stub
    bundle, /lookup interprets that as 'unknown LEI' and 404s."""
    r = client.get("/lookup", params={"lei": "ZZZZ00000000000000ZZ"})
    assert r.status_code == 404
    assert "No GLEIF record" in r.json()["detail"]


def _mock_lei_record_chain(httpx_mock: HTTPXMock, lei: str, attrs: dict) -> None:
    """Helper: mock a GLEIF LEI record + parent-less reporting exception
    chain so a /lookup call doesn't 404 for missing parent endpoints."""
    api = "https://api.gleif.org/api/v1"
    httpx_mock.add_response(
        url=f"{api}/lei-records/{lei}",
        json={"data": {"id": lei, "type": "lei-records", "attributes": attrs}},
    )
    for path in (
        "direct-parent",
        "direct-parent-reporting-exception",
        "ultimate-parent",
        "ultimate-parent-reporting-exception",
    ):
        httpx_mock.add_response(
            url=f"{api}/lei-records/{lei}/{path}", status_code=404
        )


def _mock_wikidata_lei_lookup_empty(
    httpx_mock: HTTPXMock, lei: str
) -> None:
    """Wikidata SPARQL ``?item wdt:P1278 "LEI"`` returns no bindings —
    /lookup should fail-soft and skip the Wikidata branch."""
    import urllib.parse
    query = (
        'SELECT ?item WHERE { ?item wdt:P1278 "%s" } LIMIT 1' % lei
    )
    url = (
        "https://query.wikidata.org/sparql?query="
        + urllib.parse.quote(query, safe="")
    )
    httpx_mock.add_response(
        url=url,
        json={"head": {"vars": ["item"]}, "results": {"bindings": []}},
    )


def _mock_icij_empty(httpx_mock: HTTPXMock) -> None:
    """Register an ICIJ reconciliation endpoint that returns no matches."""
    httpx_mock.add_response(
        url="https://offshoreleaks.icij.org/reconcile",
        method="POST",
        json={},
    )


def _mock_openaleph_lei_lookup_empty(httpx_mock: HTTPXMock, lei: str) -> None:
    """Mock the OpenAleph fetch_by_lei call to return no results."""
    from urllib.parse import quote
    url = (
        "https://search.openaleph.org/api/2/entities?"
        f"filter:properties.leiCode={quote(lei)}&filter:schema=LegalEntity&limit=5"
    )
    httpx_mock.add_response(url=url, json={"results": []})


def _mock_openaleph_reg_lookup_empty(
    httpx_mock: HTTPXMock, jurisdiction: str, reg_number: str
) -> None:
    """Mock the OpenAleph fetch_by_registration call to return no results."""
    from urllib.parse import quote
    url = (
        "https://search.openaleph.org/api/2/entities?"
        f"filter:properties.registrationNumber={quote(reg_number)}"
        f"&filter:properties.jurisdiction={quote(jurisdiction.lower())}"
        "&filter:schema=LegalEntity&limit=5"
    )
    httpx_mock.add_response(url=url, json={"results": []})


def test_lookup_drives_full_synthesis_for_a_gb_lei(
    client: TestClient, monkeypatch, httpx_mock: HTTPXMock
) -> None:
    """End-to-end: GLEIF returns a GB record, the lookup derives gb_coh,
    and the response carries the legal name + jurisdiction + bridges."""
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()

    lei = "213800LH1BZH3DI6G760"  # real BP LEI, used for shape only
    _mock_lei_record_chain(
        httpx_mock,
        lei,
        {
            "lei": lei,
            "entity": {
                "legalName": {"name": "Demo Company P.L.C."},
                "jurisdiction": "GB",
                "registeredAs": "00102498",
            },
            "registration": {"status": "ISSUED"},
        },
    )
    _mock_wikidata_lei_lookup_empty(httpx_mock, lei)
    _mock_icij_empty(httpx_mock)
    # OpenAleph: strategy 1 (leiCode) returns empty; strategy 3 (gb_coh) also empty.
    _mock_openaleph_lei_lookup_empty(httpx_mock, lei)
    _mock_openaleph_reg_lookup_empty(httpx_mock, "gb", "00102498")
    # Companies House + OpenSanctions need API keys we haven't set, so
    # they return stubs without making network calls.

    r = client.get("/lookup", params={"lei": lei})
    assert r.status_code == 200
    body = r.json()

    assert body["lei"] == lei
    assert body["legal_name"] == "Demo Company P.L.C."
    assert body["jurisdiction"] == "GB"
    assert body["derived_identifiers"]["lei"] == lei
    assert body["derived_identifiers"]["gb_coh"] == "00102498"

    # GLEIF hit is always present.
    assert any(h["source_id"] == "gleif" for h in body["hits"])
    # BODS bundle exists (GLEIF mapper emits a registeredEntity statement).
    assert body["bods"], "expected at least one BODS statement"
    assert all(s["recordType"] in {"entity", "person", "relationship"} for s in body["bods"])


def test_lookup_uses_bundle_when_no_live_mode(
    client: TestClient, monkeypatch, tmp_path
) -> None:
    """When a bods-data bundle exists for an LEI, /lookup reads the
    legal name + jurisdiction from it and skips the live GLEIF call —
    so the demo subjects work fully offline.
    """
    import json

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    get_settings.cache_clear()

    lei = "213800LH1BZH3DI6G760"
    target = tmp_path / "cache" / "bods_data" / "gleif" / f"{lei}.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(
            {
                "statementId": "e-subject",
                "recordType": "entity",
                "recordDetails": {
                    "name": "Bundle Co P.L.C.",
                    "incorporatedInJurisdiction": {"name": "United Kingdom", "code": "GB"},
                    "identifiers": [
                        {"id": lei, "scheme": "XI-LEI"},
                        {"id": "12345678", "scheme": "GB-COH"},
                    ],
                },
            }
        )
        + "\n"
    )

    r = client.get("/lookup", params={"lei": lei})
    assert r.status_code == 200
    body = r.json()
    assert body["legal_name"] == "Bundle Co P.L.C."
    assert body["jurisdiction"] == "GB"
    assert body["derived_identifiers"]["gb_coh"] == "12345678"
    # The override bundle drives the BODS list — at least the subject
    # statement should be present.
    statement_ids = {s["statementId"] for s in body["bods"]}
    assert "e-subject" in statement_ids


def test_lookup_lower_case_lei_is_normalised(
    client: TestClient, monkeypatch, httpx_mock: HTTPXMock
) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()

    lei = "213800LH1BZH3DI6G760"
    _mock_lei_record_chain(
        httpx_mock,
        lei,
        {
            "lei": lei,
            "entity": {
                "legalName": {"name": "Demo Co"},
                "jurisdiction": "GB",
            },
        },
    )
    _mock_wikidata_lei_lookup_empty(httpx_mock, lei)
    _mock_icij_empty(httpx_mock)
    # OpenAleph: only strategy 1 (leiCode) fires — no registeredAs in this fixture.
    _mock_openaleph_lei_lookup_empty(httpx_mock, lei)

    # Pass it lower-cased; backend should uppercase before the GLEIF call.
    r = client.get("/lookup", params={"lei": lei.lower()})
    assert r.status_code == 200
    assert r.json()["lei"] == lei
