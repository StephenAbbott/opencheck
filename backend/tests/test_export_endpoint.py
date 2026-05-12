"""Integration tests for /export.

The supported entry shape is ``?lei=...`` — we mock the underlying
GLEIF + Wikidata calls so the export reflects the same BODS bundle a
user would see in the UI. The free-text ``?q=...`` shape is exercised
by the slug + format-validation tests, where the actual BODS content
isn't asserted.
"""

from __future__ import annotations

import io
import json
import urllib.parse
import zipfile

import pytest
from fastapi.testclient import TestClient
from pytest_httpx import HTTPXMock

from opencheck.app import app
from opencheck.config import get_settings


_LEI = "213800LH1BZH3DI6G760"  # real BP LEI; used here for shape only.


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path):
    """Tmp data root + live mode + no API keys so the test set is
    deterministic. Tests that should NOT fire live HTTP override
    OPENCHECK_ALLOW_LIVE individually."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.delenv("OPENSANCTIONS_API_KEY", raising=False)
    monkeypatch.delenv("COMPANIES_HOUSE_API_KEY", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _mock_lei_record_chain(httpx_mock: HTTPXMock, lei: str) -> None:
    """Mock the GLEIF record + parent-less reporting-exception chain."""
    api = "https://api.gleif.org/api/v1"
    httpx_mock.add_response(
        url=f"{api}/lei-records/{lei}",
        json={
            "data": {
                "id": lei,
                "type": "lei-records",
                "attributes": {
                    "lei": lei,
                    "entity": {
                        "legalName": {"name": "Demo Holdings P.L.C."},
                        "jurisdiction": "GB",
                        "registeredAs": "00102498",
                    },
                    "registration": {"status": "ISSUED"},
                },
            }
        },
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


def _mock_icij_empty(httpx_mock: HTTPXMock) -> None:
    """Register an ICIJ reconciliation endpoint that returns no matches."""
    httpx_mock.add_response(
        url="https://offshoreleaks.icij.org/reconcile",
        method="POST",
        json={},
    )


def _mock_wikidata_lei_lookup_empty(httpx_mock: HTTPXMock, lei: str) -> None:
    query = 'SELECT ?item WHERE { ?item wdt:P1278 "%s" } LIMIT 1' % lei
    url = (
        "https://query.wikidata.org/sparql?query="
        + urllib.parse.quote(query, safe="")
    )
    httpx_mock.add_response(
        url=url,
        json={"head": {"vars": ["item"]}, "results": {"bindings": []}},
    )


def _mock_openaleph_lei_lookup_empty(httpx_mock: HTTPXMock, lei: str) -> None:
    """Mock the OpenAleph fetch_by_lei call to return no results."""
    url = (
        "https://search.openaleph.org/api/2/entities?"
        f"filter:properties.leiCode={urllib.parse.quote(lei)}"
        "&filter:schema=LegalEntity&limit=5"
    )
    httpx_mock.add_response(url=url, json={"results": []})


def _mock_openaleph_reg_lookup_empty(
    httpx_mock: HTTPXMock, jurisdiction: str, reg_number: str
) -> None:
    """Mock the OpenAleph fetch_by_registration call to return no results."""
    url = (
        "https://search.openaleph.org/api/2/entities?"
        f"filter:properties.registrationNumber={urllib.parse.quote(reg_number)}"
        f"&filter:properties.jurisdiction={urllib.parse.quote(jurisdiction.lower())}"
        "&filter:schema=LegalEntity&limit=5"
    )
    httpx_mock.add_response(url=url, json={"results": []})


def _mock_openaleph_name_lookup_empty(httpx_mock: HTTPXMock, name: str) -> None:
    """Mock the OpenAleph fetch_by_name (strategy 4) call to return no results."""
    url = (
        "https://search.openaleph.org/api/2/entities?"
        f"q={urllib.parse.quote(name)}&filter:schema=LegalEntity&limit=5"
    )
    httpx_mock.add_response(url=url, json={"results": []})


def test_export_json_returns_bods_array(
    client: TestClient, httpx_mock: HTTPXMock
) -> None:
    _mock_lei_record_chain(httpx_mock, _LEI)
    _mock_wikidata_lei_lookup_empty(httpx_mock, _LEI)
    _mock_icij_empty(httpx_mock)
    _mock_openaleph_lei_lookup_empty(httpx_mock, _LEI)
    _mock_openaleph_reg_lookup_empty(httpx_mock, "gb", "00102498")
    _mock_openaleph_name_lookup_empty(httpx_mock, "Demo Holdings P.L.C.")

    r = client.get("/export", params={"lei": _LEI, "format": "json"})
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/json")
    cd = r.headers["content-disposition"]
    assert "attachment" in cd
    assert _LEI.lower() in cd.lower()
    assert ".json" in cd

    body = json.loads(r.content)
    assert isinstance(body, list)
    # GLEIF emits at least one entityStatement for the subject record.
    assert len(body) >= 1
    for stmt in body:
        assert stmt.get("recordType") in {"entity", "person", "relationship"}


def test_export_jsonl_emits_one_statement_per_line(
    client: TestClient, httpx_mock: HTTPXMock
) -> None:
    _mock_lei_record_chain(httpx_mock, _LEI)
    _mock_wikidata_lei_lookup_empty(httpx_mock, _LEI)
    _mock_icij_empty(httpx_mock)
    _mock_openaleph_lei_lookup_empty(httpx_mock, _LEI)
    _mock_openaleph_reg_lookup_empty(httpx_mock, "gb", "00102498")
    _mock_openaleph_name_lookup_empty(httpx_mock, "Demo Holdings P.L.C.")

    r = client.get("/export", params={"lei": _LEI, "format": "jsonl"})
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/x-ndjson")
    text = r.content.decode("utf-8")
    lines = [ln for ln in text.split("\n") if ln.strip()]
    assert lines, "no statements in jsonl export"
    for ln in lines:
        obj = json.loads(ln)
        assert obj.get("recordType") in {"entity", "person", "relationship"}


def test_export_zip_contains_full_bundle(
    client: TestClient, httpx_mock: HTTPXMock
) -> None:
    _mock_lei_record_chain(httpx_mock, _LEI)
    _mock_wikidata_lei_lookup_empty(httpx_mock, _LEI)
    _mock_icij_empty(httpx_mock)
    _mock_openaleph_lei_lookup_empty(httpx_mock, _LEI)
    _mock_openaleph_reg_lookup_empty(httpx_mock, "gb", "00102498")
    _mock_openaleph_name_lookup_empty(httpx_mock, "Demo Holdings P.L.C.")

    r = client.get("/export", params={"lei": _LEI, "format": "zip"})
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/zip"

    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        names = zf.namelist()
        prefixes = {n.split("/", 1)[0] for n in names}
        assert len(prefixes) == 1, f"expected single top-level dir, got {prefixes}"
        prefix = next(iter(prefixes))
        assert f"{prefix}/bods.json" in names
        assert f"{prefix}/bods.jsonl" in names
        assert f"{prefix}/manifest.json" in names
        assert f"{prefix}/LICENSES.md" in names

        manifest = json.loads(zf.read(f"{prefix}/manifest.json"))
        assert manifest["query"] == _LEI
        # GLEIF always contributes when /lookup succeeds.
        assert "gleif" in manifest["contributing_source_ids"]


def test_export_zip_licenses_md_lists_gleif(
    client: TestClient, httpx_mock: HTTPXMock
) -> None:
    """LICENSES.md should always carry the GLEIF entry + re-use guidance."""
    _mock_lei_record_chain(httpx_mock, _LEI)
    _mock_wikidata_lei_lookup_empty(httpx_mock, _LEI)
    _mock_icij_empty(httpx_mock)
    _mock_openaleph_lei_lookup_empty(httpx_mock, _LEI)
    _mock_openaleph_reg_lookup_empty(httpx_mock, "gb", "00102498")
    _mock_openaleph_name_lookup_empty(httpx_mock, "Demo Holdings P.L.C.")

    r = client.get("/export", params={"lei": _LEI, "format": "zip"})
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        prefix = zf.namelist()[0].split("/", 1)[0]
        md = zf.read(f"{prefix}/LICENSES.md").decode("utf-8")

    assert "GLEIF" in md
    assert "Re-use guidance" in md


def test_export_unknown_format_rejected(client: TestClient) -> None:
    r = client.get(
        "/export", params={"lei": _LEI, "format": "yaml"}
    )
    assert r.status_code == 422


def test_export_filename_slug_strips_unsafe_chars(
    client: TestClient, monkeypatch
) -> None:
    """Filename should be slugified (no spaces, no special chars).

    Exercised via the legacy ``?q=`` form because the LEI form already
    yields a clean alphanumeric slug. Forced offline so the free-text
    fan-out doesn't reach Wikidata live.
    """
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()

    r = client.get(
        "/export", params={"q": "Vladimir Putin!", "kind": "person", "format": "json"}
    )
    cd = r.headers["content-disposition"]
    assert "vladimir-putin" in cd
    assert " " not in cd.split("filename=", 1)[1]
    assert "!" not in cd
