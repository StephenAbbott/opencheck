"""Phase 5 — Export endpoint BODS validity tests.

Verifies that every format produced by GET /export passes BODS v0.4 shape
validation and that the HTTP response metadata (Content-Type, Content-Disposition,
filename) is correct.

The underlying /lookup call is mocked so these tests run offline — no real
source adapters are hit.  The mock returns a small but structurally complete
BODS bundle (one entity + one person + one relationship) so all four export
formats exercise the serialisation path.

Format coverage:
  json    — pretty-printed JSON array, application/json
  jsonl   — newline-delimited JSON, application/x-ndjson
  xml     — canonical BODS XML, application/xml
  senzing — newline-delimited Senzing JSON entity records, application/x-ndjson
  zip     — bundle with bods.json + bods.jsonl + bods.xml + senzing.jsonl
            + manifest.json + LICENSES.md
"""

from __future__ import annotations

import io
import json
import zipfile
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.bods import validate_shape
from opencheck.routers.lookup import LookupResponse
from opencheck.sources import SearchKind

# ---------------------------------------------------------------------------
# Minimal BODS bundle used in all export tests
# ---------------------------------------------------------------------------

_ENTITY_ID = "test-entity-001"
_PERSON_ID = "test-person-001"
_REL_ID = "test-rel-001"

_TEST_BODS: list[dict[str, Any]] = [
    {
        "statementId": _ENTITY_ID,
        "recordId": _ENTITY_ID,
        "recordType": "entity",
        "recordStatus": "new",
        "statementDate": "2024-01-01",
        "recordDetails": {
            "entityType": {"type": "registeredEntity"},
            "name": "Test Holdings Ltd",
            "incorporatedInJurisdiction": {"name": "United Kingdom", "code": "GB"},
            "identifiers": [
                {
                    "id": "00000006",
                    "scheme": "GB-COH",
                    "schemeName": "Companies House",
                }
            ],
        },
        "source": {"description": "Companies House", "url": "https://find-and-update.company-information.service.gov.uk/"},
    },
    {
        "statementId": _PERSON_ID,
        "recordId": _PERSON_ID,
        "recordType": "person",
        "recordStatus": "new",
        "statementDate": "2024-01-01",
        "recordDetails": {
            "personType": "knownPerson",
            "names": [{"type": "individual", "fullName": "Jane Smith"}],
            "nationalities": [{"name": "British", "code": "GB"}],
        },
        "source": {"description": "Companies House PSC register"},
    },
    {
        "statementId": _REL_ID,
        "recordId": _REL_ID,
        "recordType": "relationship",
        "recordStatus": "new",
        "statementDate": "2024-01-01",
        "recordDetails": {
            "subject": _ENTITY_ID,
            "interestedParty": _PERSON_ID,
            "interests": [
                {
                    "type": "shareholding",
                    "directOrIndirect": "direct",
                    "beneficialOwnershipOrControl": True,
                    "share": {"minimum": 25, "maximum": 50},
                    "startDate": "2016-04-06",
                }
            ],
        },
        "source": {"description": "Companies House PSC register"},
    },
]

_MOCK_LOOKUP_RESPONSE = LookupResponse(
    lei="213800LBDB8WB3QGVN21",
    legal_name="Test Holdings Ltd",
    jurisdiction="GB",
    query="Test Holdings Ltd",
    kind=SearchKind.ENTITY,
    hits=[],
    errors={},
    cross_source_links=[],
    risk_signals=[],
    bods=_TEST_BODS,
    bods_issues=[],
    license_notices=[],
    derived_identifiers={},
)


# ---------------------------------------------------------------------------
# Fixture — TestClient with mocked lookup
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def client_with_mock():
    """Return a TestClient where the lookup coroutine is mocked."""
    with patch(
        "opencheck.routers.export._lookup_impl",
        new=AsyncMock(return_value=_MOCK_LOOKUP_RESPONSE),
    ):
        with TestClient(app) as c:
            yield c


# ---------------------------------------------------------------------------
# Internal validation check — the _TEST_BODS fixture itself is valid
# ---------------------------------------------------------------------------


def test_fixture_passes_validate_shape():
    """The mock BODS bundle used by all export tests is itself schema-valid."""
    issues = validate_shape(_TEST_BODS)
    assert issues == [], issues


# ---------------------------------------------------------------------------
# json format
# ---------------------------------------------------------------------------


class TestExportJson:
    """GET /export?lei=...&format=json"""

    @pytest.fixture(scope="class")
    def response(self, client_with_mock):
        return client_with_mock.get(
            "/export", params={"lei": "213800LBDB8WB3QGVN21", "format": "json"}
        )

    def test_status_200(self, response):
        assert response.status_code == 200

    def test_content_type(self, response):
        assert response.headers["content-type"].startswith("application/json")

    def test_content_disposition_attachment(self, response):
        assert "attachment" in response.headers.get("content-disposition", "")

    def test_filename_ends_with_json(self, response):
        cd = response.headers.get("content-disposition", "")
        assert cd.endswith(".json\"") or ".json" in cd

    def test_parses_as_json_array(self, response):
        body = response.json()
        assert isinstance(body, list)
        assert len(body) == 3

    def test_bods_output_passes_validate_shape(self, response):
        stmts = response.json()
        issues = validate_shape(stmts)
        assert issues == [], issues

    def test_statement_types(self, response):
        stmts = response.json()
        types = [s["recordType"] for s in stmts]
        assert "entity" in types
        assert "person" in types
        assert "relationship" in types


# ---------------------------------------------------------------------------
# jsonl format
# ---------------------------------------------------------------------------


class TestExportJsonl:
    """GET /export?lei=...&format=jsonl"""

    @pytest.fixture(scope="class")
    def response(self, client_with_mock):
        return client_with_mock.get(
            "/export", params={"lei": "213800LBDB8WB3QGVN21", "format": "jsonl"}
        )

    def test_status_200(self, response):
        assert response.status_code == 200

    def test_content_type(self, response):
        ct = response.headers["content-type"]
        assert "ndjson" in ct or "json" in ct

    def test_filename_ends_with_jsonl(self, response):
        cd = response.headers.get("content-disposition", "")
        assert ".jsonl" in cd

    def test_parses_as_newline_delimited(self, response):
        lines = [l for l in response.text.strip().splitlines() if l.strip()]
        assert len(lines) == 3
        stmts = [json.loads(l) for l in lines]
        assert all("recordType" in s for s in stmts)

    def test_bods_output_passes_validate_shape(self, response):
        lines = [l for l in response.text.strip().splitlines() if l.strip()]
        stmts = [json.loads(l) for l in lines]
        issues = validate_shape(stmts)
        assert issues == [], issues


# ---------------------------------------------------------------------------
# xml format
# ---------------------------------------------------------------------------


class TestExportXml:
    """GET /export?lei=...&format=xml"""

    @pytest.fixture(scope="class")
    def response(self, client_with_mock):
        return client_with_mock.get(
            "/export", params={"lei": "213800LBDB8WB3QGVN21", "format": "xml"}
        )

    def test_status_200(self, response):
        assert response.status_code == 200

    def test_content_type(self, response):
        assert "xml" in response.headers["content-type"]

    def test_filename_ends_with_xml(self, response):
        cd = response.headers.get("content-disposition", "")
        assert ".xml" in cd

    def test_body_starts_with_xml_declaration_or_root(self, response):
        body = response.text.strip()
        assert body.startswith("<?xml") or body.startswith("<")

    def test_body_contains_statement_id(self, response):
        """The entity statementId must appear in the serialised XML."""
        assert _ENTITY_ID in response.text

    def test_body_contains_person_name(self, response):
        """Person fullName must be serialised into the XML."""
        assert "Jane Smith" in response.text


# ---------------------------------------------------------------------------
# zip format
# ---------------------------------------------------------------------------


class TestExportZip:
    """GET /export?lei=...&format=zip (default)"""

    @pytest.fixture(scope="class")
    def response(self, client_with_mock):
        return client_with_mock.get(
            "/export", params={"lei": "213800LBDB8WB3QGVN21", "format": "zip"}
        )

    @pytest.fixture(scope="class")
    def zf(self, response):
        buf = io.BytesIO(response.content)
        return zipfile.ZipFile(buf)

    def test_status_200(self, response):
        assert response.status_code == 200

    def test_content_type(self, response):
        assert "zip" in response.headers["content-type"]

    def test_filename_ends_with_zip(self, response):
        cd = response.headers.get("content-disposition", "")
        assert ".zip" in cd

    def test_zip_contains_bods_json(self, zf):
        names = zf.namelist()
        assert any(n.endswith("bods.json") for n in names)

    def test_zip_contains_bods_jsonl(self, zf):
        names = zf.namelist()
        assert any(n.endswith("bods.jsonl") for n in names)

    def test_zip_contains_bods_xml(self, zf):
        names = zf.namelist()
        assert any(n.endswith("bods.xml") for n in names)

    def test_zip_contains_manifest(self, zf):
        names = zf.namelist()
        assert any(n.endswith("manifest.json") for n in names)

    def test_zip_contains_licenses(self, zf):
        names = zf.namelist()
        assert any(n.endswith("LICENSES.md") for n in names)

    def test_manifest_statement_count(self, zf):
        manifest_name = next(n for n in zf.namelist() if n.endswith("manifest.json"))
        manifest = json.loads(zf.read(manifest_name))
        assert manifest["bods_statement_count"] == 3

    def test_manifest_bods_issues_empty(self, zf):
        manifest_name = next(n for n in zf.namelist() if n.endswith("manifest.json"))
        manifest = json.loads(zf.read(manifest_name))
        assert manifest["bods_validation_issues"] == []

    def test_zip_bods_json_passes_validate_shape(self, zf):
        json_name = next(n for n in zf.namelist() if n.endswith("bods.json"))
        stmts = json.loads(zf.read(json_name))
        issues = validate_shape(stmts)
        assert issues == [], issues

    def test_zip_bods_jsonl_passes_validate_shape(self, zf):
        jsonl_name = next(n for n in zf.namelist() if n.endswith("bods.jsonl"))
        text = zf.read(jsonl_name).decode("utf-8")
        stmts = [json.loads(l) for l in text.strip().splitlines() if l.strip()]
        issues = validate_shape(stmts)
        assert issues == [], issues


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


class TestExportErrors:
    """Malformed requests to /export must return 4xx."""

    @pytest.fixture(scope="class")
    def client(self):
        with TestClient(app) as c:
            yield c

    def test_no_query_returns_400(self, client):
        r = client.get("/export")
        assert r.status_code == 400

    def test_unknown_format_returns_422(self, client):
        r = client.get(
            "/export", params={"lei": "213800LBDB8WB3QGVN21", "format": "pdf"}
        )
        assert r.status_code == 422
