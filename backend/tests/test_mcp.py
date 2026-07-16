"""Tests for the in-process MCP server, its tools, shaping, and the
/resolve-national-id endpoint that backs the resolve tool."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

# The MCP SDK is an optional dependency of the opencheck.mcp module. Skip
# this module cleanly when it isn't installed rather than aborting
# collection with a hard ModuleNotFoundError (same pattern as the
# libcovebods / bods-fixtures guards).
pytest.importorskip(
    "mcp",
    reason="mcp SDK not installed — run `uv sync` / install the mcp extra",
)

from opencheck.app import app
from opencheck.config import get_settings
from opencheck.mcp import descriptor, TOOL_NAMES
from opencheck.mcp import server as mcp_server
from opencheck.mcp import shaping
from opencheck.sources import REGISTRY, SearchKind, SourceHit


@pytest.fixture(autouse=True)
def _isolated_data_root(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


# --------------------------------------------------------------------------
# Descriptor + mount
# --------------------------------------------------------------------------


def test_descriptor_shape() -> None:
    d = descriptor()
    assert d["name"] == "opencheck"
    assert d["transport"] == "streamable-http"
    assert d["endpoint"].endswith("/mcp")
    assert d["tools"] == TOOL_NAMES
    assert len(TOOL_NAMES) == 5


def test_descriptor_route_served_with_cors(client: TestClient) -> None:
    r = client.get("/.well-known/mcp.json")
    assert r.status_code == 200
    assert r.headers.get("access-control-allow-origin") == "*"
    assert r.json()["tools"] == TOOL_NAMES


def test_mcp_app_mounted() -> None:
    paths = {getattr(r, "path", None) for r in app.routes}
    assert "/mcp" in paths


async def test_registered_tools_match_declared_names() -> None:
    tools = await mcp_server.mcp.list_tools()
    assert sorted(t.name for t in tools) == sorted(TOOL_NAMES)
    # Every tool exposes an input schema for agents.
    assert all(t.inputSchema for t in tools)


# --------------------------------------------------------------------------
# Tools (offline / deterministic)
# --------------------------------------------------------------------------


async def test_list_sources_tool_matches_registry() -> None:
    out = await mcp_server.opencheck_list_sources()
    assert out["count"] == len(REGISTRY)
    assert {s["id"] for s in out["sources"]} == set(REGISTRY.keys())
    assert all({"id", "name", "license"} <= set(s) for s in out["sources"])


async def test_lookup_tool_rejects_bad_lei() -> None:
    out = await mcp_server.opencheck_lookup(lei="not-a-lei")
    assert "error" in out
    assert out["status"] == 400


async def test_export_bods_tool_validates_format() -> None:
    out = await mcp_server.opencheck_export_bods(lei="213800LH1BZH3DI6G760", format="csv")
    assert "error" in out and "format" in out["error"]


async def test_export_bods_tool_senzing_format(monkeypatch) -> None:
    bods = [
        {"statementId": "e1", "recordType": "entity",
         "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": "Acme"}},
        {"statementId": "p1", "recordType": "person",
         "recordDetails": {"personType": "knownPerson",
                           "names": [{"type": "legal", "fullName": "Jane"}]}},
        {"statementId": "r1", "recordType": "relationship",
         "recordDetails": {"subject": "e1", "interestedParty": "p1",
                           "interests": [{"type": "shareholding", "share": {"exact": 100}}]}},
    ]

    async def _fake_lookup(*, lei, deepen_top=3):
        return SimpleNamespace(lei=lei, bods=bods, bods_issues=[], license_notices=[])

    monkeypatch.setattr("opencheck.routers.lookup.lookup", _fake_lookup)
    out = await mcp_server.opencheck_export_bods(
        lei="213800LH1BZH3DI6G760", format="senzing"
    )
    assert out["format"] == "senzing"
    assert out["record_count"] == 2  # one ORGANIZATION + one PERSON record
    assert all(r["DATA_SOURCE"] == "OPENCHECK" for r in out["records"])


async def test_export_bods_tool_ftm_format(monkeypatch) -> None:
    bods = [
        {"statementId": "e1", "recordType": "entity",
         "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": "Acme"}},
        {"statementId": "p1", "recordType": "person",
         "recordDetails": {"personType": "knownPerson",
                           "names": [{"type": "legal", "fullName": "Jane"}]}},
        {"statementId": "r1", "recordType": "relationship",
         "recordDetails": {"subject": "e1", "interestedParty": "p1",
                           "interests": [{"type": "shareholding", "share": {"exact": 100}}]}},
    ]

    async def _fake_lookup(*, lei, deepen_top=3):
        return SimpleNamespace(lei=lei, bods=bods, bods_issues=[], license_notices=[])

    monkeypatch.setattr("opencheck.routers.lookup.lookup", _fake_lookup)
    out = await mcp_server.opencheck_export_bods(
        lei="213800LH1BZH3DI6G760", format="ftm"
    )
    assert out["format"] == "ftm"
    assert out["record_count"] == 3  # Company + Person + Ownership
    schemas = {r["schema"] for r in out["records"]}
    assert schemas == {"Company", "Person", "Ownership"}


async def test_search_tool_validates_kind() -> None:
    out = await mcp_server.opencheck_search(query="x", kind="banana")
    assert "error" in out


async def test_search_tool_returns_candidate_envelope() -> None:
    out = await mcp_server.opencheck_search(query="Rosneft", kind="entity")
    assert out["query"] == "Rosneft"
    assert out["kind"] == "entity"
    assert "candidates" in out and isinstance(out["candidates"], list)


# --------------------------------------------------------------------------
# resolve_national_id endpoint + tool
# --------------------------------------------------------------------------


def test_resolve_national_id_maps_country_to_ra(client: TestClient) -> None:
    # Offline: GLEIF returns no live matches, but the RA resolution and echo
    # must still work so the contract is verifiable without network.
    r = client.get("/resolve-national-id", params={"number": "00102498", "country": "gb"})
    assert r.status_code == 200
    body = r.json()
    assert body["country"] == "GB"
    assert body["ra_code"] == "RA000585"
    assert body["matches"] == []


def test_resolve_national_id_ra_code_overrides_country(client: TestClient) -> None:
    r = client.get(
        "/resolve-national-id",
        params={"number": "123", "country": "GB", "ra_code": "RA000463"},
    )
    assert r.json()["ra_code"] == "RA000463"


def test_resolve_national_id_requires_number(client: TestClient) -> None:
    r = client.get("/resolve-national-id", params={"country": "GB"})
    assert r.status_code == 422  # missing required 'number'


async def test_resolve_tool_delegates_to_endpoint() -> None:
    out = await mcp_server.opencheck_resolve_national_id(number="00102498", country="GB")
    assert out["ra_code"] == "RA000585"
    assert out["matches"] == []


# --------------------------------------------------------------------------
# Shaping (synthetic payloads — no network)
# --------------------------------------------------------------------------


def _fake_lookup_payload() -> SimpleNamespace:
    lei = "213800LH1BZH3DI6G760"
    bods = [
        {
            "recordType": "entity",
            "recordDetails": {
                "identifiers": [
                    {"scheme": "XI-LEI", "id": lei, "schemeName": "LEI"},
                    {"scheme": "ISO-9362", "id": "BARCGB22", "schemeName": "BIC"},
                    {"scheme": "OpenCorporates", "id": "gb/00102498",
                     "uri": "https://opencorporates.com/companies/gb/00102498"},
                ]
            },
        },
        {"recordType": "relationship", "recordDetails": {}},
        {"recordType": "relationship", "recordDetails": {}},
    ]
    hits = [
        SourceHit(source_id="gleif", hit_id=lei, kind=SearchKind.ENTITY,
                  name="BP P.L.C.", summary="LEI", identifiers={"lei": lei},
                  raw={}, is_stub=False),
        SourceHit(source_id="opensanctions", hit_id="x", kind=SearchKind.ENTITY,
                  name="BP", summary="match", identifiers={}, raw={}, is_stub=True),
    ]
    return SimpleNamespace(
        lei=lei,
        legal_name="BP P.L.C.",
        jurisdiction="GB",
        bods=bods,
        risk_signals=[{"code": "NON_EU_JURISDICTION", "confidence": "high",
                       "summary": "Outside the EU", "evidence": {}}],
        hits=hits,
        errors={"kvk": "timeout"},
        license_notices=[{"source": "opensanctions", "notice": "CC-BY-NC"}],
        derived_identifiers={"gb_coh": "00102498"},
    )


def test_shape_lookup_extracts_subject_identifiers_and_risk() -> None:
    shaped = shaping.shape_lookup(_fake_lookup_payload())
    schemes = {i["scheme"] for i in shaped["identifiers"]}
    assert {"XI-LEI", "ISO-9362", "OpenCorporates"} <= schemes
    assert shaped["counts"]["relationships"] == 2
    assert shaped["risk_signals"][0]["code"] == "NON_EU_JURISDICTION"
    # license notices preserved end to end (CC-BY-NC sources)
    assert shaped["license_notices"]
    # sources summary: gleif found, opensanctions stub (not found), kvk errored
    by_id = {s["id"]: s for s in shaped["sources"]}
    assert by_id["gleif"]["found"] is True
    assert by_id["opensanctions"]["found"] is False
    assert "error" in by_id["kvk"]


def test_shape_lookup_summary_is_a_string() -> None:
    shaped = shaping.shape_lookup(_fake_lookup_payload())
    assert isinstance(shaped["summary"], str)
    assert "213800LH1BZH3DI6G760" in shaped["summary"]
