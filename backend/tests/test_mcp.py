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
    assert len(TOOL_NAMES) == 15  # opencheck_knowability (the MCP pre-flight ticket)


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


# The readOnlyHint ticket: every tool declares its annotations, and only
# opencheck_save_report writes. A new tool must be placed in one of these sets.
_WRITES = {"opencheck_save_report"}
_CLOSED_WORLD = {"opencheck_knowability", "opencheck_list_sources"}


async def test_every_tool_declares_its_annotations() -> None:
    tools = {t.name: t for t in await mcp_server.mcp.list_tools()}
    assert _WRITES | _CLOSED_WORLD <= set(tools)
    for name, tool in tools.items():
        ann = tool.annotations
        assert ann is not None, f"{name} declares no annotations"
        assert ann.readOnlyHint is (name not in _WRITES), name
        assert ann.openWorldHint is (name not in _CLOSED_WORLD), name


async def test_save_report_is_additive_and_not_idempotent() -> None:
    tools = {t.name: t for t in await mcp_server.mcp.list_tools()}
    ann = tools["opencheck_save_report"].annotations
    # Stores a new report on each call: never destroys, never repeats itself.
    assert ann.destructiveHint is False
    assert ann.idempotentHint is False


async def test_annotations_serialise_as_tools_list_sends_them() -> None:
    tools = {t.name: t.model_dump(by_alias=True, exclude_none=True) for t in await mcp_server.mcp.list_tools()}
    assert tools["opencheck_lookup"]["annotations"] == {"readOnlyHint": True, "openWorldHint": True}
    assert tools["opencheck_save_report"]["annotations"] == {
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": True,
    }


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

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _fake_lookup)
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

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _fake_lookup)
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


async def test_person_check_tool_shapes_response(monkeypatch) -> None:
    """opencheck_person_check drops raw payloads but keeps evidence."""
    from opencheck.mcp.server import opencheck_person_check

    result = await opencheck_person_check(name="Jane Example")
    assert result["query"] == "Jane Example"
    assert "strong_matches" in result
    assert "caveats" in result and len(result["caveats"]) == 3
    assert "hint" in result
    # Raw hit payloads must not leak through the shaper.
    for m in result["strong_matches"]:
        assert "raw" not in m


async def test_person_check_tool_validates_input() -> None:
    from opencheck.mcp.server import opencheck_person_check

    assert "error" in await opencheck_person_check(name="x")
    assert "error" in await opencheck_person_check(
        name="Jane Example", birth_year=1500
    )


def _fake_shell_payload():
    """A Shell-plc-shaped payload: two context signals (one repeated per
    deepened source), two risk signals, CC-BY-NC + share-alike contributors."""
    lei = "21380068P1DRHMJ8KU70"
    bods = [{"recordType": "entity", "recordDetails": {"identifiers": [
        {"scheme": "XI-LEI", "id": lei, "schemeName": "LEI"}]}}]
    hits = [
        SourceHit(source_id=sid, hit_id=f"{sid}-1", kind=SearchKind.ENTITY,
                  name="SHELL PLC", summary="", identifiers={}, raw={}, is_stub=False)
        for sid in ("gleif", "opensanctions", "opencorporates")
    ]
    non_eu = "Ownership chain reaches jurisdictions outside the EU/EEA: GB."
    signals = [
        {"code": "GLEIF_REPORTING_EXCEPTION", "confidence": "high", "kind": "context",
         "source_id": "gleif", "summary": "No parent, permitted exception."},
        {"code": "NON_EU_JURISDICTION", "confidence": "low", "kind": "context",
         "source_id": "gleif", "summary": non_eu},
        {"code": "NON_EU_JURISDICTION", "confidence": "low", "kind": "context",
         "source_id": "opencorporates", "summary": non_eu},
        {"code": "NON_EU_JURISDICTION", "confidence": "low", "kind": "context",
         "source_id": "wikidata", "summary": non_eu},
        {"code": "RELATED_COUNTER_SANCTIONED", "confidence": "medium", "kind": "risk",
         "source_id": "openaleph", "summary": "Possible name match only."},
        {"code": "RELATED_PEP", "confidence": "medium",  # no kind → risk
         "source_id": "openaleph", "summary": "Possible name match only."},
    ]
    return SimpleNamespace(
        lei=lei, legal_name="SHELL PLC", jurisdiction="GB", bods=bods,
        risk_signals=signals, hits=hits, errors={}, license_notices=[],
        derived_identifiers={"gb_coh": "04366849"},
        verdict="A politically exposed person among the parties named.",
    )


def test_shape_lookup_keeps_kind_and_never_calls_context_a_risk() -> None:
    # Phase 153. The web, PDF and share card have split risk findings from
    # structural context since Phases 111/116; the MCP row dropped `kind`
    # and its summary said "Risk signals: GLEIF_REPORTING_EXCEPTION,
    # NON_EU_JURISDICTION, ..." on Shell plc.
    shaped = shaping.shape_lookup(_fake_shell_payload())
    by_code = {r["code"]: r for r in shaped["risk_signals"]}
    assert by_code["GLEIF_REPORTING_EXCEPTION"]["kind"] == "context"
    assert by_code["NON_EU_JURISDICTION"]["kind"] == "context"
    assert by_code["RELATED_COUNTER_SANCTIONED"]["kind"] == "risk"
    assert by_code["RELATED_PEP"]["kind"] == "risk"  # missing kind defaults to risk
    s = shaped["summary"]
    risk_clause = s.split("Risk signals: ", 1)[1].split(".", 1)[0]
    assert "GLEIF_REPORTING_EXCEPTION" not in risk_clause
    assert "NON_EU_JURISDICTION" not in risk_clause
    assert "RELATED_COUNTER_SANCTIONED" in risk_clause and "RELATED_PEP" in risk_clause
    assert "Structural context (not risk findings): GLEIF_REPORTING_EXCEPTION, NON_EU_JURISDICTION." in s
    assert shaped["counts"]["risk_signals"] == 2
    assert shaped["counts"]["context_signals"] == 2
    assert shaped["verdict"] == "A politically exposed person among the parties named."


def test_shape_lookup_merges_identical_rows_and_keeps_their_sources() -> None:
    # NON_EU_JURISDICTION fires once per deepened bundle; four identical
    # rows crossed the wire. One row, three sources.
    shaped = shaping.shape_lookup(_fake_shell_payload())
    rows = [r for r in shaped["risk_signals"] if r["code"] == "NON_EU_JURISDICTION"]
    assert len(rows) == 1
    assert rows[0]["sources"] == ["gleif", "opencorporates", "wikidata"]


def test_shape_lookup_reports_the_composite_licence() -> None:
    # `license_notices` is only what adapters attach per bundle — usually
    # nothing. The Download panel's verdict must reach agents too.
    shaped = shaping.shape_lookup(_fake_shell_payload())
    lic = shaped["licensing"]
    assert lic is not None
    assert lic["commercial_use"] == "no"  # OpenSanctions is CC-BY-NC
    assert lic["share_alike"] is True  # OpenCorporates OC-Terms
    assert any("OpenSanctions" in w for w in lic["warnings"])
    assert "Licensing: NOT for commercial use" in shaped["summary"]


def test_shape_lookup_with_no_context_signals_has_no_context_clause() -> None:
    p = _fake_shell_payload()
    p.risk_signals = [s for s in p.risk_signals if s.get("kind") != "context"]
    shaped = shaping.shape_lookup(p)
    assert "Structural context" not in shaped["summary"]
    assert shaped["counts"]["context_signals"] == 0


def test_shape_lookup_carries_knowability_from_the_frozen_fields() -> None:
    """Phase 226: ``knowability`` is read from the payload's fields, never
    re-rendered, and the subject sentence closes the summary line."""
    payload = _fake_lookup_payload()
    subject = {"code": "KY", "name": "Cayman Islands", "sentence": "Cayman frozen.", "sentences": ["Cayman frozen."],
               "stated_absence": False, "review_status": "unverified", "last_verified": None,
               "access": "legitimate_interest", "fields": {}, "sources": [],
               "opencheck_reads": [], "as_of": "2026-09-16"}
    top = {**subject, "code": "BM", "name": "Bermuda", "sentence": "Bermuda frozen.",
           "opencheck_reads": [{"source_id": "x", "name": "X", "requires_api_key": False, "reads_beneficial_owners": True}]}
    payload.knowability = subject
    payload.knowability_chain = {"subject": "KY", "codes": ["KY", "BM"], "as_of": "2026-09-16",
                                 "statements": [subject, top]}
    shaped = shaping.shape_lookup(payload)
    k = shaped["knowability"]
    assert k["subject"]["code"] == "KY" and k["subject"]["sentence"] == "Cayman frozen."
    assert [c["code"] for c in k["chain"]] == ["BM"]
    assert k["chain"][0]["opencheck_reads"] == ["x"]
    assert k["as_of"] == "2026-09-16"
    assert shaped["summary"].endswith("What can be known (KY): Cayman frozen.")
    assert "hint" in k


def test_shape_lookup_knowability_is_null_for_an_older_payload() -> None:
    payload = _fake_lookup_payload()
    shaped = shaping.shape_lookup(payload)
    assert shaped["knowability"] is None
    assert "What can be known" not in shaped["summary"]



# --------------------------------------------------------------------------
# Phase 292: ranked, capped search candidates with structured identifiers
# --------------------------------------------------------------------------


async def test_search_tool_ranks_caps_and_carries_identifiers(monkeypatch) -> None:
    """Non-LEI candidates from three registers each carry the scheme and
    number ``opencheck_register_lookup`` takes — not only the summary prose —
    and the tool passes ``limit`` through to an honest cap."""
    from opencheck.routers import search as search_router
    from opencheck.sources import SearchKind as SK

    def _h(source, hit_id, name, summary, **ids):
        return SourceHit(source_id=source, hit_id=hit_id, kind=SK.ENTITY, name=name,
                         summary=summary, identifiers=ids, raw={}, is_stub=False)

    async def _fake_run(q, kind):
        return {
            "abr_australia": [_h("abr_australia", "51000000001", "M GROW INVEST",
                                 "AU-ABN 51000000001", au_abn="51000000001")],
            "brreg": [_h("brreg", "923000001", "METASTAR INVEST AS",
                         "NO-ORGNR 923000001 · AS", no_orgnr="923000001")],
            "companies_house": [_h("companies_house", "OC346224", "METASTAR INVEST LLP",
                                   "Company OC346224 · active", gb_coh="OC346224")],
        }, {}

    monkeypatch.setattr(search_router, "_run_adapters", _fake_run)
    out = await mcp_server.opencheck_search(query="Metastar Invest", limit=2)
    assert out["total"] == 3 and out["count"] == 2 and out["truncated"] is True
    by_name = {c["name"]: c for c in out["candidates"]}
    assert set(by_name) == {"METASTAR INVEST AS", "METASTAR INVEST LLP"}
    assert by_name["METASTAR INVEST LLP"]["identifiers"] == [
        {"scheme": "GB-COH", "id": "OC346224"}
    ]
    assert by_name["METASTAR INVEST AS"]["identifiers"] == [
        {"scheme": "NO-BRC", "id": "923000001"}
    ]
    assert all(c["match"] == "same_name" for c in out["candidates"])

    full = await mcp_server.opencheck_search(query="Metastar Invest")
    abr = next(c for c in full["candidates"] if c["source"] == "abr_australia")
    assert abr["identifiers"] == [{"scheme": "AU-ABN", "id": "51000000001"}]
    assert abr["match"] == "fuzzy" and full["candidates"][-1] is abr


# --------------------------------------------------------------------------
# The MCP pre-flight ticket: opencheck_knowability, and the handshake
# instructions that say the order of calls
# --------------------------------------------------------------------------


def _tool_json(result: object) -> dict:
    """The dict a ``mcp.call_tool`` answer carries, whichever shape the SDK
    returns it in (structured content, or a single text part)."""
    import json

    if isinstance(result, tuple):
        parts, structured = result
        if isinstance(structured, dict):
            return structured.get("result", structured)
        return json.loads(parts[0].text)
    if isinstance(result, dict):
        return result
    return json.loads(result[0].text)


async def test_knowability_tool_answers_known_and_unknown_codes_through_call_tool() -> None:
    """Through ``mcp.call_tool`` (the Phase 341 lesson), not only as a direct
    function: a code the table holds gets its dated sentence and the sources
    that read it; an unknown code is a stated absence, never an error."""
    out = _tool_json(
        await mcp_server.mcp.call_tool(
            "opencheck_knowability", {"jurisdictions": ["gb", "ZZ", "GB"]}
        )
    )
    assert "error" not in out
    assert [st["code"] for st in out["statements"]] == ["GB", "ZZ"]  # upper-cased, de-duplicated
    gb, zz = out["statements"]
    assert gb["stated_absence"] is False
    assert gb["sentence"] and gb["sentences"]
    assert gb["access"] in {
        "public", "public_with_registration_or_justification", "legitimate_interest",
        "restricted_no_lia_route_yet", "authorities_and_obliged_entities_only", "no_register",
        "in_progress",
    }
    assert gb["register"] and gb["register"]["name"]
    assert gb["amld6_legitimate_interest_access"] is None  # not EU/EEA
    reads = {r["source_id"] for r in gb["opencheck_reads"]}
    assert "companies_house" in reads
    expected = {"source_id", "name", "requires_api_key", "reads_beneficial_owners"}
    assert expected <= set(gb["opencheck_reads"][0])
    assert all({"url", "title"} <= set(src) for src in gb["sources"])
    assert zz["stated_absence"] is True and zz["review_status"] == "absent"
    assert "no register notes" in zz["sentence"]
    assert zz["register"] is None and zz["opencheck_reads"] == []
    assert "GB" in out["known_codes"] and "ZZ" not in out["known_codes"]
    assert out["as_of"] and out["generated_at"]
    assert "hint" in out
    # The working-tracker fields stay out, as on the results page.
    assert "fields" not in gb and "pending_changes" not in gb and "fatf" not in gb


async def test_knowability_tool_says_which_sources_read_beneficial_owners() -> None:
    out = _tool_json(
        await mcp_server.mcp.call_tool("opencheck_knowability", {"jurisdictions": ["CY", "EE"]})
    )
    cy, ee = out["statements"]
    # EU/EEA: Yes / No only, else null.
    assert cy["amld6_legitimate_interest_access"] in {"Yes", "No", None}
    # Estonia's register publishes beneficial owners and the adapter reads them.
    assert any(r["reads_beneficial_owners"] for r in ee["opencheck_reads"])


async def test_knowability_tool_validates_input() -> None:
    out = await mcp_server.opencheck_knowability(jurisdictions=[])
    assert out["status"] == 400 and "error" in out
    out = await mcp_server.opencheck_knowability(jurisdictions=["  ", ""])
    assert out["status"] == 400
    out = await mcp_server.opencheck_knowability(jurisdictions=["United Kingdom"])
    assert out["status"] == 400 and "United Kingdom".upper() in out["error"]
    # US-DE style subdivisions are accepted (and fall back to the country row).
    out = await mcp_server.opencheck_knowability(jurisdictions=["US-DE"])
    assert out["statements"][0]["code"] == "US-DE" or out["statements"][0]["code"] == "US"
    # Capped like GET /knowability.
    many = [f"A{chr(65 + i % 26)}" for i in range(60)]
    out = await mcp_server.opencheck_knowability(jurisdictions=many)
    assert len(out["statements"]) <= mcp_server._KNOWABILITY_MAX_CODES


def test_knowability_tool_is_on_the_default_tier_and_spends_no_budget() -> None:
    """No pipeline runs, so the tool is not on a lookup/heavy request tier
    (the guard's default per-request unit still applies)."""
    from opencheck.mcp.guard import TOOL_TIERS

    assert "opencheck_knowability" not in TOOL_TIERS


def test_instructions_name_every_tool_and_stay_short() -> None:
    """The handshake instructions ticket: a new tool cannot be left out of the
    guide again, and the guide stays short enough for clients to load."""
    text = mcp_server._INSTRUCTIONS
    missing = [name for name in TOOL_NAMES if name not in text]
    assert missing == [], f"tools missing from _INSTRUCTIONS: {missing}"
    assert len(text.split()) <= mcp_server._INSTRUCTIONS_MAX_WORDS
    # The order of calls and how to read the answers, as the ticket asks.
    steps = (
        "1. Find", "2. Set expectations", "3. Check", "4. Read it honestly", "5. Keep", "6. Many",
        "Cost.",
    )
    for phrase in steps:
        assert phrase in text
    keys = (
        "degraded_sources", "kind: risk", "context", "license_notices", "retry_after_s", "releases",
    )
    for key in keys:
        assert key in text


def test_instructions_reach_the_client_at_initialize() -> None:
    assert mcp_server.mcp._mcp_server.instructions == mcp_server._INSTRUCTIONS

