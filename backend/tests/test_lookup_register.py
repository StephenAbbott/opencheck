"""Phase 290 — ``/lookup-register``, ``/lookup-register-stream`` and the
``opencheck_register_lookup`` MCP tool: due diligence on a company that has
no LEI, anchored on its register number.

``opencheck_search("Metastar Invest")`` returns the exact Companies House
match METASTAR INVEST LLP (OC346224) with ``lei: null`` and a hint to pass a
``lei`` — a dead end for the 238 of 347 entities in the Azerbaijani
Laundromat thesaurus that are UK company numbers without an LEI. The
register that owns the scheme is now an anchor in its own right: one
register read, then the same screens, risk engine, verdict, profile and
knowability the LEI pipeline runs, through the same replay cache, flights,
gate and lookup budget.

Offline and deterministic: the Companies House fetch and mapper are stood in
for, the screens are observed rather than run.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from opencheck import lookup_budget
from opencheck.app import app
from opencheck.bods.mapper import _stable_id
from opencheck.config import get_settings
from opencheck.lookup_replay import _IN_FLIGHT, _REPLAY_CACHE
from opencheck.mcp import shaping
from opencheck.risk import RiskSignal
from opencheck.routers import lookup as lk

_NUMBER = "OC346224"  # METASTAR INVEST LLP
_MEMBER = "SC123456"
_NAME = "METASTAR INVEST LLP"


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    monkeypatch.delenv("OPENCHECK_BOT_GATE_LOOKUP_STREAM", raising=False)
    get_settings.cache_clear()
    _REPLAY_CACHE.clear()
    _IN_FLIGHT.clear()
    yield
    _REPLAY_CACHE.clear()
    _IN_FLIGHT.clear()
    get_settings.cache_clear()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _metastar_bundle(number: str) -> list[dict]:
    """What the Companies House mapper emits for an LLP whose designated
    member is an offshore company: the subject keyed on its number, the
    corporate member on its own register's number, one person officer, and
    the control relationship."""
    subj = _stable_id("companies_house", "entity", number)
    member = _stable_id("companies_house", "entity", _MEMBER)
    return [
        {
            "statementId": subj,
            "recordType": "entity",
            "recordDetails": {
                "entityType": {"type": "registeredEntity"},
                "name": _NAME,
                "jurisdiction": {"name": "United Kingdom", "code": "GB"},
                "identifiers": [
                    {"id": number, "scheme": "GB-COH", "schemeName": "Companies House"}
                ],
            },
        },
        {
            "statementId": member,
            "recordType": "entity",
            "recordDetails": {
                "entityType": {"type": "registeredEntity"},
                "name": "HILUX SERVICES LP",
                "jurisdiction": {"name": "Seychelles", "code": "SC"},
                "identifiers": [{"id": _MEMBER, "scheme": "REG-SC"}],
            },
        },
        {
            "statementId": "person-1",
            "recordType": "person",
            "recordDetails": {
                "personType": "knownPerson",
                "names": [{"type": "legal", "fullName": "Example Officer"}],
            },
        },
        {
            "statementId": "rel-1",
            "recordType": "relationship",
            "recordDetails": {
                "subject": subj,
                "interestedParty": member,
                "interests": [{"type": "shareholding"}],
            },
        },
    ]


def _patch_register(monkeypatch, *, fetched: list[str], screened: list[dict[str, Any]]):
    """Stand in for the Companies House fetch and mapper; record what each
    screen was asked (the bundle and the subject reference)."""

    async def _fake_fetch(adapter, hit_id, **kwargs):
        assert adapter.id == "companies_house"
        fetched.append(hit_id)
        return (
            {
                "source_id": "companies_house",
                "company_number": hit_id,
                "profile": {"company_name": _NAME, "company_number": hit_id,
                            "company_status": "active", "type": "llp"},
                "officers": [], "pscs": [], "psc_statements": [],
            },
            None,
        )

    def _fake_mapper(source_id):
        assert source_id == "companies_house"
        return lambda raw: _metastar_bundle(raw["company_number"])

    async def _fake_cross(bods, *, degraded=None, screen=None, subject_lei=None, screen_subject=False, **kw):
        screened.append({"check": "cross", "bods": bods, "subject": subject_lei, "screen_subject": screen_subject})
        subj = _stable_id("companies_house", "entity", _NUMBER)
        return [RiskSignal(
            code="SANCTIONED", confidence="high", source_id="opensanctions",
            hit_id="os-metastar", summary="The looked-up company's name matches a record on OpenSanctions: sanctioned.",
            evidence={"statement_id": subj, "subject": True},
        )]

    async def _fake_icij(bods, *, degraded=None, subject_lei=None, **kw):
        screened.append({"check": "icij", "bods": bods, "subject": subject_lei})
        return []

    async def _fake_oa(bods, *, degraded=None, screening=None, subject_lei=None, **kw):
        screened.append({"check": "openaleph", "bods": bods, "subject": subject_lei})
        return []

    monkeypatch.setattr(lk, "_fetch_with_provenance", _fake_fetch)
    monkeypatch.setattr(lk, "_mapper_for", _fake_mapper)
    monkeypatch.setattr(lk, "assess_cross_source_names", _fake_cross)
    monkeypatch.setattr(lk, "assess_icij_names", _fake_icij)
    monkeypatch.setattr(lk, "assess_openaleph_names", _fake_oa)


# ---------------------------------------------------------------------------
# The anchor
# ---------------------------------------------------------------------------


def test_unknown_scheme_is_a_400_that_names_the_known_ones(client):
    r = client.get("/lookup-register", params={"scheme": "XX-NOPE", "id": "1"})
    assert r.status_code == 400
    assert "GB-COH" in r.json()["detail"] and "/expand-schemes" in r.json()["detail"]


def test_a_value_that_is_not_a_number_of_that_register_is_a_400(client):
    r = client.get("/lookup-register", params={"scheme": "GB-COH", "id": "Uk"})
    assert r.status_code == 400
    assert "GB-COH" in r.json()["detail"]


def test_the_subject_reference_is_the_canonical_scheme_and_number():
    hop, local_id = lk._register_anchor("reg-gb", "oc346224")
    assert (hop.scheme, local_id) == ("GB-COH", "OC346224")
    assert lk.register_subject(hop.scheme, local_id) == "GB-COH:OC346224"


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------


def test_register_lookup_returns_the_lookup_shape_with_no_lei(client, monkeypatch):
    fetched: list[str] = []
    screened: list[dict[str, Any]] = []
    _patch_register(monkeypatch, fetched=fetched, screened=screened)

    r = client.get("/lookup-register", params={"scheme": "GB-COH", "id": _NUMBER})
    assert r.status_code == 200, r.text
    body = r.json()

    # The anchor: the register number, no LEI anywhere.
    assert body["lei"] is None
    assert (body["scheme"], body["id"]) == ("GB-COH", _NUMBER)
    assert body["legal_name"] == _NAME and body["jurisdiction"] == "GB"
    assert body["derived_identifiers"] == {"gb_coh": _NUMBER}
    assert "lei" not in body["derived_identifiers"]
    assert body["listing"] is None
    # One register read, once.
    assert fetched == [_NUMBER]
    assert [h["source_id"] for h in body["hits"]] == ["companies_house"]
    assert body["hits"][0]["hit_id"] == _NUMBER
    assert body["sources_applicable"] == ["companies_house"]  # no live screen offline
    # The bundle: the Seychelles corporate member is visible.
    names = {s["recordDetails"].get("name") for s in body["bods"] if s["recordType"] == "entity"}
    assert names == {_NAME, "HILUX SERVICES LP"}
    assert body["graph_shape"]["relationships"] == 1
    # Everything the LEI pipeline says about a subject, said here too.
    assert body["knowability"]["code"] == "GB"
    assert body["knowability_chain"]["codes"][:2] == ["GB", "SC"]
    assert body["subject_profile"]["jurisdiction"] == "GB"
    assert body["subject_profile"]["lei_registration"] is None
    assert "SANCTIONED" in {sig["code"] for sig in body["risk_signals"]}
    assert body["verdict"]
    assert body["run_completed_at"]
    assert body["replayed"] is False


def test_the_screens_get_the_register_reference_as_the_subject(client, monkeypatch):
    """Phase 235's exclusion of the subject from the related-party screens
    holds without an LEI — and the cross-source screen is told to screen the
    subject's own name, since nothing else will."""
    screened: list[dict[str, Any]] = []
    _patch_register(monkeypatch, fetched=[], screened=screened)
    assert client.get("/lookup-register", params={"scheme": "REG-GB", "id": _NUMBER}).status_code == 200
    by_check = {s["check"]: s for s in screened}
    assert set(by_check) == {"cross", "icij", "openaleph"}
    for s in by_check.values():
        assert s["subject"] == "GB-COH:OC346224"
        assert len(s["bods"]) == 4
    assert by_check["cross"]["screen_subject"] is True


def test_an_unknown_number_is_a_404_not_a_crash(client, monkeypatch):
    async def _stub(adapter, hit_id, **kwargs):
        return {"is_stub": True}, None

    monkeypatch.setattr(lk, "_fetch_with_provenance", _stub)
    r = client.get("/lookup-register", params={"scheme": "GB-COH", "id": "00000001"})
    assert r.status_code == 404
    assert "00000001" in r.json()["detail"]


# ---------------------------------------------------------------------------
# Cache, budget, gate
# ---------------------------------------------------------------------------


def test_the_replay_cache_is_keyed_on_scheme_and_id(client, monkeypatch):
    fetched: list[str] = []
    _patch_register(monkeypatch, fetched=fetched, screened=[])
    first = client.get("/lookup-register", params={"scheme": "GB-COH", "id": _NUMBER}).json()
    second = client.get("/lookup-register", params={"scheme": "reg-gb", "id": "oc346224"}).json()
    assert fetched == [_NUMBER]  # the second spelling replayed the first run
    assert second["replayed"] is True and second["fetched_at"] == first["run_completed_at"]
    assert "GB-COH:OC346224:5" in _REPLAY_CACHE
    assert lk.replay_entry("GB-COH:OC346224") is not None
    assert client.get(
        "/lookup-register", params={"scheme": "GB-COH", "id": _NUMBER, "refresh": "true"}
    ).json()["replayed"] is False
    assert fetched == [_NUMBER, _NUMBER]


@pytest.fixture
def budget_on(monkeypatch):
    """Rate limiting on (the suite turns it off), a lookup budget of 1/minute."""
    monkeypatch.setenv("OPENCHECK_RATE_LIMIT_ENABLED", "1")
    monkeypatch.setenv("OPENCHECK_RATE_LIMIT_LOOKUP", "1/minute")
    get_settings.cache_clear()
    lookup_budget.reset_for_tests()
    yield
    get_settings.cache_clear()
    lookup_budget.reset_for_tests()


async def test_a_fresh_register_run_is_charged_to_the_lookup_budget(budget_on, monkeypatch):
    """Phase 234: one budget for every full lookup, however it is asked for.
    A register-anchored run is a fresh pipeline, so it costs one slot — and a
    spent budget refuses it with 429 + Retry-After, exactly as ``/lookup``."""
    _patch_register(monkeypatch, fetched=[], screened=[])
    with lookup_budget.client_scope("198.51.100.90"):
        first = await lk._register_lookup_impl("GB-COH", _NUMBER)
        assert first.lei is None and first.id == _NUMBER
        # A replay is free.
        assert (await lk._register_lookup_impl("GB-COH", _NUMBER)).replayed is True
        with pytest.raises(HTTPException) as exc:
            await lk._register_lookup_impl("GB-COH", "01234567")
    assert exc.value.status_code == 429
    assert int(exc.value.headers["Retry-After"]) >= 1
    assert lookup_budget._budget.used("198.51.100.90") == 1


async def test_a_bad_scheme_or_number_costs_nothing(budget_on):
    with lookup_budget.client_scope("198.51.100.91"):
        for scheme, ident in (("XX-NOPE", "1"), ("GB-COH", "Uk")):
            with pytest.raises(HTTPException) as exc:
                await lk._register_lookup_impl(scheme, ident)
            assert exc.value.status_code == 400
    assert lookup_budget._budget.used("198.51.100.91") == 0


_BOT_UA = "python-httpx/0.27.0"
_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)


def test_the_stream_variant_is_bot_gated_and_the_json_route_is_not(client, monkeypatch):
    _patch_register(monkeypatch, fetched=[], screened=[])
    gated = client.get(
        "/lookup-register-stream",
        params={"scheme": "GB-COH", "id": _NUMBER},
        headers={"user-agent": _BOT_UA},
    )
    assert gated.status_code == 403
    assert "/lookup-register?scheme=" in gated.json()["detail"]
    # The JSON route is the one for scripts and agents.
    assert client.get(
        "/lookup-register", params={"scheme": "GB-COH", "id": _NUMBER},
        headers={"user-agent": _BOT_UA},
    ).status_code == 200


def test_the_stream_carries_register_done_where_the_lei_stream_says_gleif_done(client, monkeypatch):
    _patch_register(monkeypatch, fetched=[], screened=[])
    with client.stream(
        "GET", "/lookup-register-stream",
        params={"scheme": "GB-COH", "id": _NUMBER},
        headers={"user-agent": _BROWSER_UA},
    ) as r:
        assert r.status_code == 200
        text = "".join(r.iter_text())
    assert "event: register_done" in text and "event: gleif_done" not in text
    assert "event: knowability" in text and "event: risk_signals" in text and "event: done" in text
    assert "event: listing" not in text


# ---------------------------------------------------------------------------
# MCP
# ---------------------------------------------------------------------------


def test_mcp_register_lookup_shapes_the_anchor_honestly(monkeypatch):
    from opencheck.mcp.server import opencheck_register_lookup

    _patch_register(monkeypatch, fetched=[], screened=[])
    out = asyncio.run(opencheck_register_lookup("GB-COH", _NUMBER))
    assert out["lei"] is None and (out["scheme"], out["id"]) == ("GB-COH", _NUMBER)
    assert out["legal_name"] == _NAME
    assert out["summary"].startswith(f"{_NAME} (GB-COH {_NUMBER}, GB; no LEI")
    assert "screened by name" in out["summary"]
    assert "lei" not in out["derived_identifiers"]
    assert out["identifiers"] == [
        {"scheme": "GB-COH", "schemeName": "Companies House", "id": _NUMBER}
    ]
    assert out["listing"] is None
    assert out["knowability"]["subject"]["code"] == "GB"
    assert "SANCTIONED" in {sig["code"] for sig in out["risk_signals"]}
    assert "no LEI" in out["hint"]


def test_mcp_register_lookup_reports_errors_like_the_other_tools():
    from opencheck.mcp.server import opencheck_register_lookup

    out = asyncio.run(opencheck_register_lookup("XX-NOPE", "1"))
    assert out["status"] == 400 and "GB-COH" in out["error"]


def test_search_candidates_carry_structured_identifiers_to_chain_on():
    from types import SimpleNamespace

    from opencheck.sources import SearchKind, SourceHit

    hits = [
        SourceHit(source_id="companies_house", hit_id=_NUMBER, name=_NAME, kind=SearchKind.ENTITY, is_stub=False,
                  summary="LLP", identifiers={"gb_coh": _NUMBER}),
        SourceHit(source_id="gleif", hit_id="213800LH1BZH3DI6G760", name="BP P.L.C.", kind=SearchKind.ENTITY, is_stub=False,
                  summary="", identifiers={"lei": "213800LH1BZH3DI6G760", "gb_coh": "00102498",
                                           "ocid": "gb/00102498"}),
    ]
    out = shaping.shape_search(SimpleNamespace(query="x", kind=SearchKind.ENTITY, hits=hits))
    first, second = out["candidates"]
    assert first["lei"] is None
    assert first["identifiers"] == [{"scheme": "GB-COH", "id": _NUMBER}]
    assert second["identifiers"] == [
        {"scheme": "XI-LEI", "id": "213800LH1BZH3DI6G760"},
        {"scheme": "GB-COH", "id": "00102498"},
    ]  # an OpenCorporates id is not something a tool can act on
    assert "opencheck_register_lookup" in out["hint"]


# ---------------------------------------------------------------------------
# Follow-up (5 Oct 2026, production checks): the register's own 404, and a
# register that answers without a name
# ---------------------------------------------------------------------------


def test_the_registers_own_404_is_a_404_not_a_502(client, monkeypatch):
    """Companies House raises on an unknown number; in production that read
    as "fetch failed … HTTP 404" with status 502. The register's answer is a
    404 of ours."""
    import httpx

    async def _raise_404(adapter, hit_id, **kwargs):
        req = httpx.Request("GET", "https://api.company-information.service.gov.uk/company/00000001")
        raise httpx.HTTPStatusError("404", request=req, response=httpx.Response(404, request=req))

    monkeypatch.setattr(lk, "_fetch_with_provenance", _raise_404)
    r = client.get("/lookup-register", params={"scheme": "GB-COH", "id": "00000001"})
    assert r.status_code == 404
    assert "00000001" in r.json()["detail"] and "fetch failed" not in r.json()["detail"]


def _patch_nameless_register(monkeypatch, *, fetched: list[tuple[str, str]]):
    """A KvK-shaped register: answers with a record that carries no name, and
    whose mapper emits nothing without one — unless the caller supplied it."""

    async def _fake_fetch(adapter, hit_id, **kwargs):
        fetched.append((hit_id, kwargs.get("legal_name", "")))
        return (
            {"source_id": "kvk", "kvk_number": hit_id,
             "company": {"rechtsvormCode": "NV", "datumAanvang": "18730127", "actief": "J"},
             "legal_name": kwargs.get("legal_name", ""), "is_stub": False},
            None,
        )

    def _fake_mapper(source_id):
        def _map(raw):
            if not raw.get("legal_name"):
                return []
            return [{
                "statementId": _stable_id("kvk", "entity", raw["kvk_number"]),
                "recordType": "entity",
                "recordDetails": {
                    "entityType": {"type": "registeredEntity"},
                    "name": raw["legal_name"],
                    "jurisdiction": {"code": "NL"},
                    "identifiers": [{"id": raw["kvk_number"], "scheme": "NL-KVK"}],
                },
            }]
        return _map

    async def _no_signals(bods, **kw):
        return []

    monkeypatch.setattr(lk, "_fetch_with_provenance", _fake_fetch)
    monkeypatch.setattr(lk, "_mapper_for", _fake_mapper)
    monkeypatch.setattr(lk, "assess_cross_source_names", _no_signals)
    monkeypatch.setattr(lk, "assess_icij_names", _no_signals)
    monkeypatch.setattr(lk, "assess_openaleph_names", _no_signals)


def test_a_register_that_answers_without_a_name_is_degraded_not_clean(client, monkeypatch):
    fetched: list[tuple[str, str]] = []
    _patch_nameless_register(monkeypatch, fetched=fetched)
    r = client.get("/lookup-register", params={"scheme": "NL-KVK", "id": "33011433"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["legal_name"] is None and body["bods"] == []
    assert fetched == [("33011433", "")]
    # Nothing was mapped or screened, and the response says so where every
    # reader looks for it — never "no risk signals surfaced".
    degraded = body["degraded_sources"]
    assert [(d["source_id"], d["check"]) for d in degraded] == [("kvk", "source_read")]
    assert "name=" in degraded[0]["detail"]
    assert body["risk_signals"] == []
    assert body["verdict"] == "No risk signals surfaced, but one source answered only in part."


def test_a_name_from_the_caller_reaches_the_register_and_the_mapper(client, monkeypatch):
    fetched: list[tuple[str, str]] = []
    _patch_nameless_register(monkeypatch, fetched=fetched)
    r = client.get(
        "/lookup-register",
        params={"scheme": "NL-KVK", "id": "33011433", "name": "Heineken N.V."},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert fetched == [("33011433", "Heineken N.V.")]
    assert body["legal_name"] == "Heineken N.V." and body["jurisdiction"] == "NL"
    assert [s["recordDetails"]["name"] for s in body["bods"]] == ["Heineken N.V."]
    assert body["degraded_sources"] == []
    assert body["subject_profile"] is not None
    # A named run and a nameless one are different runs: neither replays the other.
    assert "NL-KVK:33011433#Heineken N.V.:5" in _REPLAY_CACHE
    nameless = client.get("/lookup-register", params={"scheme": "NL-KVK", "id": "33011433"}).json()
    assert nameless["replayed"] is False and nameless["legal_name"] is None
    assert len(fetched) == 2


def test_mcp_register_lookup_passes_the_name_through(monkeypatch):
    from opencheck.mcp.server import opencheck_register_lookup

    fetched: list[tuple[str, str]] = []
    _patch_nameless_register(monkeypatch, fetched=fetched)
    out = asyncio.run(opencheck_register_lookup("NL-KVK", "33011433", name="Heineken N.V."))
    assert fetched == [("33011433", "Heineken N.V.")]
    assert out["legal_name"] == "Heineken N.V." and out["lei"] is None
