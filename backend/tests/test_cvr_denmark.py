"""Tests for the CVR Denmark adapter and BODS mapper.

Uses fixture data modelled on live Datafordeler GraphQL responses:
  - Novo Nordisk A/S (CVR 24256790) — active A/S with address and industry code.

No network calls are made; the HTTP client is mocked at the httpx level.
``CVR_DENMARK_API_KEY`` and ``OPENCHECK_ALLOW_LIVE`` are monkeypatched.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from opencheck.sources.cvr_denmark import (
    CvrDenmarkAdapter,
    DK_CVR_RA_CODE,
    normalise_cvr,
    _current,
    _best_navn,
    _best_address,
    _best_branche,
    _best_form,
)
from opencheck.bods.mapper import map_cvr_denmark


# ---------------------------------------------------------------------------
# Fixtures — raw GraphQL response snapshots
# ---------------------------------------------------------------------------

# CVR 24256790 — Novo Nordisk A/S (simplified)
_VIRKSOMHED_NODES: list[dict[str, Any]] = [
    {
        "CVRNummer": 24256790,
        "id": "312108",  # CVREnhedsId is a numeric string from the API (Long type)
        "status": "AKTIV",
        "virksomhedStartdato": "1989-09-14",
        "virksomhedOphoersdato": None,
        "virkningFra": "1989-09-14T00:00:00.000Z",
        "virkningTil": None,
    }
]

_NAVN_NODES: list[dict[str, Any]] = [
    {
        "vaerdi": "Novo Nordisk A/S",
        "sekvens": 0,  # sekvens=0 is primary in Datafordeler CVR API
        "virkningFra": "1989-09-14T00:00:00.000Z",
        "virkningTil": None,
    },
    # Old name — should be filtered out by _current()
    {
        "vaerdi": "Novo Industri A/S",
        "sekvens": 0,
        "virkningFra": "1974-01-01T00:00:00.000Z",
        "virkningTil": "1989-09-13T23:59:59.000Z",
    },
]

_ADDR_NODES: list[dict[str, Any]] = [
    {
        "AdresseringAnvendelse": "BELIGGENHEDSADRESSE",
        "CVRAdresse_vejnavn": "Novo Allé",
        "CVRAdresse_husnummerFra": "1",
        "CVRAdresse_postnummer": "2880",
        "CVRAdresse_postdistrikt": "Bagsværd",
        "CVRAdresse_kommunenavn": "Gladsaxe",
        "CVRAdresse_landekode": "DK",
        "virkningFra": "2000-01-01T00:00:00.000Z",
        "virkningTil": None,
    }
]

_BRANCHE_NODES: list[dict[str, Any]] = [
    {
        "vaerdi": "21.10",
        "sekvens": 0,  # sekvens=0 is primary in Datafordeler CVR API
        "virkningFra": "2007-01-01T00:00:00.000Z",
        "virkningTil": None,
    }
]

_FORM_NODES: list[dict[str, Any]] = [
    {
        "vaerdi": "30",
        "vaerdiTekst": "Aktieselskab",
        "virkningFra": "1989-09-14T00:00:00.000Z",
        "virkningTil": None,
    }
]

_DELTAGER_NODES: list[dict[str, Any]] = []

# Datafordeler forbids aliases and multi-root queries — each entity type
# is fetched in a separate request.  Responses keyed by the actual root field.
_GRAPHQL_RESP_VIRKSOMHED: dict[str, Any] = {
    "data": {"CVR_Virksomhed": {"nodes": _VIRKSOMHED_NODES}}
}
_GRAPHQL_RESP_NAVN: dict[str, Any] = {
    "data": {"CVR_Navn": {"nodes": _NAVN_NODES}}
}
_GRAPHQL_RESP_ADRESSERING: dict[str, Any] = {
    "data": {"CVR_Adressering": {"nodes": _ADDR_NODES}}
}
_GRAPHQL_RESP_BRANCHE: dict[str, Any] = {
    "data": {"CVR_Branche": {"nodes": _BRANCHE_NODES}}
}
_GRAPHQL_RESP_FORM: dict[str, Any] = {
    "data": {"CVR_Virksomhedsform": {"nodes": _FORM_NODES}}
}
_GRAPHQL_RESP_DELTAGER: dict[str, Any] = {
    "data": {"CVR_FuldtAnsvarligDeltagerRelation": {"nodes": _DELTAGER_NODES}}
}

# Ordered list matching asyncio.gather call order in _fetch_bundle:
# virksomhed, then navn/adressering/branche/form/deltager in parallel.
_DETAIL_RESPONSES = [
    _GRAPHQL_RESP_NAVN,
    _GRAPHQL_RESP_ADRESSERING,
    _GRAPHQL_RESP_BRANCHE,
    _GRAPHQL_RESP_FORM,
    _GRAPHQL_RESP_DELTAGER,
]


def _make_bundle(
    cvr_number: str = "24256790",
    name: str = "Novo Nordisk A/S",
    status: str = "active",
    start_date: str | None = "1989-09-14",
    end_date: str | None = None,
    legal_form_code: str | None = "30",
    legal_form_text: str | None = "Aktieselskab",  # vaerdiTekst from API takes priority over local map
    branche_code: str | None = "21.10",
    address: dict | None = None,
    source_url: str = "https://datacvr.virk.dk/enhed/virksomhed/24256790",
) -> dict[str, Any]:
    if address is None:
        address = _ADDR_NODES[0]
    return {
        "cvr_number": cvr_number,
        "cvr_enhed_id": "312108",
        "name": name,
        "status": status,
        "start_date": start_date,
        "end_date": end_date,
        "legal_form_code": legal_form_code,
        "legal_form_text": legal_form_text,
        "branche_code": branche_code,
        "address": address,
        "source_url": source_url,
        "fully_liable_participant_ids": [],
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TestNormaliseCvr:
    def test_pure_digits_unchanged(self) -> None:
        assert normalise_cvr("24256790") == "24256790"

    def test_strips_whitespace(self) -> None:
        assert normalise_cvr("  24256790  ") == "24256790"

    def test_pads_to_8_digits(self) -> None:
        assert normalise_cvr("1000") == "00001000"

    def test_int_input(self) -> None:
        assert normalise_cvr(24256790) == "24256790"

    def test_zero_padded_string(self) -> None:
        assert normalise_cvr("00001234") == "00001234"


class TestCurrent:
    def test_returns_null_virkningTil_nodes(self) -> None:
        nodes = [
            {"vaerdi": "Old", "virkningTil": "2020-01-01"},
            {"vaerdi": "Current", "virkningTil": None},
        ]
        result = _current(nodes)
        assert len(result) == 1
        assert result[0]["vaerdi"] == "Current"

    def test_fallback_returns_all_when_none_current(self) -> None:
        nodes = [{"vaerdi": "Old", "virkningTil": "2020-01-01"}]
        result = _current(nodes)
        assert result == nodes

    def test_empty_list(self) -> None:
        assert _current([]) == []


class TestBestNavn:
    def test_picks_primary_sekvens1(self) -> None:
        assert _best_navn(_NAVN_NODES) == "Novo Nordisk A/S"

    def test_empty_list(self) -> None:
        assert _best_navn([]) is None

    def test_falls_back_to_any_current_when_no_sekvens0(self) -> None:
        nodes = [{"vaerdi": "Only Name", "sekvens": 1, "virkningTil": None}]
        assert _best_navn(nodes) == "Only Name"


class TestBestBranche:
    def test_picks_sekvens1(self) -> None:
        assert _best_branche(_BRANCHE_NODES) == "21.10"

    def test_empty(self) -> None:
        assert _best_branche([]) is None


class TestBestForm:
    def test_returns_code_and_text(self) -> None:
        code, text = _best_form(_FORM_NODES)
        assert code == "30"
        assert text == "Aktieselskab"

    def test_empty(self) -> None:
        assert _best_form([]) == (None, None)


class TestConstant:
    def test_ra_code(self) -> None:
        assert DK_CVR_RA_CODE == "RA000170"


# ---------------------------------------------------------------------------
# BODS mapper
# ---------------------------------------------------------------------------


class TestMapCvrDenmark:
    def test_stub_yields_nothing(self) -> None:
        stmts = list(map_cvr_denmark({"is_stub": True, "cvr_number": "24256790"}))
        assert stmts == []

    def test_empty_bundle_yields_nothing(self) -> None:
        stmts = list(map_cvr_denmark({}))
        assert stmts == []

    def test_entity_statement_produced(self) -> None:
        stmts = list(map_cvr_denmark(_make_bundle()))
        entity_stmts = [s for s in stmts if s["recordType"] == "entity"]
        assert len(entity_stmts) == 1

    def test_entity_name(self) -> None:
        stmts = list(map_cvr_denmark(_make_bundle()))
        entity = next(s for s in stmts if s["recordType"] == "entity")
        assert entity["recordDetails"]["name"] == "Novo Nordisk A/S"

    def test_dk_cvr_identifier(self) -> None:
        stmts = list(map_cvr_denmark(_make_bundle()))
        entity = next(s for s in stmts if s["recordType"] == "entity")
        ids = {i["scheme"]: i["id"] for i in entity["recordDetails"]["identifiers"]}
        assert ids["DK-CVR"] == "24256790"

    def test_entity_jurisdiction(self) -> None:
        stmts = list(map_cvr_denmark(_make_bundle()))
        entity = next(s for s in stmts if s["recordType"] == "entity")
        assert entity["recordDetails"]["jurisdiction"]["code"] == "DK"

    def test_founding_date(self) -> None:
        stmts = list(map_cvr_denmark(_make_bundle()))
        entity = next(s for s in stmts if s["recordType"] == "entity")
        assert entity["recordDetails"]["foundingDate"] == "1989-09-14"

    def test_dissolution_date_absent_when_active(self) -> None:
        stmts = list(map_cvr_denmark(_make_bundle(end_date=None)))
        entity = next(s for s in stmts if s["recordType"] == "entity")
        assert "dissolutionDate" not in entity["recordDetails"]

    def test_dissolution_date_set_when_dissolved(self) -> None:
        stmts = list(map_cvr_denmark(_make_bundle(end_date="2023-12-31")))
        entity = next(s for s in stmts if s["recordType"] == "entity")
        assert entity["recordDetails"]["dissolutionDate"] == "2023-12-31"

    def test_entity_type_registered_entity(self) -> None:
        stmts = list(map_cvr_denmark(_make_bundle()))
        entity = next(s for s in stmts if s["recordType"] == "entity")
        assert entity["recordDetails"]["entityType"]["type"] == "registeredEntity"

    def test_legal_form_subtype(self) -> None:
        stmts = list(map_cvr_denmark(_make_bundle()))
        entity = next(s for s in stmts if s["recordType"] == "entity")
        assert "subtype" in entity["recordDetails"]["entityType"]

    def test_address_included(self) -> None:
        stmts = list(map_cvr_denmark(_make_bundle()))
        entity = next(s for s in stmts if s["recordType"] == "entity")
        addrs = entity["recordDetails"].get("addresses") or []
        assert len(addrs) >= 1
        assert "Novo Allé" in addrs[0].get("address", "")

    def test_industry_code(self) -> None:
        stmts = list(map_cvr_denmark(_make_bundle()))
        entity = next(s for s in stmts if s["recordType"] == "entity")
        assert entity["recordDetails"]["primaryIndustryCode"] == "21.10"

    def test_no_industry_code_when_absent(self) -> None:
        stmts = list(map_cvr_denmark(_make_bundle(branche_code=None)))
        entity = next(s for s in stmts if s["recordType"] == "entity")
        assert "primaryIndustryCode" not in entity["recordDetails"]

    def test_only_entity_statement_no_persons(self) -> None:
        stmts = list(map_cvr_denmark(_make_bundle()))
        for stmt in stmts:
            assert stmt["recordType"] != "person"
            assert stmt["recordType"] != "relationship"

    def test_official_register_source_type(self) -> None:
        stmts = list(map_cvr_denmark(_make_bundle()))
        entity = next(s for s in stmts if s["recordType"] == "entity")
        assert "officialRegister" in entity["source"]["type"]

    def test_required_fields_present(self) -> None:
        for stmt in map_cvr_denmark(_make_bundle()):
            assert "statementId" in stmt
            assert "recordType" in stmt
            assert "recordDetails" in stmt
            assert "source" in stmt

    def test_deterministic_ids(self) -> None:
        ids1 = [s["statementId"] for s in map_cvr_denmark(_make_bundle())]
        ids2 = [s["statementId"] for s in map_cvr_denmark(_make_bundle())]
        assert ids1 == ids2


# ---------------------------------------------------------------------------
# Adapter: fetch (unit — mocked HTTP)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_returns_bundle(monkeypatch, tmp_path) -> None:
    """fetch() with successful GraphQL responses builds a non-stub bundle."""
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("CVR_DENMARK_API_KEY", "test-key")
    get_settings.cache_clear()

    # 6 requests: 1 virksomhed + 5 parallel detail queries.
    # Route by inspecting the query string in the POST body.
    _QUERY_ROUTE = {
        "CVR_Navn": _GRAPHQL_RESP_NAVN,
        "CVR_Adressering": _GRAPHQL_RESP_ADRESSERING,
        "CVR_Branche": _GRAPHQL_RESP_BRANCHE,
        "CVR_Virksomhedsform": _GRAPHQL_RESP_FORM,
        "CVR_FuldtAnsvarligDeltagerRelation": _GRAPHQL_RESP_DELTAGER,
        "CVR_Virksomhed": _GRAPHQL_RESP_VIRKSOMHED,
    }

    call_count = [0]

    async def mock_post(url: str, **kwargs: Any) -> MagicMock:
        call_count[0] += 1
        query_str = (kwargs.get("json") or {}).get("query", "")
        payload = next(
            v for k, v in _QUERY_ROUTE.items() if k in query_str
        )
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = payload
        return resp

    mock_client = AsyncMock()
    mock_client.post = mock_post
    mock_client.aclose = AsyncMock()

    with patch("opencheck.sources.cvr_denmark.build_client", return_value=mock_client):
        adapter = CvrDenmarkAdapter()
        bundle = await adapter.fetch("24256790", legal_name="Novo Nordisk A/S")

    get_settings.cache_clear()

    assert bundle.get("is_stub") is not True
    assert bundle["cvr_number"] == "24256790"
    assert bundle["name"] == "Novo Nordisk A/S"
    assert bundle["status"] == "active"
    assert bundle["start_date"] == "1989-09-14"
    assert bundle["end_date"] is None
    assert bundle["legal_form_code"] == "30"
    assert bundle["branche_code"] == "21.10"
    assert call_count[0] == 6  # 1 virksomhed + 5 detail queries


@pytest.mark.asyncio
async def test_fetch_not_found_raises(monkeypatch, tmp_path) -> None:
    """fetch() raises LookupError when the CVR number is not in CVR."""
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("CVR_DENMARK_API_KEY", "test-key")
    get_settings.cache_clear()

    resp1 = MagicMock()
    resp1.raise_for_status = MagicMock()
    resp1.json.return_value = {"data": {"CVR_Virksomhed": {"nodes": []}}}  # not found

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=resp1)
    mock_client.aclose = AsyncMock()

    with patch("opencheck.sources.cvr_denmark.build_client", return_value=mock_client):
        adapter = CvrDenmarkAdapter()
        with pytest.raises(LookupError):
            await adapter.fetch("99999999")

    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_fetch_no_api_key_raises(monkeypatch, tmp_path) -> None:
    """fetch() raises RuntimeError when CVR_DENMARK_API_KEY is absent."""
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.delenv("CVR_DENMARK_API_KEY", raising=False)
    get_settings.cache_clear()

    adapter = CvrDenmarkAdapter()
    with pytest.raises(RuntimeError, match="CVR_DENMARK_API_KEY"):
        await adapter.fetch("24256790")

    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_search_returns_empty(monkeypatch, tmp_path) -> None:
    """search() always returns [] — CVR is identifier-keyed only."""
    from opencheck.config import get_settings
    from opencheck.sources.base import SearchKind

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()

    adapter = CvrDenmarkAdapter()
    hits = await adapter.search("Novo Nordisk", kind=SearchKind.ENTITY)
    assert hits == []
    get_settings.cache_clear()
