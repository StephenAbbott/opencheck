"""Tests for the Corporations Canada adapter and BODS mapper.

Uses fixture data modelled on live ISED API responses:
  - Abbotsford Chamber of Commerce (corpId 1007)  — active, Boards of Trade Act
  - Acasta Enterprises Inc. (corpId 659770)        — active, CBCA, with directors

No network calls are made; ``_cache`` is mocked out and
``live_available`` is forced True so the code paths that build bundles
from API responses are exercised.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from opencheck.sources.corporations_canada import (
    CorporationsCanadaAdapter,
    CA_CORP_RA_CODE,
    normalise_corp_id,
    extract_current_name,
)
from opencheck.bods.mapper import map_corporations_canada


# ---------------------------------------------------------------------------
# Fixtures — raw API response snapshots
# ---------------------------------------------------------------------------

CORP_1007: dict[str, Any] = {
    "corporationId": "1007",
    "act": "Boards of Trade Act - Part II",
    "status": "Active",
    "corporationNames": [
        {
            "CorporationName": {
                "name": "Abbotsford Chamber of Commerce",
                "nameType": "Primary",
                "current": True,
                "effectiveDate": "1995-02-06",
            }
        }
    ],
    "adresses": [
        {
            "address": {
                "addressLine": ["207 - 32900 SOUTH FRASER WAY"],
                "city": "ABBOTSFORD",
                "postalCode": "V2S 5A1",
                "provinceCode": "BC",
                "countryCode": "CA",
            }
        }
    ],
    "businessNumbers": {"businessNumber": "106679285"},
    "activities": [
        {"activity": {"activity": "Incorporation", "date": "1947-01-10"}}
    ],
    "annualReturns": [],
}

DIRECTORS_1007: dict[str, Any] = {
    "_embedded": {
        "directors": [
            {
                "firstName": "ALICE",
                "lastName": "SMITH",
                "serviceAddress": {
                    "line1": "207 - 32900 SOUTH FRASER WAY",
                    "city": "ABBOTSFORD",
                    "subdivisionCode": "BC",
                    "postalCode": "V2S 5A1",
                    "countryCode": "CA",
                },
            },
            {
                "firstName": "BOB",
                "lastName": "JONES",
                "serviceAddress": {
                    "line1": "1 MAIN ST",
                    "city": "VANCOUVER",
                    "subdivisionCode": "BC",
                    "postalCode": "V6B 1A1",
                    "countryCode": "CA",
                },
            },
        ]
    }
}

CORP_NOT_FOUND_RESPONSE: list = [
    "could not find corporation 999999",
    "La société 999999 est inconnue.",
]

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


class TestNormaliseCorpId:
    def test_pure_digits_unchanged(self) -> None:
        assert normalise_corp_id("1007") == "1007"

    def test_strips_whitespace(self) -> None:
        assert normalise_corp_id("  1007  ") == "1007"

    def test_strips_non_digit_prefix(self) -> None:
        assert normalise_corp_id("CA-1007") == "1007"

    def test_empty_string(self) -> None:
        assert normalise_corp_id("") == ""

    def test_large_number(self) -> None:
        assert normalise_corp_id("659770") == "659770"


class TestExtractCurrentName:
    def test_current_primary_name(self) -> None:
        assert extract_current_name(CORP_1007) == "Abbotsford Chamber of Commerce"

    def test_fallback_to_any_current(self) -> None:
        corp = {
            "corporationNames": [
                {
                    "CorporationName": {
                        "name": "Some Corp",
                        "nameType": "Legal",
                        "current": True,
                    }
                }
            ]
        }
        assert extract_current_name(corp) == "Some Corp"

    def test_fallback_to_last_if_none_current(self) -> None:
        corp = {
            "corporationNames": [
                {
                    "CorporationName": {
                        "name": "Old Name",
                        "nameType": "Primary",
                        "current": False,
                    }
                }
            ]
        }
        assert extract_current_name(corp) == "Old Name"

    def test_empty_names(self) -> None:
        assert extract_current_name({"corporationNames": []}) == ""

    def test_missing_names_key(self) -> None:
        assert extract_current_name({}) == ""


class TestConstant:
    def test_ra_code(self) -> None:
        assert CA_CORP_RA_CODE == "RA000072"


# ---------------------------------------------------------------------------
# BODS mapper
# ---------------------------------------------------------------------------


class TestMapCorporationsCanada:
    def _build_bundle(
        self,
        corp: dict | None = None,
        directors: list | None = None,
    ) -> dict:
        return {
            "source_id": "corporations_canada",
            "corp_id": "1007",
            "corporation": corp if corp is not None else CORP_1007,
            "directors": directors if directors is not None else [],
            "legal_name": "Abbotsford Chamber of Commerce",
            "is_stub": False,
        }

    def test_stub_yields_nothing(self) -> None:
        stmts = list(map_corporations_canada({"is_stub": True, "corp_id": "1007"}))
        assert stmts == []

    def test_empty_bundle_yields_nothing(self) -> None:
        stmts = list(map_corporations_canada({}))
        assert stmts == []

    def test_entity_statement_produced(self) -> None:
        stmts = list(map_corporations_canada(self._build_bundle()))
        entity_stmts = [s for s in stmts if s["recordType"] == "entity"]
        assert len(entity_stmts) == 1

    def test_entity_name(self) -> None:
        stmts = list(map_corporations_canada(self._build_bundle()))
        entity = next(s for s in stmts if s["recordType"] == "entity")
        assert entity["recordDetails"]["name"] == "Abbotsford Chamber of Commerce"

    def test_entity_jurisdiction(self) -> None:
        stmts = list(map_corporations_canada(self._build_bundle()))
        entity = next(s for s in stmts if s["recordType"] == "entity")
        assert entity["recordDetails"]["incorporatedInJurisdiction"]["code"] == "CA"

    def test_ca_corp_identifier(self) -> None:
        stmts = list(map_corporations_canada(self._build_bundle()))
        entity = next(s for s in stmts if s["recordType"] == "entity")
        ids = {i["scheme"]: i["id"] for i in entity["recordDetails"]["identifiers"]}
        assert ids["CA-CORP"] == "1007"

    def test_business_number_identifier(self) -> None:
        stmts = list(map_corporations_canada(self._build_bundle()))
        entity = next(s for s in stmts if s["recordType"] == "entity")
        ids = {i["scheme"]: i["id"] for i in entity["recordDetails"]["identifiers"]}
        assert ids["CA-BN"] == "106679285"

    def test_founding_date(self) -> None:
        stmts = list(map_corporations_canada(self._build_bundle()))
        entity = next(s for s in stmts if s["recordType"] == "entity")
        assert entity["recordDetails"]["foundingDate"] == "1947-01-10"

    def test_address(self) -> None:
        stmts = list(map_corporations_canada(self._build_bundle()))
        entity = next(s for s in stmts if s["recordType"] == "entity")
        addrs = entity["recordDetails"].get("addresses") or []
        assert any("ABBOTSFORD" in a.get("address", "") for a in addrs)

    def test_no_directors_no_person_stmts(self) -> None:
        stmts = list(map_corporations_canada(self._build_bundle(directors=[])))
        person_stmts = [s for s in stmts if s["recordType"] == "person"]
        assert person_stmts == []

    def test_directors_produce_person_stmts(self) -> None:
        bundle = self._build_bundle(directors=DIRECTORS_1007["_embedded"]["directors"])
        stmts = list(map_corporations_canada(bundle))
        person_stmts = [s for s in stmts if s["recordType"] == "person"]
        assert len(person_stmts) == 2
        names = {p["recordDetails"]["names"][0]["fullName"] for p in person_stmts}
        assert "ALICE SMITH" in names
        assert "BOB JONES" in names

    def test_directors_produce_relationship_stmts(self) -> None:
        bundle = self._build_bundle(directors=DIRECTORS_1007["_embedded"]["directors"])
        stmts = list(map_corporations_canada(bundle))
        rel_stmts = [s for s in stmts if s["recordType"] == "relationship"]
        assert len(rel_stmts) == 2
        for rel in rel_stmts:
            assert rel["recordDetails"]["interests"][0]["type"] == "seniorManagingOfficial"

    def test_relationships_reference_entity(self) -> None:
        bundle = self._build_bundle(directors=DIRECTORS_1007["_embedded"]["directors"])
        stmts = list(map_corporations_canada(bundle))
        entity_id = next(s["statementId"] for s in stmts if s["recordType"] == "entity")
        for rel in (s for s in stmts if s["recordType"] == "relationship"):
            assert rel["recordDetails"]["subject"] == entity_id

    def test_all_statements_have_required_fields(self) -> None:
        bundle = self._build_bundle(directors=DIRECTORS_1007["_embedded"]["directors"])
        for stmt in map_corporations_canada(bundle):
            assert "statementId" in stmt
            assert "recordType" in stmt
            assert "recordDetails" in stmt
            assert "source" in stmt
            assert stmt["source"]["type"] == ["officialRegister"]

    def test_deterministic_ids(self) -> None:
        bundle1 = self._build_bundle(directors=DIRECTORS_1007["_embedded"]["directors"])
        bundle2 = self._build_bundle(directors=DIRECTORS_1007["_embedded"]["directors"])
        ids1 = [s["statementId"] for s in map_corporations_canada(bundle1)]
        ids2 = [s["statementId"] for s in map_corporations_canada(bundle2)]
        assert ids1 == ids2


# ---------------------------------------------------------------------------
# Adapter: fetch (unit — mocked HTTP)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_returns_bundle(monkeypatch, tmp_path) -> None:
    """fetch() with a successful HTTP response builds a non-stub bundle."""
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("CORPORATIONS_CANADA_API_KEY", "test-key")
    get_settings.cache_clear()

    mock_cache = MagicMock()
    mock_cache.get_payload.return_value = None
    mock_cache.has.return_value = False
    mock_cache.put.return_value = None

    # V1 response: [corpObject]
    v1_resp = MagicMock()
    v1_resp.is_success = True
    v1_resp.json.return_value = [CORP_1007]

    # V2 response: directors payload
    v2_resp = MagicMock()
    v2_resp.is_success = True
    v2_resp.status_code = 200
    v2_resp.json.return_value = DIRECTORS_1007

    call_urls: list[str] = []

    async def mock_get(url: str, **kwargs: Any) -> MagicMock:
        call_urls.append(url)
        if "/v1/corporations/" in url and "/directors" not in url:
            return v1_resp
        return v2_resp

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.get = mock_get

    with (
        patch("opencheck.sources.corporations_canada.Cache", return_value=mock_cache),
        patch("opencheck.sources.corporations_canada.build_client", return_value=mock_client),
    ):
        adapter = CorporationsCanadaAdapter()
        bundle = await adapter.fetch("1007", legal_name="Abbotsford Chamber of Commerce")

    get_settings.cache_clear()

    assert bundle["is_stub"] is False
    assert bundle["corp_id"] == "1007"
    assert bundle["corporation"] == CORP_1007
    assert len(bundle["directors"]) == 2
    assert len(call_urls) == 2


@pytest.mark.asyncio
async def test_fetch_not_found_returns_stub(monkeypatch, tmp_path) -> None:
    """fetch() returns a stub when the API returns the not-found array."""
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("CORPORATIONS_CANADA_API_KEY", "test-key")
    get_settings.cache_clear()

    mock_cache = MagicMock()
    mock_cache.get_payload.return_value = None
    mock_cache.has.return_value = False
    mock_cache.put.return_value = None

    v1_resp = MagicMock()
    v1_resp.is_success = True
    v1_resp.json.return_value = CORP_NOT_FOUND_RESPONSE

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.get = AsyncMock(return_value=v1_resp)

    with (
        patch("opencheck.sources.corporations_canada.Cache", return_value=mock_cache),
        patch("opencheck.sources.corporations_canada.build_client", return_value=mock_client),
    ):
        adapter = CorporationsCanadaAdapter()
        bundle = await adapter.fetch("999999")

    get_settings.cache_clear()

    assert bundle["is_stub"] is True
    assert bundle["corporation"] is None


@pytest.mark.asyncio
async def test_fetch_no_live_returns_stub(monkeypatch, tmp_path) -> None:
    """fetch() returns a stub when live is disabled and cache is empty."""
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()

    mock_cache = MagicMock()
    mock_cache.get_payload.return_value = None
    mock_cache.has.return_value = False

    with patch("opencheck.sources.corporations_canada.Cache", return_value=mock_cache):
        adapter = CorporationsCanadaAdapter()
        bundle = await adapter.fetch("1007", legal_name="Test Corp")

    get_settings.cache_clear()

    assert bundle["is_stub"] is True
    assert bundle["corporation"] is None


@pytest.mark.asyncio
async def test_search_returns_stub(monkeypatch, tmp_path) -> None:
    """search() returns a single stub hit (lookup-only — no live name search)."""
    from opencheck.config import get_settings
    from opencheck.sources.base import SearchKind

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("CORPORATIONS_CANADA_API_KEY", "test-key")
    get_settings.cache_clear()

    adapter = CorporationsCanadaAdapter()
    hits = await adapter.search("Abbotsford Chamber", SearchKind.ENTITY)
    assert len(hits) == 1
    assert hits[0].is_stub is True
    assert hits[0].source_id == "corporations_canada"
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_search_person_returns_empty(monkeypatch, tmp_path) -> None:
    """search() for a person always returns empty (entity-only adapter)."""
    from opencheck.config import get_settings
    from opencheck.sources.base import SearchKind

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()

    adapter = CorporationsCanadaAdapter()
    hits = await adapter.search("Alice Smith", SearchKind.PERSON)
    assert hits == []
    get_settings.cache_clear()
