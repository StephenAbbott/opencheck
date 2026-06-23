"""Tests for the New Zealand Companies Register (NZBN) adapter and BODS mapper.

Covers the company-number → NZBN resolution, the FullEntity normaliser
(directors, shareholders with share allocations, ultimate holding company), and
the BODS mapping (entity + seniorManagingOfficial + shareholding share.exact +
ultimate-holding-company control). No network — the HTTP client is mocked.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from opencheck.bods import validate_shape
from opencheck.bods.mapper import map_nz_companies
from opencheck.sources.base import SearchKind
from opencheck.sources.nz_companies import (
    NZ_RA_CODE,
    NzCompaniesAdapter,
    normalise_nz_company_number,
)

_NUMBER = "1166320"
_NZBN = "9429000035170"

SEARCH_RESPONSE: dict[str, Any] = {
    "pageSize": 10, "page": 0, "totalItems": 1,
    "items": [{
        "entityName": "FONTERRA CO-OPERATIVE GROUP LIMITED",
        "nzbn": _NZBN,
        "entityTypeCode": "COOP",
        "entityStatusDescription": "Registered",
        "sourceRegisterUniqueId": _NUMBER,
        "registrationDate": "2001-10-16T00:00:00",
    }],
}

FULL_ENTITY: dict[str, Any] = {
    "entityName": "FONTERRA CO-OPERATIVE GROUP LIMITED",
    "nzbn": _NZBN,
    "entityTypeCode": "COOP",
    "entityTypeDescription": "NZ Co-operative Company",
    "entityStatusDescription": "Registered",
    "registrationDate": "2001-10-16T00:00:00",
    "sourceRegisterUniqueIdentifier": _NUMBER,
    "addresses": {"addressList": [
        {"address1": "109 Fanshawe Street", "address4": "Auckland",
         "postCode": "1010", "countryCode": "NZ", "addressType": "REGISTERED"},
    ]},
    "tradingNames": [{"name": "Fonterra"}],
    "previousEntityNames": ["OLD NAME LIMITED"],
    "company-details": {
        "shareholding": {
            "numberOfShares": 1000,
            "shareAllocation": [
                {"allocation": 600, "shareholder": [{
                    "type": "Individual", "appointmentDate": "2010-05-01T00:00:00Z",
                    "individualShareholder": {"firstName": "Jane", "lastName": "Smith",
                                              "fullName": "Jane Smith"},
                }]},
                {"allocation": 400, "shareholder": [{
                    "type": "Corporate",
                    "otherShareholder": {"currentEntityName": "HOLDCO LIMITED",
                                         "nzbn": "9429000099999", "companyNumber": "9999999",
                                         "entityType": "LTD"},
                }]},
            ],
        },
        "ultimateHoldingCompany": {"yn": True, "name": "GLOBAL PARENT LIMITED",
                                   "nzbn": "9429000088888", "number": "8888888", "country": "NZ"},
    },
    "roles": [
        {"roleType": "Director", "roleStatus": "ACTIVE", "startDate": "2015-03-01T00:00:00Z",
         "rolePerson": {"firstName": "John", "lastName": "Doe"}},
        {"roleType": "Director", "roleStatus": "CEASED", "startDate": "2008-01-01T00:00:00Z",
         "endDate": "2014-12-31T00:00:00Z", "rolePerson": {"firstName": "Alice", "lastName": "Brown"}},
    ],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client(route):
    client = AsyncMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)
    client.get = route
    return client


def _resp(status: int, payload: dict | None = None) -> MagicMock:
    m = MagicMock()
    m.status_code = status
    m.is_success = 200 <= status < 300
    m.json.return_value = payload if payload is not None else {}
    return m


def _live(monkeypatch, tmp_path, *, key: str | None = "test-key") -> None:
    from opencheck.config import get_settings
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    if key is None:
        monkeypatch.delenv("NZBN_API_KEY", raising=False)
    else:
        monkeypatch.setenv("NZBN_API_KEY", key)
    get_settings.cache_clear()


def _bundle() -> dict[str, Any]:
    return NzCompaniesAdapter()._normalise(_NUMBER, _NZBN, FULL_ENTITY, legal_name="")


# ---------------------------------------------------------------------------
# Basics
# ---------------------------------------------------------------------------

def test_ra_code() -> None:
    assert NZ_RA_CODE == "RA000466"


def test_normalise_company_number() -> None:
    assert normalise_nz_company_number(" 1166320 ") == "1166320"


@pytest.mark.asyncio
async def test_search_returns_empty() -> None:
    assert await NzCompaniesAdapter().search("Fonterra", SearchKind.ENTITY) == []


# ---------------------------------------------------------------------------
# Fetch — search → resolve NZBN → full entity
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_resolves_and_normalises(monkeypatch, tmp_path) -> None:
    _live(monkeypatch, tmp_path)
    cache = MagicMock(); cache.has.return_value = False
    cache.get_payload.return_value = None; cache.put.return_value = None

    async def route(url, **kw):
        if "search-term" in url:
            return _resp(200, SEARCH_RESPONSE)
        return _resp(200, FULL_ENTITY)

    with (
        patch("opencheck.sources.nz_companies.Cache", return_value=cache),
        patch("opencheck.sources.nz_companies.build_client", return_value=_make_client(route)),
    ):
        bundle = await NzCompaniesAdapter().fetch(" 1166320 ", legal_name="Fonterra")

    assert bundle["is_stub"] is False
    assert bundle["nzbn"] == _NZBN
    assert bundle["company"]["name"].startswith("FONTERRA")
    assert len(bundle["roles"]) == 2
    sh = bundle["shareholders"]
    assert len(sh) == 2
    jane = next(s for s in sh if s["kind"] == "person")
    assert jane["percent"] == 60.0
    holdco = next(s for s in sh if s["kind"] == "entity")
    assert holdco["percent"] == 40.0 and holdco["company_number"] == "9999999"
    assert bundle["ultimate_holding_company"]["name"] == "GLOBAL PARENT LIMITED"


@pytest.mark.asyncio
async def test_fetch_stub_without_key(monkeypatch, tmp_path) -> None:
    _live(monkeypatch, tmp_path, key=None)  # live mode but no NZBN_API_KEY
    cache = MagicMock(); cache.has.return_value = False; cache.get_payload.return_value = None
    with patch("opencheck.sources.nz_companies.Cache", return_value=cache):
        bundle = await NzCompaniesAdapter().fetch(_NUMBER, legal_name="Fonterra")
    assert bundle["is_stub"] is True


@pytest.mark.asyncio
async def test_fetch_unresolved_returns_named_nonstub(monkeypatch, tmp_path) -> None:
    _live(monkeypatch, tmp_path)
    cache = MagicMock(); cache.has.return_value = False
    cache.get_payload.return_value = None; cache.put.return_value = None

    async def route(url, **kw):
        return _resp(200, {"items": []})  # search finds nothing

    with (
        patch("opencheck.sources.nz_companies.Cache", return_value=cache),
        patch("opencheck.sources.nz_companies.build_client", return_value=_make_client(route)),
    ):
        bundle = await NzCompaniesAdapter().fetch(_NUMBER, legal_name="Fonterra")
    assert bundle["is_stub"] is False
    assert bundle["company"]["name"] == "Fonterra"
    assert bundle["nzbn"] == ""


# ---------------------------------------------------------------------------
# BODS mapper
# ---------------------------------------------------------------------------

def test_mapper_produces_valid_ownership_graph() -> None:
    stmts = list(map_nz_companies(_bundle()))
    assert validate_shape(stmts) == []

    entities = [s for s in stmts if s["recordType"] == "entity"]
    persons = [s for s in stmts if s["recordType"] == "person"]
    rels = [s for s in stmts if s["recordType"] == "relationship"]
    # company + HOLDCO + GLOBAL PARENT
    assert len(entities) == 3
    # John, Alice, Jane
    assert len(persons) == 3
    # 2 directors + 2 shareholders + 1 UHC
    assert len(rels) == 5

    company = next(
        e for e in entities
        if "FONTERRA" in (e["recordDetails"]["name"] or "").upper()
    )
    schemes = {i["scheme"] for i in company["recordDetails"]["identifiers"]}
    assert {"NZ-NZBN", "NZ-COH"} <= schemes

    interest_types = [
        i["type"] for r in rels for i in r["recordDetails"]["interests"]
    ]
    assert "seniorManagingOfficial" in interest_types
    assert "otherInfluenceOrControl" in interest_types  # ultimate holding company

    shares = [
        i for r in rels for i in r["recordDetails"]["interests"]
        if i["type"] == "shareholding"
    ]
    assert any(i.get("share", {}).get("exact") == 60.0 for i in shares)
    assert any(i.get("share", {}).get("exact") == 40.0 for i in shares)


def test_mapper_skips_stub() -> None:
    assert list(map_nz_companies({"is_stub": True})) == []
