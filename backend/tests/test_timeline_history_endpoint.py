"""Tests for the /history (Time Machine) service + endpoint.

GLEIF and Companies House HTTP are mocked with respx; the route function is
called directly so the app lifespan (warm-ups) is not involved. Morrisons shapes
(LEI 213800IN6LSRGTZSOS29 / company 00358949).
"""

from __future__ import annotations

import re

import pytest
import respx
from fastapi import HTTPException
from httpx import Response

from opencheck.config import get_settings
from opencheck.routers.history import history

_LEI = "213800IN6LSRGTZSOS29"
_LEI_PREFIX = "/lei:LEIData/lei:LEIRecords/lei:LEIRecord/"
_RR_PREFIX = "/rr:RelationshipData/rr:RelationshipRecords/rr:RelationshipRecord/"

_GLEIF_RECORD = {
    "data": {
        "attributes": {
            "entity": {
                "legalName": {"name": "WM MORRISON SUPERMARKETS LIMITED"},
                "registeredAs": "00358949",
                "registeredAt": {"id": "RA000585"},
                "jurisdiction": "GB",
            }
        }
    }
}


def _mod(attrs: dict) -> dict:
    return {"type": "field-modifications", "id": "x", "attributes": attrs}


_GLEIF_MODS = {
    "data": [
        _mod({
            "lei": _LEI, "recordType": "LEI", "modificationType": "UPDATE",
            "field": _LEI_PREFIX + "lei:Entity/lei:LegalName",
            "date": "2021-12-09T16:00:00Z",
            "valueOld": "WM MORRISON SUPERMARKETS P L C",
            "valueNew": "WM MORRISON SUPERMARKETS LIMITED",
        }),
        _mod({
            "lei": _LEI, "recordType": "LEI", "modificationType": "UPDATE",
            "field": _LEI_PREFIX + "lei:Registration/lei:NextRenewalDate",
            "date": "2025-11-20T00:00:00Z",
            "valueOld": "2026-01-11T00:00:00Z", "valueNew": "2027-01-11T00:00:00Z",
        }),
        _mod({
            "lei": _LEI, "recordType": "RR", "modificationType": "INITIAL",
            "field": _RR_PREFIX + "rr:Relationship/rr:RelationshipType",
            "date": "2023-11-25T00:00:00Z", "valueOld": None,
            "valueNew": "IS_DIRECTLY_CONSOLIDATED_BY",
            "context": {"relationshipType": "IS_DIRECTLY_CONSOLIDATED_BY",
                        "endNode": "549300RKU7UEPSC42U63"},
        }),
        _mod({
            "lei": _LEI, "recordType": "RR", "modificationType": "INITIAL",
            "field": _RR_PREFIX
            + "rr:Relationship/rr:RelationshipPeriods/rr:RelationshipPeriod/rr:StartDate",
            "date": "2023-11-25T00:00:00Z", "valueOld": None,
            "valueNew": "2021-11-01T00:00:00Z",
            "context": {"relationshipType": "IS_DIRECTLY_CONSOLIDATED_BY",
                        "endNode": "549300RKU7UEPSC42U63"},
        }),
    ],
    "meta": {"pagination": {"lastPage": 1}},
}

_CH_FILINGS = {
    "items": [
        {"category": "change-of-name", "type": "CONNOT", "date": "2022-01-05",
         "action_date": "2021-12-01", "links": {"self": "/company/00358949/filing-history/a"}},
        {"category": "persons-with-significant-control", "type": "PSC02",
         "date": "2021-11-15", "links": {"self": "/company/00358949/filing-history/b"}},
        {"category": "confirmation-statement", "type": "CS01", "date": "2022-03-01",
         "links": {"self": "/company/00358949/filing-history/c"}},
    ],
    "total_count": 3,
}


def _mock_live():
    respx.get(f"https://api.gleif.org/api/v1/lei-records/{_LEI}").mock(
        return_value=Response(200, json=_GLEIF_RECORD)
    )
    respx.get(url__regex=rf"https://api\.gleif\.org/api/v1/lei-records/{_LEI}/field-modifications").mock(
        return_value=Response(200, json=_GLEIF_MODS)
    )
    respx.get(url__regex=r"https://api\.company-information\.service\.gov\.uk/company/00358949/filing-history").mock(
        return_value=Response(200, json=_CH_FILINGS)
    )


@pytest.mark.asyncio
async def test_history_invalid_lei_returns_400():
    with pytest.raises(HTTPException) as exc:
        await history(lei="not-a-lei", include_noise=False)
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_history_stub_mode_is_unavailable(monkeypatch):
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    get_settings.cache_clear()
    resp = await history(lei=_LEI, include_noise=False)
    get_settings.cache_clear()
    assert resp.available is False
    assert resp.notable == []


@pytest.mark.asyncio
async def test_history_live_merges_gleif_and_companies_house(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    # The dedicated history key is used (separate from the lookup adapter's key).
    monkeypatch.setenv("COMPANIES_HOUSE_HISTORY_API_KEY", "test-history-key")
    get_settings.cache_clear()
    with respx.mock:
        _mock_live()
        resp = await history(lei=_LEI, include_noise=True)
    get_settings.cache_clear()

    assert resp.available is True
    assert resp.company_number == "00358949"
    assert set(resp.sources) == {"gleif", "companies_house"}

    # Name change corroborated across both sources, CH (effective) date wins.
    name = [e for e in resp.notable if e.change_type == "LEGAL_NAME_CHANGE"]
    assert len(name) == 1
    assert name[0].sources == ["companies_house", "gleif"]
    assert name[0].date == "2021-12-01"
    assert name[0].date_basis == "effective"

    # GLEIF corporate-parent add carries the period start + parent LEI.
    gleif_owner = [e for e in resp.notable
                   if e.change_type == "OWNER_ADDED" and "gleif" in e.sources]
    assert len(gleif_owner) == 1
    assert gleif_owner[0].interest_start_date == "2021-11-01"
    assert gleif_owner[0].counterparty == "549300RKU7UEPSC42U63"

    # CH PSC add is a separate ownership entry.
    ch_owner = [e for e in resp.notable
                if e.change_type == "OWNER_ADDED" and "companies_house" in e.sources]
    assert len(ch_owner) == 1

    # include_noise exposed the Tier-3 rows (NextRenewalDate, CS01).
    assert any(ev.tier == 3 for ev in resp.events)
    assert resp.notable_count == len(resp.notable)


@pytest.mark.asyncio
async def test_history_degrades_to_gleif_only_without_ch_key(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.delenv("COMPANIES_HOUSE_API_KEY", raising=False)
    monkeypatch.delenv("COMPANIES_HOUSE_HISTORY_API_KEY", raising=False)
    get_settings.cache_clear()
    with respx.mock:
        _mock_live()  # CH route present but should never be called
        resp = await history(lei=_LEI, include_noise=False)
    get_settings.cache_clear()

    assert resp.available is True
    assert resp.sources == ["gleif"]  # Companies House not contacted
    assert any(e.change_type == "LEGAL_NAME_CHANGE" for e in resp.notable)
