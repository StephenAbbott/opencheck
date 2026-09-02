"""Tests for the India MCA Company Master Data adapter + mapper.

All HTTP is mocked via respx; settings are patched to force live mode (key
present + allow_live). Fixtures mirror live OGD Platform API responses
captured against ``api.data.gov.in`` on 2026-08-08 (resource updated_date
2026-07-22), trimmed to the envelope keys the adapter reads.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import respx
from httpx import Response

from opencheck.bods.mapper import map_mca_india
from opencheck.sources.base import SearchKind
from opencheck.sources.mca_india import (
    MCA_RA_CODE,
    McaIndiaAdapter,
    looks_like_cin,
    normalise_cin,
)
from opencheck.sources.schemas import SourceSchemaError, validate_raw
from opencheck.sources.schemas.mca_india import MCABundle

# ---------------------------------------------------------------------------
# Fixtures — modelled on live api.data.gov.in responses (captured 2026-08-08)
# ---------------------------------------------------------------------------

INFOSYS_RECORD = {
    "CIN": "L85110KA1981PLC013115",
    "CompanyName": "INFOSYS LIMITED",
    "CompanyROCcode": "ROC Bangalore",
    "CompanyCategory": "Company limited by shares",
    "CompanySubCategory": "Non-government company",
    "CompanyClass": "Public",
    "AuthorizedCapital": "24000000000.00",
    "PaidupCapital": "20278293815.00",
    "CompanyRegistrationdate_date": "1981-07-02",
    "Registered_Office_Address": (
        "ELECTRONICS CITY,HOSUR ROAD,   BANGALORE,KARNATAKA,Karnataka,560100-India"
    ),
    "Listingstatus": "Listed",
    "CompanyStatus": "Active",
    "CompanyStateCode": "karnataka",
    "CompanyIndian/Foreign Company": "India",
    "nic_code": "85110",
    "CompanyIndustrialClassification": "Community, personal and Social Services",
}

CIN_HIT_RESPONSE = {
    "index_name": "4dbe5667-7b6b-41d7-82af-211562424d9a",
    "title": "Registrars of Companies (RoC)-wise Company Master Data",
    "status": "ok",
    "total": 1,
    "count": 1,
    "limit": "1",
    "offset": "0",
    "records": [INFOSYS_RECORD],
}

CIN_MISS_RESPONSE = {
    "index_name": "4dbe5667-7b6b-41d7-82af-211562424d9a",
    "title": "Registrars of Companies (RoC)-wise Company Master Data",
    "status": "ok",
    "total": 0,
    "count": 0,
    "limit": "1",
    "offset": "0",
    "records": [],
}

RATE_LIMIT_RESPONSE = {"error": "Rate limit exceeded"}

_API_PREFIX = "https://api.data.gov.in/resource/4dbe5667-7b6b-41d7-82af-211562424d9a"


def _live_settings(*_a, **_k):
    return SimpleNamespace(allow_live=True, data_gov_in_api_key="test-key-1234")


def _stub_settings(*_a, **_k):
    return SimpleNamespace(allow_live=False, data_gov_in_api_key=None)


@pytest.fixture
def adapter():
    return McaIndiaAdapter()


# ---------------------------------------------------------------------------
# Unit helpers
# ---------------------------------------------------------------------------

def test_ra_code():
    assert MCA_RA_CODE == "RA000394"


def test_normalise_cin_strips_and_uppercases():
    assert normalise_cin(" l85110ka1981plc013115 ") == "L85110KA1981PLC013115"
    assert normalise_cin("L85110-KA-1981-PLC-013115") == "L85110KA1981PLC013115"


def test_normalise_cin_raises_on_empty():
    with pytest.raises(ValueError):
        normalise_cin("")
    with pytest.raises(ValueError):
        normalise_cin("---")


def test_looks_like_cin():
    assert looks_like_cin("L85110KA1981PLC013115")
    assert looks_like_cin("U74999DL2016PTC298850")
    assert not looks_like_cin("AAB-1234")        # LLPIN shape
    assert not looks_like_cin("33000167000101")  # CNPJ digits


def test_requires_api_key(adapter, monkeypatch):
    monkeypatch.setattr("opencheck.sources.mca_india.get_settings", _stub_settings)
    info = adapter.info
    assert info.requires_api_key
    assert not info.live_available
    assert info.country == "IN"
    assert info.is_national_register
    assert info.license == "GODL-India"


def test_lookup_deriver_declared(adapter):
    assert adapter.lookup_keys() == ("in_cin",)
    (deriver,) = adapter.lookup_derivers
    assert deriver.ra_codes == frozenset({MCA_RA_CODE})
    assert deriver.normalise("l85110ka1981plc013115") == "L85110KA1981PLC013115"


def test_lookup_hit_builder_wired():
    # lookup.py enforces this pairing at import time; assert it directly too.
    from opencheck.routers.lookup import _bh_mca_india  # noqa: PLC0415

    assert callable(_bh_mca_india)


# ---------------------------------------------------------------------------
# Fetch (HTTP mocked)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_by_cin(adapter, monkeypatch):
    monkeypatch.setattr("opencheck.sources.mca_india.get_settings", _live_settings)
    with respx.mock:
        route = respx.get(url__startswith=_API_PREFIX).mock(
            return_value=Response(200, json=CIN_HIT_RESPONSE)
        )
        bundle = await adapter.fetch("L85110KA1981PLC013115")
    assert route.called
    sent = route.calls[0].request.url
    assert "filters%5BCIN%5D=L85110KA1981PLC013115" in str(sent)
    assert bundle["is_stub"] is False
    assert bundle["cin"] == "L85110KA1981PLC013115"
    assert bundle["name"] == "INFOSYS LIMITED"
    assert bundle["status"] == "Active"
    assert bundle["company_class"] == "Public"
    assert bundle["category"] == "Company limited by shares"
    assert bundle["registration_date"] == "1981-07-02"
    assert bundle["roc_code"] == "ROC Bangalore"
    assert bundle["listing_status"] == "Listed"
    assert bundle["nic_code"] == "85110"
    assert "BANGALORE" in bundle["address"]


@pytest.mark.asyncio
async def test_fetch_normalises_before_query(adapter, monkeypatch):
    monkeypatch.setattr("opencheck.sources.mca_india.get_settings", _live_settings)
    with respx.mock:
        route = respx.get(url__startswith=_API_PREFIX).mock(
            return_value=Response(200, json=CIN_HIT_RESPONSE)
        )
        await adapter.fetch(" l85110ka1981plc013115 ")
    assert "filters%5BCIN%5D=L85110KA1981PLC013115" in str(route.calls[0].request.url)


@pytest.mark.asyncio
async def test_fetch_miss_returns_stub(adapter, monkeypatch):
    monkeypatch.setattr("opencheck.sources.mca_india.get_settings", _live_settings)
    with respx.mock:
        respx.get(url__startswith=_API_PREFIX).mock(
            return_value=Response(200, json=CIN_MISS_RESPONSE)
        )
        bundle = await adapter.fetch("U99999ZZ2099PLC999999", legal_name="Ghost Ltd")
    assert bundle["is_stub"] is True
    assert bundle["cin"] == "U99999ZZ2099PLC999999"
    assert bundle["name"] == "Ghost Ltd"


@pytest.mark.asyncio
async def test_fetch_rate_limited_returns_stub(adapter, monkeypatch):
    # The OGD platform answers HTTP 429 when a key's quota is exhausted —
    # notably the globally shared documented sample key.
    monkeypatch.setattr("opencheck.sources.mca_india.get_settings", _live_settings)
    with respx.mock:
        respx.get(url__startswith=_API_PREFIX).mock(
            return_value=Response(429, json=RATE_LIMIT_RESPONSE)
        )
        bundle = await adapter.fetch("L85110KA1981PLC013115", legal_name="Infosys")
    assert bundle["is_stub"] is True
    assert bundle["name"] == "Infosys"


@pytest.mark.asyncio
async def test_fetch_without_key_is_stub_no_http(adapter, monkeypatch):
    monkeypatch.setattr("opencheck.sources.mca_india.get_settings", _stub_settings)
    with respx.mock:  # no routes: any HTTP call would raise
        bundle = await adapter.fetch("L85110KA1981PLC013115", legal_name="Infosys")
    assert bundle["is_stub"] is True
    assert bundle["cin"] == "L85110KA1981PLC013115"
    assert bundle["name"] == "Infosys"


@pytest.mark.asyncio
async def test_fetch_garbage_identifier_is_stub(adapter, monkeypatch):
    monkeypatch.setattr("opencheck.sources.mca_india.get_settings", _live_settings)
    bundle = await adapter.fetch("///")
    assert bundle["is_stub"] is True


# ---------------------------------------------------------------------------
# Search (exact name)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_exact_name(adapter, monkeypatch):
    monkeypatch.setattr("opencheck.sources.mca_india.get_settings", _live_settings)
    with respx.mock:
        route = respx.get(url__startswith=_API_PREFIX).mock(
            return_value=Response(200, json=CIN_HIT_RESPONSE)
        )
        hits = await adapter.search("Infosys  Limited", SearchKind.ENTITY)
    # Query is uppercased + whitespace-collapsed (fields are exact keyword).
    assert "filters%5BCompanyName%5D=INFOSYS+LIMITED" in str(route.calls[0].request.url)
    assert len(hits) == 1
    hit = hits[0]
    assert hit.hit_id == "L85110KA1981PLC013115"
    assert hit.identifiers == {"in_cin": "L85110KA1981PLC013115"}
    assert "IN-CIN L85110KA1981PLC013115" in hit.summary
    assert "ROC Bangalore" in hit.summary
    assert hit.is_stub is False


@pytest.mark.asyncio
async def test_search_person_kind_empty(adapter, monkeypatch):
    monkeypatch.setattr("opencheck.sources.mca_india.get_settings", _live_settings)
    assert await adapter.search("Anyone", SearchKind.PERSON) == []


@pytest.mark.asyncio
async def test_search_stub_mode(adapter, monkeypatch):
    monkeypatch.setattr("opencheck.sources.mca_india.get_settings", _stub_settings)
    hits = await adapter.search("Infosys", SearchKind.ENTITY)
    assert len(hits) == 1
    assert hits[0].is_stub is True


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def test_schema_requires_cin():
    with pytest.raises(SourceSchemaError):
        validate_raw("mca_india", MCABundle, {"name": "NO CIN LTD"})


def test_schema_accepts_full_bundle():
    validate_raw(
        "mca_india",
        MCABundle,
        {"cin": "L85110KA1981PLC013115", "name": "INFOSYS LIMITED", "extra_field": 1},
    )


# ---------------------------------------------------------------------------
# BODS mapper
# ---------------------------------------------------------------------------

def _bundle(**overrides):
    base = {
        "source_id": "mca_india",
        "cin": "L85110KA1981PLC013115",
        "name": "INFOSYS LIMITED",
        "status": "Active",
        "category": "Company limited by shares",
        "sub_category": "Non-government company",
        "company_class": "Public",
        "listing_status": "Listed",
        "authorized_capital": "24000000000.00",
        "paidup_capital": "20278293815.00",
        "registration_date": "1981-07-02",
        "address": "ELECTRONICS CITY,HOSUR ROAD, BANGALORE,KARNATAKA,Karnataka,560100-India",
        "state_code": "karnataka",
        "roc_code": "ROC Bangalore",
        "indian_foreign": "India",
        "nic_code": "85110",
        "industrial_classification": "Community, personal and Social Services",
        "link": "https://www.data.gov.in/resource/registrars-companies-roc-wise-company-master-data",
        "is_stub": False,
    }
    base.update(overrides)
    return base


def test_map_entity_statement():
    (stmt,) = list(map_mca_india(_bundle()))
    assert stmt["recordType"] == "entity"
    details = stmt["recordDetails"]
    assert details["name"] == "INFOSYS LIMITED"
    assert details["jurisdiction"] == {"name": "India", "code": "IN"}
    assert details["foundingDate"] == "1981-07-02"
    assert details["identifiers"] == [{
        "id": "L85110KA1981PLC013115",
        "scheme": "IN-MCA",
        "schemeName": "Corporate Identification Number (CIN) — Ministry of Corporate Affairs",
    }]
    (addr,) = details["addresses"]
    assert addr["country"] == {"code": "IN", "name": "India"}
    assert "BANGALORE" in addr["address"]
    assert details["entityType"]["subtype"] == "Public"
    assert details["entityType"]["details"] == (
        "Company limited by shares — Non-government company"
    )
    assert "dissolutionDate" not in details


def test_map_terminal_status_sets_dissolution():
    """MCA publishes no date, so the terminal status is a liveness annotation
    and ``dissolutionDate`` is NOT set (Phase 151 — it used to be the literal
    "unknown", which the BODS schema forbids)."""
    from opencheck.bods.liveness import read_register_status

    (stmt,) = list(map_mca_india(_bundle(status="Strike Off")))
    assert "dissolutionDate" not in stmt["recordDetails"]
    status = read_register_status(stmt)
    assert status is not None
    assert status["liveness"] == "terminal"
    assert status["raw"] == "Strike Off"
    assert status["since"] is None


def test_map_in_progress_status_is_not_dissolution():
    from opencheck.bods.liveness import read_register_status

    (stmt,) = list(map_mca_india(_bundle(status="Under Process of Striking Off")))
    assert "dissolutionDate" not in stmt["recordDetails"]
    assert read_register_status(stmt)["liveness"] == "pending"
    (stmt,) = list(map_mca_india(_bundle(status="Dormant")))
    assert "dissolutionDate" not in stmt["recordDetails"]
    assert read_register_status(stmt)["liveness"] == "live"


def test_map_skips_stub_and_incomplete():
    assert list(map_mca_india({})) == []
    assert list(map_mca_india(_bundle(is_stub=True))) == []
    assert list(map_mca_india(_bundle(name=""))) == []
    assert list(map_mca_india(_bundle(cin=""))) == []
