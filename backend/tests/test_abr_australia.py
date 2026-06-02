"""Tests for the Australian Business Register (ABN Lookup) adapter + mapper.

The ABR JSON endpoints return JSONP (``callback({...})``). All HTTP is mocked
via respx; settings are patched to force live mode (GUID present + allow_live).
Fixtures mirror the live AbnDetails/AcnDetails/MatchingNames payload shapes
observed against the public endpoints.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import respx
from httpx import Response

from opencheck.sources.abr_australia import (
    AbrAustraliaAdapter,
    ABR_ASIC_RA_CODE,
    ABR_ABR_RA_CODE,
    normalise_abn,
    normalise_acn,
    _unwrap_jsonp,
)
from opencheck.sources.base import SearchKind
from opencheck.bods.mapper import map_abr_australia


# ---------------------------------------------------------------------------
# Fixtures — JSONP bodies modelled on live ABR responses
# ---------------------------------------------------------------------------

ABN_DETAILS_JSONP = (
    'callback({"Abn":"74172177893","AbnStatus":"Active",'
    '"AbnStatusEffectiveFrom":"2017-09-27","Acn":"172177893","AddressDate":"2017-09-27",'
    '"AddressPostcode":"2600","AddressState":"ACT","BusinessName":["Digital Transformation Agency"],'
    '"EntityName":"DIGITAL TRANSFORMATION AGENCY","EntityTypeCode":"CGE",'
    '"EntityTypeName":"Commonwealth Government Entity","Gst":"2017-09-27","Message":""})'
)

ACN_DETAILS_JSONP = (
    'callback({"Abn":"74172177893","AbnStatus":"Active",'
    '"AbnStatusEffectiveFrom":"2017-09-27","Acn":"172177893","AddressDate":null,'
    '"AddressPostcode":"2600","AddressState":"ACT","BusinessName":[],'
    '"EntityName":"DIGITAL TRANSFORMATION AGENCY","EntityTypeCode":"CGE",'
    '"EntityTypeName":"Commonwealth Government Entity","Gst":"2017-09-27","Message":""})'
)

MATCHING_NAMES_JSONP = (
    'callback({"Message":"","Names":[{"Abn":"74172177893","AbnStatus":"Active",'
    '"IsCurrent":true,"Name":"DIGITAL TRANSFORMATION AGENCY","NameType":"Entity Name",'
    '"Score":100,"State":"ACT","Postcode":"2600"}]})'
)

ERROR_JSONP = 'callback({"Abn":"","Acn":"","EntityName":"","BusinessName":[],"Message":"The GUID entered is not recognised as a Registered Party"})'


def _live_settings(*_a, **_k):
    return SimpleNamespace(allow_live=True, abn_guid="test-guid-1234")


@pytest.fixture
def adapter():
    return AbrAustraliaAdapter()


# ---------------------------------------------------------------------------
# Unit helpers
# ---------------------------------------------------------------------------

def test_ra_codes():
    assert ABR_ASIC_RA_CODE == "RA000014"
    assert ABR_ABR_RA_CODE == "RA000013"


def test_normalisers_strip_spaces():
    assert normalise_acn("676 964 677") == "676964677"
    assert normalise_abn("31 976 733 718") == "31976733718"


def test_unwrap_jsonp():
    assert _unwrap_jsonp('callback({"a":1})') == {"a": 1}
    assert _unwrap_jsonp('cb({"x":"y"});') == {"x": "y"}
    assert _unwrap_jsonp("not json") == {}


def test_requires_api_key(adapter):
    assert adapter.info.requires_api_key


# ---------------------------------------------------------------------------
# Fetch (HTTP mocked)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_by_abn(adapter, monkeypatch):
    monkeypatch.setattr("opencheck.sources.abr_australia.get_settings", _live_settings)
    with respx.mock:
        respx.get(url__startswith="https://abr.business.gov.au/json/AbnDetails.aspx").mock(
            return_value=Response(200, text=ABN_DETAILS_JSONP, headers={"content-type": "text/javascript"})
        )
        bundle = await adapter.fetch("31976733718", legal_name="Fallback")
    assert bundle["is_stub"] is False
    assert bundle["abn"] == "74172177893"
    assert bundle["acn"] == "172177893"
    assert bundle["name"] == "DIGITAL TRANSFORMATION AGENCY"
    assert bundle["entity_type_name"] == "Commonwealth Government Entity"
    assert bundle["state"] == "ACT"
    assert bundle["business_names"] == ["Digital Transformation Agency"]


@pytest.mark.asyncio
async def test_fetch_by_acn_routes_to_acndetails(adapter, monkeypatch):
    monkeypatch.setattr("opencheck.sources.abr_australia.get_settings", _live_settings)
    with respx.mock:
        route = respx.get(url__startswith="https://abr.business.gov.au/json/AcnDetails.aspx").mock(
            return_value=Response(200, text=ACN_DETAILS_JSONP, headers={"content-type": "text/javascript"})
        )
        bundle = await adapter.fetch("172177893")  # 9 digits → ACN endpoint
    assert route.called
    assert bundle["is_stub"] is False
    assert bundle["abn"] == "74172177893"


@pytest.mark.asyncio
async def test_fetch_stub_on_error_message(adapter, monkeypatch):
    monkeypatch.setattr("opencheck.sources.abr_australia.get_settings", _live_settings)
    with respx.mock:
        respx.get(url__startswith="https://abr.business.gov.au/json/AbnDetails.aspx").mock(
            return_value=Response(200, text=ERROR_JSONP, headers={"content-type": "text/javascript"})
        )
        bundle = await adapter.fetch("31976733718", legal_name="Fallback")
    assert bundle["is_stub"] is True


@pytest.mark.asyncio
async def test_fetch_stub_when_not_live(adapter, monkeypatch):
    monkeypatch.setattr(
        "opencheck.sources.abr_australia.get_settings",
        lambda *a, **k: SimpleNamespace(allow_live=False, abn_guid=None),
    )
    bundle = await adapter.fetch("31976733718")
    assert bundle["is_stub"] is True


@pytest.mark.asyncio
async def test_fetch_stub_on_bad_length(adapter, monkeypatch):
    monkeypatch.setattr("opencheck.sources.abr_australia.get_settings", _live_settings)
    bundle = await adapter.fetch("123")  # neither 9 nor 11 digits
    assert bundle["is_stub"] is True


@pytest.mark.asyncio
async def test_search_by_name(adapter, monkeypatch):
    monkeypatch.setattr("opencheck.sources.abr_australia.get_settings", _live_settings)
    with respx.mock:
        respx.get(url__startswith="https://abr.business.gov.au/json/MatchingNames.aspx").mock(
            return_value=Response(200, text=MATCHING_NAMES_JSONP, headers={"content-type": "text/javascript"})
        )
        hits = await adapter.search("digital transformation", SearchKind.ENTITY)
    assert len(hits) == 1
    assert hits[0].is_stub is False
    assert hits[0].identifiers["au_abn"] == "74172177893"


@pytest.mark.asyncio
async def test_search_returns_stub_when_not_live(adapter, monkeypatch):
    monkeypatch.setattr(
        "opencheck.sources.abr_australia.get_settings",
        lambda *a, **k: SimpleNamespace(allow_live=False, abn_guid=None),
    )
    hits = await adapter.search("anything", SearchKind.ENTITY)
    assert hits and hits[0].is_stub is True


# ---------------------------------------------------------------------------
# BODS mapper
# ---------------------------------------------------------------------------

def _bundle():
    return {
        "source_id": "abr_australia",
        "abn": "74172177893",
        "acn": "172177893",
        "name": "DIGITAL TRANSFORMATION AGENCY",
        "entity_type_code": "CGE",
        "entity_type_name": "Commonwealth Government Entity",
        "abn_status": "Active",
        "abn_status_from": "2017-09-27",
        "state": "ACT",
        "postcode": "2600",
        "gst": "2017-09-27",
        "business_names": ["Digital Transformation Agency"],
        "link": "https://abr.business.gov.au/ABN/View?abn=74172177893",
        "is_stub": False,
    }


def test_mapper_emits_single_entity_statement():
    statements = list(map_abr_australia(_bundle()))
    assert len(statements) == 1
    stmt = statements[0]
    assert stmt["recordType"] == "entity"
    rd = stmt["recordDetails"]
    assert rd["name"] == "DIGITAL TRANSFORMATION AGENCY"
    assert rd["jurisdiction"]["code"] == "AU"
    schemes = {i["scheme"]: i["id"] for i in rd["identifiers"]}
    assert schemes == {"AU-ABN": "74172177893", "AU-ACN": "172177893"}
    assert rd["alternateNames"] == ["Digital Transformation Agency"]
    assert rd["entityType"]["subtype"] == "Commonwealth Government Entity"


def test_mapper_marks_cancelled_status():
    b = _bundle()
    b["abn_status"] = "Cancelled"
    b["abn_status_from"] = "2020-01-01"
    stmt = next(iter(map_abr_australia(b)))
    assert stmt["recordDetails"]["dissolutionDate"] == "2020-01-01"


def test_mapper_skips_stub():
    assert list(map_abr_australia({"is_stub": True})) == []
