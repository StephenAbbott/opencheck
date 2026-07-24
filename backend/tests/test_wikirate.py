"""Tests for the Wikirate adapter and BODS mapper.

HTTP is mocked with respx. Fixtures mirror the live API shapes verified
2026-07-07: the nested ``filter[company_identifier[value]]`` param, the
``~{card_id}`` addressing, and bare-integer ``view=count`` responses.
Per the Wikirate team (2026-07-24), the answers list now queries
``filter[metric_type][]=researched&sort_by=year&sort_dir=desc``.
"""

from __future__ import annotations

import httpx
import pytest
import respx

import opencheck.sources.wikirate as wikirate_mod
from opencheck.bods import map_wikirate
from opencheck.sources.wikirate import WikirateAdapter

BASE = "https://wikirate.org"
LEI = "213800LH1BZH3DI6G760"
QID = "Q152057"

COMPANY_ITEM = {
    "id": 637,
    "name": "BP plc.",
    "type": "Company",
    "url": "https://wikirate.org/BP_plc.json",
    "headquarters": "United Kingdom",
    "website": "https://www.bp.com/",
    "legal_entity_identifier": LEI,
    "wikidata_id": QID,
    "open_corporates_id": "00102498",
    "uk_company_number": None,
    "sec_central_index_key": "313807",
    "australian_business_number": None,
    "international_securities_identification_number": ["GB0007980591"],
}

ANSWER_ITEMS = {
    "items": [
        {
            "id": 20885480,
            "name": "Net Zero Tracker+Accountability+BP plc.+2024",
            "metric": "Net Zero Tracker+Accountability",
            "company": "BP plc.",
            "year": 2024,
            "value": "Not Specified",
            "answer_url": "https://wikirate.org/Net_Zero_Tracker+Accountability+BP_plc.json",
        },
        {
            "id": 1,
            "name": "GreenDex+Annual Revenue+BP plc.+2024",
            "metric": "GreenDex+Annual Revenue",
            "company": "BP plc.",
            "year": 2024,
            "value": 248111000000.0,
            "answer_url": "https://wikirate.org/GreenDex+Annual_Revenue+BP_plc.json",
        },
    ]
}


@pytest.fixture
def adapter(monkeypatch) -> WikirateAdapter:
    monkeypatch.setattr(wikirate_mod, "_api_key", lambda: "test-key")
    a = WikirateAdapter()
    a._cache.get_payload = lambda *args, **kwargs: None  # type: ignore[method-assign]
    a._cache.put = lambda *args, **kwargs: None  # type: ignore[method-assign]
    return a


def _mock_companies(respx_mock, items: list[dict], identifier: str) -> None:
    respx_mock.get(
        f"{BASE}/Companies.json",
        params={"filter[company_identifier[value]]": identifier},
    ).mock(return_value=httpx.Response(200, json={"items": items}))


def _mock_answers(respx_mock) -> None:
    respx_mock.get(f"{BASE}/~637+Answer.json", params={"view": "count"}).mock(
        return_value=httpx.Response(200, json=2290)
    )
    respx_mock.get(
        f"{BASE}/~637+Answer.json",
        params={
            "filter[metric_type][]": "researched",
            "sort_by": "year",
            "sort_dir": "desc",
        },
    ).mock(return_value=httpx.Response(200, json=ANSWER_ITEMS))


@respx.mock
async def test_fetch_by_lei_resolves_and_builds_bundle(adapter, respx_mock):
    _mock_companies(respx_mock, [COMPANY_ITEM], LEI)
    _mock_answers(respx_mock)

    bundle = await adapter.fetch_by_lei(LEI)

    assert bundle is not None
    assert bundle["card_id"] == 637
    assert bundle["name"] == "BP plc."
    assert bundle["matched_by"] == "lei"
    assert bundle["wikirate_url"] == "https://wikirate.org/~637"
    assert bundle["total_answers"] == 2290
    assert bundle["identifiers"]["legal_entity_identifier"] == LEI
    assert bundle["identifiers"]["wikidata_id"] == QID
    # None/empty identifier fields are dropped.
    assert "uk_company_number" not in bundle["identifiers"]
    answers = bundle["latest_answers"]
    assert len(answers) == 2
    assert answers[0]["metric_designer"] == "Net Zero Tracker"
    assert answers[0]["metric_name"] == "Accountability"
    assert answers[0]["year"] == 2024
    # answer_url has the .json extension stripped for the HTML page.
    assert answers[0]["answer_url"].endswith("+BP_plc")


@respx.mock
async def test_fetch_by_lei_falls_back_to_qid(adapter, respx_mock):
    _mock_companies(respx_mock, [], LEI)
    _mock_companies(respx_mock, [COMPANY_ITEM], QID)
    _mock_answers(respx_mock)

    bundle = await adapter.fetch_by_lei(LEI, qid=QID)

    assert bundle is not None
    assert bundle["matched_by"] == "wikidata_qid"


@respx.mock
async def test_fetch_by_lei_no_match_returns_none(adapter, respx_mock):
    _mock_companies(respx_mock, [], LEI)
    assert await adapter.fetch_by_lei(LEI) is None


async def test_fetch_by_lei_without_key_skips(monkeypatch):
    monkeypatch.setattr(wikirate_mod, "_api_key", lambda: None)
    adapter = WikirateAdapter()
    # No respx mock active: any HTTP attempt would raise.
    assert await adapter.fetch_by_lei(LEI) is None


@respx.mock
async def test_fetch_by_lei_api_error_returns_none(adapter, respx_mock):
    respx_mock.get(f"{BASE}/Companies.json").mock(
        return_value=httpx.Response(500, json={"error_status": 500})
    )
    assert await adapter.fetch_by_lei(LEI) is None


@respx.mock
async def test_answer_failures_degrade_to_empty(adapter, respx_mock):
    _mock_companies(respx_mock, [COMPANY_ITEM], LEI)
    respx_mock.get(f"{BASE}/~637+Answer.json").mock(
        return_value=httpx.Response(500, json={"error_status": 500})
    )

    bundle = await adapter.fetch_by_lei(LEI)

    assert bundle is not None
    assert bundle["total_answers"] == 0
    assert bundle["latest_answers"] == []


@respx.mock
async def test_fetch_by_card_id(adapter, respx_mock):
    respx_mock.get(f"{BASE}/~637.json").mock(
        return_value=httpx.Response(200, json=COMPANY_ITEM)
    )
    _mock_answers(respx_mock)

    bundle = await adapter.fetch("637")

    assert bundle["card_id"] == 637
    assert bundle["matched_by"] == "card"
    assert bundle["is_stub"] is False


@respx.mock
async def test_fetch_unknown_card_returns_stub(adapter, respx_mock):
    respx_mock.get(f"{BASE}/~999.json").mock(return_value=httpx.Response(404))
    bundle = await adapter.fetch("999")
    assert bundle["is_stub"] is True


async def test_search_returns_empty(adapter):
    from opencheck.sources.base import SearchKind

    assert await adapter.search("BP", SearchKind.ENTITY) == []


def test_info_declares_esg_category_and_key():
    info = WikirateAdapter().info
    assert info.category == "esg"
    assert info.requires_api_key is True
    assert info.license == "CC-BY-4.0"


# ----------------------------------------------------------------------
# BODS mapper
# ----------------------------------------------------------------------


def _bundle() -> dict:
    return {
        "source_id": "wikirate",
        "card_id": 637,
        "name": "BP plc.",
        "wikirate_url": "https://wikirate.org/~637",
        "matched_by": "lei",
        "identifiers": {
            "legal_entity_identifier": LEI,
            "wikidata_id": QID,
            "open_corporates_id": "00102498",
            "sec_central_index_key": "313807",
        },
        "total_answers": 2290,
        "latest_answers": [],
        "is_stub": False,
    }


def test_map_wikirate_entity_statement():
    statements = list(map_wikirate(_bundle()))
    assert len(statements) == 1
    stmt = statements[0]
    assert stmt["recordType"] == "entity"
    details = stmt["recordDetails"]
    assert details["name"] == "BP plc."
    schemes = {i.get("scheme"): i["id"] for i in details["identifiers"]}
    assert schemes["XI-LEI"] == LEI
    assert schemes["WIKIDATA"] == QID
    assert schemes["US-SEC-CIK"] == "313807"
    # open_corporates_id carries a schemeName but no org-id scheme code.
    unschemed = [i for i in details["identifiers"] if "scheme" not in i]
    assert any(i["id"] == "00102498" for i in unschemed)
    src = stmt["source"]
    assert src["url"] == "https://wikirate.org/~637"


def test_map_wikirate_stub_and_empty():
    assert list(map_wikirate({})) == []
    assert list(map_wikirate({"is_stub": True, "card_id": 1, "name": "X"})) == []
    assert list(map_wikirate({**_bundle(), "name": ""})) == []


def test_map_wikirate_list_identifier_uses_first():
    bundle = _bundle()
    bundle["identifiers"]["legal_entity_identifier"] = [LEI, "OTHER"]
    statements = list(map_wikirate(bundle))
    schemes = {
        i.get("scheme"): i["id"]
        for i in statements[0]["recordDetails"]["identifiers"]
    }
    assert schemes["XI-LEI"] == LEI
