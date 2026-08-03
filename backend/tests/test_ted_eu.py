"""Tests for the TED (Tenders Electronic Daily) adapter, parser, mapper
and lookup wiring.

The XML fixtures are trimmed synthetic eForms contract-award notices whose
``efac:NoticeResult`` structure mirrors the live notice verified during the
2026-08-03 investigation (Orange notice 74598-2025): LotResult → LotTender →
TenderingParty → Tenderer → Organization → ``cbc:CompanyID``.
"""

from __future__ import annotations

import json

import pytest
from pytest_httpx import HTTPXMock

from opencheck.bods import map_ted_eu, validate_shape
from opencheck.config import get_settings
from opencheck.routers.lookup import (
    _bh_ted_eu,
    _build_result_hit,
    _dispatch,
    _LookupCtx,
)
from opencheck.sources import REGISTRY, SearchKind
from opencheck.sources.ted_eu import (
    TED_JURISDICTIONS,
    TedEuAdapter,
    build_identifier_set,
    parse_notice_xml,
)

_SEARCH_URL = "https://api.ted.europa.eu/v3/notices/search"
_LEI = "969500MCOONR8990S771"


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _notice_xml(
    company_id: str = "380129866",
    status: str = "selec-w",
    with_amount: bool = True,
) -> str:
    """A minimal eForms CAN with one winning lot for ``company_id``."""
    amount = (
        '<cac:LegalMonetaryTotal>'
        '<cbc:PayableAmount currencyID="EUR">207000</cbc:PayableAmount>'
        "</cac:LegalMonetaryTotal>"
        if with_amount
        else ""
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<ContractAwardNotice
    xmlns="urn:oasis:names:specification:ubl:schema:xsd:ContractAwardNotice-2"
    xmlns:cac="urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2"
    xmlns:cbc="urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2"
    xmlns:ext="urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2"
    xmlns:efac="http://data.europa.eu/p27/eforms-ubl-extension-aggregate-components/1"
    xmlns:efbc="http://data.europa.eu/p27/eforms-ubl-extension-basic-components/1">
  <ext:UBLExtensions><ext:UBLExtension><ext:ExtensionContent>
    <efac:EformsExtension>
      <efac:NoticeResult>
        <efac:LotResult>
          <cbc:ID schemeName="result">RES-0001</cbc:ID>
          <cbc:TenderResultCode listName="winner-selection-status">{status}</cbc:TenderResultCode>
          <efac:LotTender><cbc:ID schemeName="tender">TEN-0001</cbc:ID></efac:LotTender>
          <efac:SettledContract><cbc:ID schemeName="contract">CON-0001</cbc:ID></efac:SettledContract>
          <efac:TenderLot><cbc:ID schemeName="Lot">LOT-0001</cbc:ID></efac:TenderLot>
        </efac:LotResult>
        <efac:LotTender>
          <cbc:ID schemeName="tender">TEN-0001</cbc:ID>
          <cbc:RankCode>1</cbc:RankCode>
          {amount}
          <efac:TenderingParty><cbc:ID schemeName="tendering-party">TPA-0001</cbc:ID></efac:TenderingParty>
          <efac:TenderLot><cbc:ID>LOT-0001</cbc:ID></efac:TenderLot>
        </efac:LotTender>
        <efac:SettledContract>
          <cbc:ID schemeName="contract">CON-0001</cbc:ID>
          <cbc:AwardDate>2024-10-18+02:00</cbc:AwardDate>
          <efac:ContractReference><cbc:ID>24104000</cbc:ID></efac:ContractReference>
          <efac:LotTender><cbc:ID schemeName="tender">TEN-0001</cbc:ID></efac:LotTender>
        </efac:SettledContract>
        <efac:TenderingParty>
          <cbc:ID schemeName="tendering-party">TPA-0001</cbc:ID>
          <efac:Tenderer><cbc:ID schemeName="organization">ORG-0003</cbc:ID></efac:Tenderer>
        </efac:TenderingParty>
      </efac:NoticeResult>
      <efac:Organizations>
        <efac:Organization>
          <efac:Company>
            <cac:PartyIdentification><cbc:ID schemeName="organization">ORG-0001</cbc:ID></cac:PartyIdentification>
            <cac:PartyLegalEntity><cbc:CompanyID>17540111600512</cbc:CompanyID></cac:PartyLegalEntity>
          </efac:Company>
        </efac:Organization>
        <efac:Organization>
          <efac:Company>
            <cac:PartyIdentification><cbc:ID schemeName="organization">ORG-0003</cbc:ID></cac:PartyIdentification>
            <cac:PartyLegalEntity><cbc:CompanyID>{company_id}</cbc:CompanyID></cac:PartyLegalEntity>
          </efac:Company>
        </efac:Organization>
      </efac:Organizations>
    </efac:EformsExtension>
  </ext:ExtensionContent></ext:UBLExtension></ext:UBLExtensions>
</ContractAwardNotice>"""


def _search_payload(pubs: list[str], total: int | None = None) -> dict:
    return {
        "notices": [
            {
                "publication-number": pub,
                "publication-date": "2025-02-04+01:00",
                "notice-type": "can-standard",
                "notice-title": {"eng": "Telephone services", "fra": "Téléphonie"},
                "buyer-name": {"fra": ["ARTE G.E.I.E."]},
                "buyer-country": ["FRA"],
                "total-value": 207000,
                "total-value-cur": ["EUR"],
                "classification-cpv": ["64210000", "64210000", "64215000"],
                "winner-selection-status": ["selec-w"],
                "contract-conclusion-date": ["2024-12-05+01:00"],
            }
            for pub in pubs
        ],
        "totalNoticeCount": total if total is not None else len(pubs),
        "iterationNextToken": None,
        "timedOut": False,
    }


# ---------------------------------------------------------------------------
# Identifier-set construction
# ---------------------------------------------------------------------------


def test_identifier_set_lei_first_then_verbatim_then_stripped() -> None:
    ids = build_identifier_set(_LEI, "HRB 6684", {"lei": _LEI, "siren": "380129866"})
    assert ids[0] == _LEI
    assert ids[1] == "HRB 6684"  # GLEIF-verbatim form matches TED (space kept)
    assert "HRB6684" in ids  # stripped variant
    assert "380129866" in ids
    # 'lei' derived key is skipped (already queued), no duplicates:
    assert len(ids) == len(set(ids))


def test_identifier_set_strips_quotes_and_caps() -> None:
    ids = build_identifier_set('LEI"X', 'A"B', {f"k{i}": f"v{i}" for i in range(20)})
    assert ids[0] == "LEIX"
    assert ids[1] == "AB"
    assert all('"' not in v for v in ids)
    assert len(ids) <= 8


def test_identifier_set_empty_inputs() -> None:
    assert build_identifier_set("", "", {}) == []


# ---------------------------------------------------------------------------
# eForms XML winner-chain parsing
# ---------------------------------------------------------------------------


def test_parse_notice_xml_won() -> None:
    result = parse_notice_xml(_notice_xml(), ["380129866"])
    assert result is not None
    assert result["role"] == "won"
    assert result["lots_won"] == ["LOT-0001"]
    assert result["awarded_values"] == [{"amount": "207000", "currency": "EUR"}]
    assert result["award_dates"] == ["2024-10-18"]
    assert result["contract_references"] == ["24104000"]
    assert result["matched_company_ids"] == ["380129866"]


def test_parse_notice_xml_matches_normalised_forms() -> None:
    """``HRB 6684`` in GLEIF matches ``HRB6684`` in the notice, and vice versa."""
    xml = _notice_xml(company_id="HRB 6684")
    result = parse_notice_xml(xml, ["hrb6684"])
    assert result is not None and result["role"] == "won"
    assert result["matched_company_ids"] == ["HRB 6684"]


def test_parse_notice_xml_tendered_not_won() -> None:
    result = parse_notice_xml(_notice_xml(status="selec-nw"), ["380129866"])
    assert result is not None
    assert result["role"] == "tendered"
    assert result["lots_won"] == []
    assert result["awarded_values"] == []


def test_parse_notice_xml_unmatched_org_is_unknown() -> None:
    result = parse_notice_xml(_notice_xml(), ["999999999"])
    assert result is not None
    assert result["role"] == "unknown"
    assert result["matched_company_ids"] == []


def test_parse_notice_xml_invalid_xml_returns_none() -> None:
    assert parse_notice_xml("<not-xml", ["380129866"]) is None
    assert parse_notice_xml(_notice_xml(), []) is None


# ---------------------------------------------------------------------------
# fetch_by_identifiers gates
# ---------------------------------------------------------------------------


async def test_fetch_returns_none_when_not_live() -> None:
    adapter = TedEuAdapter()
    result = await adapter.fetch_by_identifiers(_LEI, "380129866", "FR")
    assert result is None


async def test_fetch_returns_none_outside_ted_jurisdictions(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    adapter = TedEuAdapter()
    assert await adapter.fetch_by_identifiers(_LEI, "12-3456789", "US") is None
    assert await adapter.fetch_by_identifiers(_LEI, "12-3456789", "US-DE") is None
    assert "US" not in TED_JURISDICTIONS
    assert "FR" in TED_JURISDICTIONS and "GB" in TED_JURISDICTIONS


async def test_fetch_returns_none_without_identifiers(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    adapter = TedEuAdapter()
    assert await adapter.fetch_by_identifiers("", "", "FR") is None


# ---------------------------------------------------------------------------
# Live bundle building (mocked HTTP)
# ---------------------------------------------------------------------------


async def test_bundle_zero_hits(monkeypatch, httpx_mock: HTTPXMock) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    httpx_mock.add_response(url=_SEARCH_URL, json=_search_payload([]))
    adapter = TedEuAdapter()
    bundle = await adapter.fetch_by_identifiers(_LEI, "380129866", "FR")
    assert bundle is not None
    assert bundle["total_notice_count"] == 0
    assert bundle["notices"] == []
    # The pipeline drops the card for a zero-notice bundle:
    ctx = _LookupCtx(lei=_LEI)
    assert _build_result_hit("ted_eu", bundle, ctx) is None


async def test_bundle_with_confirmed_win(monkeypatch, httpx_mock: HTTPXMock) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    httpx_mock.add_response(url=_SEARCH_URL, json=_search_payload(["74598-2025"]))
    httpx_mock.add_response(
        url="https://ted.europa.eu/en/notice/74598-2025/xml",
        text=_notice_xml(),
    )
    adapter = TedEuAdapter()
    bundle = await adapter.fetch_by_identifiers(
        _LEI, "380129866", "FR", legal_name="ORANGE"
    )
    assert bundle is not None
    assert bundle["total_notice_count"] == 1
    assert bundle["confirmed_wins"] == 1
    assert bundle["matched_company_ids"] == ["380129866"]
    notice = bundle["notices"][0]
    assert notice["role"] == "won"
    assert notice["confirmed"] is True
    assert notice["title"] == "Telephone services"  # eng preferred
    assert notice["buyer_name"] == "ARTE G.E.I.E."
    assert notice["buyer_country"] == "FRA"
    assert notice["publication_date"] == "2025-02-04"
    assert notice["cpv"] == ["64210000", "64215000"]  # deduped, order kept
    assert notice["url"].endswith("/detail/74598-2025")
    # The search body used the identifier IN() query with scope ALL:
    request = httpx_mock.get_requests()[0]
    body = json.loads(request.content)
    assert 'organisation-identifier-tenderer IN ("' in body["query"]
    assert f'"{_LEI}"' in body["query"]
    assert '"380129866"' in body["query"]
    assert "SORT BY publication-date DESC" in body["query"]
    assert body["scope"] == "ALL"


async def test_bundle_xml_failure_degrades_to_unknown(
    monkeypatch, httpx_mock: HTTPXMock
) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    httpx_mock.add_response(url=_SEARCH_URL, json=_search_payload(["9-2025"]))
    httpx_mock.add_response(
        url="https://ted.europa.eu/en/notice/9-2025/xml", status_code=404
    )
    adapter = TedEuAdapter()
    bundle = await adapter.fetch_by_identifiers(_LEI, "380129866", "FR")
    assert bundle is not None
    notice = bundle["notices"][0]
    assert notice["role"] == "unknown"
    assert notice["confirmed"] is False
    assert bundle["confirmed_wins"] == 0
    # The notice is kept — never dropped on confirmation failure.
    assert bundle["total_notice_count"] == 1


# ---------------------------------------------------------------------------
# Hit builder + corroboration rule
# ---------------------------------------------------------------------------


def _won_bundle(matched: list[str]) -> dict:
    return {
        "source_id": "ted_eu",
        "lei": _LEI,
        "legal_name": "ORANGE",
        "identifiers_queried": [_LEI, "380129866"],
        "total_notice_count": 5,
        "confirmed_wins": 3,
        "matched_company_ids": matched,
        "notices": [
            {
                "publication_number": "74598-2025",
                "publication_date": "2025-02-04",
                "role": "won",
            }
        ],
        "is_stub": False,
    }


def test_bh_ted_eu_summary_and_no_borrowed_identifiers() -> None:
    ctx = _LookupCtx(lei=_LEI)
    ctx.legal_name = "ORANGE"
    hit = _bh_ted_eu(_won_bundle(["380129866"]), ctx)
    assert hit.source_id == "ted_eu"
    assert "5 EU award notices" in hit.summary
    assert "3 confirmed wins" in hit.summary
    assert "latest 2025-02-04" in hit.summary
    assert "eForms era" in hit.summary
    # Corroboration rule: national ids are not echoed back; no LEI matched → {}.
    assert hit.identifiers == {}


def test_bh_ted_eu_asserts_lei_only_when_ted_published_it() -> None:
    ctx = _LookupCtx(lei=_LEI)
    hit = _bh_ted_eu(_won_bundle(["380129866", _LEI]), ctx)
    assert hit.identifiers == {"lei": _LEI}


# ---------------------------------------------------------------------------
# Lookup dispatch wiring
# ---------------------------------------------------------------------------


def test_dispatch_includes_ted_eu_with_anchor_identifiers() -> None:
    ctx = _LookupCtx(lei=_LEI)
    ctx.jurisdiction = "FR"
    ctx.registered_as = "380129866"
    tasks = _dispatch(ctx, only="ted_eu")
    assert [sid for sid, _ in tasks] == ["ted_eu"]
    for _, coro in tasks:
        coro.close()  # avoid un-awaited coroutine warnings


def test_dispatch_skips_ted_eu_without_lei_or_registration() -> None:
    ctx = _LookupCtx(lei="")
    ctx.registered_as = ""
    assert _dispatch(ctx, only="ted_eu") == []


def test_registry_entry_and_source_info() -> None:
    adapter = REGISTRY["ted_eu"]
    info = adapter.info
    assert info.id == "ted_eu"
    assert info.requires_api_key is False
    assert SearchKind.ENTITY in info.supports
    assert "2011/833/EU" in info.license
    from opencheck.licensing import classify

    assert classify(info.license).commercial_use == "yes"


async def test_search_is_intentionally_empty() -> None:
    assert await REGISTRY["ted_eu"].search("Orange", SearchKind.ENTITY) == []


# ---------------------------------------------------------------------------
# BODS mapper
# ---------------------------------------------------------------------------


def test_map_ted_eu_entity_statement_only() -> None:
    statements = list(map_ted_eu(_won_bundle(["380129866"])))
    assert len(statements) == 1
    stmt = statements[0]
    assert stmt["recordType"] == "entity"
    assert stmt["recordDetails"]["name"] == "ORANGE"
    identifiers = stmt["recordDetails"]["identifiers"]
    assert identifiers == [
        {
            "id": "380129866",
            "schemeName": "Organisation identifier — eForms BT-501 (via TED notice)",
        }
    ]
    validate_shape(statements)


def test_map_ted_eu_lei_gets_scheme_when_matched() -> None:
    statements = list(map_ted_eu(_won_bundle([_LEI])))
    identifiers = statements[0]["recordDetails"]["identifiers"]
    assert identifiers[0]["scheme"] == "XI-LEI"


def test_map_ted_eu_zero_notices_yields_nothing() -> None:
    bundle = _won_bundle([])
    bundle["total_notice_count"] = 0
    assert list(map_ted_eu(bundle)) == []
    assert list(map_ted_eu({})) == []
    assert list(map_ted_eu({"is_stub": True})) == []
