"""The primary stock-exchange listing from LSEG PermID (Phase 236).

Pins the decisions on the PermID ticket (Stephen, 24 Sept 2026): no key → no
request and no event; a PermID failure is a degraded source, never "not
listed"; the organisation must carry the LEI itself; links only for verified
venue patterns; BODS gets ``securitiesListings`` on the subject's GLEIF
statement, copied, with a ``linking`` annotation, and never a filings URL.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
import respx

from opencheck import degradation, listing
from opencheck.bods.mapper import SOURCE_NAMES
from opencheck.bods.validator import validate_shape
from opencheck.config import get_settings

LEI = "21380068P1DRHMJ8KU70"
TOKEN = "test-permid-token-7f3a9c"

SEARCH = "https://api-eit.refinitiv.com/permid/search"
ORG = "https://permid.org/1-4295885039"
QUOTE = "https://permid.org/1-55836049491"

_ORG_RECORD = {
    "@id": ORG,
    "@type": "tr-org:Organization",
    "tr-org:hasLEI": LEI,
    "hasOrganizationPrimaryQuote": QUOTE,
    "hasPrimaryInstrument": "https://permid.org/1-8590936103",
    "hasHoldingClassification": "tr-org:publiclyHeld",
    "vcard:organization-name": "Shell PLC",
}
_QUOTE_RECORD = {
    "@id": QUOTE,
    "@type": "tr-fin:Quote",
    "tr-common:hasName": "SHELL ORD",
    "tr-fin:hasExchangeCode": "LSE",
    "tr-fin:hasExchangeTicker": "SHEL",
    "tr-fin:hasMic": "XLON",
    "tr-fin:hasRic": "SHEL.L",
}


def _search_body(org: str = ORG) -> dict[str, Any]:
    return {
        "result": {
            "organizations": {
                "entityType": "organizations",
                "total": 1,
                "entities": [{"@id": org, "organizationName": "Shell PLC"}],
            }
        }
    }


@pytest.fixture
def keyed(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("PERMID_API_KEY", TOKEN)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# --- no key, no request ------------------------------------------------------


async def test_no_key_means_no_request_and_no_payload(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.delenv("PERMID_API_KEY", raising=False)
    get_settings.cache_clear()
    try:
        with respx.mock(assert_all_called=False) as mock:
            route = mock.get(SEARCH)
            with degradation.recording() as recorded:
                assert await listing.fetch_listing(LEI) is None
            assert not route.called
            assert recorded == []  # not configured is silence, not a degradation
    finally:
        get_settings.cache_clear()


async def test_key_without_live_calls_is_off(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    monkeypatch.setenv("PERMID_API_KEY", TOKEN)
    get_settings.cache_clear()
    try:
        assert listing.available() is False
        assert await listing.fetch_listing(LEI) is None
    finally:
        get_settings.cache_clear()


# --- the three-request chain ---------------------------------------------------


@respx.mock
async def test_listed_company_resolves_to_its_primary_quote(keyed) -> None:
    s = respx.get(SEARCH).mock(return_value=httpx.Response(200, json=_search_body()))
    respx.get(ORG).mock(return_value=httpx.Response(200, json=_ORG_RECORD))
    respx.get(QUOTE).mock(return_value=httpx.Response(200, json=_QUOTE_RECORD))

    got = await listing.fetch_listing(LEI)

    assert got is not None and got["status"] == "listed"
    assert got["quote"]["ticker"] == "SHEL"
    assert got["quote"]["mic"] == "XLON"
    assert got["quote"]["ric"] == "SHEL.L"
    assert got["quote"]["instrument_permid"] == "8590936103"
    assert got["exchange"] == {"name": "London Stock Exchange", "country": "GB"}
    assert got["link"] == {
        "url": "https://www.londonstockexchange.com/stock/SHEL/x/company-page",
        "kind": "listing",
    }
    assert got["attribution"] == "LSEG PermID (CC-BY 4.0)"
    # The search is scoped to the LEI field, not a free-text name search.
    assert s.calls.last.request.url.params["q"] == f"lei:{LEI}"


@respx.mock
async def test_second_lookup_is_served_from_the_cache(keyed) -> None:
    s = respx.get(SEARCH).mock(return_value=httpx.Response(200, json=_search_body()))
    respx.get(ORG).mock(return_value=httpx.Response(200, json=_ORG_RECORD))
    respx.get(QUOTE).mock(return_value=httpx.Response(200, json=_QUOTE_RECORD))

    first = await listing.fetch_listing(LEI)
    second = await listing.fetch_listing(LEI)

    assert first == second
    assert s.call_count == 1


@respx.mock
async def test_an_organisation_without_this_lei_is_not_attached(keyed) -> None:
    """The search is a text search; a record that does not carry the LEI
    itself says nothing about the subject."""
    respx.get(SEARCH).mock(return_value=httpx.Response(200, json=_search_body()))
    respx.get(ORG).mock(
        return_value=httpx.Response(200, json={**_ORG_RECORD, "tr-org:hasLEI": "5493001KJTIIGC8Y1R12"})
    )
    quote = respx.get(QUOTE)

    got = await listing.fetch_listing(LEI)

    assert got is not None and got["status"] == "not_listed"
    assert got["organisation"] is None
    assert not quote.called


@respx.mock
async def test_an_organisation_with_no_primary_quote_is_not_listed(keyed) -> None:
    respx.get(SEARCH).mock(return_value=httpx.Response(200, json=_search_body()))
    org = {k: v for k, v in _ORG_RECORD.items() if k != "hasOrganizationPrimaryQuote"}
    respx.get(ORG).mock(return_value=httpx.Response(200, json=org))

    got = await listing.fetch_listing(LEI)

    assert got["status"] == "not_listed"
    assert got["organisation"]["permid"] == "4295885039"
    assert listing.describe(got) is None


@respx.mock
async def test_a_venue_without_a_verified_page_has_no_link(keyed) -> None:
    respx.get(SEARCH).mock(return_value=httpx.Response(200, json=_search_body()))
    respx.get(ORG).mock(return_value=httpx.Response(200, json=_ORG_RECORD))
    respx.get(QUOTE).mock(
        return_value=httpx.Response(
            200, json={**_QUOTE_RECORD, "tr-fin:hasMic": "XTKS", "tr-fin:hasExchangeTicker": "7203"}
        )
    )

    got = await listing.fetch_listing(LEI)

    assert got["exchange"] == {"name": "Tokyo Stock Exchange", "country": "JP"}
    assert got["link"] is None
    assert listing.describe(got) == "Tokyo Stock Exchange · 7203"


# --- failures are said on the line, never "not listed", never a screen ------


@respx.mock
async def test_rate_limit_is_unavailable_and_never_leaks_the_token(keyed) -> None:
    respx.get(SEARCH).mock(return_value=httpx.Response(429, json={"message": "API rate limit exceeded"}))

    with degradation.recording() as recorded:
        got = await listing.fetch_listing(LEI)

    assert got["status"] == "unavailable"
    assert got["reason"] == "rate_limited"
    assert TOKEN not in got["detail"] and "access-token" not in got["detail"]
    assert listing.describe(got) == "Could not be checked — PermID did not answer"
    # Not a screen that failed: nothing reaches degraded_sources.
    assert recorded == []


@respx.mock
async def test_a_server_error_is_unavailable_and_not_cached(keyed) -> None:
    route = respx.get(SEARCH).mock(return_value=httpx.Response(503))

    first = await listing.fetch_listing(LEI)
    second = await listing.fetch_listing(LEI)

    assert first["status"] == second["status"] == "unavailable"
    assert first["reason"] == "upstream_error"
    assert route.call_count == 2  # a failure is retried next time, not remembered


def test_an_unavailable_listing_adds_nothing_to_bods() -> None:
    bods = [_gleif_subject()]
    down = listing.unavailable(LEI, __import__("datetime").date(2026, 9, 24), "timeout", "x")
    assert listing.apply_to_bods(bods, LEI, down) is bods


# --- venue links ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("mic", "ticker", "url", "kind"),
    [
        ("XLON", "TLW", "https://www.londonstockexchange.com/stock/TLW/x/company-page", "listing"),
        ("XNGS", "AAPL", "https://www.nasdaq.com/market-activity/stocks/aapl", "listing"),
        ("XNYS", "KO", "https://www.nyse.com/quote/XNYS:KO", "listing"),
        ("XTSE", "ABX", "https://money.tmx.com/en/quote/ABX", "listing"),
        ("XASX", "BHP", "https://www.asx.com.au/markets/company/BHP", "listing"),
        ("XCSE", "NOVO B", "https://www.nasdaq.com/european-market-activity/shares/novo-b", "listing"),
        ("XSTO", "VOLCAR B", "https://www.nasdaq.com/european-market-activity/shares/volcar-b", "listing"),
        (
            "XNSA",
            "DANGCEM",
            "https://ngxgroup.com/exchange/data/company-profile/?symbol=DANGCEM&directory=companydirectory",
            "listing",
        ),
        ("XAMS", "HEIA", "https://live.euronext.com/en/search_instruments/HEIA", "search"),
        ("XOSL", "EQNR", "https://live.euronext.com/en/search_instruments/EQNR", "search"),
    ],
)
def test_verified_venue_links(mic: str, ticker: str, url: str, kind: str) -> None:
    assert listing.venue_link(mic, ticker) == {"url": url, "kind": kind}


@pytest.mark.parametrize("mic", ["XETR", "XSWX", "XTKS", "XNSE", "BVMF", "XHEL", "ZZZZ"])
def test_unverified_or_unknown_venues_get_no_link(mic: str) -> None:
    assert listing.venue_link(mic, "ABC") is None


def test_a_ticker_is_url_escaped() -> None:
    assert listing.venue_link("XLON", "BT/A")["url"].endswith("/stock/BT%2FA/x/company-page")


def test_every_venue_names_a_two_letter_jurisdiction() -> None:
    # BODS stockExchangeJurisdiction: an ISO 3166-1 code, 2–6 characters.
    for mic, v in listing.VENUES.items():
        assert len(mic) == 4 and mic.isupper(), mic
        assert len(v.country) == 2 and v.country.isupper(), mic
        assert v.link_kind in {"listing", "search"}, mic


# --- BODS ------------------------------------------------------------------------


def _gleif_subject() -> dict[str, Any]:
    return {
        "statementId": "s-gleif",
        "recordId": "s-gleif",
        "recordType": "entity",
        "recordStatus": "new",
        "statementDate": "2026-09-24",
        "declarationSubject": "s-gleif",
        "recordDetails": {
            "isComponent": False,
            "entityType": {"type": "registeredEntity"},
            "name": "SHELL PLC",
            "jurisdiction": {"code": "GB", "name": "United Kingdom"},
            "identifiers": [{"scheme": "XI-LEI", "id": LEI}],
        },
        "source": {"type": ["officialRegister"], "description": SOURCE_NAMES["gleif"]},
    }


def _payload(**quote_over: Any) -> dict[str, Any]:
    quote = {
        "permid": "55836049491",
        "url": QUOTE,
        "name": "SHELL ORD",
        "ticker": "SHEL",
        "mic": "XLON",
        "ric": "SHEL.L",
        "exchange_code": "LSE",
        "instrument_permid": "8590936103",
        **quote_over,
    }
    mic = quote["mic"]
    v = listing.VENUES.get(mic or "")
    return {
        "source_id": "permid",
        "attribution": listing.ATTRIBUTION,
        "licence": listing.LICENCE,
        "lei": LEI,
        "status": "listed",
        "as_of": "2026-09-24",
        "organisation": {"permid": "4295885039", "url": ORG},
        "quote": quote,
        "exchange": {"name": v.name, "country": v.country} if v else None,
        "link": listing.venue_link(mic, quote["ticker"]),
    }


def test_the_listing_goes_on_the_gleif_subject_statement_as_a_copy() -> None:
    other = {**_gleif_subject(), "statementId": "s-os", "recordId": "s-os",
             "source": {"type": ["thirdParty"], "description": SOURCE_NAMES["opensanctions"]}}
    original = _gleif_subject()
    bods = [other, original]

    out = listing.apply_to_bods(bods, LEI, _payload())

    assert "publicListing" not in original["recordDetails"]  # never mutated
    assert out[0] is other  # only the GLEIF statement changes
    pl = out[1]["recordDetails"]["publicListing"]
    assert pl == {
        "hasPublicListing": True,
        "securitiesListings": [
            {
                "stockExchangeJurisdiction": "GB",
                "stockExchangeName": "London Stock Exchange",
                "security": {"ticker": "SHEL"},
                "marketIdentifierCode": "XLON",
            }
        ],
    }
    # PermID is not in the closed securitiesIdentifierSchemes codelist, and an
    # exchange page is not a filing.
    assert "idScheme" not in pl["securitiesListings"][0]["security"]
    assert "companyFilingsURLs" not in pl
    (ann,) = out[1]["annotations"]
    assert ann["motivation"] == "linking"
    assert ann["url"] == QUOTE
    assert ann["statementPointerTarget"] == "/recordDetails/publicListing"
    assert "PermID" in ann["description"]
    assert validate_shape(out) == validate_shape(bods)


def test_an_unnamed_venue_or_missing_ticker_adds_nothing() -> None:
    bods = [_gleif_subject()]
    assert listing.apply_to_bods(bods, LEI, _payload(mic="ZZZZ")) is bods
    assert listing.apply_to_bods(bods, LEI, _payload(ticker=None)) is bods
    assert listing.apply_to_bods(bods, LEI, None) is bods
    not_listed = {**_payload(), "status": "not_listed", "quote": None, "exchange": None}
    assert listing.apply_to_bods(bods, LEI, not_listed) is bods


def test_describe_falls_back_to_the_mic() -> None:
    assert listing.describe(_payload()) == "London Stock Exchange · SHEL"
    assert listing.describe(_payload(mic="ZZZZ")) == "MIC ZZZZ · SHEL"


# --- the event, folded -----------------------------------------------------------


def test_the_event_is_folded_as_recorded_and_reaches_the_export() -> None:
    from opencheck.routers.lookup import fold_lookup_events

    payload = _payload()
    resp = fold_lookup_events(
        LEI,
        [
            ("gleif_done", {"lei": LEI, "legal_name": "SHELL PLC", "jurisdiction": "GB",
                            "derived_identifiers": {}}),
            ("deepen_result", {"bods": [_gleif_subject()]}),
            ("listing", payload),
            ("done", {"bods_issues": [], "license_notices": []}),
        ],
    )
    assert resp.listing == payload
    (subject,) = resp.bods
    assert subject["recordDetails"]["publicListing"]["hasPublicListing"] is True


def test_no_event_means_no_field_and_untouched_bods() -> None:
    from opencheck.routers.lookup import fold_lookup_events

    resp = fold_lookup_events(
        LEI,
        [
            ("deepen_result", {"bods": [_gleif_subject()]}),
            ("done", {"bods_issues": [], "license_notices": []}),
        ],
    )
    assert resp.listing is None
    assert "publicListing" not in resp.bods[0]["recordDetails"]


# --- surfaces -------------------------------------------------------------------


def test_mcp_lookup_and_batch_carry_the_line() -> None:
    from opencheck.mcp.shaping import shape_batch_row, shape_lookup
    from opencheck.routers.lookup import fold_lookup_events

    resp = fold_lookup_events(
        LEI,
        [
            ("gleif_done", {"lei": LEI, "legal_name": "SHELL PLC", "jurisdiction": "GB",
                            "derived_identifiers": {}}),
            ("deepen_result", {"bods": [_gleif_subject()]}),
            ("listing", _payload()),
            ("done", {"bods_issues": [], "license_notices": []}),
        ],
    )
    shaped = shape_lookup(resp)
    assert shaped["listing"]["line"] == "London Stock Exchange · SHEL"
    assert shaped["listing"]["url_kind"] == "listing"
    assert "Primary listing (PermID): London Stock Exchange · SHEL." in shaped["summary"]
    assert shape_batch_row(resp)["primary_listing"] == "London Stock Exchange · SHEL"


def test_mcp_null_listing_when_not_checked() -> None:
    from opencheck.mcp.shaping import shape_lookup
    from opencheck.routers.lookup import fold_lookup_events

    resp = fold_lookup_events(LEI, [("done", {"bods_issues": [], "license_notices": []})])
    shaped = shape_lookup(resp)
    assert shaped["listing"] is None
    assert "Primary listing" not in shaped["summary"]


def test_reports_name_the_listing_and_its_source() -> None:
    from opencheck.reporting.html_report import build_report_html
    from opencheck.reporting.markdown_report import build_report_markdown

    report = {"lei": LEI, "legal_name": "SHELL PLC", "jurisdiction": "GB",
              "bods": [_gleif_subject()], "listing": _payload()}
    md = build_report_markdown(report)
    assert "| Primary listing (LSEG PermID) | London Stock Exchange · SHEL |" in md
    html = build_report_html(report)
    assert "Primary listing (LSEG PermID)" in html
    assert "London Stock Exchange · SHEL" in html
    assert "Primary listing" not in build_report_markdown({**report, "listing": None})


# --- the pipeline ---------------------------------------------------------------


def _pipeline_client(monkeypatch, tmp_path):
    """The offline pipeline of test_lookup_pipeline, with the PermID fetch
    stubbed — no network, no key needed."""
    from fastapi.testclient import TestClient

    import opencheck.sources.climatetrace as _ct
    from opencheck.app import app
    from tests.test_lookup_pipeline import _seed_bundle

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    monkeypatch.setattr(_ct, "_lei_index", {})
    monkeypatch.setattr(_ct, "_entity_index", {})
    get_settings.cache_clear()
    lei = "213800LH1BZH3DI6G760"
    _seed_bundle(tmp_path, lei)
    monkeypatch.setattr(listing, "available", lambda: True)
    return TestClient(app), lei


def test_pipeline_emits_listing_before_the_profile_and_folds_it(monkeypatch, tmp_path) -> None:
    from tests.test_lookup_pipeline import _stream_body, _stream_events

    client, lei = _pipeline_client(monkeypatch, tmp_path)
    payload = {**_payload(), "lei": lei}

    async def _fake(got_lei: str, today=None):
        assert got_lei == lei
        return payload

    monkeypatch.setattr(listing, "fetch_listing", _fake)
    try:
        events = _stream_events(_stream_body(client, lei))
        names = [n for n, _ in events]
        assert names.count("listing") == 1
        assert names.index("listing") < names.index("subject_profile")
        assert dict(events)["listing"] == payload

        sync = client.get("/lookup", params={"lei": lei, "refresh": "true"}).json()
        assert sync["listing"] == payload
        subject = next(s for s in sync["bods"] if s.get("recordType") == "entity")
        assert subject["recordDetails"]["publicListing"]["securitiesListings"][0]["security"] == {
            "ticker": "SHEL"
        }
    finally:
        get_settings.cache_clear()


def test_pipeline_failure_rides_the_listing_event_not_degraded_sources(monkeypatch, tmp_path) -> None:
    from datetime import date

    from tests.test_lookup_pipeline import _stream_body, _stream_events

    client, lei = _pipeline_client(monkeypatch, tmp_path)
    down = listing.unavailable(lei, date(2026, 9, 24), "upstream_error",
                               "HTTPStatusError: HTTP 503 Service Unavailable from permid.org")

    async def _fails(got_lei: str, today=None):
        return down

    monkeypatch.setattr(listing, "fetch_listing", _fails)
    try:
        events = _stream_events(_stream_body(client, lei))
        assert dict(events)["listing"]["status"] == "unavailable"
        risk = dict(events)["risk_signals"]
        assert not any(d["source_id"] == "permid" for d in risk["degraded_sources"])
        assert "did not run" not in (risk.get("verdict") or "")
    finally:
        get_settings.cache_clear()


def test_pipeline_without_a_key_never_starts_the_fetch(monkeypatch, tmp_path) -> None:
    from tests.test_lookup_pipeline import _stream_body, _stream_events

    client, lei = _pipeline_client(monkeypatch, tmp_path)
    monkeypatch.setattr(listing, "available", lambda: False)

    async def _boom(*a, **k):  # pragma: no cover — must not run
        raise AssertionError("fetch_listing ran without a key")

    monkeypatch.setattr(listing, "fetch_listing", _boom)
    try:
        events = _stream_events(_stream_body(client, lei))
        names = [n for n, _ in events]
        assert "listing" not in names and "done" in names
    finally:
        get_settings.cache_clear()
