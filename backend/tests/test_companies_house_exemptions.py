"""PSC exemptions: a company on a regulated market keeps no PSC register, and
Companies House says so at ``/company/{n}/exemptions`` (the disclosure-status
ticket, 3 Nov 2026). Shell plc (04366849) read "None on record" until the
adapter followed the PSC list's ``links.exemptions``."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pytest_httpx import HTTPXMock

from opencheck import degradation, disclosure
from opencheck.config import get_settings
from opencheck.sources.companies_house import (
    CH_EXEMPTION_DESCRIPTIONS,
    CompaniesHouseAdapter,
    current_exemptions,
)

_API = "https://api.company-information.service.gov.uk"
_PAGE = "?items_per_page=100&start_index=0"
SHELL = "04366849"
FIXTURE = Path(__file__).parent / "fixtures" / "companies_house" / "exemption_descriptions.yml"

#: The shape the register publishes (developer-specs: companyExemptions).
SHELL_EXEMPTIONS = {
    "kind": "exemptions",
    "exemptions": {
        "psc_exempt_as_trading_on_uk_regulated_market": {
            "exemption_type": "psc-exempt-as-trading-on-uk-regulated-market",
            "items": [{"exempt_from": "2021-01-01"}],
        },
        "psc_exempt_as_trading_on_eu_regulated_market": {
            "exemption_type": "psc-exempt-as-trading-on-eu-regulated-market",
            "items": [{"exempt_from": "2016-06-30", "exempt_to": "2020-12-31"}],
        },
    },
    "links": {"self": f"/company/{SHELL}/exemptions"},
}


@pytest.fixture(autouse=True)
def _live_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "test-key")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _company(httpx_mock: HTTPXMock, number: str, *, exemptions_link: bool) -> None:
    httpx_mock.add_response(url=f"{_API}/company/{number}",
                            json={"company_number": number, "company_name": "SHELL PLC"})
    httpx_mock.add_response(url=f"{_API}/company/{number}/officers{_PAGE}", json={"items": []})
    links = {"self": f"/company/{number}/persons-with-significant-control"}
    if exemptions_link:
        links["exemptions"] = f"/company/{number}/exemptions"
    httpx_mock.add_response(
        url=f"{_API}/company/{number}/persons-with-significant-control{_PAGE}",
        json={"items": [], "total_results": 0, "active_count": 0, "ceased_count": 0,
              "links": links},
    )
    httpx_mock.add_response(
        url=f"{_API}/company/{number}/persons-with-significant-control-statements{_PAGE}",
        status_code=404,
    )


def test_the_words_are_companies_house_own() -> None:
    published = yaml.safe_load(FIXTURE.read_text())["exemption_description"]
    assert CH_EXEMPTION_DESCRIPTIONS == published


def test_only_periods_that_have_not_ended_are_current() -> None:
    assert current_exemptions(SHELL_EXEMPTIONS, "2026-11-03") == [
        ("psc-exempt-as-trading-on-uk-regulated-market", "2021-01-01")
    ]
    # On a day the EU period still ran, both are current.
    assert [k for k, _ in current_exemptions(SHELL_EXEMPTIONS, "2020-06-01")] == [
        "psc-exempt-as-trading-on-eu-regulated-market",
        "psc-exempt-as-trading-on-uk-regulated-market",
    ]
    assert current_exemptions({}, "2026-11-03") == []
    assert current_exemptions(None, "2026-11-03") == []


async def test_the_link_is_followed_and_read(httpx_mock: HTTPXMock) -> None:
    _company(httpx_mock, SHELL, exemptions_link=True)
    httpx_mock.add_response(url=f"{_API}/company/{SHELL}/exemptions", json=SHELL_EXEMPTIONS)
    bundle = await CompaniesHouseAdapter().fetch(SHELL)
    assert bundle["exemptions"]["exemptions"]
    hints = disclosure.bundle_hints("companies_house", bundle)
    assert [(h["value"], h["covers"], h["reasons"]) for h in hints] == [
        ("exempt", "beneficial_owners", ["psc-exempt-as-trading-on-uk-regulated-market"])
    ]
    assert hints[0]["details"] == [
        "From 1 January 2021 the company is exempt from the requirement to obtain "
        "information of its PSC because it has voting shares admitted to trading on "
        "a UK regulated market"
    ]


async def test_no_link_no_call(httpx_mock: HTTPXMock) -> None:
    # pytest-httpx fails the test on any request it was not given.
    _company(httpx_mock, "00102498", exemptions_link=False)
    bundle = await CompaniesHouseAdapter().fetch("00102498")
    assert bundle["exemptions"] == {}
    assert disclosure.bundle_hints("companies_house", bundle) == []


async def test_a_404_is_no_exemption(httpx_mock: HTTPXMock) -> None:
    _company(httpx_mock, SHELL, exemptions_link=True)
    httpx_mock.add_response(url=f"{_API}/company/{SHELL}/exemptions", status_code=404)
    bundle = await CompaniesHouseAdapter().fetch(SHELL)
    assert bundle["exemptions"] == {}


async def test_a_failed_read_is_said_and_not_cached(httpx_mock: HTTPXMock) -> None:
    _company(httpx_mock, SHELL, exemptions_link=True)
    httpx_mock.add_response(url=f"{_API}/company/{SHELL}/exemptions", status_code=503)
    with degradation.recording() as degraded:
        bundle = await CompaniesHouseAdapter().fetch(SHELL)
    assert bundle["exemptions"] is None
    assert [(d.source_id, d.check) for d in degraded] == [("companies_house", "source_read")]
    hints = disclosure.bundle_hints("companies_house", bundle)
    assert [(h["value"], h["details"]) for h in hints] == [
        ("unreachable", ["PSC exemptions not read"])
    ]
    # Asked again, only the exemptions are asked for again: the other records
    # were cached, the failure was not.
    httpx_mock.add_response(url=f"{_API}/company/{SHELL}/exemptions", json=SHELL_EXEMPTIONS)
    again = await CompaniesHouseAdapter().fetch(SHELL)
    assert again["exemptions"]["exemptions"]


def test_shell_reads_exempt_not_none_on_record() -> None:
    """The lookup-level answer: the exemption fills the gap none_filed would."""
    from opencheck.coverage import source_coverage

    hit = {"source_id": "companies_house", "hit_id": SHELL, "raw": {}, "is_stub": False}
    cov = source_coverage([hit], {}, ["companies_house"])
    hints = {"companies_house": disclosure.bundle_hints(
        "companies_house", {"exemptions": SHELL_EXEMPTIONS})}
    out = disclosure.for_lookup(bods=[], coverage=cov, degraded=[], knowability=None,
                                hints=hints, as_of="2026-11-03", source_countries={})
    assert out["sources"]["companies_house"]["values"] == ["exempt"]


def test_a_bundle_from_before_the_read_says_nothing() -> None:
    assert disclosure.bundle_hints("companies_house", {"pscs": {"items": []}}) == []


def test_the_card_sentence_says_exempt() -> None:
    from opencheck.findings import _CH_EXEMPTION_PHRASES, finding_companies_house

    bundle = {"company_number": SHELL, "profile": {"company_name": "SHELL PLC"},
              "officers": {"items": []}, "pscs": {"items": []},
              "psc_statements": {"items": []}, "exemptions": SHELL_EXEMPTIONS}
    sentence = finding_companies_house(bundle)
    assert sentence.startswith(
        "No person with significant control named; exempt from the PSC register, "
        "with shares on a UK regulated market"
    )
    assert set(_CH_EXEMPTION_PHRASES) == set(CH_EXEMPTION_DESCRIPTIONS)
    # Without the exemption the sentence is what it was.
    assert "exempt" not in finding_companies_house({**bundle, "exemptions": {}})
