"""Phase 252 — SEC EDGAR reads the structured SCHEDULE 13D/13G forms.

Until Phase 252 the adapter asked EDGAR's filing feed for ``SC 13D`` /
``SC 13G``. The ``type=`` filter is a prefix match and the December 2024 XML
mandate renamed the forms to ``SCHEDULE 13D`` / ``SCHEDULE 13G``, so the
adapter only ever saw legacy filings, skipped each one for having no XML, and
answered "no record" for every US issuer. The unit tests mocked ``SC+13G``
URLs returning ``SCHEDULE 13G`` entries — a combination EDGAR never serves.

These tests run on Moody's Corporation's real feed and filings
(``fixtures/sec_edgar``), fetched 28 Sept 2026.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pytest_httpx import HTTPXMock

from opencheck.bods.lifecycle import statement_lifecycle
from opencheck.bods.mapper import map_sec_edgar, reports_exit
from opencheck.config import get_settings
from opencheck.sources.sec_edgar import (
    SecEdgarAdapter,
    _LEGACY_FORM_TYPES,
    _STRUCTURED_FORM_TYPES,
    _latest_per_reporter,
    _parse_filing_refs_from_atom,
    _parse_filing_xml,
    _us_date_to_iso,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "sec_edgar"
_BROWSE = "https://www.sec.gov/cgi-bin/browse-edgar"
_ARCHIVE = "https://www.sec.gov/Archives/edgar/data/1059556"
_MOODYS = "1059556"

_FILINGS = {
    "000090266426002468": "moodys_13ga_tci_hohn.xml",
    "000210011926000791": "moodys_13g_vanguard_capital_management.xml",
    "000010290926001883": "moodys_13ga_vanguard_group_exit.xml",
}

_EMPTY_FEED = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<feed xmlns="http://www.w3.org/2005/Atom"><title>EDGAR</title></feed>'
)


def _fixture(name: str) -> str:
    return (_FIXTURES / name).read_text(encoding="utf-8")


def _feed_url(form: str) -> str:
    return (
        f"{_BROWSE}?action=getcompany&CIK={_MOODYS}&type={form}"
        "&dateb=&owner=include&count=40&search_text=&output=atom"
    )


@pytest.fixture(autouse=True)
def _live_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _mock_moodys(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(url=_feed_url("SCHEDULE+13D"), text=_EMPTY_FEED)
    httpx_mock.add_response(
        url=_feed_url("SCHEDULE+13G"), text=_fixture("moodys_schedule_13g_feed.atom")
    )
    for accession, name in _FILINGS.items():
        httpx_mock.add_response(
            url=f"{_ARCHIVE}/{accession}/primary_doc.xml", text=_fixture(name)
        )


# ---------------------------------------------------------------------------
# The form types asked for
# ---------------------------------------------------------------------------


def test_structured_form_types_are_the_renamed_schedule_forms() -> None:
    # The whole bug: EDGAR's type= filter is a prefix match, so "SC+13G"
    # can never return a "SCHEDULE 13G" filing.
    assert _STRUCTURED_FORM_TYPES == ("SCHEDULE+13D", "SCHEDULE+13G")
    for form in _STRUCTURED_FORM_TYPES:
        assert not form.startswith("SC+")
    assert _LEGACY_FORM_TYPES == ("SC+13D", "SC+13G")


def test_real_feed_lists_three_structured_filings() -> None:
    refs = _parse_filing_refs_from_atom(_fixture("moodys_schedule_13g_feed.atom"))
    assert [(r["form_type"], r["filed"]) for r in refs] == [
        ("SCHEDULE 13G/A", "2026-05-15"),
        ("SCHEDULE 13G", "2026-04-30"),
        ("SCHEDULE 13G/A", "2026-03-27"),
    ]
    # Archived under the issuer's CIK in this feed.
    assert {r["filer_cik"] for r in refs} == {_MOODYS}
    assert {r["accession"] for r in refs} == set(_FILINGS)


# ---------------------------------------------------------------------------
# The filing XML, as EDGAR actually serves it
# ---------------------------------------------------------------------------


def test_parser_reads_nested_cusip_common_namespace_address_and_event_date() -> None:
    parsed = _parse_filing_xml(_fixture("moodys_13ga_tci_hohn.xml"))
    assert parsed is not None
    issuer = parsed["issuer"]
    assert issuer["cik"] == _MOODYS
    assert issuer["name"] == "Moody's Corporation"
    # issuerCusips/issuerCusipNumber — the nested path real filings use.
    assert issuer["cusip"] == "615369105"
    # issuerPrincipalExecutiveOfficeAddress, fields in the com: namespace.
    assert issuer["address"] == {
        "street1": "7 WORLD TRADE CENTER",
        "street2": "AT 250 GREENWICH STREET",
        "city": "NEW YORK",
        "stateOrCountry": "NY",
        "zipCode": "10007",
    }
    assert parsed["event_date"] == "2026-03-31"
    assert [r["name"] for r in parsed["reporters"]] == [
        "TCI Fund Management Ltd",
        "Christopher Hohn",
    ]
    assert [r["percent_of_class"] for r in parsed["reporters"]] == [8.21, 8.21]
    assert parsed["reporters"][1]["is_individual"] is True


def test_13d_event_date_and_common_namespace_address() -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/schedule13D" xmlns:com="http://www.sec.gov/edgar/common">
  <headerData><filerInfo><filer><filerCredentials><cik>0002107788</cik></filerCredentials></filer></filerInfo></headerData>
  <formData>
    <coverPageHeader>
      <dateOfEvent>09/17/2026</dateOfEvent>
      <issuerInfo>
        <issuerCIK>0001951667</issuerCIK>
        <issuerCusips><issuerCusipNumber>16307X301</issuerCusipNumber></issuerCusips>
        <issuerName>CHEETAH NET SUPPLY CHAIN SERVICE INC.</issuerName>
        <address><com:street1>8707 RESEARCH DRIVE</com:street1><com:city>IRVINE</com:city><com:stateOrCountry>CA</com:stateOrCountry></address>
      </issuerInfo>
    </coverPageHeader>
    <reportingPersons><reportingPersonInfo>
      <reportingPersonCIK>0002107788</reportingPersonCIK>
      <reportingPersonName>Takeover Time 2026 LLC</reportingPersonName>
      <typeOfReportingPerson>OO</typeOfReportingPerson>
      <aggregateAmountOwned>1846000.00</aggregateAmountOwned>
      <percentOfClass>0.9</percentOfClass>
    </reportingPersonInfo></reportingPersons>
  </formData>
</edgarSubmission>"""
    parsed = _parse_filing_xml(xml)
    assert parsed is not None
    assert parsed["event_date"] == "2026-09-17"
    assert parsed["issuer"]["cusip"] == "16307X301"
    assert parsed["issuer"]["address"] == {
        "street1": "8707 RESEARCH DRIVE",
        "city": "IRVINE",
        "stateOrCountry": "CA",
    }


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("03/31/2026", "2026-03-31"),
        ("3/1/2026", "2026-03-01"),
        ("2026-03-31", "2026-03-31"),
        ("13/01/2026", ""),
        ("31 March 2026", ""),
        ("", ""),
    ],
)
def test_us_date_to_iso(raw: str, expected: str) -> None:
    assert _us_date_to_iso(raw) == expected


# ---------------------------------------------------------------------------
# One record per reporter — joint filers stay apart
# ---------------------------------------------------------------------------


def test_joint_13g_reporters_without_ciks_are_kept_apart() -> None:
    # 13G names reporting persons without a CIK. Keyed on the filer's CIK
    # alone, TCI and Christopher Hohn collapsed into one and Hohn was lost.
    parsed = _parse_filing_xml(_fixture("moodys_13ga_tci_hohn.xml"))
    assert parsed is not None
    raw = [
        {"reporter": r, "filer_cik": parsed["filer_cik"], "filed": "2026-05-15"}
        for r in parsed["reporters"]
    ]
    kept = _latest_per_reporter(raw)
    assert [k["reporter"]["name"] for k in kept] == [
        "TCI Fund Management Ltd",
        "Christopher Hohn",
    ]


def test_a_later_amendment_by_the_same_filer_replaces_the_earlier() -> None:
    older = {"reporter": {"name": "Vanguard Capital Management"}, "filer_cik": "2100119", "filed": "2026-04-30"}
    newer = {"reporter": {"name": "VANGUARD CAPITAL  MANAGEMENT"}, "filer_cik": "2100119", "filed": "2026-08-10"}
    assert _latest_per_reporter([older, newer]) == [newer]


# ---------------------------------------------------------------------------
# End to end on Moody's
# ---------------------------------------------------------------------------


async def test_moodys_fetch_finds_all_four_reporters(httpx_mock: HTTPXMock) -> None:
    _mock_moodys(httpx_mock)
    bundle = await SecEdgarAdapter().fetch(_MOODYS)

    assert bundle.get("is_stub") is not True
    assert "coverage_note" not in bundle
    assert bundle["structured_filing_count"] == 3
    # The legacy feeds are not read when structured filings were found —
    # pytest-httpx fails the test on any unmocked request.
    assert bundle["legacy_filing_count"] is None
    assert bundle["latest_filing_date"] == "2026-05-15"

    by_name = {f["reporter"]["name"]: f for f in bundle["filings"]}
    assert set(by_name) == {
        "TCI Fund Management Ltd",
        "Christopher Hohn",
        "Vanguard Capital Management",
        "The Vanguard Group",
    }
    assert by_name["Vanguard Capital Management"]["reporter"]["percent_of_class"] == 6.41
    assert by_name["The Vanguard Group"]["event_date"] == "2026-03-13"
    for f in bundle["filings"]:
        assert f["issuer"]["cusip"] == "615369105"


async def test_moodys_bods_draws_the_vanguard_exit_as_ended(httpx_mock: HTTPXMock) -> None:
    _mock_moodys(httpx_mock)
    bundle = await SecEdgarAdapter().fetch(_MOODYS)
    statements = list(map_sec_edgar(bundle))

    names = {}
    for s in statements:
        rd = s.get("recordDetails", {})
        if s["recordType"] == "entity":
            names[s["recordId"]] = rd.get("name")
        elif s["recordType"] == "person":
            names[s["recordId"]] = rd["names"][0]["fullName"]
    rels = {
        names[s["recordDetails"]["interestedParty"]]: s
        for s in statements
        if s["recordType"] == "relationship"
    }
    assert set(rels) == {
        "TCI Fund Management Ltd",
        "Christopher Hohn",
        "Vanguard Capital Management",
        "The Vanguard Group",
    }

    today = "2026-09-28"
    exit_rel = rels["The Vanguard Group"]
    interest = exit_rel["recordDetails"]["interests"][0]
    assert interest["endDate"] == "2026-03-13"
    assert "share" not in interest  # never a current 0% shareholding
    assert "no shares held" in interest["details"]
    assert statement_lifecycle(exit_rel, as_of=today).ended is True

    for current in ("TCI Fund Management Ltd", "Christopher Hohn", "Vanguard Capital Management"):
        rel = rels[current]
        assert statement_lifecycle(rel, as_of=today).ended is False
        assert "endDate" not in rel["recordDetails"]["interests"][0]
    assert rels["TCI Fund Management Ltd"]["recordDetails"]["interests"][0]["share"] == {"exact": 8.21}


def test_exit_without_an_event_date_is_closed_with_no_invented_date() -> None:
    bundle = {
        "source_id": "sec_edgar",
        "issuer_cik": _MOODYS,
        "filings": [
            {
                "issuer": {"cik": _MOODYS, "name": "Moody's Corporation"},
                "reporter": {
                    "name": "The Vanguard Group",
                    "type_code": "IA",
                    "aggregate_amount_owned": 0.0,
                    "percent_of_class": 0.0,
                },
                "filing_url": f"{_ARCHIVE}/000010290926001883/primary_doc.xml",
                "form_type": "SCHEDULE 13G/A",
                "filed": "2026-03-27",
            }
        ],
    }
    rel = next(s for s in map_sec_edgar(bundle) if s["recordType"] == "relationship")
    assert rel["recordStatus"] == "closed"
    assert "endDate" not in rel["recordDetails"]["interests"][0]


# ---------------------------------------------------------------------------
# What counts as an exit
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("aggregate", "percent", "expected"),
    [
        (0.0, 0.0, True),
        (0.0, None, True),
        (None, 0.0, True),
        ("0", "0", True),
        (1_000_000.0, 4.8, False),  # below the threshold, still held
        (0.0, 3.0, False),  # contradictory — carried as filed
        (None, None, False),
        (None, 4.8, False),
    ],
)
def test_reports_exit(aggregate, percent, expected) -> None:
    reporter = {"aggregate_amount_owned": aggregate, "percent_of_class": percent}
    assert reports_exit(reporter) is expected
