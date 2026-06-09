"""SEC EDGAR adapter and BODS mapper tests (HTTP mocked with pytest-httpx)."""

from __future__ import annotations

import json
import time

import pytest
from pytest_httpx import HTTPXMock

from opencheck.bods.mapper import map_sec_edgar
from opencheck.cache import Cache
from opencheck.config import get_settings
from opencheck.sources import SearchKind
from opencheck.sources.sec_edgar import (
    SecEdgarAdapter,
    _parse_company_hits_from_atom,
    _parse_filing_refs_from_atom,
    _parse_filing_xml,
)

_EDGAR_BASE = "https://www.sec.gov"
_BROWSE = f"{_EDGAR_BASE}/cgi-bin/browse-edgar"

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _live_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Sample atom XML snippets
# ---------------------------------------------------------------------------

_COMPANY_SEARCH_ATOM = """\
<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>EDGAR Company Search</title>
  <entry>
    <title>RUSH STREET INTERACTIVE, INC.</title>
    <id>urn:tag:sec.gov,2008:company=0001793659</id>
    <link rel="alternate" href="https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&amp;CIK=0001793659&amp;type=&amp;dateb=&amp;owner=include&amp;count=40"/>
    <updated>2026-01-01T00:00:00-04:00</updated>
    <summary>State of Inc.: DE  |  SIC: 7993 (Coin-Operated Amusement Devices)</summary>
  </entry>
  <entry>
    <title>RUSH STREET GAMING, LLC</title>
    <id>urn:tag:sec.gov,2008:company=0001556801</id>
    <link rel="alternate" href="https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&amp;CIK=0001556801&amp;type=&amp;dateb=&amp;owner=include&amp;count=40"/>
    <updated>2026-01-01T00:00:00-04:00</updated>
    <summary>State of Inc.: DE</summary>
  </entry>
</feed>"""

_FILINGS_ATOM_13D = """\
<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>EDGAR Filing Search</title>
  <entry>
    <category scheme="https://www.sec.gov/" term="SCHEDULE 13D"/>
    <title>SCHEDULE 13D - 2026-04-15</title>
    <id>urn:tag:sec.gov,2008:accession-number=0001104659-26-057435</id>
    <link rel="alternate" href="https://www.sec.gov/Archives/edgar/data/1373161/000110465926057435/0001104659-26-057435-index.htm"/>
    <updated>2026-04-15T12:00:00-04:00</updated>
    <summary>Filed: 2026-04-15</summary>
  </entry>
</feed>"""

_FILINGS_ATOM_13G = """\
<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>EDGAR Filing Search</title>
</feed>"""

_FILING_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/schedule13D">
  <schemaVersion>X0202</schemaVersion>
  <headerData>
    <submissionType>SCHEDULE 13D</submissionType>
    <filerInfo>
      <filer>
        <filerCredentials>
          <cik>0001373161</cik>
        </filerCredentials>
      </filer>
    </filerInfo>
  </headerData>
  <formData>
    <coverPageHeader>
      <issuerInfo>
        <issuerCIK>0001793659</issuerCIK>
        <issuerCUSIP>233253103</issuerCUSIP>
        <issuerName>Rush Street Interactive, Inc.</issuerName>
        <address>
          <street1>900 N. Michigan Avenue</street1>
          <street2>Suite 950</street2>
          <city>Chicago</city>
          <stateOrCountry>IL</stateOrCountry>
          <zipCode>60611</zipCode>
        </address>
      </issuerInfo>
    </coverPageHeader>
    <reportingPersons>
      <reportingPersonInfo>
        <reportingPersonCIK>0001373161</reportingPersonCIK>
        <reportingPersonName>BLUHM NEIL</reportingPersonName>
        <typeOfReportingPerson>IN</typeOfReportingPerson>
        <citizenshipOrOrganization>X1</citizenshipOrOrganization>
        <soleVotingPower>100085274.00</soleVotingPower>
        <sharedVotingPower>0.00</sharedVotingPower>
        <aggregateAmountOwned>100085274.00</aggregateAmountOwned>
        <percentOfClass>77.0</percentOfClass>
      </reportingPersonInfo>
      <reportingPersonInfo>
        <reportingPersonCIK>0001556801</reportingPersonCIK>
        <reportingPersonName>Rush Street Interactive GP LLC</reportingPersonName>
        <typeOfReportingPerson>OO</typeOfReportingPerson>
        <citizenshipOrOrganization>DE</citizenshipOrOrganization>
        <aggregateAmountOwned>100085274.00</aggregateAmountOwned>
        <percentOfClass>77.0</percentOfClass>
      </reportingPersonInfo>
    </reportingPersons>
  </formData>
</edgarSubmission>"""


# ---------------------------------------------------------------------------
# Unit tests: pure parsing helpers
# ---------------------------------------------------------------------------


def test_parse_company_hits_from_atom_extracts_ciks():
    hits = _parse_company_hits_from_atom(_COMPANY_SEARCH_ATOM)
    assert len(hits) == 2
    assert hits[0].hit_id == "1793659"
    assert hits[0].name == "RUSH STREET INTERACTIVE, INC."
    assert hits[0].identifiers["edgar_cik"] == "1793659"
    assert hits[1].hit_id == "1556801"


def test_parse_company_hits_from_atom_empty_xml():
    assert _parse_company_hits_from_atom("") == []


def test_parse_company_hits_from_atom_malformed_xml():
    assert _parse_company_hits_from_atom("<not valid xml<<") == []


def test_parse_filing_refs_from_atom_extracts_refs():
    refs = _parse_filing_refs_from_atom(_FILINGS_ATOM_13D)
    assert len(refs) == 1
    ref = refs[0]
    assert ref["filer_cik"] == "1373161"
    assert ref["accession"] == "000110465926057435"
    assert ref["form_type"] == "SCHEDULE 13D"
    assert ref["filed"] == "2026-04-15"


def test_parse_filing_refs_from_atom_empty_feed():
    refs = _parse_filing_refs_from_atom(_FILINGS_ATOM_13G)
    assert refs == []


def test_parse_filing_xml_extracts_issuer_and_reporters():
    parsed = _parse_filing_xml(_FILING_XML, source_url="https://example.com/doc.xml")
    assert parsed is not None

    issuer = parsed["issuer"]
    assert issuer["cik"] == "1793659"
    assert issuer["name"] == "Rush Street Interactive, Inc."
    assert issuer["cusip"] == "233253103"
    assert issuer["address"]["city"] == "Chicago"
    assert issuer["address"]["stateOrCountry"] == "IL"

    reporters = parsed["reporters"]
    assert len(reporters) == 2

    neil = reporters[0]
    assert neil["name"] == "BLUHM NEIL"
    assert neil["is_individual"] is True
    assert neil["citizenship_iso"] == "US"
    assert neil["percent_of_class"] == 77.0
    assert neil["reporter_cik"] == "1373161"

    gp = reporters[1]
    assert gp["name"] == "Rush Street Interactive GP LLC"
    assert gp["is_individual"] is False
    assert gp["citizenship_iso"] == "US"   # DE state maps to US
    assert gp["percent_of_class"] == 77.0


def test_parse_filing_xml_returns_none_on_empty():
    assert _parse_filing_xml("") is None


def test_parse_filing_xml_returns_none_on_missing_structure():
    assert _parse_filing_xml("<root/>") is None


# ---------------------------------------------------------------------------
# Adapter integration tests (HTTP mocked)
# ---------------------------------------------------------------------------


async def test_search_returns_company_hits(httpx_mock: HTTPXMock) -> None:
    search_url = (
        f"{_BROWSE}?company=Rush+Street+Interactive%2C+Inc.&CIK=&type="
        f"&dateb=&owner=include&count=20&search_text=&action=getcompany&output=atom"
    )
    httpx_mock.add_response(url=search_url, text=_COMPANY_SEARCH_ATOM)

    adapter = SecEdgarAdapter()
    hits = await adapter.search("Rush Street Interactive, Inc.", SearchKind.ENTITY)

    assert len(hits) == 2
    assert hits[0].source_id == "sec_edgar"
    assert hits[0].hit_id == "1793659"
    assert hits[0].name == "RUSH STREET INTERACTIVE, INC."
    assert hits[0].is_stub is False


async def test_search_returns_empty_for_person_kind() -> None:
    adapter = SecEdgarAdapter()
    hits = await adapter.search("Alice Smith", SearchKind.PERSON)
    assert hits == []


async def test_fetch_parses_filings(httpx_mock: HTTPXMock) -> None:
    subject_cik = "1793659"

    # Atom feed for SC 13D
    httpx_mock.add_response(
        url=(
            f"{_BROWSE}?action=getcompany&CIK={subject_cik}"
            f"&type=SC+13D&dateb=&owner=include&count=40&search_text=&output=atom"
        ),
        text=_FILINGS_ATOM_13D,
    )
    # Atom feed for SC 13G (empty)
    httpx_mock.add_response(
        url=(
            f"{_BROWSE}?action=getcompany&CIK={subject_cik}"
            f"&type=SC+13G&dateb=&owner=include&count=40&search_text=&output=atom"
        ),
        text=_FILINGS_ATOM_13G,
    )
    # Primary XML document
    httpx_mock.add_response(
        url=(
            f"{_EDGAR_BASE}/Archives/edgar/data/1373161/000110465926057435/primary_doc.xml"
        ),
        text=_FILING_XML,
    )

    adapter = SecEdgarAdapter()
    bundle = await adapter.fetch(subject_cik)

    assert bundle["source_id"] == "sec_edgar"
    assert bundle["issuer_cik"] == subject_cik
    filings = bundle["filings"]
    assert len(filings) == 2  # two reporters from the single filing

    # Both reporters reference the same issuer
    for f in filings:
        assert f["issuer"]["name"] == "Rush Street Interactive, Inc."
        assert f["issuer"]["cusip"] == "233253103"

    # Individual reporter
    individuals = [f for f in filings if f["reporter"]["is_individual"]]
    assert len(individuals) == 1
    assert individuals[0]["reporter"]["name"] == "BLUHM NEIL"
    assert individuals[0]["reporter"]["percent_of_class"] == 77.0


async def test_fetch_returns_stub_when_not_live(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()

    adapter = SecEdgarAdapter()
    bundle = await adapter.fetch("1793659")
    assert bundle.get("is_stub") is True


async def test_fetch_deduplicates_by_reporter_cik(httpx_mock: HTTPXMock) -> None:
    """When same reporter CIK appears in both 13D and 13G feeds, keep newest."""
    subject_cik = "1793659"
    older_filing_atom = _FILINGS_ATOM_13D  # filed 2026-04-15

    # Build a 13G feed referencing a newer filing (same filer CIK, different accession)
    newer_filing_atom = """\
<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <category scheme="https://www.sec.gov/" term="SCHEDULE 13G"/>
    <title>SCHEDULE 13G - 2026-05-01</title>
    <id>urn:tag:sec.gov,2008:accession-number=0001373161-26-000999</id>
    <link rel="alternate" href="https://www.sec.gov/Archives/edgar/data/1373161/000137316126000999/0001373161-26-000999-index.htm"/>
    <updated>2026-05-01T12:00:00-04:00</updated>
    <summary>Filed: 2026-05-01</summary>
  </entry>
</feed>"""

    newer_xml = _FILING_XML.replace(
        "<percentOfClass>77.0</percentOfClass>",
        "<percentOfClass>65.0</percentOfClass>",
    )

    httpx_mock.add_response(
        url=f"{_BROWSE}?action=getcompany&CIK={subject_cik}&type=SC+13D&dateb=&owner=include&count=40&search_text=&output=atom",
        text=older_filing_atom,
    )
    httpx_mock.add_response(
        url=f"{_BROWSE}?action=getcompany&CIK={subject_cik}&type=SC+13G&dateb=&owner=include&count=40&search_text=&output=atom",
        text=newer_filing_atom,
    )
    # Older filing XML
    httpx_mock.add_response(
        url=f"{_EDGAR_BASE}/Archives/edgar/data/1373161/000110465926057435/primary_doc.xml",
        text=_FILING_XML,
    )
    # Newer filing XML
    httpx_mock.add_response(
        url=f"{_EDGAR_BASE}/Archives/edgar/data/1373161/000137316126000999/primary_doc.xml",
        text=newer_xml,
    )

    adapter = SecEdgarAdapter()
    bundle = await adapter.fetch(subject_cik)

    # Neil Bluhm (CIK 1373161) and GP LLC (CIK 1556801) — each should appear once.
    filings = bundle["filings"]
    reporter_names = {f["reporter"]["name"] for f in filings}
    assert "BLUHM NEIL" in reporter_names
    assert "Rush Street Interactive GP LLC" in reporter_names
    assert len(filings) == 2  # not 4

    # The newer 13G filing (65 %) should win for Neil Bluhm
    neil = next(f for f in filings if f["reporter"]["name"] == "BLUHM NEIL")
    assert neil["reporter"]["percent_of_class"] == 65.0


# ---------------------------------------------------------------------------
# BODS mapper tests
# ---------------------------------------------------------------------------


def _make_bundle(percent: float | None = 77.0) -> dict:
    return {
        "source_id": "sec_edgar",
        "issuer_cik": "1793659",
        "filings": [
            {
                "issuer": {
                    "cik": "1793659",
                    "name": "Rush Street Interactive, Inc.",
                    "cusip": "233253103",
                    "address": {
                        "street1": "900 N. Michigan Avenue",
                        "street2": "Suite 950",
                        "city": "Chicago",
                        "stateOrCountry": "IL",
                        "zipCode": "60611",
                    },
                },
                "reporter": {
                    "reporter_cik": "1373161",
                    "name": "BLUHM NEIL",
                    "type_code": "IN",
                    "is_individual": True,
                    "citizenship_iso": "US",
                    "percent_of_class": percent,
                    "sole_voting_power": 100085274.0,
                    "shared_voting_power": 0.0,
                    "aggregate_amount_owned": 100085274.0,
                },
                "filing_url": (
                    "https://www.sec.gov/Archives/edgar/data/1373161/"
                    "000110465926057435/primary_doc.xml"
                ),
                "form_type": "SCHEDULE 13D",
                "filed": "2026-04-15",
            },
            {
                "issuer": {
                    "cik": "1793659",
                    "name": "Rush Street Interactive, Inc.",
                    "cusip": "233253103",
                    "address": {},
                },
                "reporter": {
                    "reporter_cik": "1556801",
                    "name": "Rush Street Interactive GP LLC",
                    "type_code": "OO",
                    "is_individual": False,
                    "citizenship_iso": "US",
                    "percent_of_class": percent,
                    "sole_voting_power": None,
                    "shared_voting_power": None,
                    "aggregate_amount_owned": None,
                },
                "filing_url": (
                    "https://www.sec.gov/Archives/edgar/data/1373161/"
                    "000110465926057435/primary_doc.xml"
                ),
                "form_type": "SCHEDULE 13D",
                "filed": "2026-04-15",
            },
        ],
    }


def test_map_sec_edgar_produces_correct_statement_types():
    bundle = map_sec_edgar(_make_bundle())
    stmts = list(bundle)

    record_types = [s["recordType"] for s in stmts]
    # entity (issuer) + person (Bluhm) + relationship + entity (GP LLC) + relationship
    assert record_types.count("entity") == 2
    assert record_types.count("person") == 1
    assert record_types.count("relationship") == 2
    assert len(stmts) == 5


def test_map_sec_edgar_issuer_entity_identifiers():
    bundle = map_sec_edgar(_make_bundle())
    issuer_stmt = next(s for s in bundle if s["recordType"] == "entity"
                       and "Rush Street" in s["recordDetails"]["name"])
    details = issuer_stmt["recordDetails"]
    assert details["name"] == "Rush Street Interactive, Inc."
    assert details["jurisdiction"]["code"] == "US"

    ids_by_scheme = {i["scheme"]: i["id"] for i in details["identifiers"]}
    assert ids_by_scheme["US-SEC-CIK"] == "1793659"
    assert ids_by_scheme["CUSIP"] == "233253103"


def test_map_sec_edgar_person_statement():
    bundle = map_sec_edgar(_make_bundle())
    person_stmt = next(s for s in bundle if s["recordType"] == "person")
    details = person_stmt["recordDetails"]
    assert details["names"][0]["fullName"] == "BLUHM NEIL"
    assert details["personType"] == "knownPerson"
    nats = details["nationalities"]
    assert any(n["code"] == "US" for n in nats)
    ids_by_scheme = {i["scheme"]: i["id"] for i in details["identifiers"]}
    assert ids_by_scheme["US-SEC-CIK"] == "1373161"


def test_map_sec_edgar_relationship_share_exact():
    bundle = map_sec_edgar(_make_bundle(percent=77.0))
    rel_stmts = [s for s in bundle if s["recordType"] == "relationship"]
    assert len(rel_stmts) == 2
    for rel in rel_stmts:
        interests = rel["recordDetails"]["interests"]
        assert len(interests) == 1
        interest = interests[0]
        assert interest["type"] == "shareholding"
        assert interest["beneficialOwnershipOrControl"] is True
        assert interest["share"]["exact"] == 77.0


def test_map_sec_edgar_relationship_no_percent():
    """When percent_of_class is None, share key is omitted but interest is still emitted."""
    bundle = map_sec_edgar(_make_bundle(percent=None))
    rel_stmts = [s for s in bundle if s["recordType"] == "relationship"]
    for rel in rel_stmts:
        interests = rel["recordDetails"]["interests"]
        assert len(interests) == 1
        interest = interests[0]
        assert interest["type"] == "shareholding"
        assert "share" not in interest


def test_map_sec_edgar_source_block():
    bundle = map_sec_edgar(_make_bundle())
    for stmt in bundle:
        src = stmt["source"]
        assert src["type"] == ["officialRegister"]
        assert "SEC EDGAR" in src["description"]


def test_map_sec_edgar_empty_filings():
    bundle = map_sec_edgar({"source_id": "sec_edgar", "issuer_cik": "123", "filings": []})
    assert len(bundle) == 0


def test_map_sec_edgar_relationship_links_correct_entities():
    bundle = map_sec_edgar(_make_bundle())
    stmts = list(bundle)

    issuer_sid = next(
        s["statementId"] for s in stmts
        if s["recordType"] == "entity" and "Rush Street Interactive, Inc." in s["recordDetails"]["name"]
    )
    # In BODS v0.4, subject is a plain statementId string, not a nested dict.
    for rel in (s for s in stmts if s["recordType"] == "relationship"):
        assert rel["recordDetails"]["subject"] == issuer_sid


# ---------------------------------------------------------------------------
# Cache TTL tests
# ---------------------------------------------------------------------------


def test_cache_get_payload_respects_max_age_days(tmp_path):
    """Live-tier entries older than max_age_days are treated as a cache miss."""
    cache = Cache(root=tmp_path)
    key = "sec_edgar/company/99999"

    # Write an entry then backdating its _cached_at to 10 days ago.
    cache.put(key, {"some": "data"})
    live_path = tmp_path / "cache" / "live" / f"{key}.json"
    wrapper = json.loads(live_path.read_text())
    wrapper["_cached_at"] = time.time() - (10 * 86_400)  # 10 days ago
    live_path.write_text(json.dumps(wrapper))

    # With a 7-day TTL the entry is stale → miss.
    assert cache.get_payload(key, max_age_days=7) is None

    # With a 14-day TTL the entry is still fresh → hit.
    result = cache.get_payload(key, max_age_days=14)
    assert result is not None
    assert result[0] == {"some": "data"}


def test_cache_get_payload_no_ttl_always_returns_hit(tmp_path):
    """Without max_age_days, entries never expire regardless of age."""
    cache = Cache(root=tmp_path)
    key = "sec_edgar/company/11111"
    cache.put(key, "value")

    live_path = tmp_path / "cache" / "live" / f"{key}.json"
    wrapper = json.loads(live_path.read_text())
    wrapper["_cached_at"] = 0.0  # epoch — ancient
    live_path.write_text(json.dumps(wrapper))

    result = cache.get_payload(key)
    assert result is not None
    assert result[0] == "value"


# ---------------------------------------------------------------------------
# Directional filter tests
# ---------------------------------------------------------------------------

# An XML where the issuer is a DIFFERENT company (not the subject).
# This simulates a filing BY the subject company about its own position
# in another company (subject = reporter, not issuer).
_FILING_XML_WRONG_ISSUER = """\
<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/schedule13D">
  <schemaVersion>X0202</schemaVersion>
  <headerData>
    <submissionType>SCHEDULE 13D</submissionType>
    <filerInfo>
      <filer>
        <filerCredentials>
          <cik>0001793659</cik>
        </filerCredentials>
      </filer>
    </filerInfo>
  </headerData>
  <formData>
    <coverPageHeader>
      <issuerInfo>
        <issuerCIK>0009999999</issuerCIK>
        <issuerCUSIP>XXXXXXXXX</issuerCUSIP>
        <issuerName>Some Other Corp</issuerName>
      </issuerInfo>
    </coverPageHeader>
    <reportingPersons>
      <reportingPersonInfo>
        <reportingPersonCIK>0001793659</reportingPersonCIK>
        <reportingPersonName>RUSH STREET INTERACTIVE INC</reportingPersonName>
        <typeOfReportingPerson>OO</typeOfReportingPerson>
        <citizenshipOrOrganization>DE</citizenshipOrOrganization>
        <aggregateAmountOwned>5000000.00</aggregateAmountOwned>
        <percentOfClass>6.5</percentOfClass>
      </reportingPersonInfo>
    </reportingPersons>
  </formData>
</edgarSubmission>"""


@pytest.mark.asyncio
async def test_directional_filter_discards_filings_by_subject(
    httpx_mock: HTTPXMock,
) -> None:
    """Filings where issuerCIK != subject_cik are excluded from filings[] and
    counted in filing_by_count, not legacy_filing_count."""
    subject_cik = "1793659"

    # 13D atom returns one post-mandate filing stored under the subject's CIK
    # path — but the XML itself shows a DIFFERENT company as the issuer (the
    # subject is actually the reporter/filer here).
    wrong_issuer_atom = f"""\
<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <category scheme="https://www.sec.gov/" term="SCHEDULE 13D"/>
    <title>SCHEDULE 13D - 2025-03-01</title>
    <id>urn:tag:sec.gov,2008:accession-number=0001793659-25-000001</id>
    <link rel="alternate" href="https://www.sec.gov/Archives/edgar/data/{subject_cik}/000179365925000001/0001793659-25-000001-index.htm"/>
    <updated>2025-03-01T12:00:00-04:00</updated>
    <summary>Filed: 2025-03-01</summary>
  </entry>
</feed>"""

    httpx_mock.add_response(
        url=f"{_BROWSE}?action=getcompany&CIK={subject_cik}&type=SC+13D&dateb=&owner=include&count=40&search_text=&output=atom",
        text=wrong_issuer_atom,
    )
    httpx_mock.add_response(
        url=f"{_BROWSE}?action=getcompany&CIK={subject_cik}&type=SC+13G&dateb=&owner=include&count=40&search_text=&output=atom",
        text=_FILINGS_ATOM_13G,
    )
    httpx_mock.add_response(
        url=f"{_EDGAR_BASE}/Archives/edgar/data/{subject_cik}/000179365925000001/primary_doc.xml",
        text=_FILING_XML_WRONG_ISSUER,
    )

    adapter = SecEdgarAdapter()
    bundle = await adapter.fetch(subject_cik)

    # The filing was found (structured_filing_count = 1) but filtered out.
    assert bundle["filings"] == []
    assert bundle["structured_filing_count"] == 1
    assert bundle["legacy_filing_count"] == 0
    # coverage_note should mention the by-count
    assert "1" in bundle.get("coverage_note", "")
    assert "excluded" in bundle.get("coverage_note", "")


@pytest.mark.asyncio
async def test_legacy_filing_count_excludes_by_filings(
    httpx_mock: HTTPXMock,
) -> None:
    """legacy_filing_count counts pre-mandate filings about the subject only.

    The atom can include both filings about the subject AND filings by the
    subject (stored under the same CIK path in EDGAR).  Legacy pre-mandate
    filings are counted without fetching their XML, so the directional filter
    cannot be applied to them.  However, their count should be surfaced
    accurately — this test confirms that legacy_filing_count increments for
    ALL pre-mandate entries seen in the atom regardless of direction (since
    we can't know direction without fetching the XML), and that the field
    is present in the bundle.
    """
    subject_cik = "1793659"

    legacy_atom = f"""\
<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <category scheme="https://www.sec.gov/" term="SCHEDULE 13G"/>
    <title>SCHEDULE 13G - 2024-11-14</title>
    <id>urn:tag:sec.gov,2008:accession-number=0001793659-24-000100</id>
    <link rel="alternate" href="https://www.sec.gov/Archives/edgar/data/{subject_cik}/000179365924000100/0001793659-24-000100-index.htm"/>
    <updated>2024-11-14T12:00:00-05:00</updated>
    <summary>Filed: 2024-11-14</summary>
  </entry>
  <entry>
    <category scheme="https://www.sec.gov/" term="SCHEDULE 13G"/>
    <title>SCHEDULE 13G - 2024-06-01</title>
    <id>urn:tag:sec.gov,2008:accession-number=0001793659-24-000050</id>
    <link rel="alternate" href="https://www.sec.gov/Archives/edgar/data/{subject_cik}/000179365924000050/0001793659-24-000050-index.htm"/>
    <updated>2024-06-01T12:00:00-04:00</updated>
    <summary>Filed: 2024-06-01</summary>
  </entry>
</feed>"""

    httpx_mock.add_response(
        url=f"{_BROWSE}?action=getcompany&CIK={subject_cik}&type=SC+13D&dateb=&owner=include&count=40&search_text=&output=atom",
        text=_FILINGS_ATOM_13G,  # empty
    )
    httpx_mock.add_response(
        url=f"{_BROWSE}?action=getcompany&CIK={subject_cik}&type=SC+13G&dateb=&owner=include&count=40&search_text=&output=atom",
        text=legacy_atom,
    )

    adapter = SecEdgarAdapter()
    bundle = await adapter.fetch(subject_cik)

    assert bundle["filings"] == []
    assert bundle["structured_filing_count"] == 0
    assert bundle["legacy_filing_count"] == 2
    assert bundle["latest_filing_date"] == "2024-11-14"
    assert "coverage_note" in bundle
