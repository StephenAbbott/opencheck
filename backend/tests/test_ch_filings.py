"""Declarant and filing provenance — which Companies House filing notified a PSC.

The ticket "Declarant and filing provenance in BODS statements". The shapes
are the register's own, read on 3 Nov 2026: Monzo Bank Limited (09446231)
and a Scottish company (SC651281). Stephen's decisions: an annotation (not
the ``source`` block), every company in the bundle, and a stated absence
that is never a signal.
"""

from __future__ import annotations

from typing import Any

import pytest
from pytest_httpx import HTTPXMock

from opencheck.bods import ch_filings, mapper
from opencheck.bods.annotations import validate_annotations
from opencheck.config import get_settings
from opencheck.sources.companies_house import CompaniesHouseAdapter

_API = "https://api.company-information.service.gov.uk"
_SITE = "https://find-and-update.company-information.service.gov.uk"


def _filing(type_: str, date: str, txid: str, *, category: str, **values: str) -> dict[str, Any]:
    return {
        "transaction_id": txid,
        "type": type_,
        "date": date,
        "category": category,
        "description_values": values or None,
        "links": {
            "self": f"/company/X/filing-history/{txid}",
            "document_metadata": f"https://document-api.company-information.service.gov.uk/document/{txid}",
        },
    }


# Monzo Bank Limited's PSC filings as the register lists them (newest first).
MONZO_FILINGS = {
    "filing_history_status": "filing-history-available",
    "total_count": 5,
    "items": [
        _filing(
            "PSC02", "2023-10-30", "MzM5NzMzMDY2NWFkaXF6a2N4",
            category="persons-with-significant-control",
            psc_name="Monzo Bank Holding Group Limited", notification_date="2023-09-12",
        ),
        _filing(
            "PSC07", "2021-07-07", "MzMwNjgwMDI0MGFkaXF6a2N4",
            category="persons-with-significant-control",
            psc_name="Passion Capital Fs (Gp) Llp", cessation_date="2021-02-11",
        ),
        _filing(
            "PSC02", "2019-09-19", "TXGPLLP",
            category="persons-with-significant-control",
            psc_name="Passion Capital Fs (Gp) Llp", notification_date="2016-04-06",
        ),
        {**_filing("CS01", "2017-02-20", "TXCS01", category="confirmation-statement"),
         "description_values": {"made_up_date": "2017-02-06"}},
        # A legacy annual return shares the category and is never cited.
        {**_filing("AR01", "2016-05-09", "TXAR01", category="confirmation-statement"),
         "description_values": {"made_up_date": "2016-05-01"}},
    ],
}


def _psc(name: str, notified: str, ceased: str | None = None, *, kind: str = "corporate-entity-person-with-significant-control", **extra: Any) -> dict[str, Any]:
    return {
        "kind": kind,
        "name": name,
        "notified_on": notified,
        "ceased_on": ceased,
        "natures_of_control": ["ownership-of-shares-75-to-100-percent"],
        **extra,
    }


def _history(payload: dict[str, Any]) -> ch_filings.FilingHistory:
    history = ch_filings.history_of(payload)
    assert history is not None
    return history


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------


def test_a_notification_matches_on_date_and_name() -> None:
    facts = ch_filings.match_notification(
        _psc("Monzo Bank Holding Group Limited", "2023-09-12"),
        _history(MONZO_FILINGS), number="09446231", date_of_creation="2015-02-06",
    )
    assert facts == {
        "event": "notification",
        "match": "filing",
        "declarant": "company",
        "type": "PSC02",
        "transactionId": "MzM5NzMzMDY2NWFkaXF6a2N4",
        "date": "2023-10-30",
        "documentUrl": f"{_SITE}/company/09446231/filing-history/MzM5NzMzMDY2NWFkaXF6a2N4/document?format=pdf&download=0",
    }


def test_the_date_alone_never_matches_another_partys_filing() -> None:
    """Passion Capital Fs Lp was notified on 6 April 2016, the day the only
    PSC02 of that date names Passion Capital Fs (Gp) Llp. It is not its
    filing; the 2016 confirmation-statement route is named instead."""
    facts = ch_filings.match_notification(
        _psc("Passion Capital Fs Lp", "2016-04-06", "2016-04-06"),
        _history(MONZO_FILINGS), number="09446231", date_of_creation="2015-02-06",
    )
    assert facts is not None
    assert facts["match"] == "confirmation_statement"
    assert facts["type"] == "CS01" and facts["transactionId"] == "TXCS01"


def test_a_title_on_the_psc_record_does_not_stop_a_match() -> None:
    history = _history({"items": [_filing(
        "PSC01", "2020-02-10", "MzI1Njg5MDE0MGFkaXF6a2N4",
        category="persons-with-significant-control",
        psc_name="Nicola Wingate", notification_date="2020-02-10",
    )]})
    psc = _psc(
        "Mrs Nicola Wingate", "2020-02-10",
        kind="individual-person-with-significant-control",
        name_elements={"title": "Mrs", "forename": "Nicola", "surname": "Wingate"},
    )
    facts = ch_filings.match_notification(psc, history, number="SC651281", date_of_creation="2020-01-13")
    assert facts is not None and facts["match"] == "filing" and facts["type"] == "PSC01"


def test_a_psc_named_at_incorporation_is_on_the_incorporation_filing() -> None:
    history = _history({"items": [
        _filing("NEWINC", "2020-01-13", "MzI1NDMxNDcxNWFkaXF6a2N4", category="incorporation"),
    ]})
    psc = _psc("Mr Timothy Wingate", "2020-01-13", kind="individual-person-with-significant-control")
    facts = ch_filings.match_notification(psc, history, number="SC651281", date_of_creation="2020-01-13")
    assert facts is not None
    assert (facts["match"], facts["type"]) == ("incorporation", "NEWINC")
    assert "at incorporation" in ch_filings.describe(facts, psc)


def test_no_matching_filing_is_said_when_the_history_was_read_whole() -> None:
    psc = _psc("Someone Else Limited", "2022-05-01")
    facts = ch_filings.match_notification(
        psc, _history(MONZO_FILINGS), number="09446231", date_of_creation="2015-02-06"
    )
    assert facts == {"event": "notification", "match": "none", "declarant": "company"}
    assert "No filing" in ch_filings.describe(facts, psc)


def test_an_absence_is_not_claimed_from_a_history_read_in_part() -> None:
    partial = {**MONZO_FILINGS, "total_count": 400}
    assert ch_filings.history_of(partial).complete is False  # type: ignore[union-attr]
    assert ch_filings.match_notification(
        _psc("Someone Else Limited", "2022-05-01"),
        _history(partial), number="09446231", date_of_creation=None,
    ) is None


def test_an_unread_history_says_nothing() -> None:
    assert ch_filings.history_of(None) is None
    assert ch_filings.history_of({"items": [], "opencheck_unavailable": "HTTP 503"}) is None
    assert ch_filings.psc_annotations(
        _psc("Monzo Bank Holding Group Limited", "2023-09-12"), None,
        number="09446231", date_of_creation=None,
    ) == []


def test_a_cessation_matches_its_psc07() -> None:
    psc = _psc("Passion Capital Fs (Gp) Llp", "2016-04-06", "2021-02-11")
    facts = ch_filings.match_cessation(psc, _history(MONZO_FILINGS), number="09446231")
    assert facts is not None
    assert (facts["type"], facts["transactionId"]) == ("PSC07", "MzMwNjgwMDI0MGFkaXF6a2N4")


def test_an_old_filing_with_no_document_links_to_the_filing_history() -> None:
    filing = {"transaction_id": "T1", "type": "PSC01", "links": {"self": "/x"}}
    assert ch_filings.document_url("01234567", filing) == f"{_SITE}/company/01234567/filing-history"


# ---------------------------------------------------------------------------
# The BODS mapping
# ---------------------------------------------------------------------------


def _bundle(pscs: list[dict[str, Any]], filing_history: Any) -> dict[str, Any]:
    return {
        "source_id": "companies_house",
        "company_number": "09446231",
        "profile": {"company_number": "09446231", "company_name": "MONZO BANK LIMITED", "date_of_creation": "2015-02-06"},
        "officers": {"items": []},
        "pscs": {"items": pscs},
        "psc_statements": {"items": []},
        "filing_history": filing_history,
    }


def _psc_relationships(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    out = list(mapper.map_companies_house(bundle))
    return [
        s for s in out
        if s.get("recordType") == "relationship"
        and (s["recordDetails"].get("interests") or [{}])[0].get("type") != "seniorManagingOfficial"
    ]


def test_psc_relationships_carry_their_filings_as_commenting_annotations() -> None:
    pscs = [
        _psc("Monzo Bank Holding Group Limited", "2023-09-12"),
        _psc("Passion Capital Fs (Gp) Llp", "2016-04-06", "2021-02-11"),
    ]
    rels = _psc_relationships(_bundle(pscs, MONZO_FILINGS))
    assert len(rels) == 2
    for rel in rels:
        assert validate_annotations(rel) == []
        assert rel["source"]["url"].endswith("/company/09446231")  # source is the register record
    current, ceased = rels
    note = ch_filings.filing_of(current)
    assert note is not None and note["transactionId"] == "MzM5NzMzMDY2NWFkaXF6a2N4"
    ann = next(a for a in current["annotations"] if ch_filings.FILING_PROPERTY in a)
    assert ann["motivation"] == "commenting"
    assert ann["statementPointerTarget"] == "/recordDetails/interests"
    assert ann["url"] == note["documentUrl"]
    assert "PSC02" in ann["description"] and "MzM5NzMzMDY2NWFkaXF6a2N4" in ann["description"]
    assert "does not publish who presented it" in ann["description"]
    assert ch_filings.filing_of(ceased)["type"] == "PSC02"  # type: ignore[index]
    cessation = ch_filings.filing_of(ceased, "cessation")
    assert cessation is not None and cessation["type"] == "PSC07"
    end_ann = next(a for a in ceased["annotations"] if (a.get(ch_filings.FILING_PROPERTY) or {}).get("event") == "cessation")
    assert end_ann["statementPointerTarget"] == "/recordDetails/interests/0/endDate"


def test_the_filing_does_not_move_a_statement_id() -> None:
    """Annotations are outside the content-addressed digest."""
    pscs = [_psc("Monzo Bank Holding Group Limited", "2023-09-12")]
    with_filing = _psc_relationships(_bundle(pscs, MONZO_FILINGS))
    without = _psc_relationships(_bundle(pscs, None))
    assert [r["statementId"] for r in with_filing] == [r["statementId"] for r in without]
    assert "annotations" not in without[0] or all(
        ch_filings.FILING_PROPERTY not in a for a in without[0]["annotations"]
    )


def test_a_psc_with_no_matching_filing_says_so_on_its_statement() -> None:
    rels = _psc_relationships(_bundle([_psc("Someone Else Limited", "2022-05-01")], MONZO_FILINGS))
    facts = ch_filings.filing_of(rels[0])
    assert facts == {"event": "notification", "match": "none", "declarant": "company"}
    ann = next(a for a in rels[0]["annotations"] if ch_filings.FILING_PROPERTY in a)
    assert "url" not in ann
    assert validate_annotations(rels[0]) == []


# ---------------------------------------------------------------------------
# The adapter
# ---------------------------------------------------------------------------


@pytest.fixture
def _live(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "test-key")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _list_url(number: str, path: str, start: int = 0) -> str:
    sep = "&" if "?" in path else "?"
    return f"{_API}/company/{number}{path}{sep}items_per_page=100&start_index={start}"


_FH_PATH = f"/filing-history?category={ch_filings.FILING_CATEGORIES}"


def _mock_company(httpx_mock: HTTPXMock, number: str, pscs: list[dict[str, Any]]) -> None:
    httpx_mock.add_response(url=f"{_API}/company/{number}", json={"company_number": number, "company_name": "X"})
    httpx_mock.add_response(url=_list_url(number, "/officers"), json={"items": []})
    httpx_mock.add_response(
        url=_list_url(number, "/persons-with-significant-control"), json={"items": pscs}
    )
    httpx_mock.add_response(
        url=_list_url(number, "/persons-with-significant-control-statements"), status_code=404, json={}
    )


@pytest.mark.usefixtures("_live")
async def test_the_adapter_reads_the_filing_history_whole_on_total_count(httpx_mock: HTTPXMock) -> None:
    _mock_company(httpx_mock, "09446231", [_psc("Monzo Bank Holding Group Limited", "2023-09-12")])
    first = {**MONZO_FILINGS, "total_count": 6}
    httpx_mock.add_response(url=_list_url("09446231", _FH_PATH), json=first)
    httpx_mock.add_response(
        url=_list_url("09446231", _FH_PATH, 5),
        json={"total_count": 6, "items": [_filing("CS01", "2018-02-20", "TX2", category="confirmation-statement")]},
    )
    bundle = await CompaniesHouseAdapter().fetch("09446231")
    assert len(bundle["filing_history"]["items"]) == 6
    assert ch_filings.history_of(bundle["filing_history"]).complete  # type: ignore[union-attr]


@pytest.mark.usefixtures("_live")
async def test_a_company_with_no_pscs_spends_no_filing_history_call(httpx_mock: HTTPXMock) -> None:
    _mock_company(httpx_mock, "04366849", [])
    bundle = await CompaniesHouseAdapter().fetch("04366849")
    assert bundle["filing_history"] is None
    assert not any("filing-history" in str(r.url) for r in httpx_mock.get_requests())


@pytest.mark.usefixtures("_live")
async def test_a_failed_filing_history_read_leaves_the_lookup_whole(httpx_mock: HTTPXMock) -> None:
    _mock_company(httpx_mock, "09446231", [_psc("Monzo Bank Holding Group Limited", "2023-09-12")])
    httpx_mock.add_response(url=_list_url("09446231", _FH_PATH), status_code=503)
    adapter = CompaniesHouseAdapter()
    bundle = await adapter.fetch("09446231")
    assert bundle["filing_history"] == {"items": [], "opencheck_unavailable": "HTTP 503"}
    assert not adapter._cache.has("companies_house/company/09446231/filing-history-psc")
    rels = _psc_relationships(bundle)
    assert ch_filings.filing_of(rels[0]) is None


# ---------------------------------------------------------------------------
# Validity, the RDF export and the frontend copy
# ---------------------------------------------------------------------------


def test_the_annotated_statements_pass_lib_cove_bods() -> None:
    pytest.importorskip("libcovebods")
    from tests.test_bods_libcovebods import assert_valid

    pscs = [
        _psc("Monzo Bank Holding Group Limited", "2023-09-12"),
        _psc("Passion Capital Fs (Gp) Llp", "2016-04-06", "2021-02-11"),
        _psc("Someone Else Limited", "2022-05-01"),
    ]
    assert_valid(mapper.map_companies_house(_bundle(pscs, MONZO_FILINGS)), "ch filings")


def test_the_rdf_export_keeps_the_sentence_and_the_filing_link() -> None:
    pytest.importorskip("rdflib")
    from opencheck.bods import rdf

    bundle = _bundle([_psc("Monzo Bank Holding Group Limited", "2023-09-12")], MONZO_FILINGS)
    note = ch_filings.filing_of(_psc_relationships(bundle)[0])
    text = rdf.to_rdf(list(mapper.map_companies_house(bundle)), fmt="nquads")
    assert note is not None and note["documentUrl"] in text
    assert "Notified to Companies House on form PSC02" in text


def test_the_frontend_copy_names_the_same_property_and_matches() -> None:
    import re
    from pathlib import Path

    ts = (Path(__file__).resolve().parents[2] / "frontend/src/lib/filingProvenance.ts").read_text()
    prop = re.search(r'export const FILING_PROPERTY = "([^"]+)"', ts)
    assert prop and prop.group(1) == ch_filings.FILING_PROPERTY
    block = re.search(r"export const FILING_MATCHES = \[([^\]]*)\]", ts)
    assert block
    assert tuple(re.findall(r'"([^"]+)"', block.group(1))) == ch_filings.MATCHES


# ---------------------------------------------------------------------------
# The second tier — found on production data, 3 Nov 2026
# ---------------------------------------------------------------------------


def test_an_llp_files_the_ll_forms() -> None:
    """OC302604 (an LLP in the ASDA bundle) notifies on LLPSC01 and ceases on
    LLPSC07, and files the members without their middle names."""
    history = _history({"items": [
        _filing("LLPSC01", "2023-03-03", "TXLL1", category="persons-with-significant-control",
                psc_name="Thomas Mitchell", notification_date="2023-01-26"),
        _filing("LLPSC07", "2023-02-10", "TXLL7", category="persons-with-significant-control",
                psc_name="Stephen James Robertson", cessation_date="2022-12-22"),
    ]})
    member = _psc("Mr Thomas Andrew Mitchell", "2023-01-26", kind="individual-person-with-significant-control",
                  name_elements={"title": "Mr", "forename": "Thomas", "middle_name": "Andrew", "surname": "Mitchell"})
    assert ch_filings.match_notification(member, history, number="OC302604", date_of_creation=None)["type"] == "LLPSC01"  # type: ignore[index]
    former = _psc("Mr Stephen James Robertson", "2016-04-06", "2022-12-22", kind="individual-person-with-significant-control")
    assert ch_filings.match_cessation(former, history, number="OC302604")["type"] == "LLPSC07"  # type: ignore[index]


def test_a_company_that_re_registered_is_still_the_company_its_filing_named() -> None:
    """Bellis Finco Plc was notified (PSC02 of 2020-09-09) as Bellis Finco Limited."""
    history = _history({"items": [
        _filing("PSC02", "2020-09-09", "TXBF", category="persons-with-significant-control",
                psc_name="Bellis Finco Limited", notification_date="2020-09-05"),
        _filing("PSC07", "2020-09-09", "TXZV", category="persons-with-significant-control",
                psc_name="Zuber Vali Issa", cessation_date="2020-09-05"),
    ]})
    facts = ch_filings.match_notification(_psc("Bellis Finco Plc", "2020-09-05"), history, number="12855280", date_of_creation="2020-09-03")
    assert facts is not None and facts["transactionId"] == "TXBF"


def test_a_near_match_counts_only_when_it_is_the_only_one() -> None:
    history = _history({"items": [
        _filing("PSC01", "2019-01-24", "A", category="persons-with-significant-control",
                psc_name="Lee Madine", notification_date="2019-01-01"),
        _filing("PSC01", "2019-01-24", "B", category="persons-with-significant-control",
                psc_name="Lee Madine", notification_date="2019-01-01"),
    ]})
    psc = _psc("Mr Lee Micheal Madine", "2019-01-01", kind="individual-person-with-significant-control",
               name_elements={"forename": "Lee", "middle_name": "Micheal", "surname": "Madine"})
    assert ch_filings.match_notification(psc, history, number="11730085", date_of_creation=None)["match"] == "none"  # type: ignore[index]
    # …and a single shared token is never enough.
    one = _history({"items": [_filing("PSC01", "2019-01-24", "C", category="persons-with-significant-control",
                                      psc_name="Madine", notification_date="2019-01-01")]})
    assert ch_filings.match_notification(psc, one, number="11730085", date_of_creation=None)["match"] == "none"  # type: ignore[index]


def test_a_psc_given_before_june_2017_is_on_the_next_confirmation_statement() -> None:
    """HSBC Bank plc (00014259): HSBC Holdings plc notified 26 Oct 2016, on no
    PSC form. The confirmation statement made up to 7 Feb 2017 carried it, not
    the one made up to 20 Dec 2017, and not an annual return."""
    def cs(made_up: str, filed: str, txid: str) -> dict[str, Any]:
        return {**_filing("CS01", filed, txid, category="confirmation-statement"),
                "description_values": {"made_up_date": made_up}}

    history = _history({"items": [
        cs("2017-12-20", "2017-12-20", "LATER"),
        cs("2017-02-07", "2017-02-09", "FIRST"),
        {**_filing("AR01", "2016-05-09", "AR", category="confirmation-statement"),
         "description_values": {"made_up_date": "2016-05-01"}},
    ]})
    psc = _psc("Hsbc Holdings Plc", "2016-10-26", "2018-10-01")
    facts = ch_filings.match_notification(psc, history, number="00014259", date_of_creation="1836-01-01")
    assert facts is not None and (facts["match"], facts["transactionId"]) == ("confirmation_statement", "FIRST")
    assert "made up to on or after 2016-10-26" in ch_filings.describe(facts, psc)
    # After 26 June 2017 a PSC form was required: no confirmation statement is cited.
    late = _psc("Someone Plc", "2017-07-01")
    assert ch_filings.match_notification(late, history, number="00014259", date_of_creation=None)["match"] == "none"  # type: ignore[index]
