"""Phase 100 — statementDate from the register's own declaration date.

Phase 99 separated the clocks and gave `statementDate` a sane fallback: the
date OpenCheck retrieved the payload, rather than today. This phase wires the
first three sources that actually publish a declaration date of their own, so
those statements say when the *register* asserted the fact rather than when we
happened to fetch it.

It also corrects a misfiling. Those dates already existed in the mapper — but
they were passed as `publicationDetails.publicationDate`, which BODS defines as
the date *this* statement was published by the publisher named in that same
block. That publisher is OpenCheck. A PSC notified in 2016 was therefore
emitting a statement OpenCheck had "published" in 2016. Open Ownership's own
bundles model it the other way round, and correctly: statementDate 2024-06-06
(the source's date), publicationDate 2025-02-28 (OO's own).

Two further sources were passing something worse still — an interest *start*
date (`appointed_on`, INPI `start_date`) as a publication date. That is the
first clock, "when it was true", copied into the fourth. Those are removed
rather than promoted: neither register publishes a per-officer notification
date, so there is no declaration date to use and the retrieval-date fallback is
the honest answer.
"""

from __future__ import annotations

from datetime import date

import pytest

from opencheck.bods.mapper import (
    _gleif_registration_date,
    map_companies_house,
    map_gleif,
    map_sec_edgar,
)


TODAY = date.today().isoformat()


def _rels(bundle):
    return [s for s in bundle if s["recordType"] == "relationship"]


def _entities(bundle):
    return [s for s in bundle if s["recordType"] == "entity"]


# ---------------------------------------------------------------------------
# GLEIF — registration.lastUpdateDate
# ---------------------------------------------------------------------------


def _gleif_bundle(last_update: str = "2023-03-31T07:01:00Z") -> dict:
    return {
        "lei": "213800LH1BZH3DI6G760",
        "record": {
            "attributes": {
                "lei": "213800LH1BZH3DI6G760",
                "registration": {"lastUpdateDate": last_update},
                "entity": {
                    "legalName": {"name": "EXAMPLE PLC"},
                    "jurisdiction": "GB",
                },
            }
        },
    }


class TestGleifRegistrationDate:
    def test_helper_takes_the_date_portion(self):
        # GLEIF publishes an ISO 8601 datetime; BODS date fields want YYYY-MM-DD.
        assert (
            _gleif_registration_date({"registration": {"lastUpdateDate": "2023-03-31T07:01:00Z"}})
            == "2023-03-31"
        )

    def test_helper_tolerates_a_missing_registration_block(self):
        assert _gleif_registration_date({}) is None
        assert _gleif_registration_date(None) is None

    def test_helper_tolerates_an_empty_date(self):
        assert _gleif_registration_date({"registration": {"lastUpdateDate": ""}}) is None

    def test_entity_statement_date_is_the_lei_record_update(self):
        stmt = _entities(map_gleif(_gleif_bundle()))[0]
        assert stmt["statementDate"] == "2023-03-31"

    def test_publication_date_stays_openchecks_own(self):
        stmt = _entities(map_gleif(_gleif_bundle()))[0]
        assert stmt["publicationDetails"]["publicationDate"] == TODAY
        assert stmt["publicationDetails"]["publisher"]["name"] == "OpenCheck"

    def test_the_two_clocks_are_distinguishable(self):
        stmt = _entities(map_gleif(_gleif_bundle()))[0]
        assert stmt["statementDate"] != stmt["publicationDetails"]["publicationDate"]

    def test_missing_registration_date_falls_back(self):
        bundle = _gleif_bundle()
        bundle["record"]["attributes"].pop("registration")
        stmt = _entities(map_gleif(bundle))[0]
        # No provenance is active in a bare mapper call, so the Phase 99 chain
        # lands on today. The point is that it does not crash or emit None.
        assert stmt["statementDate"] == TODAY

    def test_level_2_relationship_uses_the_subject_record_date(self):
        """With no relationship (RR) record in the bundle, a parent edge falls
        back to the subject's Level 1 date — the subject is the start node
        that reports the relationship (Phase 313; before it this was the only
        date available, because the RR record was never fetched)."""
        bundle = _gleif_bundle()
        bundle["direct_parent"] = {
            "attributes": {
                "lei": "5493001KJTIIGC8Y1R12",
                "registration": {"lastUpdateDate": "2020-01-02T00:00:00Z"},
                "entity": {
                    "legalName": {"name": "PARENT HOLDINGS"},
                    "jurisdiction": "GB",
                },
            }
        }
        out = map_gleif(bundle)
        rel = _rels(out)[0]
        assert rel["statementDate"] == "2023-03-31"  # subject's, not the parent's

        # The parent's own entity statement still carries the parent's date.
        parent = [e for e in _entities(out) if e["recordDetails"]["name"] == "PARENT HOLDINGS"]
        assert parent and parent[0]["statementDate"] == "2020-01-02"

    def test_level_2_parent_relationship_uses_its_own_rr_record(self):
        """Phase 313: the RR record's own lastUpdateDate dates the edge, and its
        RELATIONSHIP_PERIOD becomes the interest's startDate/endDate."""
        bundle = _gleif_bundle()
        bundle["direct_parent"] = _gleif_l1("5493001KJTIIGC8Y1R12", "PARENT", "2020-01-02")
        bundle["direct_parent_relationship"] = _gleif_rr(
            "213800LH1BZH3DI6G760", "5493001KJTIIGC8Y1R12",
            last_update="2025-11-03T09:12:44Z", start="2014-01-01T00:00:00Z",
        )
        rel = _rels(map_gleif(bundle))[0]
        assert rel["statementDate"] == "2025-11-03"
        assert rel["recordDetails"]["interests"][0]["startDate"] == "2014-01-01"
        assert "endDate" not in rel["recordDetails"]["interests"][0]

    def test_level_2_child_relationship_uses_its_rr_record_or_the_childs_date(self):
        """A direct-child edge is reported by the child. Its RR record dates it;
        without one, the CHILD's Level 1 date does — never the looked-up
        parent's, which said nothing about the child's filing (Phase 313)."""
        bundle = _gleif_bundle()
        bundle["direct_children"] = [
            _gleif_l1("CHILDAAAAAAAAAAAAA01", "CHILD ONE", "2024-02-05"),
            _gleif_l1("CHILDBBBBBBBBBBBBB02", "CHILD TWO", "2022-07-19"),
        ]
        bundle["direct_child_relationships"] = {
            "CHILDAAAAAAAAAAAAA01": _gleif_rr(
                "CHILDAAAAAAAAAAAAA01", "213800LH1BZH3DI6G760",
                last_update="2026-01-15T00:00:00Z", start="2019-06-30T00:00:00Z",
            )
        }
        rels = {r["recordDetails"]["subject"]: r for r in _rels(map_gleif(bundle))}
        by_child = {
            e["recordDetails"]["name"]: rels[e["statementId"]]
            for e in _entities(map_gleif(bundle))
            if e["statementId"] in rels
        }
        assert by_child["CHILD ONE"]["statementDate"] == "2026-01-15"
        assert by_child["CHILD ONE"]["recordDetails"]["interests"][0]["startDate"] == "2019-06-30"
        assert by_child["CHILD TWO"]["statementDate"] == "2022-07-19"
        assert "startDate" not in by_child["CHILD TWO"]["recordDetails"]["interests"][0]

    def test_level_2_statement_ids_do_not_move(self):
        """Dating an edge from its RR record must not re-key it: a saved report
        or watchlist baseline holding the old statementId still matches."""
        plain = _gleif_bundle()
        plain["direct_parent"] = _gleif_l1("5493001KJTIIGC8Y1R12", "PARENT", "2020-01-02")
        dated = dict(plain)
        dated["direct_parent_relationship"] = _gleif_rr(
            "213800LH1BZH3DI6G760", "5493001KJTIIGC8Y1R12", last_update="2025-11-03T00:00:00Z",
        )
        assert [s["statementId"] for s in map_gleif(plain)] == [
            s["statementId"] for s in map_gleif(dated)
        ]

    def test_reporting_exception_bridge_carries_the_subject_date(self):
        """The bridge party and its relationship are one claim filed with the
        subject's record, so both carry its date (Phase 313; the bridge used to
        fall to the day it was read)."""
        bundle = _gleif_bundle()
        bundle["direct_parent_exception"] = {
            "attributes": {"category": "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT",
                           "reason": "NATURAL_PERSONS"}
        }
        out = map_gleif(bundle)
        bridge = [s for s in out if s["recordType"] == "person"]
        assert bridge and bridge[0]["statementDate"] == "2023-03-31"
        assert _rels(out)[0]["statementDate"] == "2023-03-31"


def _gleif_l1(lei: str, name: str, last_update: str) -> dict:
    return {
        "attributes": {
            "lei": lei,
            "registration": {"lastUpdateDate": f"{last_update}T00:00:00Z"},
            "entity": {"legalName": {"name": name}, "jurisdiction": "GB"},
        }
    }


def _gleif_rr(child: str, parent: str, *, last_update: str, start: str | None = None,
              end: str | None = None) -> dict:
    periods = []
    if start or end:
        period = {"type": "RELATIONSHIP_PERIOD"}
        if start:
            period["startDate"] = start
        if end:
            period["endDate"] = end
        periods.append(period)
    return {
        "type": "relationship-records",
        "attributes": {
            "relationship": {
                "startNode": {"id": child, "type": "LEI"},
                "endNode": {"id": parent, "type": "LEI"},
                "type": "IS_DIRECTLY_CONSOLIDATED_BY",
                "periods": periods,
            },
            "registration": {"lastUpdateDate": last_update},
        },
    }


# ---------------------------------------------------------------------------
# Companies House — PSC notified_on / ceased_on
# ---------------------------------------------------------------------------


def _ch_bundle() -> dict:
    return {
        "company_number": "00102498",
        "profile": {
            "company_number": "00102498",
            "company_name": "BP P.L.C.",
            "date_of_creation": "1909-04-14",
        },
        "officers": {
            "items": [
                {
                    "name": "SMITH, Jane",
                    "officer_role": "director",
                    "appointed_on": "1998-06-01",
                    "links": {"officer": {"appointments": "/officers/abc/appointments"}},
                }
            ]
        },
        "pscs": {
            "items": [
                {
                    "kind": "individual-person-with-significant-control",
                    "name": "Jane SMITH",
                    "name_elements": {"forename": "Jane", "surname": "Smith"},
                    "etag": "abc123",
                    "notified_on": "2016-04-06",
                    "natures_of_control": ["ownership-of-shares-50-to-75-percent"],
                }
            ]
        },
    }


class TestCompaniesHousePscDates:
    def test_psc_statement_date_is_the_notification_date(self):
        out = map_companies_house(_ch_bundle())
        psc_rels = [
            r
            for r in _rels(out)
            if any(
                i.get("type") == "shareholding"
                for i in r["recordDetails"].get("interests", [])
            )
        ]
        assert psc_rels, "expected a PSC relationship"
        assert psc_rels[0]["statementDate"] == "2016-04-06"

    def test_psc_publication_date_is_not_the_notification_date(self):
        """The regression this phase fixes: a PSC notified in 2016 was emitting
        a statement OpenCheck claimed to have published in 2016."""
        out = map_companies_house(_ch_bundle())
        for stmt in out:
            assert stmt["publicationDetails"]["publicationDate"] != "2016-04-06"
            assert stmt["publicationDetails"]["publicationDate"] == TODAY

    def test_cessation_date_wins_for_a_closed_record(self):
        bundle = _ch_bundle()
        bundle["pscs"]["items"][0]["ceased_on"] = "2024-01-01"
        out = map_companies_house(bundle)
        closed = [r for r in _rels(out) if r["recordStatus"] == "closed"]
        assert closed
        # A closed record asserts "this ended", declared at cessation.
        assert closed[0]["statementDate"] == "2024-01-01"

    def test_officer_appointment_date_is_not_a_publication_date(self):
        """appointed_on is when the appointment began — already interest.startDate.

        It is neither the register's declaration date (Companies House
        publishes no per-officer notification date) nor OpenCheck's publication
        date, so it must appear in neither.
        """
        out = map_companies_house(_ch_bundle())
        officer_rels = [
            r
            for r in _rels(out)
            if any(
                i.get("type") == "seniorManagingOfficial"
                for i in r["recordDetails"].get("interests", [])
            )
        ]
        assert officer_rels, "expected an officer relationship"
        rel = officer_rels[0]
        assert rel["publicationDetails"]["publicationDate"] != "1998-06-01"
        assert rel["statementDate"] != "1998-06-01"
        # But the appointment date itself is not lost — it is the interest's
        # start date, which is the clock it actually belongs to.
        assert rel["recordDetails"]["interests"][0]["startDate"] == "1998-06-01"


# ---------------------------------------------------------------------------
# SEC EDGAR — 13D/13G filing date
# ---------------------------------------------------------------------------


def _sec_bundle(filed: str = "2025-02-14") -> dict:
    return {
        "issuer_cik": "0000320193",
        "filings": [
            {
                "filed": filed,
                "filing_url": "https://www.sec.gov/example",
                "issuer": {"name": "APPLE INC", "cik": "0000320193"},
                "reporter": {
                    "name": "Example Capital LP",
                    "reporter_cik": "0001234567",
                    "is_individual": False,
                    "percent_of_class": 6.5,
                },
            }
        ],
    }


class TestSecEdgarFilingDates:
    def test_relationship_statement_date_is_the_filing_date(self):
        rel = _rels(map_sec_edgar(_sec_bundle()))[0]
        assert rel["statementDate"] == "2025-02-14"

    def test_publication_date_is_openchecks_own(self):
        rel = _rels(map_sec_edgar(_sec_bundle()))[0]
        assert rel["publicationDetails"]["publicationDate"] == TODAY

    def test_reporter_entity_carries_the_filing_date(self):
        out = map_sec_edgar(_sec_bundle())
        reporter = [
            e for e in _entities(out) if e["recordDetails"]["name"] == "Example Capital LP"
        ]
        assert reporter and reporter[0]["statementDate"] == "2025-02-14"

    def test_issuer_uses_the_latest_filing_date(self):
        """The issuer block is read off the filings, so the most recent filing
        is when the SEC last published those details."""
        bundle = _sec_bundle("2024-01-05")
        second = _sec_bundle("2025-02-14")["filings"][0]
        second["reporter"] = dict(second["reporter"], name="Other Fund LP", reporter_cik="0007654321")
        bundle["filings"].append(second)
        out = map_sec_edgar(bundle)
        issuer = [e for e in _entities(out) if e["recordDetails"]["name"] == "APPLE INC"]
        assert issuer and issuer[0]["statementDate"] == "2025-02-14"

    def test_missing_filing_date_does_not_crash(self):
        bundle = _sec_bundle()
        bundle["filings"][0].pop("filed")
        rel = _rels(map_sec_edgar(bundle))[0]
        assert rel["statementDate"] == TODAY


def _gleif_rr_bundle() -> dict:
    """A GLEIF bundle whose parent edge carries its relationship record."""
    bundle = _gleif_bundle()
    bundle["direct_parent"] = _gleif_l1("5493001KJTIIGC8Y1R12", "PARENT", "2020-01-02")
    bundle["direct_parent_relationship"] = _gleif_rr(
        "213800LH1BZH3DI6G760", "5493001KJTIIGC8Y1R12", last_update="2025-11-03T09:12:44Z",
    )
    return bundle


# ---------------------------------------------------------------------------
# Cross-source canary
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mapper,bundle,expected",
    [
        (map_gleif, _gleif_bundle(), "2023-03-31"),
        (map_gleif, _gleif_rr_bundle(), "2025-11-03"),
        (map_companies_house, _ch_bundle(), "2016-04-06"),
        (map_sec_edgar, _sec_bundle(), "2025-02-14"),
    ],
)
def test_register_date_never_leaks_into_publication_date(mapper, bundle, expected):
    """No statement may claim OpenCheck published it on the register's date.

    This is the specific regression being fixed, stated once per source so a
    future change that re-routes a register date back into publicationDetails
    fails loudly.
    """
    for stmt in mapper(bundle):
        assert stmt["publicationDetails"]["publicationDate"] != expected
        assert stmt["publicationDetails"]["publisher"]["name"] == "OpenCheck"


@pytest.mark.parametrize(
    "mapper,bundle,expected",
    [
        (map_gleif, _gleif_bundle(), "2023-03-31"),
        (map_gleif, _gleif_rr_bundle(), "2025-11-03"),
        (map_companies_house, _ch_bundle(), "2016-04-06"),
        (map_sec_edgar, _sec_bundle(), "2025-02-14"),
    ],
)
def test_at_least_one_statement_carries_the_register_date(mapper, bundle, expected):
    """The other half of the canary: the date must actually reach statementDate,
    not merely be absent from publicationDate."""
    dates = {stmt["statementDate"] for stmt in mapper(bundle)}
    assert expected in dates
