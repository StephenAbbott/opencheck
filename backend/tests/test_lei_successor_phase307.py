"""Phase 307 — the successor GLEIF names on an LEI record, followed forward.

The record shapes are GLEIF's own, read from the live API and the 7 Oct 2026
Golden Copy on 8 Oct 2026: Barrick Gold Inc. (``5493002CWGHR03YL8X75``,
dissolved, no successor — the ordinary case), Diamond Bank PLC
(``029200738G7T8AI6H992``, merged into Access Bank), a Slovak leasing
company with one LEI and one name-only successor, and a DUPLICATE
registration.
"""

from __future__ import annotations

import copy
from typing import Any

import pytest

from opencheck import lei_successor as ls
from opencheck.bods.mapper import map_gleif
from opencheck.entity_pages import EntityRow, gleif_record_from_row
from opencheck.mcp.shaping import shape_lookup
from opencheck.reporting.html_report import _identifiers as html_identifiers
from opencheck.reporting.markdown_report import _identifiers as md_identifiers
from opencheck.subject_profile import build_subject_profile

BARRICK = "5493002CWGHR03YL8X75"
DIAMOND = "029200738G7T8AI6H992"
ACCESS = "029200328C3N9YI2D660"


def _record(lei: str, name: str, **entity: Any) -> dict:
    """A trimmed GLEIF ``lei-records`` resource."""
    registration = entity.pop("registration", {"status": "RETIRED"})
    return {
        "type": "lei-records",
        "id": lei,
        "attributes": {
            "lei": lei,
            "entity": {
                "legalName": {"name": name, "language": "en"},
                "jurisdiction": "CA-ON",
                "status": "INACTIVE",
                "legalAddress": {"city": "Toronto", "country": "CA"},
                "successorEntity": {"lei": None, "name": None},
                "successorEntities": [],
                "eventGroups": [],
                **entity,
            },
            "registration": {
                "initialRegistrationDate": "2017-12-21T15:31:00Z",
                "lastUpdateDate": "2026-06-26T19:33:17Z",
                "nextRenewalDate": "2026-06-16T21:32:00Z",
                "managingLou": "5493001KJTIIGC8Y1R12",
                **registration,
            },
        },
    }


def _event(etype: str, status: str = "COMPLETED", effective: str = "2020-04-17T11:33:58Z") -> dict:
    return {
        "groupType": "STANDALONE",
        "events": [
            {
                "type": etype,
                "status": status,
                "effectiveDate": effective,
                "recordedDate": "2020-04-20T00:00:00Z",
                "validationDocuments": "SUPPORTING_DOCUMENTS",
            }
        ],
    }


BARRICK_RECORD = _record(
    BARRICK,
    "BARRICK GOLD INC.",
    eventGroups=[_event("DISSOLUTION", effective="2025-11-26T00:00:00Z")],
)

DIAMOND_RECORD = _record(
    DIAMOND,
    "DIAMOND BANK PLC",
    successorEntity={"lei": ACCESS, "name": "ACCESS BANK PLC"},
    successorEntities=[{"lei": ACCESS, "name": "ACCESS BANK PLC"}],
    eventGroups=[_event("MERGERS_AND_ACQUISITIONS")],
)


class _Store:
    """A stand-in for ``entity_pages.EntityStore`` over a few rows."""

    def __init__(self, rows: dict[str, tuple[str, str, str, list[dict] | None]]) -> None:
        self.rows = {
            lei: EntityRow(
                lei=lei,
                name=name,
                slug=name.lower(),
                entity_status=es,
                registration_status=rs,
                jurisdiction=None,
                legal_form=None,
                city=None,
                region=None,
                country=None,
                first_registered=None,
                last_updated=None,
                successor_lei=(succ or [{}])[0].get("lei") if succ else None,
                direct_parent_lei=None,
                ultimate_parent_lei=None,
                detail={"successorEntities": succ} if succ else {},
            )
            for lei, (name, es, rs, succ) in rows.items()
        }

    def get(self, lei: str) -> EntityRow | None:
        return self.rows.get(lei)


# --- reading the record -------------------------------------------------------------


def test_the_ordinary_dissolved_company_names_no_successor() -> None:
    """Barrick Gold Inc.: dissolved 26 Nov 2025, LEI retired, GLEIF names
    nothing — 192,528 INACTIVE records look like this and 0.3 % of them
    carry a successor. Since Phase 308 the absence is said in one sentence
    (``relation: none``) and nothing is invented: no name, no link, no walk."""
    out = ls.from_gleif_record(BARRICK_RECORD)
    assert out["relation"] == "none" and out["named"] == [] and out["chain"] == []
    assert out["sentence"] == ls.NONE_NAMED == "GLEIF names no successor on this LEI record."
    assert out["event"]["type"] == "DISSOLUTION" and out["event"]["effective_day"] == "2025-11-26"
    assert ls.follow(out, _Store({})) == out
    assert ls.final(out) is None
    assert ls.follow(None, _Store({})) is None
    # A live company with no successor named is simply not the subject.
    live = copy.deepcopy(BARRICK_RECORD)
    live["attributes"]["entity"]["status"] = "ACTIVE"
    assert ls.from_gleif_record(live) is None


def test_a_merger_names_its_successor_with_the_event_that_explains_it() -> None:
    out = ls.from_gleif_record(DIAMOND_RECORD)
    assert out is not None
    assert out["relation"] == "successor"
    assert out["named"] == [{"lei": ACCESS, "name": "ACCESS BANK PLC"}]
    assert out["event"] == {
        "type": "MERGERS_AND_ACQUISITIONS",
        "status": "COMPLETED",
        "effective_day": "2020-04-17",
    }
    assert out["chain"] == [] and out["chain_source"] is None and not out["chain_complete"]
    assert out["sentence"] == (
        "GLEIF names ACCESS BANK PLC (029200328C3N9YI2D660) as this entity's successor, "
        "on a merger or acquisition completed on 17 April 2020. "
        "The trail was not followed further."
    )


def test_the_singular_field_alone_is_read_and_not_doubled() -> None:
    rec = copy.deepcopy(DIAMOND_RECORD)
    rec["attributes"]["entity"]["successorEntities"] = []
    assert ls.from_gleif_record(rec)["named"] == [{"lei": ACCESS, "name": "ACCESS BANK PLC"}]


def test_a_name_only_successor_is_a_name_with_no_link() -> None:
    rec = _record(
        "08RI2FIP1DED1BHGKY41",
        "ConocoPhillips Canada Funding Company II",
        successorEntities=[{"name": "CONOCOPHILLIPS CANADA FUNDING COMPANY I"}],
        eventGroups=[_event("MERGERS_AND_ACQUISITIONS", effective="2013-11-01T00:00:00Z")],
    )
    out = ls.follow(ls.from_gleif_record(rec), _Store({}))
    assert out["named"] == [{"lei": None, "name": "CONOCOPHILLIPS CANADA FUNDING COMPANY I"}]
    assert out["sentence"] == (
        "GLEIF names CONOCOPHILLIPS CANADA FUNDING COMPANY I as this entity's successor, "
        "on a merger or acquisition completed on 1 November 2013, with no LEI."
    )
    assert ls.final(out) is None


def test_an_lei_filed_in_the_name_field_is_read_as_an_lei() -> None:
    """INKA-376 (``529900IVTP86TL794O69``) files ``LEI529900HJF4HKHYY69L55``
    as its successor's *name* — one record in the Golden Copy."""
    rec = _record("529900IVTP86TL794O69", "INKA-376", successorEntities=[{"name": "LEI529900HJF4HKHYY69L55"}])
    assert ls.from_gleif_record(rec)["named"] == [{"lei": "529900HJF4HKHYY69L55", "name": None}]


def test_a_duplicate_registration_is_the_same_entity_not_a_merger() -> None:
    rec = _record(
        "029200098C3K8BI2D551",
        "STANBIC IBTC BANK PLC",
        status=None,
        successorEntities=[{"lei": "549300NIVXF92ZIOVW61", "name": "STANBIC IBTC BANK PLC"}],
        registration={"status": "DUPLICATE"},
    )
    out = ls.from_gleif_record(rec)
    assert out["relation"] == "duplicate"
    assert out["event"] is None
    assert out["sentence"] == (
        "GLEIF records this LEI as a duplicate: the same entity is registered as "
        "STANBIC IBTC BANK PLC (549300NIVXF92ZIOVW61). "
        "This is the status of the LEI record, not of the company."
    )
    assert "merger" not in out["sentence"] and "successor" not in out["sentence"]


def test_several_successors_are_listed_and_not_followed() -> None:
    rec = _record(
        "3157008DY7NRFD22D743",
        "Seznam.cz, a.s.",
        status="ACTIVE",
        successorEntities=[{"lei": "3157001NQ3REFQTSJL87", "name": "Seznam.cz Holding"}, {"name": "Seznam Media"}],
        eventGroups=[_event("DEMERGER", effective="2022-07-31T22:00:00Z")],
        registration={"status": "ISSUED"},
    )
    store = _Store({"3157001NQ3REFQTSJL87": ("Seznam.cz Holding", "ACTIVE", "ISSUED", None)})
    out = ls.follow(ls.from_gleif_record(rec), store)
    assert out["chain"] == [] and out["chain_source"] is None
    assert out["sentence"] == (
        "GLEIF names Seznam.cz Holding (3157001NQ3REFQTSJL87) and Seznam Media as this "
        "entity's successors, on a demerger completed on 1 August 2022."
    )
    assert ls.final(out) is None


def test_a_dissolution_with_a_successor_is_reported_as_a_dissolution() -> None:
    rec = _record(
        BARRICK,
        "X",
        successorEntities=[{"lei": ACCESS, "name": "Y"}],
        eventGroups=[_event("DISSOLUTION", effective="2025-11-26T00:00:00Z")],
    )
    out = ls.from_gleif_record(rec)
    assert out["event"]["type"] == "DISSOLUTION"
    assert "on a dissolution completed on 26 November 2025" in out["sentence"]


# --- following the chain through the mirror -----------------------------------------


def test_one_hop_to_a_live_record() -> None:
    store = _Store({ACCESS: ("ACCESS BANK PLC", "ACTIVE", "ISSUED", None)})
    out = ls.follow(ls.from_gleif_record(DIAMOND_RECORD), store)
    assert out["chain_source"] == "mirror" and out["chain_complete"] and out["hops"] == 1
    assert out["chain"] == [
        {"lei": ACCESS, "name": "ACCESS BANK PLC", "entity_status": "ACTIVE", "registration_status": "ISSUED"}
    ]
    assert out["sentence"].endswith("completed on 17 April 2020. Its LEI is issued.")
    assert ls.final(out) == {"lei": ACCESS, "name": "ACCESS BANK PLC", "registration_status": "ISSUED"}


def test_a_chain_is_walked_to_its_end_and_counted() -> None:
    """2,734 INACTIVE records name a successor that is itself retired; the
    link must point at the end of the trail, not the first hop."""
    store = _Store(
        {
            ACCESS: ("ACCESS BANK PLC", "INACTIVE", "RETIRED", [{"lei": "549300AAAAAAAAAAAAA1", "name": "MID"}]),
            "549300AAAAAAAAAAAAA1": ("MID", "INACTIVE", "RETIRED", [{"lei": "549300BBBBBBBBBBBBB2", "name": "END"}]),
            "549300BBBBBBBBBBBBB2": ("END", "ACTIVE", "LAPSED", None),
        }
    )
    out = ls.follow(ls.from_gleif_record(DIAMOND_RECORD), store)
    assert [h["lei"] for h in out["chain"]] == [ACCESS, "549300AAAAAAAAAAAAA1", "549300BBBBBBBBBBBBB2"]
    assert out["chain_complete"] and out["hops"] == 3
    assert out["sentence"] == (
        "GLEIF names ACCESS BANK PLC (029200328C3N9YI2D660) as this entity's successor, "
        "on a merger or acquisition completed on 17 April 2020. That record is itself "
        "retired; GLEIF's successor records lead on to END (549300BBBBBBBBBBBBB2), whose "
        "LEI is lapsed (2 hops on)."
    )
    assert ls.final(out)["lei"] == "549300BBBBBBBBBBBBB2"


def test_a_successor_the_mirror_lacks_leaves_the_trail_unfollowed() -> None:
    out = ls.follow(ls.from_gleif_record(DIAMOND_RECORD), _Store({}))
    assert out["chain_source"] == "mirror" and out["chain"] == [] and not out["chain_complete"]
    assert out["sentence"].endswith("The trail was not followed further.")
    # The one LEI GLEIF named is still where a reader should go.
    assert ls.final(out)["lei"] == ACCESS


def test_a_cycle_and_a_fork_both_end_the_walk_honestly() -> None:
    cycle = _Store(
        {
            ACCESS: ("A", "INACTIVE", "RETIRED", [{"lei": "549300AAAAAAAAAAAAA1"}]),
            "549300AAAAAAAAAAAAA1": ("B", "INACTIVE", "RETIRED", [{"lei": ACCESS}]),
        }
    )
    out = ls.follow(ls.from_gleif_record(DIAMOND_RECORD), cycle)
    assert out["hops"] == 2 and not out["chain_complete"]
    assert "the trail was not followed further" in out["sentence"]
    fork = _Store(
        {ACCESS: ("A", "INACTIVE", "RETIRED", [{"lei": "549300AAAAAAAAAAAAA1"}, {"lei": "549300BBBBBBBBBBBBB2"}])}
    )
    out = ls.follow(ls.from_gleif_record(DIAMOND_RECORD), fork)
    assert out["hops"] == 1 and not out["chain_complete"]
    assert out["sentence"].endswith(
        "Its LEI is retired. That record names a successor of its own; the trail was not followed further."
    )


def test_the_walk_is_capped() -> None:
    rows: dict = {}
    prev = ACCESS
    for i in range(10):
        nxt = f"549300C{i:013d}"
        rows[prev] = (f"N{i}", "INACTIVE", "RETIRED", [{"lei": nxt}])
        prev = nxt
    rows[prev] = ("LAST", "ACTIVE", "ISSUED", None)
    out = ls.follow(ls.from_gleif_record(DIAMOND_RECORD), _Store(rows))
    assert out["hops"] == ls.MAX_HOPS and not out["chain_complete"]


def test_no_mirror_means_unfollowed_never_ended() -> None:
    # Exercise the real accessor path with the store absent.
    import opencheck.entity_pages as ep

    original = ep.get_store
    ep.get_store = lambda: None  # type: ignore[assignment]
    try:
        out = ls.follow(ls.from_gleif_record(DIAMOND_RECORD))
    finally:
        ep.get_store = original  # type: ignore[assignment]
    assert out["chain_source"] is None and not out["chain_complete"]
    assert out["sentence"].endswith("The trail was not followed further.")


@pytest.mark.parametrize(
    "record",
    [DIAMOND_RECORD, BARRICK_RECORD],
)
def test_no_sentence_passes_judgement(record: dict) -> None:
    out = ls.follow(ls.from_gleif_record(record), _Store({ACCESS: ("A", "ACTIVE", "ISSUED", None)}))
    text = ((out or {}).get("sentence") or "").lower()
    for word in ls.BANNED_WORDS:
        assert word not in text


def test_the_mirror_record_carries_the_same_block() -> None:
    row = EntityRow(
        lei=DIAMOND,
        name="DIAMOND BANK PLC",
        slug="diamond-bank-plc",
        entity_status="INACTIVE",
        registration_status="RETIRED",
        jurisdiction="NG",
        legal_form=None,
        city=None,
        region=None,
        country="NG",
        first_registered=None,
        last_updated=None,
        successor_lei=ACCESS,
        direct_parent_lei=None,
        ultimate_parent_lei=None,
        detail={
            "successorEntities": [{"lei": ACCESS, "name": "ACCESS BANK PLC"}],
            "events": [
                {"type": "MERGERS_AND_ACQUISITIONS", "status": "COMPLETED", "effectiveDate": "2020-04-17T11:33:58Z"}
            ],
        },
    )
    out = ls.from_gleif_record(gleif_record_from_row(row))
    assert out["named"] == [{"lei": ACCESS, "name": "ACCESS BANK PLC"}]
    assert out["event"]["type"] == "MERGERS_AND_ACQUISITIONS"


# --- the profile, the MCP tool and the reports --------------------------------------


def _bods(record: dict) -> list[dict]:
    return list(map_gleif({"record": record, "direct_parents": [], "ultimate_parents": [], "direct_children": []}))


def _successor() -> dict:
    return ls.follow(ls.from_gleif_record(DIAMOND_RECORD), _Store({ACCESS: ("ACCESS BANK PLC", "ACTIVE", "ISSUED", None)}))


def test_the_profile_carries_it_and_the_ordinary_case_is_null() -> None:
    succ = _successor()
    profile = build_subject_profile(DIAMOND, _bods(DIAMOND_RECORD), lei_successor=succ)
    assert profile["lei_successor"] == succ
    assert build_subject_profile(DIAMOND, _bods(DIAMOND_RECORD))["lei_successor"] is None
    # Never a status of the company: nothing here touches register status.
    assert profile["register_status"]["liveness"] == "terminal"


class _Payload:
    def __init__(self, succ: dict | None) -> None:
        self.lei = DIAMOND
        self.legal_name = "DIAMOND BANK PLC"
        self.jurisdiction = "NG"
        self.hits = []
        self.errors = []
        self.bods = _bods(DIAMOND_RECORD)
        self.risk_signals = []
        self.derived_identifiers = {}
        self.license_notices = []
        self.degraded_sources = []
        self.sources_applicable = []
        self.verdict = None
        self.subject_profile = build_subject_profile(DIAMOND, self.bods, lei_successor=succ)


def test_mcp_says_the_successor_up_front_and_names_where_to_go() -> None:
    out = shape_lookup(_Payload(_successor()))
    assert out["lei_successor"]["follow_forward"] == {
        "lei": ACCESS,
        "name": "ACCESS BANK PLC",
        "registration_status": "ISSUED",
    }
    assert out["profile"]["lei_successor"]["named"][0]["lei"] == ACCESS
    assert "GLEIF names ACCESS BANK PLC (029200328C3N9YI2D660) as this entity's successor" in out["summary"]
    assert out["summary"].index("GLEIF names") < out["summary"].index("Risk signals")
    assert ACCESS not in str(out["risk_signals"])


def test_mcp_is_silent_when_gleif_names_none() -> None:
    out = shape_lookup(_Payload(None))
    assert out["lei_successor"] is None
    assert "successor" not in out["summary"].lower()


def test_mcp_and_reports_say_none_named_for_an_ended_company() -> None:
    none = ls.from_gleif_record(BARRICK_RECORD)
    out = shape_lookup(_Payload(none))
    assert out["lei_successor"]["relation"] == "none"
    assert out["lei_successor"]["follow_forward"] is None
    assert ls.NONE_NAMED in out["summary"]
    assert "| Successor (GLEIF) | GLEIF names no successor on this LEI record. |" in "\n".join(
        md_identifiers(_report(none), None)
    )


def _report(succ: dict | None) -> dict:
    return {"lei": DIAMOND, "subject_profile": {"lei_successor": succ}}


def test_both_reports_carry_the_successor_sentence_and_nothing_otherwise() -> None:
    succ = _successor()
    md = "\n".join(md_identifiers(_report(succ), None))
    assert "| Successor (GLEIF) | GLEIF names ACCESS BANK PLC (029200328C3N9YI2D660)" in md
    html = html_identifiers(_report(succ), None)
    assert '<th scope="row">Successor (GLEIF)</th><td>GLEIF names' in html
    assert "Successor" not in "\n".join(md_identifiers(_report(None), None))
    assert "Successor" not in html_identifiers(_report(None), None)
