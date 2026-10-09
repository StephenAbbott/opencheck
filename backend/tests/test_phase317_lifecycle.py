"""Phase 317 — one lifecycle rule for ended ownership, and the dates it rests on.

Phase D of the BODS dates audit (Notion "📆 Audit of dates captured across
OpenCheck", §7). Decided with Stephen on 9 Oct 2026: **ownership yes, officers
no**.

* Ended ownership and control is emitted ``closed`` with its ``endDate``, never
  dropped: the OpenCorporates network, ARES shareholders and partners, NZ, RPVS,
  SEC, Wikidata and Estonia. The rule lives once, in
  ``make_relationship_statement``: every interest ended on or before today →
  ``closed``.
* Officer and board-role lists (Companies House officers, OpenCorporates
  officers, brreg roles, ARES directors) stay limited to people serving now, as
  Phase 192 decided. That is a scope choice, not a missing date, and the tests
  at the bottom pin it so a later change makes it on purpose.
* Where a ``startDate`` is the register's entry date rather than the day the
  interest began (ARES ``datumZapisu``, UR ``registered_on``) it says so; where
  a date describes another publisher's clock (OpenCorporates' retrieval of the
  register, a Wikidata reference's P813) it travels as a comment.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import opencheck.bods as bods_pkg
from opencheck import provenance
from opencheck.bods.mapper import map_opencorporates
from opencheck.bods.statements import make_relationship_statement
from opencheck.provenance import Provenance

TODAY = dt.datetime.now(dt.timezone.utc).date()
PAST = "2020-01-01"
FUTURE = (TODAY + dt.timedelta(days=400)).isoformat()
_LIVE = Provenance(liveness="live", retrieved_at=dt.datetime.now(dt.timezone.utc))


def _map(mapper: Any, bundle: dict[str, Any]) -> list[dict[str, Any]]:
    with provenance.mapping_provenance(_LIVE):
        return list(mapper(bundle))


def _rel(interests: list[dict[str, Any]], **kw: Any) -> dict[str, Any]:
    return make_relationship_statement(
        source_id="test",
        local_id="pair-1",
        subject_statement_id="s",
        interested_party_statement_id="p",
        interests=interests,
        **kw,
    )


def _annotations(stmt: dict[str, Any], target: str) -> list[dict[str, Any]]:
    return [a for a in stmt.get("annotations") or [] if a["statementPointerTarget"] == target]


# ---------------------------------------------------------------------------
# The rule, in the factory
# ---------------------------------------------------------------------------


class TestOneLifecycleRule:
    def test_every_interest_ended_closes_the_record(self) -> None:
        rel = _rel([{"type": "shareholding", "endDate": PAST}])
        assert rel["recordStatus"] == "closed"
        assert rel["recordDetails"]["interests"][0]["endDate"] == PAST

    def test_closing_keeps_the_record_id_and_varies_the_statement_id(self) -> None:
        open_ = _rel([{"type": "shareholding"}])
        ended = _rel([{"type": "shareholding", "endDate": PAST}])
        assert ended["recordId"] == open_["recordId"]
        assert ended["statementId"] != open_["statementId"]

    def test_an_end_dated_today_has_ended(self) -> None:
        rel = _rel([{"type": "shareholding", "endDate": TODAY.isoformat()}])
        assert rel["recordStatus"] == "closed"

    def test_a_scheduled_end_stays_open(self) -> None:
        assert _rel([{"type": "shareholding", "endDate": FUTURE}])["recordStatus"] == "new"

    def test_one_open_interest_keeps_the_relationship_open(self) -> None:
        rel = _rel([
            {"type": "shareholding", "endDate": PAST},
            {"type": "votingRights"},
        ])
        assert rel["recordStatus"] == "new"

    def test_no_interests_is_not_ended(self) -> None:
        assert _rel([])["recordStatus"] == "new"

    def test_a_caller_that_knows_it_ended_without_a_date_still_closes_it(self) -> None:
        rel = _rel([{"type": "shareholding"}], record_status="closed")
        assert rel["recordStatus"] == "closed"
        assert "endDate" not in rel["recordDetails"]["interests"][0]

    def test_a_generator_of_interests_is_read_once(self) -> None:
        rel = _rel(i for i in [{"type": "shareholding", "endDate": PAST}])  # type: ignore[arg-type]
        assert rel["recordStatus"] == "closed"
        assert rel["recordDetails"]["interests"] == [{"type": "shareholding", "endDate": PAST}]


# ---------------------------------------------------------------------------
# OpenCorporates: ended network edges, and OC's own retrieval of the register
# ---------------------------------------------------------------------------


def _oc_network_rel(source_no: str, end_date: str | None) -> dict[str, Any]:
    return {"relationship": {
        "relationship_type": "control_statement",
        "source": {"company": {
            "name": f"Parent {source_no}", "jurisdiction_code": "gb",
            "company_number": source_no,
        }},
        "target": {"company": {
            "name": "Test Co Ltd", "jurisdiction_code": "gb", "company_number": "00102498",
        }},
        "start_date": "2015-06-01",
        "end_date": end_date,
    }}


def _oc_bundle(**company: Any) -> dict[str, Any]:
    return {
        "source_id": "opencorporates",
        "hit_id": "gb/00102498",
        "ocid": "gb/00102498",
        "company": {
            "name": "Test Co Ltd", "company_number": "00102498",
            "jurisdiction_code": "gb", "incorporation_date": "2000-01-01",
            "opencorporates_url": "https://opencorporates.com/companies/gb/00102498",
            **company,
        },
        "officers": [],
        "network": {"relationships": [
            _oc_network_rel("11111111", None),
            _oc_network_rel("22222222", PAST),
        ]},
    }


def test_opencorporates_ended_network_edge_is_closed_with_its_dates() -> None:
    out = list(map_opencorporates(_oc_bundle()))
    rels = [s for s in out if s["recordType"] == "relationship"]
    by_status = {r["recordStatus"]: r for r in rels}
    assert set(by_status) == {"new", "closed"}
    for interest in by_status["closed"]["recordDetails"]["interests"]:
        assert interest["startDate"] == "2015-06-01"
        assert interest["endDate"] == PAST
    for interest in by_status["new"]["recordDetails"]["interests"]:
        assert "endDate" not in interest


def test_opencorporates_ended_edge_is_its_own_record() -> None:
    """A later holding between the same pair is a new record, not a reopening."""
    bundle = _oc_bundle()
    bundle["network"]["relationships"][1] = _oc_network_rel("11111111", PAST)
    rels = [s for s in map_opencorporates(bundle) if s["recordType"] == "relationship"]
    assert len({r["recordId"] for r in rels}) == 2


def test_opencorporates_retrieval_of_the_register_travels_as_a_comment() -> None:
    bundle = _oc_bundle(source={
        "publisher": "UK Companies House",
        "retrieved_at": "2026-08-02T05:40:11+00:00",
    })
    subject = next(s for s in map_opencorporates(bundle) if s["recordType"] == "entity")
    [note] = _annotations(subject, "/source")
    assert note["motivation"] == "commenting"
    assert "UK Companies House on 2026-08-02" in note["description"]


def test_opencorporates_without_a_retrieval_date_says_nothing() -> None:
    subject = next(s for s in map_opencorporates(_oc_bundle()) if s["recordType"] == "entity")
    assert not _annotations(subject, "/source")


# ---------------------------------------------------------------------------
# Wikidata: the statementDate is a reference's P813, and says so
# ---------------------------------------------------------------------------


def test_wikidata_owner_edge_explains_its_statement_date() -> None:
    bundle = {"summary": {
        "qid": "Q154950", "label": "Shell plc",
        "controlling_owners": [{
            "qid": "Q1", "name": "Owner Co", "bods_kind": "entity",
            "entity_type": "registeredEntity",
            "references": [{"url": "https://a", "retrieved": "2024-03-11T00:00:00Z"}],
        }, {
            "qid": "Q2", "name": "Unreferenced Co", "bods_kind": "entity",
            "entity_type": "registeredEntity", "references": [],
        }],
    }}
    rels = [s for s in _map(bods_pkg.map_wikidata, bundle) if s["recordType"] == "relationship"]
    dated = next(r for r in rels if r["statementDate"] == "2024-03-11")
    [note] = _annotations(dated, "/statementDate")
    assert note["motivation"] == "commenting"
    assert "P813" in note["description"] and "2024-03-11" in note["description"]
    undated = next(r for r in rels if r is not dated)
    assert not _annotations(undated, "/statementDate")


# ---------------------------------------------------------------------------
# Entry dates used as startDate say so (UR Latvia here; ARES in test_ares.py)
# ---------------------------------------------------------------------------


def test_latvia_start_date_is_annotated_as_the_register_entry_date() -> None:
    bundle = {
        "lv_regcode": "40003229495",
        "entity": {"name": "SIA Example", "regcode": "40003229495"},
        "beneficial_owners": [{
            "id": 7, "forename": "Jānis", "surname": "Bērziņš", "nationality": "LV",
            "registered_on": "2019-05-02T00:00:00",
        }],
        "officers": [{
            "id": 8, "name": "Anna Ozola", "position": "BOARD_MEMBER",
            "entity_type": "NATURAL_PERSON",
            "registered_on": "2021-02-03T00:00:00",
        }],
    }
    rels = [s for s in _map(bods_pkg.map_ur_latvia, bundle) if s["recordType"] == "relationship"]
    assert rels
    for rel in rels:
        if not rel["recordDetails"]["interests"][0].get("startDate"):
            continue
        [note] = _annotations(rel, "/recordDetails/interests/0/startDate")
        assert note["motivation"] == "transformation"
        assert "registered_on" in note["description"]


# ---------------------------------------------------------------------------
# Officers no: serving people only, by decision (Phase 192, reaffirmed 317)
# ---------------------------------------------------------------------------


def test_opencorporates_ended_officer_is_still_left_out() -> None:
    bundle = _oc_bundle()
    bundle["network"] = None
    bundle["officers"] = [
        {"officer": {"id": "1", "name": "Jane Smith", "position": "director", "end_date": PAST}},
        {"officer": {"id": "2", "name": "Bob Jones", "position": "director"}},
    ]
    people = [s for s in map_opencorporates(bundle) if s["recordType"] == "person"]
    assert [p["recordDetails"]["names"][0]["fullName"] for p in people] == ["Bob Jones"]


def test_brreg_terminated_role_is_still_left_out() -> None:
    def role(first: str, **flags: Any) -> dict[str, Any]:
        return {
            "type": {"kode": "DAGL", "beskrivelse": "Daglig leder"},
            "person": {"navn": {"fornavn": first, "etternavn": "Nordmann"}},
            **flags,
        }

    bundle = {
        "orgnr": "923609016",
        "entity": {"navn": "EQUINOR ASA", "organisasjonsnummer": "923609016"},
        "roles": [role("Kari"), role("Ola", fratraadt=True), role("Per", avregistrert=True)],
    }
    people = [s for s in _map(bods_pkg.map_brreg, bundle) if s["recordType"] == "person"]
    assert [p["recordDetails"]["names"][0]["fullName"] for p in people] == ["Kari Nordmann"]
