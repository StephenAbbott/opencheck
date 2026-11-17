"""The BODS publication ledger: what an edition says of each record, given
what the last edition said (the ledger ticket, 17 Nov 2026).

The shapes come from two pairs of published editions diffed on 17 Nov 2026:
Estonia 19 → 25 Oct 2026 (mapper fixes dominate: a share that stopped being
written, beside a handful of real name changes) and the Laundromat 5 → 6 Oct
2026 (relationships re-keyed onto another party record).
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest

from opencheck.bods import publication_ledger as pl
from opencheck.bods.annotations import validate_annotations
from opencheck.bods.statements import (
    content_statement_id,
    make_entity_statement,
    make_relationship_statement,
)

E1, E2, E3, E4 = "2026-11-01", "2026-12-01", "2027-01-01", "2027-02-01"


def entity(local: str, name: str, **kw: Any) -> dict[str, Any]:
    return make_entity_statement(
        source_id="gleif", local_id=local, name=name, statement_date="2026-10-20", **kw
    )


def edge(local: str, subject: dict[str, Any], party: dict[str, Any], share: float | None = None) -> dict[str, Any]:
    interest: dict[str, Any] = {"type": "shareholding", "beneficialOwnershipOrControl": False}
    if share is not None:
        interest["share"] = {"exact": share}
    return make_relationship_statement(
        source_id="gleif",
        local_id=local,
        subject_statement_id=subject["recordId"],
        interested_party_statement_id=party["recordId"],
        interested_party_type="entity",
        interests=[interest],
        statement_date="2026-10-20",
    )


@pytest.fixture
def ledger(tmp_path: Path) -> pl.PublicationLedger:
    return pl.PublicationLedger(tmp_path / "ledger.sqlite")


def run(
    ledger: pl.PublicationLedger,
    edition: str,
    parties: list[dict[str, Any]],
    relationships: list[dict[str, Any]],
    *,
    reading: int = 1,
    close: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    out: list[dict[str, Any]] = []
    with ledger.edition(edition) as ed:
        out += ed.apply("gleif", copy.deepcopy(parties), reading=reading)
        out += ed.apply("gleif", copy.deepcopy(relationships), reading=reading)
        if close:
            out += ed.close_missing("gleif")
    return out, ledger.edition_counts(edition)


def network() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    child = entity("CHILD", "Ruum3 kinnisvaragrupp OÜ")
    parent = entity("PARENT", "Parent AS")
    return child, parent, edge("CHILD:direct-parent:PARENT", child, parent, 100.0)


def test_a_first_edition_is_every_record_new_under_the_lookups_own_ids(ledger):
    child, parent, rel = network()
    out, counts = run(ledger, E1, [child, parent], [rel])
    assert [s["recordStatus"] for s in out] == ["new", "new", "new"]
    assert [s["recordId"] for s in out] == [child["recordId"], parent["recordId"], rel["recordId"]]
    for published, mapped in zip(out, (child, parent, rel), strict=True):
        assert published["statementId"] == content_statement_id(
            mapped["recordId"], mapped["recordDetails"], "new"
        )
        assert published["publicationDetails"]["publicationDate"] == E1
        assert published["statementDate"] == "2026-10-20"
    assert counts == {"gleif|new": 3}
    assert ledger.editions() == [E1]


def test_an_unchanged_record_publishes_nothing(ledger):
    child, parent, rel = network()
    run(ledger, E1, [child, parent], [rel])
    # A re-read moves the dates and the notes, never the record.
    again = copy.deepcopy(child)
    again["statementDate"] = "2026-11-29"
    again["source"]["retrievedAt"] = "2026-12-01T07:00:00Z"
    again["annotations"] = [{"statementPointerTarget": "/recordDetails", "motivation": "commenting",
                             "description": "a note"}]
    out, counts = run(ledger, E2, [again, parent], [rel])
    assert out == []
    assert counts == {"gleif|unchanged": 3}


def test_a_changed_record_is_updated_under_the_same_record_id(ledger):
    child, parent, rel = network()
    first, _ = run(ledger, E1, [child, parent], [rel])
    renamed = entity("CHILD", "Ruum3 Kliimalahendused OÜ")
    out, counts = run(ledger, E2, [renamed, parent], [rel])
    assert len(out) == 1
    (stmt,) = out
    assert stmt["recordStatus"] == "updated"
    assert stmt["recordId"] == child["recordId"]
    assert stmt["statementId"] != first[0]["statementId"]
    assert stmt["recordDetails"]["name"] == "Ruum3 Kliimalahendused OÜ"
    assert stmt["publicationDetails"]["publicationDate"] == E2
    assert not any(a["motivation"] == "correcting" for a in stmt.get("annotations") or [])
    assert counts == {"gleif|unchanged": 2, "gleif|updated": 1}


def test_a_change_across_a_reading_version_carries_a_correcting_note(ledger):
    """The Estonia shape: between two editions the mapper stopped writing a
    share, and 17,460 of 20,000 changed records differed only by that."""
    child, parent, rel = network()
    run(ledger, E1, [child, parent], [rel], reading=1)
    out, counts = run(ledger, E2, [child, parent], [edge("CHILD:direct-parent:PARENT", child, parent)], reading=2)
    (stmt,) = out
    assert stmt["recordStatus"] == "updated"
    notes = [a for a in stmt["annotations"] if a["motivation"] == "correcting"]
    assert len(notes) == 1
    assert notes[0]["statementPointerTarget"] == "/recordDetails"
    assert "reading version 1" in notes[0]["description"]
    assert "reading version 2" in notes[0]["description"]
    assert validate_annotations(stmt) == []
    assert counts == {"gleif|corrected": 1, "gleif|unchanged": 2}
    # The records that did not differ moved to the new reading silently, so
    # a later change of theirs is the source's, not a correction.
    out, counts = run(ledger, E3, [entity("CHILD", "Renamed OÜ"), parent],
                      [edge("CHILD:direct-parent:PARENT", child, parent)], reading=2)
    (stmt,) = out
    assert stmt["recordStatus"] == "updated"
    assert not any(a["motivation"] == "correcting" for a in stmt.get("annotations") or [])


def test_a_record_the_source_dropped_is_closed_with_what_was_last_said(ledger):
    child, parent, rel = network()
    run(ledger, E1, [child, parent], [rel])
    out, counts = run(ledger, E2, [child], [])
    # The relationship closes before the party it names.
    assert [(s["recordType"], s["recordStatus"]) for s in out] == [
        ("relationship", "closed"), ("entity", "closed"),
    ]
    closed_rel = out[0]
    assert closed_rel["recordId"] == rel["recordId"]
    assert closed_rel["recordDetails"] == rel["recordDetails"]
    assert closed_rel["statementDate"] == E2
    assert closed_rel["publicationDetails"]["publicationDate"] == E2
    assert closed_rel["source"] == rel["source"]
    (note,) = closed_rel["annotations"]
    assert note["motivation"] == "commenting"
    assert "no end date" in note["description"]
    assert validate_annotations(closed_rel) == []
    assert counts == {"gleif|closed": 2, "gleif|unchanged": 1}
    # Nothing follows a closed statement while the record stays away.
    out, _ = run(ledger, E3, [child], [])
    assert out == []


def test_a_record_that_returns_starts_a_second_life_under_a_new_record_id(ledger):
    child, parent, rel = network()
    run(ledger, E1, [child, parent], [rel])
    run(ledger, E2, [child], [])
    out, counts = run(ledger, E3, [child, parent], [rel])
    by_type = {s["recordType"]: s for s in out}
    assert [s["recordStatus"] for s in out] == ["new", "new"]
    new_parent = by_type["entity"]
    assert new_parent["recordId"] == pl.life_record_id(parent["recordId"], 2)
    assert new_parent["recordId"] != parent["recordId"]
    # The relationship is in its second life too, and names the party's.
    new_rel = by_type["relationship"]
    assert new_rel["recordId"] == pl.life_record_id(rel["recordId"], 2)
    assert new_rel["recordDetails"]["interestedParty"] == new_parent["recordId"]
    assert new_rel["recordDetails"]["subject"] == child["recordId"]
    assert counts == {"gleif|returned": 2, "gleif|unchanged": 1}
    assert ledger.is_live(new_parent["recordId"])
    assert not ledger.is_live(parent["recordId"])
    # The alias survives a reopen, and the second life is then unchanged.
    reopened = pl.PublicationLedger(ledger.path)
    out, counts = run(reopened, E4, [child, parent], [rel])
    assert out == []
    assert counts == {"gleif|unchanged": 3}


def test_a_party_returning_after_relationships_were_applied_is_refused(ledger):
    child, parent, rel = network()
    run(ledger, E1, [child, parent], [rel])
    run(ledger, E2, [child], [])
    with pytest.raises(pl.OrderError), ledger.edition(E3) as ed:
        list(ed.apply("gleif", [child], reading=1))
        list(ed.apply("gleif", [rel], reading=1))
        list(ed.apply("gleif", [parent], reading=1))
    # The edition was rolled back whole.
    assert ledger.editions() == [E1, E2]
    assert ledger.aliases == {}
    out, _ = run(ledger, E3, [child, parent], [rel])
    assert len(out) == 2


def test_a_record_that_goes_back_to_an_earlier_state_gets_a_new_statement_id(ledger):
    a = entity("X", "Alpha")
    b = entity("X", "Beta")
    ids = []
    for edition, stmt in ((E1, a), (E2, b), (E3, a), (E4, b)):
        (out,), _ = run(ledger, edition, [stmt], [])
        ids.append(out["statementId"])
    assert len(set(ids)) == 4


def test_a_relationship_moved_to_another_party_record(ledger):
    """The Laundromat shape: 236 relationships named a different
    interestedParty record a day later. The edge is updated, the party no
    longer named is closed, the one now named is new."""
    child, parent, rel = network()
    run(ledger, E1, [child, parent], [rel])
    other = entity("PARENT-REKEYED", "Parent AS")
    moved = make_relationship_statement(
        source_id="gleif", local_id="CHILD:direct-parent:PARENT",
        subject_statement_id=child["recordId"], interested_party_statement_id=other["recordId"],
        interested_party_type="entity",
        interests=[{"type": "shareholding", "beneficialOwnershipOrControl": False, "share": {"exact": 100.0}}],
        statement_date="2026-10-20",
    )
    assert moved["recordId"] == rel["recordId"]
    out, counts = run(ledger, E2, [child, other], [moved])
    assert sorted((s["recordType"], s["recordStatus"]) for s in out) == [
        ("entity", "closed"), ("entity", "new"), ("relationship", "updated"),
    ]
    assert counts == {"gleif|closed": 1, "gleif|new": 1, "gleif|unchanged": 1, "gleif|updated": 1}


def test_a_statement_the_mapper_closed_is_published_closed_once(ledger):
    child, parent, rel = network()
    run(ledger, E1, [child, parent], [rel])
    ended = copy.deepcopy(rel)
    ended["recordStatus"] = "closed"
    ended["recordDetails"]["interests"][0]["endDate"] = "2026-11-15"
    out, counts = run(ledger, E2, [child, parent], [ended])
    (stmt,) = out
    assert stmt["recordStatus"] == "closed"
    assert stmt["recordDetails"]["interests"][0]["endDate"] == "2026-11-15"
    out, _ = run(ledger, E3, [child, parent], [ended])
    assert out == []


def test_a_record_seen_twice_in_one_edition_is_published_once(ledger):
    child, parent, rel = network()
    out, counts = run(ledger, E1, [child, parent, copy.deepcopy(parent)], [rel])
    assert len(out) == 3
    assert counts["gleif|duplicate"] == 1


def test_a_mass_disappearance_is_refused_and_nothing_is_closed(ledger):
    parties = [entity(f"L{i}", f"Company {i}") for i in range(pl.CLOSE_GUARD_FLOOR)]
    run(ledger, E1, parties, [])
    with pytest.raises(pl.SuspectEditionError):
        run(ledger, E2, parties[: int(len(parties) * 0.8)], [])
    assert ledger.editions() == [E1]
    assert ledger.live_counts() == {"gleif|entity": pl.CLOSE_GUARD_FLOOR}
    # A month's ordinary churn passes.
    out, counts = run(ledger, E2, parties[:-5], [])
    assert counts["gleif|closed"] == 5


def test_editions_are_applied_in_date_order_once_each(ledger):
    child, parent, rel = network()
    run(ledger, E2, [child, parent], [rel])
    for bad in (E1, E2):
        with pytest.raises(pl.EditionOrderError):
            ledger.edition(bad)
    with pytest.raises(ValueError):
        ledger.edition("December 2026")


def test_bodies_are_deflated_against_the_ledgers_own_dictionary(tmp_path):
    child, parent, rel = network()
    plain = pl.PublicationLedger(tmp_path / "plain.sqlite")
    trained = pl.PublicationLedger(tmp_path / "trained.sqlite")
    assert trained.train([child, parent, rel]) is True
    for ledger in (plain, trained):
        run(ledger, E1, [child, parent], [rel])
        latest = list(ledger.latest())
        assert [s["recordType"] for s in latest] == ["entity", "entity", "relationship"]
        by_id = {s["recordId"]: s for s in latest}
        assert by_id[rel["recordId"]]["recordDetails"] == rel["recordDetails"]
        assert by_id[rel["recordId"]]["publicationDetails"]["publicationDate"] == E1
        assert by_id[child["recordId"]]["statementId"] == content_statement_id(
            child["recordId"], child["recordDetails"], "new"
        )
    assert trained.train([child]) is False  # fixed for the file's life
    size = lambda ledger: sum(  # noqa: E731
        r[0] for r in ledger.conn.execute("SELECT length(body) FROM records")
    )
    assert size(trained) < size(plain)
    # A reopened ledger reads its dictionary back.
    reopened = pl.PublicationLedger(tmp_path / "trained.sqlite")
    assert {s["recordId"] for s in reopened.latest()} == {s["recordId"] for s in (child, parent, rel)}


def test_another_publishers_ids_are_kept_as_written(ledger):
    stmt = {
        "statementId": "oecd-abc", "recordId": "meip-entity-7", "recordType": "entity",
        "recordStatus": "new", "statementDate": "2026-09-01",
        "recordDetails": {"isComponent": False, "entityType": {"type": "registeredEntity"}, "name": "X"},
    }
    (out,), _ = run(ledger, E1, [stmt], [])
    assert out["recordId"] == "meip-entity-7"
    assert ledger.is_live("meip-entity-7")
    assert next(ledger.latest())["recordId"] == "meip-entity-7"


def test_the_ledgers_migrations_are_contiguous():
    from opencheck import sqlite_schema

    sqlite_schema.check_contiguous(pl.MIGRATIONS)
