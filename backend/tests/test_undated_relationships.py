"""Undated relationships and GEM's republished tracker (the undated-relationships
ticket, 11 Nov 2026 — the Rosneft / BP Russian Investments case).

Pins: one rule for "undated" (``bods.undated``) and its note; OpenSanctions'
copy of GEM's Global Energy Ownership Tracker labelled as GEM's claim with
the day OpenSanctions first recorded it; and the fold of that copy into GEM's
own read on a lookup (``bods.republished``), through ``fold_lookup_events``.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from opencheck.bods import map_ftm
from opencheck.bods import republished
from opencheck.bods.annotations import resolve_pointer, validate_all
from opencheck.bods.mappers.ftm import (
    FIRST_SEEN_NOTE,
    GEM_ASSERTED_BY,
    is_gem_republished,
)
from opencheck.bods.statements import (
    make_entity_statement,
    make_relationship_statement,
)
from opencheck.bods.undated import (
    UNDATED_LABEL,
    UNDATED_NOTE,
    is_undated,
    note_undated,
)

_FRONTEND = Path(__file__).resolve().parents[2] / "frontend" / "src" / "lib" / "undated.ts"


# ---------------------------------------------------------------------------
# The rule
# ---------------------------------------------------------------------------


def _rel(interests: list[dict[str, Any]], **kw: Any) -> dict[str, Any]:
    return make_relationship_statement(
        source_id="opensanctions",
        local_id="r1",
        subject_statement_id="opencheck-a",
        interested_party_statement_id="opencheck-b",
        interested_party_type="entity",
        interests=interests,
        **kw,
    )


def test_an_undated_holding_is_undated() -> None:
    assert is_undated(_rel([{"type": "shareholding", "share": {"exact": 19.75}}]))


def test_a_start_date_or_an_end_date_dates_it() -> None:
    assert not is_undated(_rel([{"type": "shareholding", "startDate": "2013-03-21"}]))
    assert not is_undated(_rel([{"type": "votingRights", "endDate": "2099-01-01"}]))


def test_a_role_is_never_called_undated() -> None:
    assert not is_undated(_rel([{"type": "seniorManagingOfficial"}]))
    assert not is_undated(_rel([{"type": "boardMember"}]))
    officer = {"type": "otherInfluenceOrControl", "opencheckOfficerPosition": "secretary"}
    assert not is_undated(_rel([officer]))


def test_a_record_closed_without_a_date_is_ended_not_undated() -> None:
    assert not is_undated(_rel([{"type": "shareholding"}], record_status="closed"))


def test_the_note_comes_and_goes_with_the_dates() -> None:
    rel = note_undated(_rel([{"type": "shareholding"}]))
    (note,) = rel["annotations"]
    assert note["motivation"] == "commenting"
    assert note["description"] == UNDATED_NOTE
    assert isinstance(resolve_pointer(rel, note["statementPointerTarget"]), list)
    assert validate_all([rel]) == []
    # Idempotent.
    assert len(note_undated(rel)["annotations"]) == 1
    rel["recordDetails"]["interests"][0]["startDate"] = "2013-03-21"
    assert "annotations" not in note_undated(rel)


def test_another_publishers_statement_is_left_alone() -> None:
    rel = _rel([{"type": "shareholding"}])
    rel["recordId"] = "meip-relationship-1"
    assert "annotations" not in note_undated(rel)


def test_the_note_describes_and_never_judges() -> None:
    from opencheck.knowability import BANNED_VOCABULARY

    for text in (UNDATED_NOTE, UNDATED_LABEL):
        words = set(re.findall(r"[a-z]+", text.lower()))
        assert not words & set(BANNED_VOCABULARY), text


def test_the_frontend_copy_matches() -> None:
    ts = _FRONTEND.read_text(encoding="utf-8")
    assert f'UNDATED_LABEL = "{UNDATED_LABEL}"' in ts
    roles = set(re.findall(r'"(\w+)"', ts.split("ROLE_INTEREST_TYPES", 1)[1].split("]", 1)[0]))
    from opencheck.bods.undated import ROLE_INTEREST_TYPES

    assert roles == set(ROLE_INTEREST_TYPES)


# ---------------------------------------------------------------------------
# OpenSanctions' copy of GEM's tracker, labelled (the Rosneft shape)
# ---------------------------------------------------------------------------


def _rosneft(*edges: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": "NK-rosneft",
        "schema": "Company",
        "properties": {
            "name": ["Rosneft Oil Company"],
            "leiCode": ["253400JT3MQWNDKMJE44"],
            "ownershipAsset": list(edges),
        },
        "datasets": ["us_ofac_sdn", "gem_energy_ownership", "ext_ru_egrul"],
    }


def _edge(eid: str, owner: str, datasets: list[str], **props: list[str]) -> dict[str, Any]:
    return {
        "id": eid,
        "schema": "Ownership",
        "datasets": datasets,
        "first_seen": "2024-06-30T00:00:00",
        "last_change": "2024-06-30T00:00:00",
        "properties": {
            "asset": ["NK-rosneft"],
            "owner": [
                {
                    "id": f"NK-{owner}",
                    "schema": "Company",
                    "properties": {"name": [owner]},
                    "referents": [f"gem-own-e{owner}"],
                }
            ],
            **props,
        },
    }


_BP = _edge(
    "gem-own-bp",
    "BP Russian Investments Ltd",
    ["gem_energy_ownership"],
    percentage=["19.75"],
    sourceUrl=["https://www.rosneft.com/about/Shareholder_structure/"],
)
_EGRUL = _edge(
    "egrul-1",
    "Former Holder LLC",
    ["ext_ru_egrul"],
    percentage=["10"],
    startDate=["2003-05-12"],
    endDate=["2017-12-27"],
)
_OFAC_TOO = _edge(
    "gem-ofac", "Rosneftegaz", ["gem_energy_ownership", "us_ofac_sdn"], percentage=["40.4"]
)


def _rels_by_owner(statements: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    names = {
        s["recordId"]: s["recordDetails"].get("name")
        for s in statements
        if s["recordType"] == "entity"
    }
    return {
        names[s["recordDetails"]["interestedParty"]]: s
        for s in statements
        if s["recordType"] == "relationship"
    }


def test_only_an_edge_carried_by_the_tracker_alone_is_gems() -> None:
    assert is_gem_republished(_BP)
    assert not is_gem_republished(_EGRUL)
    assert not is_gem_republished(_OFAC_TOO)
    assert not is_gem_republished({"datasets": []})


def test_the_republished_edge_names_gem_and_its_citation() -> None:
    statements = map_ftm(_rosneft(_BP, _EGRUL, _OFAC_TOO), source_id="opensanctions").statements
    rels = _rels_by_owner(statements)
    bp = rels["BP Russian Investments Ltd"]
    assert bp["source"]["assertedBy"] == [GEM_ASSERTED_BY]
    notes = [a["description"] for a in bp["annotations"]]
    assert any("republishes this relationship from Global Energy Monitor" in n for n in notes)
    assert "Global Energy Monitor cites: https://www.rosneft.com/about/Shareholder_structure/" in notes
    # Undated: the claim's age and the undated note.
    assert FIRST_SEEN_NOTE.format(publisher="OpenSanctions", day="2024-06-30") in notes
    assert UNDATED_NOTE in notes
    assert validate_all(statements) == []
    # The others are not GEM's.
    assert "assertedBy" not in rels["Rosneftegaz"]["source"]
    assert "assertedBy" not in rels["Former Holder LLC"]["source"]


def test_a_dated_edge_gets_no_age_note_and_no_undated_note() -> None:
    statements = map_ftm(_rosneft(_EGRUL), source_id="opensanctions").statements
    (rel,) = [s for s in statements if s["recordType"] == "relationship"]
    assert rel["recordStatus"] == "closed"
    assert "annotations" not in rel


def test_the_statement_id_does_not_move_with_the_labels() -> None:
    """Labels are outside the content digest: the edge keeps its id."""
    from opencheck.bods.statements import stale_statement_id

    for s in map_ftm(_rosneft(_BP), source_id="opensanctions").statements:
        assert not stale_statement_id(s)


# ---------------------------------------------------------------------------
# The fold
# ---------------------------------------------------------------------------


def _gem_read() -> list[dict[str, Any]]:
    subject = make_entity_statement(
        source_id="climatetrace", local_id="E100000000001", name="Rosneft Oil Company"
    )
    owner = make_entity_statement(
        source_id="climatetrace", local_id="E100001015988", name="BP Russian Investments Ltd"
    )
    rel = make_relationship_statement(
        source_id="climatetrace",
        local_id="E100000000001-owner-E100001015988",
        subject_statement_id=subject["recordId"],
        interested_party_statement_id=owner["recordId"],
        interested_party_type="entity",
        interests=[{"type": "shareholding", "share": {"exact": 19.75}}],
    )
    return [subject, owner, rel]


def _lookup_bundle(with_gem: bool = True) -> tuple[list[dict[str, Any]], set[str]]:
    os_statements = map_ftm(
        _rosneft(_BP, _EGRUL, _OFAC_TOO), source_id="opensanctions"
    ).statements
    os_subject = next(s for s in os_statements if s["recordDetails"].get("name") == "Rosneft Oil Company")
    gem = _gem_read() if with_gem else []
    subjects = {os_subject["statementId"]} | ({gem[0]["statementId"]} if gem else set())
    return os_statements + gem, subjects


def test_the_copy_is_folded_when_gem_answered_for_the_subject() -> None:
    bods, subjects = _lookup_bundle()
    payload = republished.fold(bods, subjects)
    assert payload["relationships"] == 1
    assert payload["folded_into"] == "climatetrace"
    kept = republished.apply(bods, payload)
    names = {s["recordDetails"].get("name") for s in kept if s["recordType"] == "entity"}
    # OpenSanctions' BP node goes with its only edge; GEM's BP stays, once.
    assert sorted(n for n in names if n and n.startswith("BP")) == ["BP Russian Investments Ltd"]
    os_rels = _rels_by_owner([s for s in kept if s["source"].get("opencheckSourceId") == "opensanctions"])
    assert set(os_rels) == {"Former Holder LLC", "Rosneftegaz"}
    # Nothing dangles.
    ids = {s["recordId"] for s in kept}
    for s in kept:
        if s["recordType"] == "relationship":
            assert s["recordDetails"]["subject"] in ids
            assert s["recordDetails"]["interestedParty"] in ids


def test_without_gems_read_the_copy_stays_labelled() -> None:
    bods, subjects = _lookup_bundle(with_gem=False)
    payload = republished.fold(bods, subjects)
    assert payload["statement_ids"] == []
    assert republished.apply(bods, payload) is bods


def test_an_edge_about_another_company_is_not_folded() -> None:
    """GEM's read names the subject's owners; Rosneft's own holding in Ruhr
    Oel, from the same tracker, is not in it, so it stays, labelled."""
    ruhr = {
        "id": "gem-own-ruhr",
        "schema": "Ownership",
        "datasets": ["gem_energy_ownership"],
        "properties": {
            "percentage": ["50.00"],
            "owner": ["NK-rosneft"],
            "asset": [{"id": "NK-ruhr", "schema": "Company", "properties": {"name": ["Ruhr Oel GmbH"]}}],
        },
    }
    payload_entity = _rosneft(_BP)
    payload_entity["properties"]["ownershipOwner"] = [ruhr]
    os_statements = map_ftm(payload_entity, source_id="opensanctions").statements
    os_subject = next(s for s in os_statements if s["recordDetails"].get("name") == "Rosneft Oil Company")
    gem = _gem_read()
    bods = os_statements + gem
    payload = republished.fold(bods, {os_subject["statementId"], gem[0]["statementId"]})
    assert payload["relationships"] == 1
    kept = republished.apply(bods, payload)
    names = {s["recordDetails"].get("name") for s in kept if s["recordType"] == "entity"}
    assert "Ruhr Oel GmbH" in names


def test_the_response_folds_from_the_event() -> None:
    from opencheck.lookup_replay import fold_lookup_events

    bods, subjects = _lookup_bundle()
    payload = republished.fold(bods, subjects)
    events = [
        ("gleif_done", {"legal_name": "Rosneft", "jurisdiction": "RU", "derived_identifiers": {}}),
        ("deepen_result", {"source_id": "opensanctions", "hit_id": "NK-rosneft", "bods": bods}),
        ("republished_folded", payload),
        ("done", {"bods_issues": [], "license_notices": []}),
    ]
    response = fold_lookup_events("253400JT3MQWNDKMJE44", events)
    got = {s["statementId"] for s in response.bods}
    assert not got & set(payload["statement_ids"])
    # A run recorded before the event folds nothing.
    before = fold_lookup_events(
        "253400JT3MQWNDKMJE44", [e for e in events if e[0] != "republished_folded"]
    )
    assert set(payload["statement_ids"]) <= {s["statementId"] for s in before.bods}


# ---------------------------------------------------------------------------
# Integrated ownership: the undated note and the give-back wording
# ---------------------------------------------------------------------------


def _io_case(dated: bool) -> tuple[list[dict[str, Any]], str]:
    subject = make_entity_statement(source_id="opensanctions", local_id="T", name="Target PJSC")
    holder = make_entity_statement(source_id="opensanctions", local_id="BP", name="BP Holder Ltd")
    treasury = make_entity_statement(source_id="opensanctions", local_id="R", name="Treasury Sub LLC")

    def own(lid: str, owner: dict, owned: dict, pct: float, start: str | None = None) -> dict:
        interest: dict[str, Any] = {"type": "shareholding", "share": {"exact": pct}}
        if start:
            interest["startDate"] = start
        return note_undated(
            make_relationship_statement(
                source_id="opensanctions",
                local_id=lid,
                subject_statement_id=owned["recordId"],
                interested_party_statement_id=owner["recordId"],
                interested_party_type="entity",
                interests=[interest],
            )
        )

    start = "2013-03-21" if dated else None
    return [
        subject, holder, treasury,
        own("a", holder, subject, 20, start),
        own("b", subject, treasury, 100, start),
        own("c", treasury, subject, 10, start),
    ], subject["statementId"]


def test_integrated_ownership_says_when_it_rests_on_undated_holdings() -> None:
    from opencheck import integrated_ownership as io

    bods, subject = _io_case(dated=False)
    payload = io.compute(bods, [subject], "2026-11-11")
    assert payload["undated_note"] == io.UNDATED_NOTE
    bp = next(h for h in payload["holders"] if h["name"] == "BP Holder Ltd")
    (fig,) = bp["figures"]
    assert fig["undated"] is True
    assert io.UNDATED_NOTE in io.report_footer(payload)

    dated, subject = _io_case(dated=True)
    payload = io.compute(dated, [subject], "2026-11-11")
    assert "undated_note" not in payload
    assert not any(f.get("undated") for h in payload["holders"] for f in h["figures"])


def test_a_give_back_is_not_called_a_route() -> None:
    """Rosneft read "at least 21.9% … through RN-NeftKapitalInvest LLC" for
    BP: a subsidiary of Rosneft holding Rosneft's own shares."""
    from opencheck import integrated_ownership as io

    bods, subject = _io_case(dated=True)
    payload = io.compute(bods, [subject], "2026-11-11")
    bp = next(h for h in payload["holders"] if h["name"] == "BP Holder Ltd")
    (fig,) = bp["figures"]
    assert fig["treasury_names"] == ["Treasury Sub LLC"]
    route = io.route_words(fig)
    assert route == "directly, with its share of the company's own shares held by Treasury Sub LLC"
    assert "through" not in route


def test_the_frontend_integrated_note_matches() -> None:
    from opencheck import integrated_ownership as io

    ts = (_FRONTEND.parent / "integratedOwnership.ts").read_text(encoding="utf-8")
    assert f'"{io.UNDATED_NOTE}"' in " ".join(ts.split())
