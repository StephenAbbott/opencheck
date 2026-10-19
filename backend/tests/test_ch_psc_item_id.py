"""A Companies House PSC party is keyed on the PSC item id, not the etag.

pKYC stage E prerequisite (Notion "Key Companies House PSC persons on the PSC
item id, not the etag"). Companies House reissues an item's ``etag`` on every
edit, so a share band moving on the same PSC gave the party — and the
relationship, keyed on the party — a new ``recordId``. The item id in
``links.self`` is the register's address for the item and survives edits; the
PSC stream's events carry the same field, so the watchlist's PSC trigger and
the mapped statement name an item by one key.
"""

from __future__ import annotations

from typing import Any

from opencheck import psc_stream
from opencheck.bods import map_companies_house
from opencheck.bods.annotations import person_identifiers_from_annotations
from opencheck.bods.mappers.companies_house import (
    PSC_ITEM_SCHEME,
    _ch_psc_local_id,
)
from opencheck.identifiers import ch_psc_item_id

_NUMBER = "00102498"
_ITEM = "Yb8mRZw1tKp0QsXe4vL2nC9hD3fA"
_PSC_PATH = f"/company/{_NUMBER}/persons-with-significant-control"


def _individual(
    *, etag: str, band: str = "25-to-50", item_id: str | None = _ITEM
) -> dict[str, Any]:
    psc: dict[str, Any] = {
        "name": "JONES, Alice",
        "kind": "individual-person-with-significant-control",
        "natures_of_control": [f"ownership-of-shares-{band}-percent"],
        "notified_on": "2016-04-06",
        "date_of_birth": {"year": 1980, "month": 7},
        "nationality": "British",
        "etag": etag,
    }
    if item_id:
        psc["links"] = {
            "self": f"/company/{_NUMBER}/persons-with-significant-control/individual/{item_id}"
        }
    return psc


def _corporate(*, etag: str, item_id: str) -> dict[str, Any]:
    return {
        "name": "NORDIC HOLDING AB",
        "kind": "corporate-entity-person-with-significant-control",
        "natures_of_control": ["ownership-of-shares-75-to-100-percent"],
        "notified_on": "2016-04-06",
        "identification": {
            "registration_number": "556000-0001",
            "country_registered": "Sweden",
            "place_registered": "Bolagsverket",
        },
        "etag": etag,
        "links": {"self": f"{_PSC_PATH}/corporate-entity/{item_id}"},
    }


def _bundle(pscs: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "source_id": "companies_house",
        "company_number": _NUMBER,
        "profile": {"company_name": "DEMO HOLDINGS PLC"},
        "officers": {"items": []},
        "pscs": {"items": pscs},
        "related_companies": {},
    }


def _party_and_relationship(pscs: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any]]:
    statements = list(map_companies_house(_bundle(pscs)).statements)
    parties = [
        s
        for s in statements
        if s["recordType"] in ("person", "entity")
        and s["recordDetails"].get("name") != "DEMO HOLDINGS PLC"
    ]
    rels = [s for s in statements if s["recordType"] == "relationship"]
    assert len(parties) == 1 and len(rels) == 1, (len(parties), len(rels))
    return parties[0], rels[0]


# ---------------------------------------------------------------------------
# The shared rule
# ---------------------------------------------------------------------------


def test_item_id_is_the_tail_of_the_self_link_and_nothing_else() -> None:
    for kind in ("individual", "corporate-entity", "legal-person", "super-secure"):
        item = {
            "links": {"self": f"/company/{_NUMBER}/persons-with-significant-control/{kind}/{_ITEM}"}
        }
        assert ch_psc_item_id(item) == _ITEM
    assert ch_psc_item_id({"links": {"self": "/officers/abc123/appointments"}}) is None
    assert ch_psc_item_id({"links": {}}) is None
    assert ch_psc_item_id({"etag": "x"}) is None
    assert ch_psc_item_id(None) is None


def test_the_stream_and_the_mapper_key_an_item_alike() -> None:
    """``psc_stream.psc_id_of`` and ``_ch_psc_local_id`` read the same field
    by the same rule, so a stream event names the statement the graph drew."""
    item = _individual(etag="e1")
    event = {
        "resource_uri": f"/company/{_NUMBER}/persons-with-significant-control/individual/{_ITEM}",
        "data": item,
    }
    assert psc_stream.psc_id_of(event) == ch_psc_item_id(item) == _ITEM
    assert _ch_psc_local_id(_NUMBER, item, "psc") == f"{_NUMBER}:psc:{_ITEM}"


# ---------------------------------------------------------------------------
# The mapping
# ---------------------------------------------------------------------------


def test_the_same_psc_with_two_etags_is_one_record() -> None:
    before_party, before_rel = _party_and_relationship([_individual(etag="e1", band="25-to-50")])
    after_party, after_rel = _party_and_relationship([_individual(etag="e2", band="50-to-75")])
    assert before_party["recordId"] == after_party["recordId"]
    assert before_rel["recordId"] == after_rel["recordId"]
    # And the change the register recorded is visible as a change, not a new party.
    assert (
        before_rel["recordDetails"]["interests"][0]["share"]
        != after_rel["recordDetails"]["interests"][0]["share"]
    )


def test_two_items_with_the_same_name_are_two_records() -> None:
    a = _individual(etag="e1", item_id="itemAAAAAAAAAAAAAAAAAAAAAAAA")
    b = _individual(etag="e1", item_id="itemBBBBBBBBBBBBBBBBBBBBBBBB")
    statements = list(map_companies_house(_bundle([a, b])).statements)
    persons = [s for s in statements if s["recordType"] == "person"]
    assert len(persons) == 2
    assert persons[0]["recordId"] != persons[1]["recordId"]


def test_the_item_id_is_published_as_an_identifying_annotation_not_an_identifier() -> None:
    party, _ = _party_and_relationship([_individual(etag="e1")])
    assert "identifiers" not in party["recordDetails"]
    idents = person_identifiers_from_annotations(party)
    assert [(i["scheme"], i["id"]) for i in idents] == [(PSC_ITEM_SCHEME, _ITEM)]
    assert idents[0]["uri"].endswith(f"/persons-with-significant-control/individual/{_ITEM}")


def test_a_corporate_psc_keeps_its_registration_number_and_notes_the_item_id() -> None:
    party, _ = _party_and_relationship([_corporate(etag="e1", item_id=_ITEM)])
    assert party["recordType"] == "entity"
    assert [i["scheme"] for i in party["recordDetails"]["identifiers"]] == ["REG-SE"]
    notes = [a for a in party.get("annotations", []) if a.get("motivation") == "identifying"]
    assert (
        len(notes) == 1
        and _ITEM in notes[0]["description"]
        and PSC_ITEM_SCHEME in notes[0]["description"]
    )
    # Keyed on the item id: a re-filed etag does not move the record.
    again, _ = _party_and_relationship([_corporate(etag="e2", item_id=_ITEM)])
    assert again["recordId"] == party["recordId"]


def test_a_withheld_psc_is_keyed_on_its_item_id_too() -> None:
    def ss(etag: str) -> dict[str, Any]:
        return {
            "kind": "super-secure-person-with-significant-control",
            "description": "super-secure-persons-with-significant-control",
            "etag": etag,
            "links": {
                "self": f"/company/{_NUMBER}/persons-with-significant-control/super-secure/{_ITEM}"
            },
        }

    a, _ = _party_and_relationship([ss("e1")])
    b, _ = _party_and_relationship([ss("e2")])
    assert a["recordDetails"]["personType"] == "anonymousPerson"
    assert a["recordId"] == b["recordId"]


def test_a_record_without_a_self_link_keeps_the_old_etag_key() -> None:
    """Fixtures older than the links block (never seen on the live API)
    still map, on the key they always had, with no item annotation."""
    party, _ = _party_and_relationship([_individual(etag="e1", item_id=None)])
    assert (
        _ch_psc_local_id(_NUMBER, _individual(etag="e1", item_id=None), "psc")
        == f"{_NUMBER}:psc:e1"
    )
    assert person_identifiers_from_annotations(party) == []
