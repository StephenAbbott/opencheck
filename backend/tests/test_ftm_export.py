"""Unit tests for the BODS v0.4 → FollowTheMoney mapper (`bods/ftm.py`).

Pure-function tests over hand-built BODS statements that mirror the shape
OpenCheck's factories emit (statementId == recordId for entity/person;
relationships referencing those ids). Mirrors `test_senzing.py`.
"""

from __future__ import annotations

import json

from opencheck.bods import map_to_ftm, to_ftm_jsonl


def _entity(sid: str, etype: str = "registeredEntity", **rd) -> dict:
    return {"statementId": sid, "recordId": sid, "recordType": "entity",
            "recordDetails": {"entityType": {"type": etype}, **rd}}


def _person(sid: str, **rd) -> dict:
    return {"statementId": sid, "recordId": sid, "recordType": "person",
            "recordDetails": {"personType": "knownPerson", **rd}}


def _rel(sid: str, subject: str, party, interests=()) -> dict:
    return {"statementId": sid, "recordType": "relationship",
            "recordDetails": {"subject": subject, "interestedParty": party,
                              "interests": list(interests)}}


def test_registered_entity_maps_to_company():
    bods = [_entity(
        "ent-1",
        name="Acme Ltd",
        alternateNames=["Acme"],
        jurisdiction={"name": "United Kingdom", "code": "GB"},
        foundingDate="1990-01-01",
        dissolutionDate="2020-06-30",
        identifiers=[
            {"id": "5493001KJTIIGC8Y1R12", "scheme": "XI-LEI", "schemeName": "LEI"},
            {"id": "00102498", "scheme": "GB-COH", "schemeName": "Companies House"},
            {"id": "Q12345", "scheme": "", "schemeName": "Wikidata"},
        ],
        addresses=[{"type": "registered", "address": "1 High St, London", "country": "GB"}],
    )]
    [ent] = map_to_ftm(bods)

    assert ent["id"] == "ent-1"
    assert ent["schema"] == "Company"
    p = ent["properties"]
    assert p["name"] == ["Acme Ltd"]
    assert p["alias"] == ["Acme"]
    assert p["leiCode"] == ["5493001KJTIIGC8Y1R12"]
    assert p["registrationNumber"] == ["00102498"]
    assert p["wikidataId"] == ["Q12345"]
    assert p["jurisdiction"] == ["gb"]  # FtM countries are lowercase
    assert p["incorporationDate"] == ["1990-01-01"]
    assert p["dissolutionDate"] == ["2020-06-30"]
    assert p["address"] == ["1 High St, London"]


def test_entity_type_schema_routing():
    bods = [
        _entity("e-state", etype="state", name="Ruritania"),
        _entity("e-body", etype="stateBody", name="Ministry of X"),
        _entity("e-arr", etype="arrangement", name="Family Trust"),
        _entity("e-anon", etype="anonymousEntity"),
    ]
    schemas = {e["id"]: e["schema"] for e in map_to_ftm(bods)}
    assert schemas["e-state"] == "PublicBody"
    assert schemas["e-body"] == "PublicBody"
    assert schemas["e-arr"] == "LegalEntity"
    assert schemas["e-anon"] == "LegalEntity"


def test_person_maps_to_person():
    bods = [_person(
        "per-1",
        names=[{"type": "legal", "fullName": "Jane Roe"},
               {"type": "alternative", "fullName": "J. Roe"}],
        birthDate="1970",
        nationalities=[{"name": "United Kingdom", "code": "GB"}],
        identifiers=[{"id": "ABC123", "scheme": "MISC", "schemeName": "Passport"}],
        addresses=[{"type": "residence", "address": "2 Park Rd", "country": "GB"}],
    )]
    [per] = map_to_ftm(bods)

    assert per["schema"] == "Person"
    p = per["properties"]
    assert p["name"] == ["Jane Roe"]
    assert p["alias"] == ["J. Roe"]
    assert p["birthDate"] == ["1970"]
    assert p["nationality"] == ["gb"]
    assert p["idNumber"] == ["ABC123"]
    assert p["address"] == ["2 Park Rd"]


def test_shareholding_becomes_ownership_link():
    bods = [
        _entity("ent-1", name="Acme Ltd"),
        _person("per-1", names=[{"type": "legal", "fullName": "Jane Roe"}]),
        _rel("rel-1", subject="ent-1", party="per-1", interests=[
            {"type": "shareholding", "share": {"exact": 50},
             "directOrIndirect": "direct", "startDate": "2010-05-01"},
        ]),
    ]
    entities = {e["id"]: e for e in map_to_ftm(bods)}
    own = entities["rel-1"]

    assert own["schema"] == "Ownership"
    p = own["properties"]
    assert p["owner"] == ["per-1"]
    assert p["asset"] == ["ent-1"]
    assert p["percentage"] == ["50"]
    assert p["ownershipType"] == ["direct"]
    assert p["startDate"] == ["2010-05-01"]
    assert p["role"] == ["shareholding"]


def test_share_bands_render_as_percentage_labels():
    bods = [
        _entity("ent-1", name="Acme"),
        _person("per-1", names=[{"type": "legal", "fullName": "Jane"}]),
        _rel("r1", "ent-1", "per-1", interests=[
            {"type": "shareholding", "share": {"exclusiveMinimum": 25, "maximum": 50}},
        ]),
    ]
    entities = {e["id"]: e for e in map_to_ftm(bods)}
    assert entities["r1"]["properties"]["percentage"] == [">25-50"]


def test_management_interest_becomes_directorship():
    bods = [
        _entity("ent-1", name="Acme Ltd"),
        _person("per-1", names=[{"type": "legal", "fullName": "Jane Roe"}]),
        _rel("rel-d", subject="ent-1", party="per-1", interests=[
            {"type": "seniorManagingOfficial", "details": "Director",
             "startDate": "2019-01-01"},
        ]),
    ]
    entities = {e["id"]: e for e in map_to_ftm(bods)}
    dire = entities["rel-d"]

    assert dire["schema"] == "Directorship"
    p = dire["properties"]
    assert p["director"] == ["per-1"]
    assert p["organization"] == ["ent-1"]
    assert p["role"] == ["senior managing official — Director"]
    assert p["startDate"] == ["2019-01-01"]


def test_multi_interest_relationship_yields_one_link_per_interest():
    bods = [
        _entity("ent-1", name="Acme"),
        _person("per-1", names=[{"type": "legal", "fullName": "Jane"}]),
        _rel("rel-m", "ent-1", "per-1", interests=[
            {"type": "shareholding", "share": {"exact": 30}},
            {"type": "votingRights"},
            {"type": "boardMember"},
        ]),
    ]
    entities = {e["id"]: e for e in map_to_ftm(bods)}

    assert entities["rel-m"]["schema"] == "Ownership"          # first interest
    assert entities["rel-m-2"]["schema"] == "Ownership"        # votingRights
    assert entities["rel-m-2"]["properties"]["role"] == ["voting rights"]
    assert entities["rel-m-3"]["schema"] == "Directorship"     # boardMember


def test_interestless_relationship_becomes_unknown_link():
    bods = [
        _entity("ent-1", name="Acme"),
        _person("per-1", names=[{"type": "legal", "fullName": "Jane"}]),
        _rel("rel-u", "ent-1", "per-1", interests=[]),
    ]
    entities = {e["id"]: e for e in map_to_ftm(bods)}
    link = entities["rel-u"]
    assert link["schema"] == "UnknownLink"
    assert link["properties"]["subject"] == ["per-1"]
    assert link["properties"]["object"] == ["ent-1"]


def test_unspecified_or_dangling_parties_are_dropped():
    bods = [
        _entity("ent-1", name="Acme"),
        # Unspecified interested party — nothing to link.
        _rel("rel-x", subject="ent-1",
             party={"unspecified": {"reason": "unknown"}},
             interests=[{"type": "shareholding"}]),
        # Party references a statement that isn't in the bundle.
        _rel("rel-y", subject="ent-1", party="ghost-1",
             interests=[{"type": "shareholding"}]),
    ]
    ids = {e["id"] for e in map_to_ftm(bods)}
    assert ids == {"ent-1"}


def test_nodes_precede_links_for_streaming_loaders():
    bods = [
        _rel("rel-1", "ent-1", "per-1", interests=[{"type": "shareholding"}]),
        _entity("ent-1", name="Acme"),
        _person("per-1", names=[{"type": "legal", "fullName": "Jane"}]),
    ]
    order = [e["id"] for e in map_to_ftm(bods)]
    assert order == ["ent-1", "per-1", "rel-1"]


def test_jsonl_serialisation_is_newline_delimited():
    bods = [
        _entity("ent-1", name="Acme"),
        _person("per-1", names=[{"type": "legal", "fullName": "Jane"}]),
    ]
    text = to_ftm_jsonl(bods)
    assert text.endswith("\n")
    lines = [ln for ln in text.split("\n") if ln.strip()]
    assert len(lines) == 2
    for ln in lines:
        obj = json.loads(ln)
        assert set(obj) == {"id", "schema", "properties"}


def test_empty_bundle_yields_no_entities():
    assert map_to_ftm([]) == []
    assert to_ftm_jsonl([]) == "\n"
