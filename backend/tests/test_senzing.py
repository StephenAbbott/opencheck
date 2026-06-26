"""Unit tests for the BODS v0.4 → Senzing JSON mapper (`bods/senzing.py`).

Pure-function tests over hand-built BODS statements that mirror the shape
OpenCheck's factories emit (statementId == recordId for entity/person;
relationships referencing those ids).
"""

from __future__ import annotations

import json

from opencheck.bods import map_to_senzing, to_senzing_jsonl


def _entity(sid: str, **rd) -> dict:
    return {"statementId": sid, "recordId": sid, "recordType": "entity",
            "recordDetails": {"entityType": {"type": "registeredEntity"}, **rd}}


def _person(sid: str, **rd) -> dict:
    return {"statementId": sid, "recordId": sid, "recordType": "person",
            "recordDetails": {"personType": "knownPerson", **rd}}


def _rel(sid: str, subject: str, party, interests=()) -> dict:
    return {"statementId": sid, "recordType": "relationship",
            "recordDetails": {"subject": subject, "interestedParty": party,
                              "interests": list(interests)}}


def _features(record: dict) -> list[dict]:
    return record["FEATURES"]


def _find(record: dict, key: str) -> list[dict]:
    return [f for f in record["FEATURES"] if key in f]


def test_entity_maps_to_organization_record():
    bods = [_entity(
        "ent-1",
        name="Acme Ltd",
        alternateNames=["Acme"],
        jurisdiction={"name": "United Kingdom", "code": "GB"},
        foundingDate="1990-01-01",
        identifiers=[
            {"id": "5493001KJTIIGC8Y1R12", "scheme": "XI-LEI", "schemeName": "LEI"},
            {"id": "00102498", "scheme": "GB-COH", "schemeName": "Companies House"},
        ],
        addresses=[{"type": "registered", "address": "1 High St, London", "country": "GB"}],
    )]
    [rec] = map_to_senzing(bods)

    assert rec["DATA_SOURCE"] == "OPENCHECK"
    assert rec["RECORD_ID"] == "ent-1"
    assert {"RECORD_TYPE": "ORGANIZATION"} in rec["FEATURES"]
    assert {"NAME_ORG": "Acme Ltd", "NAME_TYPE": "PRIMARY"} in rec["FEATURES"]
    assert {"NAME_ORG": "Acme", "NAME_TYPE": "ALTERNATE"} in rec["FEATURES"]
    assert {"LEI_NUMBER": "5493001KJTIIGC8Y1R12"} in rec["FEATURES"]
    assert {"NATIONAL_ID_NUMBER": "00102498", "NATIONAL_ID_TYPE": "GB-COH",
            "NATIONAL_ID_COUNTRY": "GB"} in rec["FEATURES"]
    assert {"REGISTRATION_DATE": "1990-01-01"} in rec["FEATURES"]
    assert {"REGISTRATION_COUNTRY": "GB"} in rec["FEATURES"]
    addr = _find(rec, "ADDR_FULL")[0]
    assert addr == {"ADDR_TYPE": "BUSINESS", "ADDR_FULL": "1 High St, London",
                    "ADDR_COUNTRY": "GB"}
    # Exactly one anchor, keyed by the statementId.
    anchors = _find(rec, "REL_ANCHOR_KEY")
    assert anchors == [{"REL_ANCHOR_DOMAIN": "OPENCHECK", "REL_ANCHOR_KEY": "ent-1"}]


def test_person_maps_to_person_record():
    bods = [_person(
        "per-1",
        names=[{"type": "legal", "fullName": "Jane Roe"}],
        birthDate="1970",
        nationalities=[{"name": "United Kingdom", "code": "GB"}],
        addresses=[{"type": "residence", "address": "2 Park Rd", "country": "GB"}],
    )]
    [rec] = map_to_senzing(bods)

    assert {"RECORD_TYPE": "PERSON"} in rec["FEATURES"]
    assert {"NAME_FULL": "Jane Roe", "NAME_TYPE": "PRIMARY"} in rec["FEATURES"]
    assert {"DATE_OF_BIRTH": "1970"} in rec["FEATURES"]
    assert {"NATIONALITY": "GB"} in rec["FEATURES"]
    assert {"ADDR_TYPE": "HOME", "ADDR_FULL": "2 Park Rd", "ADDR_COUNTRY": "GB"} \
        in rec["FEATURES"]
    assert _find(rec, "REL_ANCHOR_KEY") == \
        [{"REL_ANCHOR_DOMAIN": "OPENCHECK", "REL_ANCHOR_KEY": "per-1"}]


def test_relationship_becomes_pointer_on_interested_party():
    bods = [
        _entity("ent-1", name="Acme Ltd"),
        _person("per-1", names=[{"type": "legal", "fullName": "Jane Roe"}]),
        _rel("rel-1", subject="ent-1", party="per-1", interests=[
            {"type": "shareholding", "share": {"exclusiveMinimum": 25, "maximum": 50},
             "startDate": "2010-05-01"},
        ]),
    ]
    recs = {r["RECORD_ID"]: r for r in map_to_senzing(bods)}

    # The pointer lives on the OWNER (interested party), aimed at the company anchor.
    pointers = _find(recs["per-1"], "REL_POINTER_KEY")
    assert pointers == [{
        "REL_POINTER_DOMAIN": "OPENCHECK",
        "REL_POINTER_KEY": "ent-1",
        "REL_POINTER_ROLE": "OWNER_OF >25-50%",
        "REL_POINTER_FROM_DATE": "2010-05-01",
    }]
    # The owned company carries no pointer (only its anchor).
    assert _find(recs["ent-1"], "REL_POINTER_KEY") == []


def test_exact_share_and_role_fallback():
    bods = [
        _entity("ent-1", name="Acme"),
        _person("per-1", names=[{"type": "legal", "fullName": "Jane"}]),
        _rel("r1", "ent-1", "per-1", interests=[{"type": "shareholding", "share": {"exact": 50}}]),
        _rel("r2", "ent-1", "per-1", interests=[{"type": "votingRights"}]),
    ]
    recs = {r["RECORD_ID"]: r for r in map_to_senzing(bods)}
    roles = {p["REL_POINTER_ROLE"] for p in _find(recs["per-1"], "REL_POINTER_KEY")}
    assert "OWNER_OF 50%" in roles
    assert "VOTING_RIGHTS_IN" in roles


def test_unspecified_interested_party_is_skipped():
    bods = [
        _entity("ent-1", name="Acme"),
        _rel("rel-x", subject="ent-1",
             party={"unspecified": {"reason": "unknown"}}, interests=[]),
    ]
    [rec] = map_to_senzing(bods)
    # No record to anchor a pointer on → relationship dropped, only the anchor remains.
    assert _find(rec, "REL_POINTER_KEY") == []


def test_lei_detected_by_value_shape_without_lei_scheme():
    bods = [_entity("ent-1", name="Acme",
                    identifiers=[{"id": "5493001KJTIIGC8Y1R12", "scheme": "", "schemeName": ""}])]
    [rec] = map_to_senzing(bods)
    assert {"LEI_NUMBER": "5493001KJTIIGC8Y1R12"} in rec["FEATURES"]


def test_jsonl_serialisation_is_newline_delimited():
    bods = [
        _entity("ent-1", name="Acme"),
        _person("per-1", names=[{"type": "legal", "fullName": "Jane"}]),
    ]
    text = to_senzing_jsonl(bods)
    assert text.endswith("\n")
    lines = [ln for ln in text.split("\n") if ln.strip()]
    assert len(lines) == 2
    for ln in lines:
        obj = json.loads(ln)
        assert obj["DATA_SOURCE"] == "OPENCHECK"
        assert isinstance(obj["FEATURES"], list)


def _src(name: str) -> dict:
    return {"type": ["thirdParty"], "description": name}


def test_licensing_payload_uses_most_restrictive_source():
    # e1 is GLEIF-sourced (permissive) but is the interested party of an
    # OpenSanctions-sourced (CC-BY-NC) relationship → its record must carry the
    # non-commercial licence.
    bods = [
        {"statementId": "e1", "recordType": "entity",
         "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": "Acme"},
         "source": _src("GLEIF")},
        {"statementId": "e2", "recordType": "entity",
         "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": "Sub"},
         "source": _src("GLEIF")},
        {"statementId": "rel-1", "recordType": "relationship",
         "recordDetails": {"subject": "e2", "interestedParty": "e1",
                           "interests": [{"type": "shareholding"}]},
         "source": _src("OpenSanctions")},
    ]
    recs = {r["RECORD_ID"]: r for r in map_to_senzing(bods)}

    assert recs["e1"]["DATA_LICENSE"] == "CC-BY-NC-4.0"
    assert "OpenSanctions" in recs["e1"]["ATTRIBUTION"]
    # e2 is GLEIF-only → not the non-commercial licence.
    assert recs["e2"]["DATA_LICENSE"] != "CC-BY-NC-4.0"


def test_no_source_block_means_no_licensing_payload():
    [rec] = map_to_senzing([_entity("e1")])
    assert "DATA_LICENSE" not in rec
    assert "ATTRIBUTION" not in rec


def test_empty_bundle_yields_no_records():
    assert map_to_senzing([]) == []
    assert to_senzing_jsonl([]) == "\n"
