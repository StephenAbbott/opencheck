"""The FullCheck anchor collapse rewrites references, never text (the
structured-remap ticket, 24 Oct 2026).

``routers/expand.py::_apply_id_remap`` used to serialise a hop's BODS and
signals to JSON and string-replace the collapsed ids — every occurrence, an
annotation sentence or a ``summary`` included, with no idea which fields are
references. It now walks the reference fields ``bods/refs.py`` names:
``STATEMENT_REFERENCE_FIELDS`` on a statement and, in a signal's evidence, the
``SIGNAL_REFERENCE_FIELDS`` plus any ``statement_id`` / ``…statement_ids`` key
at any depth (Stephen, 24 Oct 2026: both).

The fixture is the MEIP shape scrambled: ``statementId != recordId`` on every
statement, ``declarationSubject`` naming the head on every statement, one
legacy-wrapped party reference and one ``UnspecifiedRecord``. The collapsed
graph must draw one node for the hopped company, and every string that is not
a reference must come back byte-identical.
"""

from __future__ import annotations

import copy
import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.bods import refs
from opencheck.bods.mapper import _stable_id
from opencheck.config import get_settings
from opencheck.routers import expand

LEI = "213800LH1BZH3DI6G760"
ANCHOR = "anchor-node-on-the-canvas"

# The hopped company as GLEIF files it (the id the pipeline predicts) …
GLEIF_RID = _stable_id("gleif", "entity", LEI)
GLEIF_SID = f"{GLEIF_RID}-content-0a1b2c3d"
# … and as a national register files it, keyed on the company number only.
NAT_RID = "opencheck-nat-record-000000000001"
NAT_SID = "opencheck-nat-content-00000000001"
OWNER_RID = "opencheck-owner-record-0000000001"
OWNER_SID = "opencheck-owner-content-000000001"
PERSON_RID = "opencheck-person-record-000000001"
PERSON_SID = "opencheck-person-content-00000001"
SUB_RID = "opencheck-sub-record-000000000001"
SUB_SID = "opencheck-sub-content-00000000001"

SENTENCE = f"Also recorded as statement {GLEIF_RID} and {NAT_SID} in the register."


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _entity(sid: str, rid: str, name: str, identifiers: list[dict]) -> dict:
    return {
        "statementId": sid,
        "recordId": rid,
        "recordType": "entity",
        "declarationSubject": GLEIF_RID,
        "recordDetails": {
            "entityType": {"type": "registeredEntity"},
            "name": name,
            "identifiers": identifiers,
        },
        "annotations": [
            {"motivation": "commenting", "statementPointerTarget": "/recordDetails",
             "description": SENTENCE},
        ],
    }


def scrambled_hop() -> list[dict]:
    """A hop's bundle in the publisher-native shape."""
    return [
        _entity(GLEIF_SID, GLEIF_RID, "HoldCo", [
            {"id": LEI, "scheme": "XI-LEI"},
            {"id": "12345678", "scheme": "GB-COH"},
        ]),
        _entity(NAT_SID, NAT_RID, "HoldCo Ltd", [{"id": "12345678", "scheme": "GB-COH"}]),
        _entity(OWNER_SID, OWNER_RID, "Parent BV", [{"id": "NL-1", "scheme": "NL-KVK"}]),
        _entity(SUB_SID, SUB_RID, "Sub GmbH", [{"id": "HRB 1", "scheme": "DE-CR"}]),
        {
            "statementId": PERSON_SID, "recordId": PERSON_RID, "recordType": "person",
            "declarationSubject": GLEIF_RID,
            "recordDetails": {"personType": "knownPerson",
                              "names": [{"type": "legal", "fullName": "Jane Roe"}]},
        },
        # v0.4 bare reference, by recordId (the standard's spelling).
        {
            "statementId": "rel-1", "recordId": "rel-1-rec", "recordType": "relationship",
            "declarationSubject": GLEIF_RID,
            "recordDetails": {"subject": GLEIF_RID, "interestedParty": OWNER_RID,
                              "interests": [{"type": "shareholding",
                                             "details": f"Filed against {GLEIF_RID}"}]},
        },
        # Legacy wrapped reference to the register's record of the same company.
        {
            "statementId": "rel-2", "recordId": "rel-2-rec", "recordType": "relationship",
            "declarationSubject": GLEIF_RID,
            "recordDetails": {"subject": {"describedByEntityStatement": NAT_RID},
                              "interestedParty": {"describedByPersonStatement": PERSON_RID},
                              "interests": [{"type": "seniorManagingOfficial"}],
                              "componentRecords": ["rel-1-rec", NAT_RID]},
        },
        # The company as the interested party (a GLEIF child), by recordId.
        {
            "statementId": "rel-4", "recordId": "rel-4-rec", "recordType": "relationship",
            "declarationSubject": GLEIF_RID,
            "recordDetails": {"subject": SUB_RID, "interestedParty": GLEIF_RID,
                              "interests": [{"type": "shareholding"}]},
        },
        # An unspecified party stays an unspecified party.
        {
            "statementId": "rel-3", "recordId": "rel-3-rec", "recordType": "relationship",
            "declarationSubject": GLEIF_RID,
            "recordDetails": {"subject": GLEIF_RID,
                              "interestedParty": {"reason": "unknown",
                                                  "description": f"see {GLEIF_RID}"},
                              "interests": [{"type": "unknownInterest"}]},
        },
    ]


def scrambled_signals() -> list[dict]:
    return [
        {"code": "SANCTIONED", "kind": "risk", "confidence": "high",
         "source_id": "opensanctions", "hit_id": GLEIF_RID,
         "summary": f"Listed; statement {GLEIF_RID}",
         "evidence": {"statement_id": GLEIF_RID, "subject_statement_ids": [GLEIF_SID, NAT_SID],
                      "detail": f"matched {NAT_RID}"}},
        {"code": "COMPLEX_OWNERSHIP_LAYERS", "kind": "context", "confidence": "high",
         "source_id": "opencheck", "hit_id": LEI,
         "evidence": {"longest_path": [GLEIF_SID, OWNER_SID],
                      "ended_relationship_statement_ids": ["rel-1"]}},
        # The nested shapes the named list does not reach.
        {"code": "STATE_CONTROLLED", "kind": "risk", "confidence": "medium",
         "source_id": "wikidata", "hit_id": "Q1",
         "evidence": {"matches": [{"statement_id": OWNER_SID, "name": "Parent BV"}],
                      "state_holdings": [{"country": "NL",
                                          "parties": [{"statement_id": NAT_SID}],
                                          "party_statement_ids": [NAT_SID, OWNER_SID],
                                          "relationship_statement_ids": ["rel-2"]}]}},
        {"code": "TRUST_OR_ARRANGEMENT", "kind": "risk", "confidence": "high",
         "source_id": "companies_house", "hit_id": "12345678",
         "evidence": {"elements": [{"element": "arrangement",
                                    "statement_ids": [GLEIF_SID, PERSON_SID]}],
                      "jurisdictions": [{"code": "GB", "statement_id": GLEIF_SID}]}},
    ]


def _repl() -> dict[str, str]:
    return expand._anchor_replacements(scrambled_hop(), LEI, ANCHOR)


# ---------------------------------------------------------------------------
# The table
# ---------------------------------------------------------------------------


def test_the_table_names_both_ids_of_every_record_of_the_company() -> None:
    repl = _repl()
    assert set(repl) == {GLEIF_RID, GLEIF_SID, NAT_RID, NAT_SID}
    assert set(repl.values()) == {ANCHOR}
    # The owner, the person and the relationships are not the company.
    for untouched in (OWNER_RID, OWNER_SID, PERSON_RID, PERSON_SID, "rel-1", "rel-1-rec"):
        assert untouched not in repl


# ---------------------------------------------------------------------------
# Statements
# ---------------------------------------------------------------------------


def _collapsed() -> list[dict]:
    return refs.remap_statement_references(scrambled_hop(), _repl())


def test_the_collapsed_graph_draws_one_node_for_the_company() -> None:
    """Built the frontend's way: dedupe by statementId, resolve every edge
    endpoint, count the distinct party nodes an edge reaches."""
    bods = _collapsed()
    parties = {s["statementId"]: s for s in bods if s["recordType"] in ("entity", "person")}
    resolve = refs.resolver(bods)
    endpoints: set[str] = set()
    for s in bods:
        if s["recordType"] != "relationship":
            continue
        for field in ("subject", "interestedParty"):
            ref = refs.party_ref(s["recordDetails"][field])
            if ref is not None:
                endpoints.add(resolve(ref))
    # Both representations of the company fold onto the anchor; nothing else moves.
    assert ANCHOR in endpoints
    assert endpoints == {ANCHOR, OWNER_SID, PERSON_SID, SUB_SID}
    holdco = [s for s in bods if s["recordType"] == "entity"
              and s["recordDetails"]["name"].startswith("HoldCo")]
    assert len(holdco) == 2 and all(s["statementId"] == ANCHOR for s in holdco)
    # Deduped by statementId, the two records are one node.
    assert len(parties) == len(bods) - sum(1 for s in bods if s["recordType"] == "relationship") - 1


def test_every_reference_field_is_rewritten_in_its_own_shape() -> None:
    by_id = {s["statementId"] if s["recordType"] != "entity" else s["recordDetails"]["name"]: s
             for s in _collapsed()}
    for name in ("HoldCo", "HoldCo Ltd"):
        assert by_id[name]["statementId"] == ANCHOR
        assert by_id[name]["recordId"] == ANCHOR
    # declarationSubject on every statement named the company's recordId.
    assert all(s["declarationSubject"] == ANCHOR for s in _collapsed())
    # A bare reference stays bare …
    assert by_id["rel-1"]["recordDetails"]["subject"] == ANCHOR
    assert by_id["rel-1"]["recordDetails"]["interestedParty"] == OWNER_RID
    # … a wrapped one keeps its wrapper key …
    assert by_id["rel-2"]["recordDetails"]["subject"] == {"describedByEntityStatement": ANCHOR}
    assert by_id["rel-2"]["recordDetails"]["interestedParty"] == {
        "describedByPersonStatement": PERSON_RID}
    assert by_id["rel-2"]["recordDetails"]["componentRecords"] == ["rel-1-rec", ANCHOR]
    assert by_id["rel-4"]["recordDetails"] == {"subject": SUB_RID, "interestedParty": ANCHOR,
                                               "interests": [{"type": "shareholding"}]}
    # … and an UnspecifiedRecord is not a reference.
    assert by_id["rel-3"]["recordDetails"]["interestedParty"] == {
        "reason": "unknown", "description": f"see {GLEIF_RID}"}


def test_free_text_is_left_as_written() -> None:
    bods = _collapsed()
    for s in bods:
        for a in s.get("annotations") or []:
            assert a["description"] == SENTENCE
    rel = next(s for s in bods if s["statementId"] == "rel-1")
    assert rel["recordDetails"]["interests"][0]["details"] == f"Filed against {GLEIF_RID}"
    # The old string replace would have left no trace of the id anywhere.
    assert GLEIF_RID in json.dumps(bods)


def test_statements_are_copied_on_change_and_shared_otherwise() -> None:
    hop = scrambled_hop()
    before = copy.deepcopy(hop)
    out = refs.remap_statement_references(hop, _repl())
    assert hop == before, "the input is never mutated"
    owner_in = next(s for s in hop if s["recordId"] == OWNER_RID)
    owner_out = next(s for s in out if s["recordId"] == OWNER_RID)
    # The owner's declarationSubject named the company, so it was copied …
    assert owner_out is not owner_in and owner_out["declarationSubject"] == ANCHOR
    # … while a statement nothing touched is the same object.
    plain = {"statementId": "x", "recordId": "x", "recordType": "person",
             "recordDetails": {"personType": "knownPerson"}}
    assert refs.remap_statement_references([plain], _repl())[0] is plain
    assert refs.remap_statement_references(hop, {}) == hop


# ---------------------------------------------------------------------------
# Signals
# ---------------------------------------------------------------------------


def test_signal_evidence_is_rewritten_at_every_depth() -> None:
    sigs = refs.remap_signal_references(scrambled_signals(), _repl())
    by_code = {s["code"]: s["evidence"] for s in sigs}
    assert by_code["SANCTIONED"]["statement_id"] == ANCHOR
    assert by_code["SANCTIONED"]["subject_statement_ids"] == [ANCHOR, ANCHOR]
    assert by_code["COMPLEX_OWNERSHIP_LAYERS"]["longest_path"] == [ANCHOR, OWNER_SID]
    assert by_code["COMPLEX_OWNERSHIP_LAYERS"]["ended_relationship_statement_ids"] == ["rel-1"]
    holding = by_code["STATE_CONTROLLED"]["state_holdings"][0]
    assert holding["parties"] == [{"statement_id": ANCHOR}]
    assert holding["party_statement_ids"] == [ANCHOR, OWNER_SID]
    assert holding["relationship_statement_ids"] == ["rel-2"]
    assert by_code["STATE_CONTROLLED"]["matches"] == [{"statement_id": OWNER_SID, "name": "Parent BV"}]
    assert by_code["TRUST_OR_ARRANGEMENT"]["elements"][0]["statement_ids"] == [ANCHOR, PERSON_SID]
    assert by_code["TRUST_OR_ARRANGEMENT"]["jurisdictions"] == [{"code": "GB", "statement_id": ANCHOR}]


def test_signal_text_and_hit_id_are_left_as_written() -> None:
    sigs = refs.remap_signal_references(scrambled_signals(), _repl())
    sanctioned = next(s for s in sigs if s["code"] == "SANCTIONED")
    assert sanctioned["summary"] == f"Listed; statement {GLEIF_RID}"
    assert sanctioned["hit_id"] == GLEIF_RID
    assert sanctioned["evidence"]["detail"] == f"matched {NAT_RID}"


def test_signals_are_copied_on_change_and_shared_otherwise() -> None:
    sigs = scrambled_signals()
    before = copy.deepcopy(sigs)
    out = refs.remap_signal_references(sigs, _repl())
    assert sigs == before
    untouched = {"code": "PEP", "evidence": {"statement_id": OWNER_SID}}
    assert refs.remap_signal_references([untouched], _repl())[0] is untouched
    assert refs.remap_signal_references([{"code": "X"}], _repl()) == [{"code": "X"}]


# ---------------------------------------------------------------------------
# The field lists are pinned
# ---------------------------------------------------------------------------


def test_the_statement_field_list_is_what_the_walk_reads() -> None:
    """Every reference position in the fixture is one of the named fields, and
    every named field is exercised — a reference field added elsewhere without
    a name here fails this, as does a name here nothing reads."""
    assert refs.STATEMENT_REFERENCE_FIELDS == (
        "statementId", "recordId", "declarationSubject",
        "recordDetails/subject", "recordDetails/interestedParty",
        "recordDetails/componentRecords",
    )
    repl = _repl()
    collapsed = refs.remap_statement_references(scrambled_hop(), repl)
    seen: set[str] = set()
    for s in collapsed:
        for field in ("statementId", "recordId", "declarationSubject"):
            if s.get(field) == ANCHOR:
                seen.add(field)
        rd = s["recordDetails"]
        for field in ("subject", "interestedParty"):
            if refs.party_ref(rd.get(field)) == ANCHOR:
                seen.add(f"recordDetails/{field}")
        if ANCHOR in (rd.get("componentRecords") or []):
            seen.add("recordDetails/componentRecords")
        # No old id survives in a reference position.
        for field in ("statementId", "recordId", "declarationSubject"):
            assert s.get(field) not in repl
        for field in ("subject", "interestedParty"):
            assert refs.party_ref(rd.get(field)) not in repl
        assert not set(rd.get("componentRecords") or []) & set(repl)
    assert seen == set(refs.STATEMENT_REFERENCE_FIELDS)


def test_the_signal_walk_covers_the_named_fields_and_the_suffix_rule() -> None:
    assert dict(refs.SIGNAL_REFERENCE_FIELDS) == {
        "statement_id": "one", "subject_statement_id": "one",
        "subject_statement_ids": "list", "longest_path": "list",
        "matches": "list_of_dicts", "jurisdictions": "list_of_dicts",
        "ended_relationship_statement_ids": "list",
        "subject_statements": "list", "exceptions": "list_of_dicts",
    }
    assert refs.EVIDENCE_ID_KEY == "statement_id"
    assert refs.EVIDENCE_ID_LIST_SUFFIX == "statement_ids"
    # Each named field is a position the suffix rule reads or a named exception.
    for field, shape in refs.SIGNAL_REFERENCE_FIELDS:
        assert (field == refs.EVIDENCE_ID_KEY or field.endswith(refs.EVIDENCE_ID_LIST_SUFFIX)
                or field in refs._NAMED_ONE_FIELDS | refs._NAMED_LIST_FIELDS
                or shape == "list_of_dicts")
    # After the walk, no reference position anywhere in evidence holds an old id.
    repl = _repl()

    def positions(value, key=None):
        if isinstance(value, dict):
            for k, v in value.items():
                if k == refs.EVIDENCE_ID_KEY or k in refs._NAMED_ONE_FIELDS:
                    yield v
                elif k.endswith(refs.EVIDENCE_ID_LIST_SUFFIX) or k in refs._NAMED_LIST_FIELDS:
                    yield from (v if isinstance(v, list) else [v])
                else:
                    yield from positions(v, k)
        elif isinstance(value, list):
            for v in value:
                yield from positions(v)

    for sig in refs.remap_signal_references(scrambled_signals(), repl):
        assert not set(positions(sig["evidence"])) & set(repl)


# ---------------------------------------------------------------------------
# Through the route
# ---------------------------------------------------------------------------


def test_expand_layer_collapses_the_scrambled_hop_onto_the_anchor(client, monkeypatch) -> None:
    async def _fake_lookup(*, lei, deepen_top=3):
        return SimpleNamespace(lei=lei, bods=scrambled_hop(), bods_issues=[],
                               risk_signals=scrambled_signals(), degraded_sources=[])

    monkeypatch.setattr("opencheck.routers.lookup._lookup_impl", _fake_lookup)
    r = client.post("/expand-layer", json={"items": [{"lei": LEI, "anchor": ANCHOR}]})
    assert r.status_code == 200
    body = r.json()
    rel = next(s for s in body["bods"] if s["statementId"] == "rel-2")
    assert rel["recordDetails"]["subject"] == {"describedByEntityStatement": ANCHOR}
    sanctioned = next(s for s in body["risk_signals"] if s["code"] == "SANCTIONED")
    assert sanctioned["evidence"]["statement_id"] == ANCHOR
    assert sanctioned["summary"] == f"Listed; statement {GLEIF_RID}"
    # The annotation sentence came through the route untouched.
    annotated = next(s for s in body["bods"] if s["statementId"] == ANCHOR)
    assert annotated["annotations"][0]["description"] == SENTENCE


def test_apply_id_remap_tells_signals_from_statements() -> None:
    repl = _repl()
    assert expand._apply_id_remap(scrambled_signals(), repl)[0]["evidence"]["statement_id"] == ANCHOR
    assert expand._apply_id_remap(scrambled_hop(), repl)[0]["statementId"] == ANCHOR
    assert expand._apply_id_remap([], repl) == []


# ---------------------------------------------------------------------------
# The two shapes the production check found (25 Oct 2026)
# ---------------------------------------------------------------------------


def _record_consistency() -> dict:
    """RECORD_CONSISTENCY as production emits it: the compared statements per
    item under ``items[].statement_ids``, and the subject's statements under
    ``subject_statements`` — a key neither suffix rule matches."""
    return {
        "code": "RECORD_CONSISTENCY", "kind": "context", "confidence": "high",
        "source_id": "opencheck", "hit_id": LEI,
        "summary": f"Four sources describe this entity ({GLEIF_SID}).",
        "evidence": {
            "items": [{"field": "jurisdiction", "statement_ids": [GLEIF_SID, NAT_SID]}],
            "subject_statements": sorted([GLEIF_SID, NAT_SID, OWNER_SID]),
        },
    }


def test_record_consistency_subject_statements_follow_the_anchor() -> None:
    """Every collapsed id that survived Ørsted's and Equinor's production
    hops sat in ``subject_statements``; the old string replace rewrote it."""
    out = refs.remap_signal_references([_record_consistency()], _repl())[0]
    assert out["evidence"]["subject_statements"] == [ANCHOR, ANCHOR, OWNER_SID]
    assert out["evidence"]["items"][0]["statement_ids"] == [ANCHOR, ANCHOR]
    assert out["summary"] == f"Four sources describe this entity ({GLEIF_SID})."


def test_record_consistency_subject_statements_are_resolved() -> None:
    """The named field is resolved too: a recordId there becomes the
    statementId every consumer keys on."""
    sig = _record_consistency()
    sig["evidence"]["subject_statements"] = [GLEIF_RID, NAT_RID]
    out = refs.resolve_signal_references([sig], scrambled_hop())[0]
    assert out["evidence"]["subject_statements"] == [GLEIF_SID, NAT_SID]


def test_gleif_exceptions_name_the_same_statement_as_matches() -> None:
    """GLEIF_REPORTING_EXCEPTION puts each bridge's id in ``exceptions[]``
    and ``matches[]``. In production ``matches[]`` was resolved to the
    statementId and ``exceptions[]`` kept the recordId — two ids for one
    node. Both are resolved now, and the exception's own fields survive."""
    sig = {
        "code": "GLEIF_REPORTING_EXCEPTION", "kind": "context", "confidence": "high",
        "source_id": "gleif", "hit_id": LEI,
        "evidence": {
            "exceptions": [{"relationship": "direct", "reason": "NON_CONSOLIDATING",
                            "category": "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT",
                            "reference": "", "statement_id": OWNER_RID}],
            "matches": [{"statement_id": OWNER_RID}],
        },
    }
    out = refs.resolve_signal_references([sig], scrambled_hop())[0]["evidence"]
    assert out["matches"] == [{"statement_id": OWNER_SID}]
    assert out["exceptions"] == [{"relationship": "direct", "reason": "NON_CONSOLIDATING",
                                  "category": "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT",
                                  "reference": "", "statement_id": OWNER_SID}]
    assert out["exceptions"][0]["statement_id"] == out["matches"][0]["statement_id"]
