"""Offline tests for the BODS -> print SVG diagram renderer."""

from __future__ import annotations

import re

from opencheck.reporting.diagram import source_diagram


def _bundle():
    entity = {
        "statementId": "ent-1",
        "recordType": "entity",
        "recordDetails": {
            "name": "Northwind Logistics Ltd",
            "identifiers": [{"scheme": "GB-COH", "id": "08123456"}],
        },
        "source": {"description": "UK Companies House"},
    }
    jane = {
        "statementId": "per-1",
        "recordType": "person",
        "recordDetails": {"names": [{"fullName": "Jane Eleanor Smith"}]},
    }
    mark = {
        "statementId": "per-2",
        "recordType": "person",
        "recordDetails": {"names": [{"fullName": "Mark Anthony Reyes"}]},
    }
    own = {
        "statementId": "rel-1",
        "recordType": "relationship",
        "recordDetails": {
            "interestedParty": "per-1",
            "subject": "ent-1",
            "interests": [{
                "type": "shareholding",
                "details": "ownership of shares",
                "share": {"exclusiveMinimum": 75, "maximum": 100},
                "startDate": "2016-04-06",
            }],
        },
        "source": {"description": "UK Companies House"},
    }
    director = {
        "statementId": "rel-2",
        "recordType": "relationship",
        "recordDetails": {
            "interestedParty": "per-2",
            "subject": "ent-1",
            "interests": [{"type": "seniorManagingOfficial", "details": "director",
                           "startDate": "2016-04-06"}],
        },
        "source": {"description": "UK Companies House"},
    }
    statements = [entity, jane, mark, own, director]
    by_id = {s["statementId"]: s for s in statements}
    rels = [own, director]
    return rels, by_id


def test_diagram_has_nodes_edges_and_labels():
    rels, by_id = _bundle()
    d = source_diagram(rels, by_id, source_name="UK Companies House")
    assert d.has_relationships
    assert d.svg.startswith("<svg")
    # Both people, the company and both interests are present in the SVG.
    for needle in ("Jane Eleanor Smith", "Mark Anthony Reyes", "Northwind Logistics Ltd",
                   "ownership of shares", "director"):
        assert needle in d.svg
    # Ownership edge is blue, control edge is purple.
    assert "#1565c0" in d.svg  # ownership
    assert "#6a1b9a" in d.svg  # control/role
    # Accessible: role=img + title + desc.
    assert 'role="img"' in d.svg and "<title" in d.svg and "<desc" in d.svg


def test_text_equivalent_rows():
    rels, by_id = _bundle()
    d = source_diagram(rels, by_id, source_name="UK Companies House")
    assert len(d.rows) == 2
    party, interest, subject = d.rows[0]
    assert party == "Jane Eleanor Smith"
    assert "75% or more" in interest
    assert subject == "Northwind Logistics Ltd"


def test_ownership_vs_control_classification():
    rels, by_id = _bundle()
    d = source_diagram(rels, by_id, source_name="UK Companies House")
    # ownership label uses the share band; control label names the role.
    assert re.search(r"ownership of shares — 75%\+ · from 2016", d.svg)
    assert "director · from 2016" in d.svg


def test_unspecified_party_renders():
    entity = {"statementId": "e", "recordType": "entity", "recordDetails": {"name": "ACME"}}
    rel = {
        "statementId": "r", "recordType": "relationship",
        "recordDetails": {"interestedParty": {"reason": "unknown"}, "subject": "e",
                          "interests": [{"type": "shareholding", "details": "ownership"}]},
    }
    by_id = {"e": entity, "r": rel}
    d = source_diagram([rel], by_id, source_name="Some register")
    assert "Unspecified party" in d.svg
    assert d.rows[0][0] == "Unspecified party (unknown)"


def test_diagram_caps_at_ten_relationships_but_table_keeps_all():
    entity = {"statementId": "ent", "recordType": "entity",
              "recordDetails": {"name": "Parent Co"}}
    by_id = {"ent": entity}
    rels = []
    for i in range(15):
        cid = f"sub-{i}"
        by_id[cid] = {"statementId": cid, "recordType": "entity",
                      "recordDetails": {"name": f"Subsidiary {i} Ltd"}}
        rels.append({
            "statementId": f"r-{i}", "recordType": "relationship",
            "recordDetails": {
                "interestedParty": "ent", "subject": cid,
                "interests": [{"type": "shareholding", "details": "ownership of shares"}],
            },
        })
    d = source_diagram(rels, by_id, source_name="GLEIF")
    assert len(d.rows) == 15          # table keeps every relationship
    assert d.shown == 10 and d.omitted == 5
    # The first subsidiary is drawn; the 15th (capped) is not in the SVG but is in rows.
    assert "Subsidiary 0 Ltd" in d.svg
    assert "Subsidiary 14 Ltd" not in d.svg
    assert any(subj == "Subsidiary 14 Ltd" for _, _, subj in d.rows)


def test_entity_only_when_no_relationships():
    entity = {"statementId": "e", "recordType": "entity", "recordDetails": {"name": "Lone Co"}}
    d = source_diagram([], {"e": entity}, source_name="GLEIF")
    assert not d.has_relationships
    assert "Lone Co" in d.svg
    assert "no ownership or control relationships" in d.svg.lower()
