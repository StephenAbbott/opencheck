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
    assert "#3b82f6" in d.svg  # ownership (oo.node.blue)
    assert "#7c3aed" in d.svg  # control/role (oo.node.purple)
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


# ---------------------------------------------------------------------------
# Phase 219 — ended relationships
# ---------------------------------------------------------------------------


def _ceased_bundle(*, closed: bool = True, end_date: str | None = "2019-06-18"):
    rels, by_id = _bundle()
    own = rels[0]
    own["recordStatus"] = "closed" if closed else "new"
    if end_date:
        own["recordDetails"]["interests"][0]["endDate"] = end_date
    return rels, by_id


def _line_for(svg: str, colour: str) -> str:
    return next(m for m in re.findall(r"<line [^>]*/>", svg) if f'stroke="{colour}"' in m and "marker-end" in m)


def test_ended_relationship_is_drawn_faint_with_its_colour_and_dated():
    rels, by_id = _ceased_bundle()
    d = source_diagram(rels, by_id, source_name="UK Companies House")
    own_line = _line_for(d.svg, "#3b82f6")
    assert 'stroke-opacity="0.5"' in own_line
    assert 'url(#aroe)' in own_line
    assert "ended 18 June 2019" in d.svg
    # The current director edge is untouched.
    ctrl_line = _line_for(d.svg, "#7c3aed")
    assert "stroke-opacity" not in ctrl_line and "url(#arc)" in ctrl_line
    # Nobody is dropped (BOVS completeness) and the legend names the fade.
    assert "Jane Eleanor Smith" in d.svg
    assert "ended relationship" in d.svg


def test_closed_record_without_a_date_says_ended_in_the_table():
    rels, by_id = _ceased_bundle(end_date=None)
    d = source_diagram(rels, by_id, source_name="UK Companies House")
    assert d.rows[0][1].endswith("(ended)")
    assert "· from 2016 · ended</text>" in d.svg
    # A dated end already reads "to <date>", so it is not said twice.
    rels, by_id = _ceased_bundle()
    d = source_diagram(rels, by_id, source_name="UK Companies House")
    assert "to 2019-06-18" in d.rows[0][1] and "(ended)" not in d.rows[0][1]


def test_open_record_with_a_past_end_date_is_ended_too():
    rels, by_id = _ceased_bundle(closed=False)
    d = source_diagram(rels, by_id, source_name="UK Companies House")
    assert 'stroke-opacity="0.5"' in _line_for(d.svg, "#3b82f6")


def test_no_ended_legend_entry_when_nothing_has_ended():
    rels, by_id = _bundle()
    d = source_diagram(rels, by_id, source_name="UK Companies House")
    assert "ended relationship" not in d.svg
    assert "stroke-opacity" not in d.svg
