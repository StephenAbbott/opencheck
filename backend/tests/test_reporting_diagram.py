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
                   "Owns 75–100%", "Director"):
        assert needle in d.svg
    # Phase 221: the canvas's kinds — ownership blue solid, a director's role
    # purple dashed.
    assert "#3b82f6" in _line_for(d.svg, "#3b82f6")
    assert 'stroke-dasharray="9 6"' in _line_for(d.svg, "#7c3aed")
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
    # The canvas's words: the ownership label carries the share band, the role
    # label names the role; the start year is a line of its own.
    assert re.search(r">Owns 75–100%</tspan><tspan [^>]*>from 2016</tspan>", d.svg)
    assert re.search(r">Director</tspan><tspan [^>]*>from 2016</tspan>", d.svg)
    # The register's own wording is in the table, not on the edge.
    assert "ownership of shares" not in d.svg
    assert "ownership of shares" in d.rows[0][1]


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
    assert 'url(#ar-ownership-ended)' in own_line
    assert "ended 18 June 2019" in d.svg
    # The current director edge is untouched.
    role_line = _line_for(d.svg, "#7c3aed")
    assert "stroke-opacity" not in role_line and "url(#ar-role)" in role_line
    # Nobody is dropped (BOVS completeness) and the legend names the fade.
    assert "Jane Eleanor Smith" in d.svg
    assert "Ended relationship" in d.svg


def test_closed_record_without_a_date_says_ended_in_the_table():
    rels, by_id = _ceased_bundle(end_date=None)
    d = source_diagram(rels, by_id, source_name="UK Companies House")
    assert d.rows[0][1].endswith("(ended)")
    assert re.search(r">from 2016 · ended</tspan></text>", d.svg)
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
    assert "Ended relationship" not in d.svg
    assert "stroke-opacity" not in d.svg


# ---------------------------------------------------------------------------
# Phase 221 — the canvas's edge kinds, and labels that fit
# ---------------------------------------------------------------------------

from opencheck.reporting.diagram import (  # noqa: E402
    EDGE_STYLE,
    categorise,
    interest_label,
    wrap_label,
)


def _saderat_bundle():
    """The BANK SADERAT PLC Companies House figure (2138008KTNTDICZU8L25): two
    PSCs with significant influence or control, one ceased 4 October 2024, both
    carrying Open Ownership's long Python-list ``details``."""
    bank = {"statementId": "bank", "recordType": "entity",
            "recordDetails": {"name": "BANK SADERAT PLC",
                              "identifiers": [{"scheme": "GB-COH", "id": "01126618"}]}}
    kafshgari = {"statementId": "k", "recordType": "person",
                 "recordDetails": {"names": [{"fullName": "Mr Mohsen Seifi Kafshgari"}]}}
    imany = {"statementId": "i", "recordType": "person",
             "recordDetails": {"names": [{"fullName": "Mr Seyed Ziya Imany"}]}}
    details = "Relationship Type: ['significant-influence-or-control']"
    current = {"statementId": "rk", "recordType": "relationship", "recordStatus": "new",
               "recordDetails": {"interestedParty": "k", "subject": "bank", "interests": [
                   {"type": "otherInfluenceOrControl", "details": details, "startDate": "2024-10-19"}]}}
    ceased = {"statementId": "ri", "recordType": "relationship", "recordStatus": "closed",
              "recordDetails": {"interestedParty": "i", "subject": "bank", "interests": [
                  {"type": "otherInfluenceOrControl", "details": details,
                   "startDate": "2022-11-16", "endDate": "2024-10-04"}]}}
    statements = [bank, kafshgari, imany, current, ceased]
    return [current, ceased], {s["statementId"]: s for s in statements}


def _labels(svg: str) -> list[list[str]]:
    """Each edge label's lines, in drawing order."""
    return [re.findall(r"<tspan [^>]*>([^<]*)</tspan>", t)
            for t in re.findall(r"<text [^>]*>(<tspan .*?)</text>", svg)]


def _label_boxes(svg: str) -> list[tuple[float, float, float, float]]:
    return [(float(x), float(y), float(x) + float(w), float(y) + float(h))
            for x, y, w, h in re.findall(
                r'<rect x="(-?[\d.]+)" y="(-?[\d.]+)" width="([\d.]+)" height="([\d.]+)" rx="3"', svg)]


def _legend(svg: str) -> list[str]:
    """Legend entries: the 9-unit captions that are not centred node sublabels."""
    return re.findall(r'<text x="[\d.]+" y="[\d.]+" font-size="9" fill="#595959">([^<]*)</text>', svg)


def test_significant_influence_is_control_orange_and_dotted_as_on_screen():
    rels, by_id = _saderat_bundle()
    d = source_diagram(rels, by_id, source_name="UK Companies House")
    control = EDGE_STYLE["control"]
    lines = [m for m in re.findall(r"<line [^>]*/>", d.svg) if "marker-end" in m]
    assert len(lines) == 2
    for line in lines:
        assert f'stroke="{control.color}"' in line
        assert 'stroke-dasharray="0.5 6"' in line and 'stroke-linecap="round"' in line
    assert "#7c3aed" not in d.svg  # no purple anywhere: nothing here is a role
    # Label text uses the darkened label colour, not the line colour (WCAG 1.4.3).
    assert f'fill="{control.text_color}"' in d.svg


def test_legend_lists_only_the_kinds_drawn():
    rels, by_id = _saderat_bundle()
    d = source_diagram(rels, by_id, source_name="UK Companies House")
    assert _legend(d.svg) == ["Control", "Ended relationship (drawn faint)"]
    rels, by_id = _bundle()
    d = source_diagram(rels, by_id, source_name="UK Companies House")
    assert _legend(d.svg) == ["Ownership", "Role"]


def test_long_companies_house_label_is_short_wrapped_and_dated_on_its_own_line():
    rels, by_id = _saderat_bundle()
    d = source_diagram(rels, by_id, source_name="UK Companies House")
    labels = _labels(d.svg)
    assert ["Controls", "from 2024"] in labels
    ended = next(lab for lab in labels if any("ended" in line for line in lab))
    assert ended[0] == "Controls"
    assert "ended 4 October 2024" in ended[-1]
    # The register's wording stays in the table.
    assert "significant-influence-or-control" not in d.svg
    assert all("significant-influence-or-control" in row[1] for row in d.rows)
    assert all(len(line) <= 30 for lab in labels for line in lab)


def test_labels_clear_the_nodes_and_each_other():
    rels, by_id = _saderat_bundle()
    d = source_diagram(rels, by_id, source_name="UK Companies House")
    boxes = _label_boxes(d.svg)
    assert len(boxes) == 2
    a, b = boxes
    assert not (a[0] < b[2] and b[0] < a[2] and a[1] < b[3] and b[1] < a[3])
    # Nothing reaches the subject's disc (BANK SADERAT PLC, drawn at x=620).
    assert all(box[2] <= 620 - 26 for box in boxes)
    # Every label sits inside the figure.
    width = int(re.search(r'viewBox="0 0 (\d+) ', d.svg).group(1))
    assert all(box[0] >= 0 and box[2] <= width for box in boxes)


def test_dense_diagram_places_every_label_without_overlap():
    rels, by_id = _converging_bundle()
    d = source_diagram(rels, by_id, source_name="GLEIF")
    boxes = _label_boxes(d.svg)
    assert len(boxes) == 10
    for k, a in enumerate(boxes):
        for b in boxes[k + 1:]:
            assert not (a[0] < b[2] and b[0] < a[2] and a[1] < b[3] and b[1] < a[3])


def _converging_bundle():
    """Ten people each owning part of one company — every edge converges."""
    by_id = {"co": {"statementId": "co", "recordType": "entity", "recordDetails": {"name": "Target Co"}}}
    rels = []
    for i in range(10):
        pid = f"p{i}"
        by_id[pid] = {"statementId": pid, "recordType": "person",
                      "recordDetails": {"names": [{"fullName": f"Person number {i}"}]}}
        rels.append({"statementId": f"r{i}", "recordType": "relationship", "recordDetails": {
            "interestedParty": pid, "subject": "co",
            "interests": [{"type": "shareholding", "share": {"minimum": 5, "maximum": 10},
                           "startDate": "2019-01-01"}]}})
    return rels, by_id


def test_categorise_matches_the_canvas_precedence():
    assert categorise([{"type": "seniorManagingOfficial"}, {"type": "shareholding"}]) == "ownership"
    assert categorise([{"type": "votingRights"}]) == "ownership"
    assert categorise([{"type": "boardChair"}, {"type": "appointmentOfBoard"}]) == "control"
    assert categorise([{"type": "boardMember"}]) == "role"
    assert categorise([{"type": "unknownInterest"}]) == "unknown"
    # Free-text details never decide the kind — the canvas does not read them.
    assert categorise([{"type": "otherInfluenceOrControl", "details": "ownership of shares"}]) == "control"
    assert categorise([]) == "unknown"


def test_interest_label_matches_the_canvas_vocabulary():
    assert interest_label({"type": "shareholding", "share": {"minimum": 75.0, "maximum": 100.0}}) == "Owns 75–100%"
    assert interest_label({"type": "shareholding", "share": {"exclusiveMinimum": 25, "maximum": 50}}) == "Owns 25–50%"
    assert interest_label({"type": "votingRights", "share": {"exact": 30}}) == "Controls 30% (votes)"
    assert interest_label({"type": "shareholding", "share": {"exact": 12.5}}) == "Owns 12.5%"
    assert interest_label({"type": "shareholding", "share": {"minimum": 25}}) == "Owns"
    assert interest_label({"type": "otherInfluenceOrControl"}) == "Controls"
    assert interest_label({"type": "someFutureType"}) == "someFutureType"
    assert interest_label({}) == "Interest"


def test_psc_with_three_natures_shows_two_lines_like_the_canvas():
    rels, by_id = _bundle()
    rels[0]["recordDetails"]["interests"] = [
        {"type": "shareholding", "share": {"minimum": 75.0, "maximum": 100.0}, "startDate": "2016-04-06"},
        {"type": "votingRights", "share": {"minimum": 75.0, "maximum": 100.0}, "startDate": "2016-04-06"},
        {"type": "appointmentOfBoard", "startDate": "2016-04-06"},
    ]
    d = source_diagram(rels, by_id, source_name="UK Companies House")
    assert ["Owns 75–100%", "Controls 75–100%", "from 2016"] in _labels(d.svg)


def test_a_current_relationship_is_labelled_from_its_current_interests():
    rels, by_id = _bundle()
    rels[0]["recordDetails"]["interests"] = [
        {"type": "shareholding", "share": {"minimum": 25, "maximum": 50},
         "startDate": "2012-01-01", "endDate": "2016-04-05"},
        {"type": "shareholding", "share": {"minimum": 75, "maximum": 100}, "startDate": "2016-04-06"},
    ]
    d = source_diagram(rels, by_id, source_name="UK Companies House")
    assert ["Owns 75–100%", "from 2016"] in _labels(d.svg)
    assert "Owns 25–50%" not in d.svg
    assert "to 2016-04-05" in d.rows[0][1]  # the history is in the table


def test_wrap_label_keeps_clauses_whole_and_never_cuts_a_word():
    assert wrap_label([["Controls"], ["from 2022", "ended 4 October 2024"]], 30) == [
        "Controls", "from 2022", "ended 4 October 2024"]
    assert wrap_label([["from 2016", "ended"]], 30) == ["from 2016 · ended"]
    assert wrap_label([["Controls 100% (votes)"]], 14) == ["Controls 100%", "(votes)"]
    assert wrap_label([["rightToProfitOrIncomeFromAssets"]], 14) == ["rightToProfitOrIncomeFromAssets"]
