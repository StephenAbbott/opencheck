"""Tests for the prototype Wikidata controlling-owner extraction.

Covers the parser/classifier only (no network): owner categorisation, the
decided drop of family-typed owners, indicative share conversion, and reference
(provenance) capture + dedup. See docs/wikidata-ownership.md.
"""

from __future__ import annotations

from opencheck.sources.wikidata import (
    _classify_owner,
    _parse_ownership,
    _proportion_to_pct,
)

_ENT = "http://www.wikidata.org/entity/"


def _row(owner, label, *, cls=None, via="P127", prop=None, stated=None, url=None, retrieved=None):
    """Build a single SPARQL-results binding row."""
    r: dict = {
        "owner": {"value": _ENT + owner},
        "ownerLabel": {"value": label},
        "via": {"value": via},
    }
    if cls:
        r["ownerClass"] = {"value": _ENT + cls}
    if prop is not None:
        r["proportion"] = {"value": prop}
    if stated:
        r["statedInLabel"] = {"value": stated}
    if url:
        r["refUrl"] = {"value": url}
    if retrieved:
        r["retrieved"] = {"value": retrieved}
    return r


def _bindings():
    return [
        # foundation by P31, with proportion + a reference (duplicated → dedup)
        _row("Q1", "Robert Bosch Stiftung", cls="Q157031", prop="0.92",
             url="https://assets.bosch.com/ownership.pdf"),
        _row("Q1", "Robert Bosch Stiftung", cls="Q157031", prop="0.92",
             url="https://assets.bosch.com/ownership.pdf"),  # exact dup
        # named person (no entityType, no share)
        _row("Q2", "Charles Koch", cls="Q5", url="https://wapo.com/koch"),
        # state body (ministry)
        _row("Q3", "Ministry of Energy", cls="Q192350"),
        # sovereign wealth fund → GLIE
        _row("Q4", "Qatar Investment Authority", cls="Q1808582"),
        # family → DROPPED
        _row("Q5", "Cargill family", cls="Q8436"),
        # plain company (generic P31, no name hint)
        _row("Q6", "Acme Holdings", cls="Q4830453"),
        # foundation by NAME hint only (generic P31)
        _row("Q7", "Example Stiftung", cls="Q4830453"),
    ]


def test_owner_categories_and_family_drop():
    out = _parse_ownership(_bindings())
    by = {o["qid"]: o for o in out}

    assert "Q5" not in by  # family dropped, not fabricated into a person/group
    assert set(by) == {"Q1", "Q2", "Q3", "Q4", "Q6", "Q7"}

    assert by["Q1"]["category"] == "foundation"
    assert by["Q1"]["entity_type"] == "registeredEntity"
    assert by["Q1"]["bods_kind"] == "entity"
    assert by["Q2"]["category"] == "person"
    assert by["Q2"]["bods_kind"] == "person"
    assert by["Q2"]["entity_type"] is None
    assert by["Q3"]["category"] == "statebody" and by["Q3"]["entity_type"] == "stateBody"
    assert by["Q4"]["category"] == "glie"
    assert by["Q6"]["category"] == "company"
    assert by["Q7"]["category"] == "foundation"  # via the "Stiftung" name hint


def test_indicative_share_and_reference_dedup():
    out = _parse_ownership(_bindings())
    bosch = next(o for o in out if o["qid"] == "Q1")
    assert bosch["share_percent"] == 92.0          # ratio 0.92 → 92%
    assert bosch["has_reference"] is True
    assert len(bosch["references"]) == 1           # the exact-duplicate ref collapsed
    assert bosch["references"][0]["url"].endswith("ownership.pdf")

    koch = next(o for o in out if o["qid"] == "Q2")
    assert koch["share_percent"] is None
    assert koch["has_reference"] is True

    qia = next(o for o in out if o["qid"] == "Q4")
    assert qia["has_reference"] is False           # no reference → flagged


def test_proportion_to_pct():
    assert _proportion_to_pct("0.92") == 92.0
    assert _proportion_to_pct("0.5") == 50.0
    assert _proportion_to_pct("75") == 75.0        # already a percent
    assert _proportion_to_pct(None) is None
    assert _proportion_to_pct("not-a-number") is None


def test_classify_owner_priority():
    # A named person who is also tagged with a family class → person wins is NOT
    # the rule; family is dropped first by design. Verify the explicit order.
    assert _classify_owner({"Q8436"}, "Smith family") == "family"
    assert _classify_owner({"Q5"}, "Jane Smith") == "person"
    assert _classify_owner({"Q192350"}, "Ministry") == "statebody"
    assert _classify_owner(set(), "Acme Trust") == "arrangement"   # name hint
    assert _classify_owner(set(), "Acme Ltd") == "company"
