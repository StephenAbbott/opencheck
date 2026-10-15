"""Phase 346 — OFAC's own worked examples pin the 50 Percent Rule arithmetic.

FAQ 401 (13 Aug 2014) gives five ownership structures and says which entities
are blocked; FAQ 399 gives the aggregation case. Each is run through
``sanctions_ownership`` exactly as OFAC states it, so the "a caught company
counts in full" reading can never drift without one of these failing. The
"not computed" note names what FAQs 398 and 402 add that OpenCheck does not
compute.
"""

from __future__ import annotations

import pytest

from opencheck import sanctions_ownership as so

AS_OF = "2026-10-15"
SDN = ["us_ofac_sdn"]


def _entity(sid: str) -> dict:
    return {"statementId": sid, "recordType": "entity", "recordDetails": {"name": sid}}


def _own(owner: str, owned: str, pct: float) -> dict:
    return {
        "statementId": f"{owner}-{owned}", "recordType": "relationship",
        "source": {"opencheckSourceId": "companies_house", "description": "UK Companies House"},
        "recordDetails": {
            "subject": owned, "interestedParty": owner,
            "interests": [{"type": "shareholding", "share": {"exact": pct}}],
        },
    }


def _blocked(sid: str) -> dict:
    return {
        "code": "RELATED_SANCTIONED", "source_id": "opensanctions", "hit_id": f"NK-{sid}",
        "confidence": "high", "kind": "risk",
        "evidence": {"subject_statement_id": sid, "matched_name": sid, "search_name": sid,
                     "corroboration": [], "name_match_only": False, "datasets": SDN,
                     "identifier_match": True},
    }


def _ofac(subject: str, rels: list[dict], blocked: list[str] = ("X",)) -> str | None:
    """OFAC's result for ``subject``: "meets", or None when no signal fires."""
    bods = [_entity(n) for n in "XABC"] + rels
    sigs = [_blocked(b) for b in blocked]
    sig = so.build_signal(sigs, bods, [subject], subject="LEI", as_of=AS_OF)
    if sig is None:
        return None
    return next(r["result"] for r in sig["evidence"]["regimes"] if r["regime"] == "OFAC")


# FAQ 401 — "X" is a blocked person throughout.


def test_faq_401_example_1_a_chain_of_two_fifty_percent_stakes():
    """X owns 50% of A, A owns 50% of B: A and B are both blocked."""
    rels = [_own("X", "A", 50), _own("A", "B", 50)]
    assert _ofac("A", rels) == so.RESULT_MEETS
    assert _ofac("B", rels) == so.RESULT_MEETS


def test_faq_401_example_2_two_blocked_intermediaries_add_up():
    """X owns 50% of A and 50% of B; A and B each own 25% of C: C is blocked."""
    rels = [_own("X", "A", 50), _own("X", "B", 50), _own("A", "C", 25), _own("B", "C", 25)]
    assert _ofac("C", rels) == so.RESULT_MEETS


def test_faq_401_example_3_direct_plus_indirect():
    """X owns 50% of A and 10% of B; A owns 40% of B: B is blocked (10 + 40)."""
    rels = [_own("X", "A", 50), _own("X", "B", 10), _own("A", "B", 40)]
    assert _ofac("B", rels) == so.RESULT_MEETS


def test_faq_401_example_4_a_stake_through_an_unblocked_company_does_not_count():
    """X owns 50% of A and 25% of B; A and B each own 25% of C: C is NOT
    blocked — B is not 50% owned, so X's stake through B does not count and
    X's total in C is 25%."""
    rels = [_own("X", "A", 50), _own("X", "B", 25), _own("A", "C", 25), _own("B", "C", 25)]
    assert _ofac("C", rels) is None
    assert _ofac("B", rels) is None


def test_faq_401_example_5_two_unblocked_intermediaries_add_nothing():
    """X owns 25% of A and 25% of B; A and B each own 50% of C: none of A, B
    or C is blocked."""
    rels = [_own("X", "A", 25), _own("X", "B", 25), _own("A", "C", 50), _own("B", "C", 50)]
    assert _ofac("A", rels) is None
    assert _ofac("C", rels) is None


# FAQ 399 — aggregation.


def test_faq_399_two_blocked_persons_at_twenty_five_each():
    rels = [_own("X", "C", 25), _own("A", "C", 25)]
    assert _ofac("C", rels, blocked=["X", "A"]) == so.RESULT_MEETS
    # One of them alone is not enough.
    assert _ofac("C", rels, blocked=["X"]) is None


# FAQs 398 and 402 — what is said to be left out.


@pytest.mark.parametrize("phrase", [
    "control", "FAQ 402", "current holdings only", "FAQ 398", "Iran",
    "Sectoral Sanctions Identifications",
])
def test_the_not_computed_note_names_each_gap(phrase):
    assert phrase in so.NOT_COMPUTED


def test_the_ofac_sources_are_the_three_pages_checked():
    assert len(so.OFAC_SOURCES) == 3
    assert all(u.startswith("https://ofac.treasury.gov/") for u in so.OFAC_SOURCES)
