"""The FATF and EU list signals are one item per lookup, not one per source.

Found on 15 Oct 2026 in the production Rosneft lookup (253400JT3MQWNDKMJE44):
``EU_HIGH_RISK_THIRD_COUNTRY`` and ``EU_TAX_NON_COOPERATIVE`` each appeared
five times — GLEIF, OECD-UNSD MEIP, GEM / Climate TRACE, OpenSanctions and
Wikidata each mapped the subject's jurisdiction and each fired the rule — with
the same sentence. ``NON_EU_JURISDICTION`` had been pooled since Phase 259;
the four list signals now join it on the same terms
(``risk.merge_listed_jurisdiction``): every source's jurisdictions pooled per
statement (so every graph badge stays), the sources in ``evidence.reported_by``,
the "including ended relationships" qualifier per country.
"""

from __future__ import annotations

import itertools

import pytest

from opencheck.risk import (
    EU_HIGH_RISK_THIRD_COUNTRY,
    EU_HRTC_INSTRUMENT,
    EU_TAX_LIST_INSTRUMENT,
    EU_TAX_NON_COOPERATIVE,
    FATF_BLACK_LIST,
    FATF_GREY_LIST,
    INCLUDING_ENDED,
    LIST_SIGNAL_CODES,
    assess_structure,
    merge_listed_jurisdiction,
)
from opencheck.routers.lookup import (
    _COLLAPSE_RESOLVERS,
    _STRUCTURAL_SIGNAL_CODES,
    _merge_signals,
)

LEI = "253400JT3MQWNDKMJE44"
SOURCES = ("gleif", "meip", "climatetrace", "opensanctions", "wikidata")
PAST = "2024-10-04"


def _entity(sid, code, name, *, lei=None):
    rd = {
        "entityType": {"type": "registeredEntity"},
        "name": sid,
        "jurisdiction": {"code": code, "name": name},
    }
    if lei:
        rd["identifiers"] = [{"id": lei, "scheme": "XI-LEI"}]
    return {"statementId": sid, "recordType": "entity", "recordDetails": rd}


def _rel(sid, subject, owner, *, ended=False):
    interest = {"type": "shareholding", "directOrIndirect": "direct"}
    if ended:
        interest["endDate"] = PAST
    return {
        "statementId": sid,
        "recordType": "relationship",
        "recordDetails": {
            "isComponent": False,
            "subject": subject,
            "interestedParty": owner,
            "interests": [interest],
        },
    }


def _signals(source_id, bods):
    return [
        s.to_dict()
        for s in assess_structure(source_id, {"entity_id": LEI}, bods, hit_id=LEI)
    ]


def _rosneft(source_id, country_name="Russian Federation"):
    return _signals(source_id, [_entity(f"{source_id}-S", "RU", country_name, lei=LEI)])


def _by_code(signals, code):
    return [s for s in signals if s["code"] == code]


def test_the_four_codes_collapse_and_have_a_resolver() -> None:
    assert LIST_SIGNAL_CODES == {
        FATF_BLACK_LIST, FATF_GREY_LIST, EU_HIGH_RISK_THIRD_COUNTRY, EU_TAX_NON_COOPERATIVE,
    }
    assert LIST_SIGNAL_CODES <= _STRUCTURAL_SIGNAL_CODES
    for code in LIST_SIGNAL_CODES:
        assert _COLLAPSE_RESOLVERS[code] is merge_listed_jurisdiction


def test_rosneft_reads_each_eu_list_once() -> None:
    per_source = [_rosneft(src) for src in SOURCES]
    # The bug, as it was: every source fires both EU lists.
    for sigs in per_source:
        assert _by_code(sigs, EU_HIGH_RISK_THIRD_COUNTRY)
        assert _by_code(sigs, EU_TAX_NON_COOPERATIVE)

    merged = _merge_signals(*per_source)
    for code, instrument, confidence in (
        (EU_HIGH_RISK_THIRD_COUNTRY, EU_HRTC_INSTRUMENT, "high"),
        (EU_TAX_NON_COOPERATIVE, EU_TAX_LIST_INSTRUMENT, "medium"),
    ):
        hits = _by_code(merged, code)
        assert len(hits) == 1
        sig = hits[0]
        ev = sig["evidence"]
        assert ev["reported_by"] == list(SOURCES)
        assert ev["instrument"] == instrument
        assert sig["confidence"] == confidence
        # Every source's node keeps its badge.
        assert {j["statement_id"] for j in ev["jurisdictions"]} == {f"{s}-S" for s in SOURCES}
        assert sig["summary"].startswith("The company is registered in Russian Federation, ")
        assert sig["summary"].split(", on the EU")[0].count("Russian Federation") == 1
        assert INCLUDING_ENDED not in sig["summary"]
    assert _by_code(merged, EU_TAX_NON_COOPERATIVE)[0]["evidence"]["annex"] == "I"
    # Section IV is said once, from the pooled hits.
    eu = _by_code(merged, EU_HIGH_RISK_THIRD_COUNTRY)[0]["summary"]
    assert eu.count("Section IV") == 1


def test_two_spellings_of_one_country_read_as_one_name() -> None:
    merged = _merge_signals(_rosneft("gleif", "Russia"), _rosneft("meip"), _rosneft("wikidata"))
    summary = _by_code(merged, EU_TAX_NON_COOPERATIVE)[0]["summary"]
    assert summary.startswith("The company is registered in Russian Federation, on the EU")


def test_fatf_lists_pool_and_keep_their_list_name() -> None:
    def bundle(src):
        return [
            _entity(f"{src}-S", "FR", "France", lei=LEI),
            _entity(f"{src}-IR", "IR", "Iran"),
            _entity(f"{src}-ZA", "ZA", "South Africa"),
            _rel(f"{src}-r1", f"{src}-S", f"{src}-IR"),
            _rel(f"{src}-r2", f"{src}-S", f"{src}-ZA"),
        ]

    merged = _merge_signals(_signals("gleif", bundle("gleif")), _signals("opensanctions", bundle("opensanctions")))
    black = _by_code(merged, FATF_BLACK_LIST)
    assert len(black) == 1
    assert black[0]["evidence"]["list"] == "black"
    assert black[0]["evidence"]["reported_by"] == ["gleif", "opensanctions"]
    assert "ownership chain reaches into Iran" in black[0]["summary"]
    grey = _by_code(merged, FATF_GREY_LIST)
    if grey:  # only while South Africa is on the grey list
        assert len(grey) == 1 and grey[0]["evidence"]["list"] == "grey"


def _iran_bundle(src, *, ended):
    return [
        _entity(f"{src}-S", "FR", "France", lei=LEI),
        _entity(f"{src}-IR", "IR", "Iran"),
        _rel(f"{src}-r", f"{src}-S", f"{src}-IR", ended=ended),
    ]


def test_a_current_link_in_another_source_clears_the_ended_qualifier() -> None:
    ended = _signals("wikidata", _iran_bundle("wikidata", ended=True))
    current = _signals("gleif", _iran_bundle("gleif", ended=False))
    assert INCLUDING_ENDED in _by_code(ended, FATF_BLACK_LIST)[0]["summary"]

    merged = _by_code(_merge_signals(ended, current), FATF_BLACK_LIST)[0]
    assert INCLUDING_ENDED not in merged["summary"]
    assert "includes_ended_relationships" not in merged["evidence"]

    both_ended = _by_code(
        _merge_signals(ended, _signals("meip", _iran_bundle("meip", ended=True))),
        FATF_BLACK_LIST,
    )[0]
    assert f"({INCLUDING_ENDED})" in both_ended["summary"]
    assert both_ended["evidence"]["ended_relationship_statement_ids"] == ["meip-r", "wikidata-r"]


@pytest.mark.parametrize("order", list(itertools.permutations(range(3))))
def test_merge_order_does_not_change_the_pooled_answer(order) -> None:
    lists = [_rosneft(src) for src in ("gleif", "meip", "wikidata")]
    merged = _by_code(_merge_signals(*(lists[i] for i in order)), EU_TAX_NON_COOPERATIVE)[0]
    assert sorted(merged["evidence"]["reported_by"]) == ["gleif", "meip", "wikidata"]
    assert len(merged["evidence"]["jurisdictions"]) == 3
    assert merged["summary"] == _by_code(_merge_signals(*lists), EU_TAX_NON_COOPERATIVE)[0]["summary"]


def test_a_single_source_signal_is_unchanged() -> None:
    sigs = _rosneft("gleif")
    assert _merge_signals(sigs) == sigs
