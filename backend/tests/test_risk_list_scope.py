"""Phase 273 — FATF / EU list signals read only the subject and the chain above it.

Until Phase 273 ``_fatf_jurisdiction_signals`` and
``_eu_high_risk_third_country_signals`` walked every entity statement in a
bundle and said "Ownership chain reaches into …" — so a group parent with one
subsidiary in a grey-listed country was told its ownership chain reached it.
Measured on 30 production subjects (1 Oct 2026): 26 of 33 list-signal firings
rested on subsidiaries alone. Stephen's decisions (1 Oct 2026):

1. The three list signals read the subject and its owners only (option A);
   subsidiary hits move to the SUBSIDIARY_LISTED_JURISDICTION context note.
2. The subject's own jurisdiction counts, and is said as its registration.
3. Side branches (neither above nor below) are dropped from chip and note.
4. Ended links follow Phase 220: kept, and said.
5. Subsidiary nodes carry the note's slate badge (evidence.jurisdictions).
6. The watchlist stamps a signal-rules version so the change is not reported
   as a change in any watched company.
"""

from __future__ import annotations

import pytest

from opencheck.config import get_settings
from opencheck.risk import (
    EU_HIGH_RISK_THIRD_COUNTRY,
    FATF_BLACK_LIST,
    FATF_GREY_LIST,
    INCLUDING_ENDED,
    SUBSIDIARY_LISTED_JURISDICTION,
    assess_bundle,
    assess_structure,
    merge_subsidiary_listed_jurisdiction,
)

PAST = "2024-10-04"
LISTS = (FATF_BLACK_LIST, FATF_GREY_LIST, EU_HIGH_RISK_THIRD_COUNTRY)


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _entity(sid, code=None, name=None, identifier=None):
    rd = {"entityType": {"type": "registeredEntity"}, "name": name or sid}
    if code:
        rd["jurisdiction"] = {"code": code, "name": name or code}
    if identifier:
        rd["identifiers"] = [{"id": identifier, "scheme": "XI-LEI"}]
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


def _run(bods, hit_id="LEI-S"):
    return assess_structure("gleif", {"entity_id": hit_id}, bods, hit_id=hit_id)


def _by_code(signals):
    return {s.code: s for s in signals}


def _subject(code="FR"):
    """The looked-up company; its jurisdiction name is just the code."""
    stmt = _entity("S", code, identifier="LEI-S")
    stmt["recordDetails"]["name"] = "Subject SA"
    return stmt


# ---------------------------------------------------------------------------
# The four cases the ticket names
# ---------------------------------------------------------------------------


def test_subsidiary_only_hit_raises_no_list_signal_but_a_context_note() -> None:
    """The production shape: a parent with one subsidiary in Vietnam. No red
    chip, no 'ownership chain' — a context note naming where it is."""
    bods = [_subject(), _entity("C", "VN", "Vietnam"), _rel("R1", "C", "S")]
    sigs = _by_code(_run(bods))
    assert not set(LISTS) & set(sigs)
    note = sigs[SUBSIDIARY_LISTED_JURISDICTION]
    assert note.kind == "context"
    assert note.summary.startswith(
        "Subsidiaries registered in listed jurisdictions: "
        "Vietnam (EU high-risk list, FATF grey list)."
    )
    assert "not a risk finding" in note.summary
    assert "ownership chain reaches" not in note.summary
    assert note.evidence["jurisdictions"] == [
        {"statement_id": "C", "code": "VN", "name": "Vietnam", "lists": ["eu", "fatf_grey"]}
    ]


def test_upstream_hit_keeps_the_ownership_chain_wording() -> None:
    bods = [_subject("GB"), _entity("P", "IR", "Iran"), _rel("R1", "S", "P")]
    sigs = _by_code(_run(bods))
    assert sigs[FATF_BLACK_LIST].summary.startswith("Its ownership chain reaches into Iran, ")
    assert sigs[EU_HIGH_RISK_THIRD_COUNTRY].summary.startswith("Its ownership chain reaches into Iran, ")
    assert sigs[FATF_BLACK_LIST].evidence["jurisdictions"][0]["position"] == "above"
    assert SUBSIDIARY_LISTED_JURISDICTION not in sigs


def test_subject_own_jurisdiction_counts_and_is_said_as_registration() -> None:
    """Decision 2. Rosneft's homepage chip rests on exactly this."""
    sigs = _by_code(_run([_subject("RU")]))
    eu = sigs[EU_HIGH_RISK_THIRD_COUNTRY]
    assert eu.summary.startswith("The company is registered in RU, on the EU list")
    assert "ownership chain" not in eu.summary
    assert eu.evidence["jurisdictions"][0]["position"] == "subject"
    assert "Section IV" in eu.summary  # Russia's suspension clause survives


def test_mixed_subject_owner_and_subsidiary() -> None:
    """Subject in Kenya, owner in Iran, subsidiary in Vietnam: the subject and
    the owner are each said their own way, and Vietnam goes to the note."""
    bods = [
        _subject("KE"),
        _entity("P", "IR", "Iran"),
        _entity("C", "VN", "Vietnam"),
        _rel("R1", "S", "P"),
        _rel("R2", "C", "S"),
    ]
    sigs = _by_code(_run(bods))
    eu = sigs[EU_HIGH_RISK_THIRD_COUNTRY]
    assert eu.summary.startswith(
        "The company is registered in KE, and its ownership chain reaches into Iran, "
    )
    assert {j["statement_id"] for j in eu.evidence["jurisdictions"]} == {"S", "P"}
    assert sigs[FATF_GREY_LIST].summary.startswith("The company is registered in KE, ")
    assert sigs[FATF_BLACK_LIST].summary.startswith("Its ownership chain reaches into Iran, ")
    assert [j["code"] for j in sigs[SUBSIDIARY_LISTED_JURISDICTION].evidence["jurisdictions"]] == ["VN"]


# ---------------------------------------------------------------------------
# Decisions 3 and 4
# ---------------------------------------------------------------------------


def test_side_branch_is_dropped_from_chip_and_note() -> None:
    """Decision 3. An Iranian entity connected to nothing on the subject's
    chain — the shape of Bank Saderat's duplicate parent records."""
    bods = [
        _subject("GB"),
        _entity("X", "IR", "Iran"),
        _entity("Y", "GB"),
        _rel("R1", "Y", "X"),  # X owns Y; neither touches S
    ]
    sigs = _by_code(_run(bods))
    assert not (set(LISTS) | {SUBSIDIARY_LISTED_JURISDICTION}) & set(sigs)


def test_sibling_owned_by_the_same_parent_is_a_side_branch() -> None:
    """The parent's OTHER subsidiary is neither above nor below the subject."""
    bods = [
        _subject("GB"),
        _entity("P", "GB"),
        _entity("SIB", "VN", "Vietnam"),
        _rel("R1", "S", "P"),
        _rel("R2", "SIB", "P"),
    ]
    sigs = _by_code(_run(bods))
    assert not (set(LISTS) | {SUBSIDIARY_LISTED_JURISDICTION}) & set(sigs)


def test_owner_reached_only_through_an_ended_link_counts_and_says_so() -> None:
    bods = [_subject("GB"), _entity("P", "IR", "Iran"), _rel("R1", "S", "P", ended=True)]
    sig = _by_code(_run(bods))[FATF_BLACK_LIST]
    assert sig.summary.startswith(f"Its ownership chain ({INCLUDING_ENDED}) reaches into Iran, ")
    assert sig.evidence["ended_relationship_statement_ids"] == ["R1"]
    assert sig.evidence["jurisdictions"][0]["via_ended_only"] is True


def test_former_subsidiary_counts_in_the_note_and_says_so() -> None:
    """The Rosbank shape: a subsidiary sold, still on record as ended."""
    bods = [_subject(), _entity("C", "RU", "Russia"), _rel("R1", "C", "S", ended=True)]
    note = _by_code(_run(bods))[SUBSIDIARY_LISTED_JURISDICTION]
    assert note.summary.startswith(f"Subsidiaries ({INCLUDING_ENDED}) registered in listed")
    assert note.evidence["ended_relationship_statement_ids"] == ["R1"]


def test_a_current_route_to_the_same_country_is_not_qualified() -> None:
    """Phase 259's rule: the qualifier is about a COUNTRY reached only
    through ended links, not about any ended link existing."""
    bods = [
        _subject(),
        _entity("C1", "VN", "Vietnam"),
        _entity("C2", "VN", "Vietnam"),
        _rel("R1", "C1", "S", ended=True),
        _rel("R2", "C2", "S"),
    ]
    note = _by_code(_run(bods))[SUBSIDIARY_LISTED_JURISDICTION]
    assert INCLUDING_ENDED not in note.summary
    assert "ended_relationship_statement_ids" not in note.evidence


def test_deep_subsidiaries_and_deep_owners_are_walked() -> None:
    bods = [
        _subject("GB"),
        _entity("P1", "GB"), _entity("P2", "KP", "North Korea"),
        _entity("C1", "GB"), _entity("C2", "KE", "Kenya"),
        _rel("R1", "S", "P1"), _rel("R2", "P1", "P2"),
        _rel("R3", "C1", "S"), _rel("R4", "C2", "C1"),
    ]
    sigs = _by_code(_run(bods))
    assert sigs[FATF_BLACK_LIST].summary.startswith("Its ownership chain reaches into North Korea")
    assert [j["code"] for j in sigs[SUBSIDIARY_LISTED_JURISDICTION].evidence["jurisdictions"]] == ["KE"]


def test_an_ownership_cycle_counts_as_above() -> None:
    """A node reachable both ways is an owner — the stronger claim — and is
    never reported twice."""
    bods = [_subject("GB"), _entity("P", "IR", "Iran"), _rel("R1", "S", "P"), _rel("R2", "P", "S")]
    sigs = _by_code(_run(bods))
    assert FATF_BLACK_LIST in sigs
    assert SUBSIDIARY_LISTED_JURISDICTION not in sigs


def test_every_statement_of_the_subject_is_walked() -> None:
    """MEIP files one subject statement per group membership. The owner above
    the SECOND statement must count too."""
    bods = [
        _entity("S1", "GB", "Alpha One", identifier="LEI-S"),
        _entity("S2", "GB", "Alpha One", identifier="LEI-S"),
        _entity("H1", "GB"),
        _entity("H2", "MM", "Myanmar"),
        _rel("R1", "S1", "H1"),
        _rel("R2", "S2", "H2"),
    ]
    sig = _by_code(_run(bods))[FATF_BLACK_LIST]
    assert [j["statement_id"] for j in sig.evidence["jurisdictions"]] == ["H2"]


# ---------------------------------------------------------------------------
# Lists, confidence and the merge
# ---------------------------------------------------------------------------


def test_note_names_every_list_regardless_of_the_element_setting(monkeypatch) -> None:
    """OPENCHECK_HIGH_RISK_JURISDICTION_LISTS narrows the Phase 272 complexity
    element only — like the standalone signals, the note is not narrowed."""
    monkeypatch.setenv("OPENCHECK_HIGH_RISK_JURISDICTION_LISTS", "eu")
    bods = [_subject(), _entity("C", "BG", "Bulgaria"), _rel("R1", "C", "S")]
    note = _by_code(_run(bods))[SUBSIDIARY_LISTED_JURISDICTION]
    assert note.evidence["jurisdictions"][0]["lists"] == ["fatf_grey"]


def test_confidence_ladder_is_unchanged() -> None:
    bods = [_subject("KE"), _entity("P", "IR"), _rel("R1", "S", "P")]
    sigs = _by_code(_run(bods))
    assert sigs[FATF_BLACK_LIST].confidence == "high"
    assert sigs[FATF_GREY_LIST].confidence == "medium"
    assert sigs[EU_HIGH_RISK_THIRD_COUNTRY].confidence == "high"


def test_a_black_listed_country_never_also_fires_grey() -> None:
    sigs = _by_code(_run([_subject("IR")]))
    assert FATF_BLACK_LIST in sigs and FATF_GREY_LIST not in sigs


def test_merge_pools_every_source_once_per_node() -> None:
    a = {
        "code": SUBSIDIARY_LISTED_JURISDICTION, "source_id": "gleif",
        "summary": "", "evidence": {"jurisdictions": [
            {"statement_id": "g1", "code": "VN", "name": "Vietnam", "lists": ["eu", "fatf_grey"]}]},
    }
    b = {
        "code": SUBSIDIARY_LISTED_JURISDICTION, "source_id": "opensanctions",
        "summary": "", "evidence": {"jurisdictions": [
            {"statement_id": "o1", "code": "KE", "name": "Kenya", "lists": ["eu", "fatf_grey"]},
            {"statement_id": "g1", "code": "VN", "name": "Vietnam", "lists": ["eu", "fatf_grey"]}]},
    }
    m = merge_subsidiary_listed_jurisdiction(a, b)
    assert [j["statement_id"] for j in m["evidence"]["jurisdictions"]] == ["g1", "o1"]
    assert m["evidence"]["reported_by"] == ["gleif", "opensanctions"]
    assert "Kenya (EU high-risk list, FATF grey list), Vietnam" in m["summary"]


def test_assess_bundle_carries_the_note() -> None:
    bods = [_subject(), _entity("C", "CD", "DR Congo"), _rel("R1", "C", "S")]
    codes = {s.code for s in assess_bundle("gleif", {}, bods, hit_id="LEI-S")}
    assert SUBSIDIARY_LISTED_JURISDICTION in codes
    assert not set(LISTS) & codes


# ---------------------------------------------------------------------------
# Decision 6 — the watchlist does not report the rule change
# ---------------------------------------------------------------------------


def _snap(signals, *, rules, verdict="v"):
    return {
        "signals": [{"code": c, "source_id": "gleif", "kind": k} for c, k in signals],
        "verdict": verdict,
        "verdict_template": 4,
        **({"signal_rules": rules} if rules else {}),
    }


def test_watchlist_rules_codes_exist_in_the_risk_engine() -> None:
    from opencheck import risk
    from opencheck.watchlist import SIGNAL_RULES, SIGNAL_RULES_CHANGED

    assert SIGNAL_RULES == max(SIGNAL_RULES_CHANGED)
    for codes in SIGNAL_RULES_CHANGED.values():
        for code in codes:
            assert getattr(risk, code) == code


def test_pre_273_baseline_does_not_report_the_moved_chips() -> None:
    """A watched group whose grey-list chip rested on a subsidiary: after
    deploy the chip goes, the note arrives, the verdict loses its clause —
    and none of it is the company's doing."""
    from opencheck.watchlist import diff_snapshots

    before = _snap([("FATF_GREY_LIST", "risk"), ("EU_HIGH_RISK_THIRD_COUNTRY", "risk")],
                   rules=None, verdict="The records show a jurisdiction on an international watch list.")
    after = _snap([("SUBSIDIARY_LISTED_JURISDICTION", "context")], rules=2, verdict="No risk signals surfaced.")
    assert diff_snapshots(before, after) == []


def test_other_codes_still_report_across_the_rules_bump() -> None:
    """Only the moved codes are excused. A sanctions listing appearing on the
    same re-run is still news."""
    from opencheck.watchlist import diff_snapshots

    before = _snap([("FATF_GREY_LIST", "risk")], rules=None)
    after = _snap([("SANCTIONED", "risk")], rules=2)
    changes = diff_snapshots(before, after)
    assert {"kind": "signal_new", "code": "SANCTIONED", "sources": ["gleif"]} in changes
    assert not any(c.get("code") == "FATF_GREY_LIST" for c in changes)


def test_same_rules_version_reports_list_changes_normally() -> None:
    """Once the baseline has moved on, a list chip appearing is a real change."""
    from opencheck.watchlist import diff_snapshots

    before = _snap([], rules=2)
    after = _snap([("FATF_GREY_LIST", "risk")], rules=2)
    assert {"kind": "signal_new", "code": "FATF_GREY_LIST", "sources": ["gleif"]} in diff_snapshots(before, after)


def test_a_country_spelled_two_ways_is_named_once() -> None:
    """Rosneft's merged bundle spells RU as both "Russia" and "Russian
    Federation"; the sentence read "registered in Russia, Russian Federation"."""
    bods = [
        _entity("S1", "RU", "Russian Federation", identifier="LEI-S"),
        _entity("S2", "RU", "Russian Federation", identifier="LEI-S"),
        _entity("S3", "RU", "Russia", identifier="LEI-S"),
    ]
    eu = _by_code(_run(bods))[EU_HIGH_RISK_THIRD_COUNTRY]
    assert eu.summary.startswith("The company is registered in Russian Federation, on the EU list")
