"""Phase 319 — EU_TAX_NON_COOPERATIVE: Annex I of the EU tax list.

On 9 October 2026 the Council moved Panama and Viet Nam off Annex I of the EU
list of non-cooperative jurisdictions for tax purposes. OpenCheck carried no
copy of that list; ``EU_HIGH_RISK_THIRD_COUNTRY`` reads the AML list
(Delegated Regulation 2016/1675), a different instrument. Stephen's decisions
(10 Oct 2026):

1. Annex I is its own risk signal — medium confidence, graph severity 1
   (below FATF grey), its own verdict clause read after the AML one, and a
   summary saying it carries no AML enhanced-due-diligence obligation.
2. Annex II (the "state of play" of commitments) is not read.
3. The complexity element counts it only where an operator opts in
   (``eu_tax`` in ``OPENCHECK_HIGH_RISK_JURISDICTION_LISTS``).
4. Subsidiaries in Annex I jurisdictions join the
   ``SUBSIDIARY_LISTED_JURISDICTION`` context note, labelled "EU tax list".
"""

from __future__ import annotations

import pytest

from opencheck import risk
from opencheck.config import get_settings
from opencheck.risk import (
    EU_HIGH_RISK_THIRD_COUNTRY,
    EU_HIGH_RISK_THIRD_COUNTRY_CODES,
    EU_TAX_LIST_INSTRUMENT,
    EU_TAX_NON_COOPERATIVE,
    EU_TAX_NON_COOPERATIVE_CODES,
    FATF_BLACK_LIST,
    FATF_GREY_LIST,
    INCLUDING_ENDED,
    SUBSIDIARY_LISTED_JURISDICTION,
    assess_bundle,
    assess_structure,
    high_risk_lists_for,
)
from opencheck.verdict import build_verdict

PAST = "2024-10-04"


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


def _subject(code="FR", name=None):
    stmt = _entity("S", code, name, identifier="LEI-S")
    stmt["recordDetails"]["name"] = "Subject SA"
    return stmt


def _run(bods, hit_id="LEI-S"):
    return {s.code: s for s in assess_structure("gleif", {"entity_id": hit_id}, bods, hit_id=hit_id)}


# ---------------------------------------------------------------------------
# The list itself
# ---------------------------------------------------------------------------


def test_annex_i_as_of_9_october_2026() -> None:
    """Council conclusions 13854/26. Panama and Viet Nam left on 9 Oct 2026;
    Annex II jurisdictions (BVI, Türkiye, …) are never on it."""
    assert EU_TAX_NON_COOPERATIVE_CODES == frozenset(
        {"AS", "AI", "GU", "PW", "RU", "TC", "VI", "VU"}
    )
    for annex_ii in ("TR", "JO", "ME", "PA", "VN", "VG", "SZ", "BN", "GL", "MA"):
        assert annex_ii not in EU_TAX_NON_COOPERATIVE_CODES
    assert "9 October 2026" in EU_TAX_LIST_INSTRUMENT and "Annex I" in EU_TAX_LIST_INSTRUMENT


def test_the_tax_list_is_not_folded_into_the_aml_list() -> None:
    """Russia and Vanuatu are on both lists; Viet Nam is on the AML list only
    now; Anguilla on the tax list only. The instruments stay apart."""
    assert {"RU", "VU"} <= EU_TAX_NON_COOPERATIVE_CODES & EU_HIGH_RISK_THIRD_COUNTRY_CODES
    assert "VN" in EU_HIGH_RISK_THIRD_COUNTRY_CODES
    assert "AI" not in EU_HIGH_RISK_THIRD_COUNTRY_CODES
    assert len(EU_HIGH_RISK_THIRD_COUNTRY_CODES) == 26


# ---------------------------------------------------------------------------
# Decision 1 — the signal
# ---------------------------------------------------------------------------


def test_subject_registered_in_anguilla_fires_the_tax_signal_only() -> None:
    sigs = _run([_subject("AI", "Anguilla")])
    sig = sigs[EU_TAX_NON_COOPERATIVE]
    assert sig.kind == "risk"
    assert sig.confidence == "medium"
    assert sig.summary.startswith("The company is registered in Anguilla, on the EU list of non-cooperative")
    assert EU_TAX_LIST_INSTRUMENT in sig.summary
    assert "does not by itself require AML enhanced due diligence" in sig.summary
    assert sig.evidence["annex"] == "I"
    assert sig.evidence["instrument"] == EU_TAX_LIST_INSTRUMENT
    assert sig.evidence["jurisdictions"] == [
        {"statement_id": "S", "code": "AI", "name": "Anguilla", "position": "subject"}
    ]
    assert not {FATF_BLACK_LIST, FATF_GREY_LIST, EU_HIGH_RISK_THIRD_COUNTRY} & set(sigs)


def test_the_summary_never_claims_an_aml_obligation() -> None:
    summary = _run([_subject("TC", "Turks and Caicos Islands")])[EU_TAX_NON_COOPERATIVE].summary
    assert "high-risk" not in summary
    assert "mandatory" not in summary
    assert "AML/CFT deficiencies" not in summary


def test_russia_fires_both_eu_signals_each_naming_its_instrument() -> None:
    sigs = _run([_subject("RU", "Russian Federation")])
    assert EU_HIGH_RISK_THIRD_COUNTRY in sigs and EU_TAX_NON_COOPERATIVE in sigs
    assert "2016/1675" in sigs[EU_HIGH_RISK_THIRD_COUNTRY].summary
    assert "2016/1675" not in sigs[EU_TAX_NON_COOPERATIVE].summary
    assert "tax purposes" in sigs[EU_TAX_NON_COOPERATIVE].summary
    assert "tax" not in sigs[EU_HIGH_RISK_THIRD_COUNTRY].summary


def test_viet_nam_and_panama_no_longer_fire_the_tax_signal() -> None:
    assert EU_TAX_NON_COOPERATIVE not in _run([_subject("VN")])
    assert EU_TAX_NON_COOPERATIVE not in _run([_subject("PA")])


def test_an_owner_above_fires_it_as_the_ownership_chain() -> None:
    bods = [_subject(), _entity("P", "VU", "Vanuatu"), _rel("R1", "S", "P")]
    sig = _run(bods)[EU_TAX_NON_COOPERATIVE]
    assert sig.summary.startswith("Its ownership chain reaches into Vanuatu, ")
    assert sig.evidence["jurisdictions"][0]["position"] == "above"


def test_an_owner_reached_only_through_an_ended_link_is_said() -> None:
    bods = [_subject(), _entity("P", "GU", "Guam"), _rel("R1", "S", "P", ended=True)]
    sig = _run(bods)[EU_TAX_NON_COOPERATIVE]
    assert INCLUDING_ENDED in sig.summary
    assert sig.evidence["ended_relationship_statement_ids"] == ["R1"]


@pytest.mark.parametrize("spelling, code", [("US-GU", "GU"), ("US-AS", "AS"), ("US-VI", "VI"), ("gu", "GU")])
def test_a_territory_written_as_a_us_subdivision_still_matches(spelling, code) -> None:
    """GLEIF writes GU / AS / VI; another source may write ISO 3166-2."""
    sig = _run([_subject(spelling, "Territory")])[EU_TAX_NON_COOPERATIVE]
    assert sig.evidence["jurisdictions"][0]["code"] == code


def test_a_us_state_is_not_folded_to_a_territory() -> None:
    assert EU_TAX_NON_COOPERATIVE not in _run([_subject("US-DE", "Delaware")])


def test_assess_bundle_emits_it() -> None:
    codes = {s.code for s in assess_bundle("gleif", {}, [_subject("PW", "Palau")], hit_id="LEI-S")}
    assert EU_TAX_NON_COOPERATIVE in codes


def test_the_code_is_a_module_constant_for_label_coverage() -> None:
    assert risk.EU_TAX_NON_COOPERATIVE == "EU_TAX_NON_COOPERATIVE"


# ---------------------------------------------------------------------------
# Decision 1 — the verdict
# ---------------------------------------------------------------------------


def _sig(code, confidence="medium", kind="risk"):
    return {"code": code, "confidence": confidence, "kind": kind}


def test_verdict_has_its_own_clause() -> None:
    v = build_verdict([_sig(EU_TAX_NON_COOPERATIVE)])
    assert v == "The records show a jurisdiction on the EU's tax non-cooperation list."


def test_verdict_reads_the_tax_clause_after_the_aml_clause() -> None:
    v = build_verdict([_sig(EU_TAX_NON_COOPERATIVE), _sig(EU_HIGH_RISK_THIRD_COUNTRY, "high")])
    assert v == (
        "The records show a jurisdiction on an international watch list and "
        "a jurisdiction on the EU's tax non-cooperation list."
    )


def test_verdict_tax_clause_yields_to_two_stronger_findings() -> None:
    v = build_verdict([_sig(EU_TAX_NON_COOPERATIVE), _sig("SANCTIONED", "high"), _sig("PEP", "high")])
    assert "tax" not in v


# ---------------------------------------------------------------------------
# Decision 3 — the complexity element is opt-in
# ---------------------------------------------------------------------------


def test_eu_tax_is_not_in_the_default_element_lists() -> None:
    assert high_risk_lists_for("AI") == []
    assert high_risk_lists_for("RU") == ["eu"]


def test_an_operator_can_opt_in(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_HIGH_RISK_JURISDICTION_LISTS", "eu,fatf_black,fatf_grey,eu_tax")
    assert high_risk_lists_for("AI") == ["eu_tax"]
    assert high_risk_lists_for("RU") == ["eu", "eu_tax"]
    assert high_risk_lists_for("US-GU") == ["eu_tax"]


def test_the_standalone_signal_ignores_the_setting(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_HIGH_RISK_JURISDICTION_LISTS", "fatf_black")
    assert EU_TAX_NON_COOPERATIVE in _run([_subject("AI", "Anguilla")])


# ---------------------------------------------------------------------------
# Decision 4 — subsidiaries go to the context note, labelled
# ---------------------------------------------------------------------------


def test_a_subsidiary_in_anguilla_is_context_named_eu_tax_list() -> None:
    bods = [_subject(), _entity("C", "AI", "Anguilla"), _rel("R1", "C", "S")]
    sigs = _run(bods)
    assert EU_TAX_NON_COOPERATIVE not in sigs
    note = sigs[SUBSIDIARY_LISTED_JURISDICTION]
    assert note.kind == "context"
    assert "Anguilla (EU tax list)" in note.summary
    assert note.evidence["jurisdictions"][0]["lists"] == ["eu_tax"]


def test_a_subsidiary_on_both_eu_lists_names_both() -> None:
    bods = [_subject(), _entity("C", "RU", "Russian Federation"), _rel("R1", "C", "S")]
    note = _run(bods)[SUBSIDIARY_LISTED_JURISDICTION]
    assert note.evidence["jurisdictions"][0]["lists"] == ["eu", "eu_tax"]
    assert "Russian Federation (EU high-risk list, EU tax list)" in note.summary


def test_the_note_is_not_narrowed_by_the_element_setting(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_HIGH_RISK_JURISDICTION_LISTS", "eu")
    bods = [_subject(), _entity("C", "TC", "Turks and Caicos Islands"), _rel("R1", "C", "S")]
    assert _run(bods)[SUBSIDIARY_LISTED_JURISDICTION].evidence["jurisdictions"][0]["lists"] == ["eu_tax"]


# ---------------------------------------------------------------------------
# The watchlist does not report the rule change
# ---------------------------------------------------------------------------


def test_signal_rules_version_3_moves_the_tax_code_and_the_note() -> None:
    from opencheck.watchlist import SIGNAL_RULES, SIGNAL_RULES_CHANGED

    assert SIGNAL_RULES == 3
    assert SIGNAL_RULES_CHANGED[3] == {EU_TAX_NON_COOPERATIVE, SUBSIDIARY_LISTED_JURISDICTION}


def test_a_pre_319_baseline_does_not_report_the_new_chip() -> None:
    from opencheck.watchlist import diff_snapshots

    before = {
        "signals": [],
        "verdict": "No risk signals surfaced across the sources that answered.",
        "verdict_template": 5,
        "signal_rules": 2,
    }
    after = {
        "signals": [{"code": EU_TAX_NON_COOPERATIVE, "source_id": "gleif", "kind": "risk"}],
        "verdict": "The records show a jurisdiction on the EU's tax non-cooperation list.",
        "verdict_template": 5,
        "signal_rules": 3,
    }
    assert diff_snapshots(before, after) == []


def test_after_the_upgrade_a_new_listing_is_reported() -> None:
    from opencheck.watchlist import diff_snapshots

    base = {"signals": [], "verdict": "x", "verdict_template": 5, "signal_rules": 3}
    later = {
        "signals": [{"code": EU_TAX_NON_COOPERATIVE, "source_id": "gleif", "kind": "risk"}],
        "verdict": "y",
        "verdict_template": 5,
        "signal_rules": 3,
    }
    kinds = [c["kind"] for c in diff_snapshots(base, later)]
    assert "signal_new" in kinds


# ---------------------------------------------------------------------------
# Review follow-ups
# ---------------------------------------------------------------------------


def test_the_note_pools_across_sources_keeping_the_tax_label() -> None:
    """Production collapses the note globally; the eu_tax label survives."""
    from opencheck.risk import merge_subsidiary_listed_jurisdiction

    a = {
        "code": SUBSIDIARY_LISTED_JURISDICTION, "source_id": "gleif", "summary": "",
        "evidence": {"jurisdictions": [
            {"statement_id": "g1", "code": "AI", "name": "Anguilla", "lists": ["eu_tax"]}]},
    }
    b = {
        "code": SUBSIDIARY_LISTED_JURISDICTION, "source_id": "opensanctions", "summary": "",
        "evidence": {"jurisdictions": [
            {"statement_id": "o1", "code": "RU", "name": "Russian Federation", "lists": ["eu", "eu_tax"]}]},
    }
    m = merge_subsidiary_listed_jurisdiction(a, b)
    assert "Anguilla (EU tax list)" in m["summary"]
    assert "Russian Federation (EU high-risk list, EU tax list)" in m["summary"]


def test_territory_folding_leaves_the_aml_signals_alone() -> None:
    """None of the folded territories is on an AML list, so a US-PR / US-GU
    entity raises no FATF or EU AML signal and no complexity element."""
    for spelling in ("US-PR", "US-GU", "US-VI"):
        sigs = _run([_subject(spelling, "Territory")])
        assert not {FATF_BLACK_LIST, FATF_GREY_LIST, EU_HIGH_RISK_THIRD_COUNTRY} & set(sigs)
        assert high_risk_lists_for(spelling) == []


def test_opted_in_layers_sentence_never_calls_a_tax_listing_high_risk() -> None:
    from opencheck.risk import ELEMENT_HIGH_RISK_JURISDICTION, _layers_summary

    text = _layers_summary(2, qualified=False, elements=[{
        "element": ELEMENT_HIGH_RISK_JURISDICTION,
        "jurisdictions": [
            {"statement_id": "a", "code": "AI", "lists": ["eu_tax"]},
            {"statement_id": "b", "code": "RU", "lists": ["eu", "eu_tax"]},
        ],
    }])
    assert "registered in a high-risk jurisdiction (RU)" in text
    assert "registered in a jurisdiction on the EU tax list (AI)" in text


def test_non_eu_note_names_the_tax_list_as_a_risk_source() -> None:
    from opencheck.risk import _non_eu_summary

    assert "the EU tax list" in _non_eu_summary(["US"], qualified=False)


def test_share_card_and_narrative_labels() -> None:
    from opencheck.narrative.packet import _RISK_LABELS
    from opencheck.og_image import SIGNAL_STYLE

    assert SIGNAL_STYLE[EU_TAX_NON_COOPERATIVE][0] == "EU tax list"
    assert "no AML enhanced-due-diligence obligation" in _RISK_LABELS[EU_TAX_NON_COOPERATIVE]
