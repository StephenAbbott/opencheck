"""Phase 293 — a low name-only offshore-leaks person match is a lead, not a
finding in the verdict (Stephen chose option b, 6 Oct 2026).

A.P. Møller - Mærsk A/S read "The records show a possible appearance in
offshore-leaks data." on two ICIJ Paradise Papers records for former chair
Michael Pram Rasmussen → "MICHAEL RASMUSSEN" (score 77, ICIJ match false,
nothing to corroborate). Dropping the match (option a) would also have
dropped DMGT's adjudicated true match NICHOLAS PAUL RATCLIFFE → "NICHOLAS
RATCLIFFE", the same shape. So the match stays on the card and only its
weight in the sentence changes. The signals below are production's own
(5–6 Oct 2026), trimmed.
"""

from __future__ import annotations

import copy
from typing import Any

from opencheck import findings_regression as fr
from opencheck.verdict import VERDICT_TEMPLATE, build_verdict, is_name_only_lead

PARTY = "opencheck-4aa3afb9b8ee331c468115ce"


def _lead(collection: str = "Malta corporate registry", node: str = "56024264",
          party: str = PARTY, name: str = "Michael Pram Rasmussen") -> dict[str, Any]:
    return {
        "code": "OFFSHORE_LEAKS", "kind": "risk", "confidence": "low", "source_id": "icij",
        "summary": f"Related party '{name}' matches a record in the Paradise Papers ({collection}).",
        "hit_id": f"https://offshoreleaks.icij.org/nodes/{node}",
        "evidence": {
            "subject_statement_id": party, "search_name": name,
            "matched_name": name.upper(), "icij_score": 77, "icij_match": False,
            "kind": "person", "dataset": "Paradise Papers", "collection": collection,
            "gates": ["jurisdiction not checked: no country on the party",
                      "name-only person match: capped at low"],
            "jurisdiction_gate": {"status": "not_checked", "party_countries": [],
                                  "record_countries": ["DK"]},
        },
    }


SUBSIDIARY_LISTED = {"code": "SUBSIDIARY_LISTED_JURISDICTION", "kind": "context",
                     "confidence": "low", "source_id": "gleif", "evidence": {}}

TRUNCATED = [{"source_id": "opencheck", "check": "icij_offshore_leaks", "reason": "truncated",
              "affected_signals": ["OFFSHORE_LEAKS"],
              "detail": "Offshore Leaks screening covered the 30 highest-ranked of 73 related parties"}]


def test_the_template_was_bumped() -> None:
    assert VERDICT_TEMPLATE == 5


def test_which_signals_are_leads() -> None:
    assert is_name_only_lead(_lead())
    corroborated = _lead()
    corroborated["confidence"] = "medium"
    corroborated["evidence"]["jurisdiction_gate"]["status"] = "corroborated"
    assert not is_name_only_lead(corroborated)
    entity = _lead()
    entity["evidence"]["kind"] = "entity"
    assert not is_name_only_lead(entity)
    subject = _lead()
    subject["evidence"]["subject"] = True
    assert not is_name_only_lead(subject)
    pep = {**_lead(), "code": "RELATED_PEP"}
    assert not is_name_only_lead(pep)


def test_maersk_reads_clean_and_names_the_lead() -> None:
    signals = [_lead(), _lead("Samoa corporate registry", "66002486"), SUBSIDIARY_LISTED]
    verdict = build_verdict(signals, [])
    assert verdict.startswith("No risk signals surfaced across the sources that answered.")
    # Two records, one person: one lead.
    assert "A possible name-only offshore-leaks match on one related party is listed for review." in verdict
    assert "offshore-leaks data" not in verdict


def test_maersk_with_its_truncated_screens_keeps_the_caveat() -> None:
    verdict = build_verdict([_lead(), SUBSIDIARY_LISTED], TRUNCATED)
    assert verdict.startswith("No risk signals surfaced, but ")
    assert verdict.endswith("is listed for review.")


def test_leads_on_two_parties_are_counted_by_party() -> None:
    verdict = build_verdict([_lead(), _lead(party="p2", name="Jane Doe", node="2")], [])
    assert "Possible name-only offshore-leaks matches on 2 related parties are listed for review." in verdict


def test_a_corroborated_leak_still_drives_the_sentence_and_the_lead_is_not_repeated() -> None:
    real = _lead(party="p2", name="Norman Benito Fiore", node="9")
    real["confidence"] = "medium"
    real["evidence"]["jurisdiction_gate"]["status"] = "corroborated"
    verdict = build_verdict([_lead(), real], [])
    assert verdict == "The records show a possible appearance in offshore-leaks data."


def test_another_finding_leads_the_sentence_and_the_lead_is_not_added() -> None:
    pep = {"code": "RELATED_PEP", "kind": "risk", "confidence": "medium", "source_id": "opensanctions",
           "evidence": {}}
    verdict = build_verdict([pep, _lead()], [])
    assert verdict == "The records show a possible politically exposed person among the parties named."


def test_structure_only_verdict_names_the_lead_after_the_structure() -> None:
    # ASDA Stores' shape: a deep chain plus a low person match.
    layers = {"code": "COMPLEX_OWNERSHIP_LAYERS", "kind": "risk", "confidence": "medium",
              "source_id": "opencheck", "evidence": {"intermediate_layers": 7}}
    verdict = build_verdict([layers, _lead()], [])
    assert "intermediate corporate layers" in verdict
    assert "offshore-leaks data" not in verdict
    assert verdict.endswith("is listed for review.")


# ---------------------------------------------------------------------
# The findings regression: a clean control may carry a lead
# ---------------------------------------------------------------------


MAERSK_GOLDEN = {
    "lei": "549300D2K6PKKKXVNN73", "name": "A.P. Møller - Mærsk A/S", "role": "anchor",
    "example_card": False, "no_risk": True, "risk": {},
    "context": ["SUBSIDIARY_LISTED_JURISDICTION"],
    "absent": ["FATF_GREY_LIST", "FATF_BLACK_LIST", "EU_HIGH_RISK_THIRD_COUNTRY"],
    "verdict": {"matches": ["^No risk signals surfaced"], "not_matches": ["watch list"]},
    "why": "fixture", "sources_found": [], "liveness": {}, "lei_confirmed_min": 0,
    "intermediate_layers_min": None,
}


RULES = fr.Rules(structural_codes=frozenset({"COMPLEX_OWNERSHIP_LAYERS"}),
                 retired_codes=frozenset())


def _payload(signals: list[dict[str, Any]], degraded=None) -> dict[str, Any]:
    return {
        "lei": MAERSK_GOLDEN["lei"], "legal_name": "A.P. MØLLER - MÆRSK A/S",
        "risk_signals": signals,
        "degraded_sources": degraded or [],
        "verdict": build_verdict(signals, degraded or []),
        "hits": [{"source_id": "gleif", "is_stub": False}],
        "source_liveness": {"gleif": {"liveness": "live"}},
        "cross_source_links": [], "graph_shape": {},
        "subject_profile": {"lei_registration": {"status": "ISSUED"}},
        "sources_applicable": [], "replayed": False,
    }


def _failed_checks(exp: dict[str, Any], payload: dict[str, Any]) -> list[str]:
    findings, _summary = fr.evaluate(exp, payload, rules=RULES)
    return [f.check for f in findings if f.severity == "fail"]


def test_maersk_passes_its_golden_file_with_the_lead_on_the_card() -> None:
    payload = _payload([_lead(), _lead("Samoa corporate registry", "66002486"), SUBSIDIARY_LISTED],
                       TRUNCATED)
    assert fr.summarise(payload, MAERSK_GOLDEN["lei"])["signals"]["OFFSHORE_LEAKS"]["leads"] == 2
    assert _failed_checks(MAERSK_GOLDEN, payload) == []


def test_a_clean_control_with_a_real_finding_still_fails() -> None:
    real = copy.deepcopy(_lead())
    real["confidence"] = "medium"
    real["evidence"]["jurisdiction_gate"]["status"] = "corroborated"
    checks = _failed_checks(MAERSK_GOLDEN, _payload([real, SUBSIDIARY_LISTED]))
    assert "unexpected_signal" in checks
