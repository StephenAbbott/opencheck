"""Phase 282 — related-party precision: false positives, double counts, ties.

Every case below was seen on a production lookup on 2 Oct 2026 and
reproduced on 3 Oct 2026, outside the Phase 277 golden set:

* Regulatory DataCorp Limited — former director "Mr. Robert Frederick Smith"
  matched Panama Papers officer "Mr. Robert Frederick White" (the person
  name gate looked at similarity only, and "Mr." lifted it over the bar).
* The National Churches Trust — trustee "JONES, Christopher Paul" matched
  two "Christopher Jones" records on name alone, at medium.
* Quantexa Limited — director Norman Benito Fiore, spelled two ways by two
  sources, never merged; four OFFSHORE_LEAKS signals, one of them against an
  Italy-only record although his own country is GB.
* Risk First Limited — Moody's Corporation, one party since Phase 279, still
  two RELATED_EXPORT_RISK signals (one copy per statement).
* Eli Lilly — ties inside a ranking tier fell back to bundle order, so the
  parties screened at the limit changed between runs.
"""

from __future__ import annotations

import json
import random
from typing import Any

import httpx
import pytest

from opencheck import names, related_targets
from opencheck.config import get_settings
from opencheck.cross_check import _collect_targets
from opencheck.icij_check import (
    _RECONCILE_URL,
    _gate,
    _passes_name_gates,
    _signal_from_match,
    assess_icij_names,
)
from opencheck.pep_merge import merge_derived_pep

LEI = "213800WMPZ7LH3F92517"


# ---------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------


def _person(sid: str, name: str, *, birth: str | None = None, source: str | None = None,
            country: str | None = None, address_type: str = "service") -> dict[str, Any]:
    rd: dict[str, Any] = {
        "personType": "knownPerson",
        "names": [{"type": "legal", "fullName": name}],
    }
    if birth:
        rd["birthDate"] = birth
    if country:
        rd["addresses"] = [{"type": address_type, "country": {"code": country}}]
    stmt: dict[str, Any] = {"statementId": sid, "recordId": sid, "recordType": "person",
                            "recordDetails": rd}
    if source:
        stmt["source"] = {"opencheckSourceId": source}
    return stmt


def _entity(sid: str, name: str, *, lei: str | None = None) -> dict[str, Any]:
    rd: dict[str, Any] = {"entityType": {"type": "registeredEntity"}, "name": name}
    if lei:
        rd["identifiers"] = [{"scheme": "XI-LEI", "id": lei}]
    return {"statementId": sid, "recordId": sid, "recordType": "entity", "recordDetails": rd}


def _rel(sid: str, subject: str, ip: str, interest: str) -> dict[str, Any]:
    return {
        "statementId": sid, "recordId": sid, "recordType": "relationship",
        "recordDetails": {
            "isComponent": False, "subject": subject, "interestedParty": ip,
            "interests": [{"type": interest, "directOrIndirect": "direct"}],
        },
    }


def _select(bods: list[dict[str, Any]], limit: int = 25, subject_ids=frozenset({"s"})):
    return related_targets.select(
        _collect_targets(bods, exclude=subject_ids), bods,
        limit=limit, subject_ids=subject_ids,
    )


def _officer(name: str) -> dict[str, Any]:
    return {"id": "1", "name": name, "score": 100.0, "match": True,
            "description": "Officer node extracted from the Panama Papers data.",
            "types": [{"id": ".../oldb/officer", "name": "Officer"}]}


_NODE_GB = {"country_codes": ["GB"], "valid_until": "The Panama Papers data is current through 2015"}


# ---------------------------------------------------------------------
# 1. Person names agree token by token
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "a, b, agrees",
    [
        ("Mr. Robert Frederick Smith", "Mr. Robert Frederick White", False),
        ("Robert John Smith", "Robert James Smith", False),
        ("JONES, Christopher Paul", "CHRISTOPHER JONES", True),
        ("JONES, Christopher Paul", "Jones - Christopher", True),
        ("FIORE, Norman Benito", "NORMAN BENITO FIORE", True),
        ("Michael Gordon", "Michael R. Gordon", True),
        ("NICHOLAS PAUL RATCLIFFE", "NICHOLAS RATCLIFFE", True),
        ("Mohammed Al Fayed", "Muhammad Al Fayed", True),
        ("Dame Carolyn Fairbairn", "FAIRBAIRN, Carolyn Julie", True),
        ("J", "John", False),
    ],
)
def test_person_token_agreement(a: str, b: str, agrees: bool) -> None:
    assert names.person_token_agreement(a, b) is agrees
    assert names.person_token_agreement(b, a) is agrees


def test_honorifics_are_stripped() -> None:
    assert names.strip_person_prefixes("Mr. Robert Frederick Smith") == "Robert Frederick Smith"
    assert names.strip_person_prefixes("Dame Carolyn Fairbairn") == "Carolyn Fairbairn"
    # Never to nothing.
    assert names.strip_person_prefixes("Mr") == "Mr"


def test_person_name_key_ignores_order_and_honorifics() -> None:
    assert names.person_name_key("FIORE, Norman Benito") == names.person_name_key(
        "Mr NORMAN BENITO FIORE"
    )
    assert names.person_name_key("Robert Smith") != names.person_name_key("Robert White")


# ---------------------------------------------------------------------
# ICIJ: the name gate, and what corroboration does
# ---------------------------------------------------------------------


def test_smith_does_not_match_white() -> None:
    target = {"kind": "person", "statement_id": "p", "name": "Mr. Robert Frederick Smith"}
    assert not _passes_name_gates(_officer("Mr. Robert Frederick White"), target, min_score=70)


def test_same_person_other_order_still_passes_the_name_gate() -> None:
    target = {"kind": "person", "statement_id": "p", "name": "FIORE, Norman Benito"}
    assert _passes_name_gates(_officer("NORMAN BENITO FIORE"), target, min_score=70)


def test_uncorroborated_person_match_is_low() -> None:
    # National Churches Trust: no country on the party, nothing corroborates.
    target = {"kind": "person", "statement_id": "p", "name": "JONES, Christopher Paul",
              "founded": "1973-05", "countries": []}
    sig = _signal_from_match(_officer("CHRISTOPHER JONES"), target, min_score=70,
                             node=_NODE_GB, details_answered=True)
    assert sig is not None
    assert sig.confidence == "low"
    assert "name-only person match: capped at low" in sig.evidence["gates"]


def test_person_match_without_node_details_is_low() -> None:
    target = {"kind": "person", "statement_id": "p", "name": "Christopher Jones",
              "countries": ["GB"]}
    sig = _signal_from_match(_officer("CHRISTOPHER JONES"), target, min_score=70,
                             node=None, details_answered=False)
    assert sig is not None and sig.confidence == "low"


def test_corroborated_person_match_is_medium_never_high() -> None:
    target = {"kind": "person", "statement_id": "p", "name": "Norman Benito Fiore",
              "countries": ["GB"]}
    sig = _signal_from_match(_officer("NORMAN BENITO FIORE"), target, min_score=70,
                             node={"country_codes": ["GB", "IT"]}, details_answered=True)
    assert sig is not None
    assert sig.confidence == "medium"
    assert sig.evidence["jurisdiction_gate"]["status"] == "corroborated"


def test_recorded_jurisdiction_mismatch_drops_the_match() -> None:
    # Quantexa: GB director, Italy-only Pandora Papers record.
    target = {"kind": "person", "statement_id": "p", "name": "Norman Benito Fiore",
              "countries": ["GB"]}
    keep, _, _ = _gate(target, dataset="Pandora Papers", collection="", node={"country_codes": ["IT"]},
                       details_answered=True)
    assert keep is False


def test_uncorroborated_entity_match_stays_medium() -> None:
    # Entities are unchanged: HSBC HOLDINGS PLC against a country-less record.
    target = {"kind": "entity", "statement_id": "e", "name": "HSBC HOLDINGS PLC",
              "countries": ["GB"]}
    keep, confidence, _ = _gate(target, dataset="Paradise Papers", collection="Appleby",
                                node={"country_codes": []}, details_answered=True)
    assert keep and confidence == "medium"


# ---------------------------------------------------------------------
# 3. Two spellings of one director are one party, and one signal
# ---------------------------------------------------------------------


def _quantexa_bundle() -> list[dict[str, Any]]:
    return [
        _entity("s", "QUANTEXA LIMITED", lei=LEI),
        _person("ch", "FIORE, Norman Benito", birth="1970-10", source="companies_house",
                country="GB"),
        _person("oc", "NORMAN BENITO FIORE", birth="1970-10", source="opencorporates"),
        _rel("r1", "s", "ch", "boardMember"),
        _rel("r2", "s", "oc", "boardMember"),
    ]


def _icij_select(bods: list[dict[str, Any]]):
    from opencheck.icij_check import _collect_targets as icij_targets
    return related_targets.select(icij_targets(bods, exclude={"s"}), bods,
                                  limit=25, subject_ids={"s"})


def test_two_spellings_of_one_director_are_one_party() -> None:
    sel = _icij_select(_quantexa_bundle())
    assert sel.total == 1
    # Read once: word order is not a different spelling to a name screen.
    assert len(sel.screened) == 1
    assert sorted(sel.screened[0]["statement_ids"]) == ["ch", "oc"]


def test_a_persons_service_address_is_not_their_country() -> None:
    # Companies House officers carry only a service address — the company's.
    sel = _icij_select(_quantexa_bundle())
    assert sel.screened[0]["countries"] == []


def test_a_persons_residence_address_is_their_country() -> None:
    bods = _quantexa_bundle()
    bods[1] = _person("ch", "FIORE, Norman Benito", birth="1970-10",
                      source="companies_house", country="GB", address_type="residence")
    sel = _icij_select(bods)
    assert sel.screened[0]["countries"] == ["GB"]


@pytest.fixture
def _live(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _fiore_responder(nodes: dict[str, Any]):
    def _respond(request: httpx.Request) -> httpx.Response:
        form = dict(httpx.QueryParams(request.content.decode()))
        if "extend" in form:
            return httpx.Response(200, json={"rows": nodes})
        out: dict[str, Any] = {}
        for key, q in json.loads(form.get("queries") or "{}").items():
            hits = []
            if "fiore" in q["query"].lower() and key.endswith("officer"):
                for node_id in nodes:
                    hits.append({**_officer("NORMAN BENITO FIORE"), "id": node_id,
                                 "description": "Officer node extracted from the Pandora Papers data."})
            out[key] = {"result": hits}
        return httpx.Response(200, json=out)
    return _respond


_FIORE_NODES = {
    "240105319": {"country_codes": [{"str": "IT"}]},
    "240045474": {"country_codes": [{"str": "GB"}, {"str": "IT"}]},
}


async def test_quantexa_shape_is_one_party_per_record(httpx_mock, _live) -> None:
    """Two spellings × two ICIJ nodes was four medium signals. Now one party:
    one signal per distinct ICIJ record, each naming both statements, and
    ``low`` — his only country is Quantexa's service address, which says
    nothing about him."""
    httpx_mock.add_callback(_fiore_responder(_FIORE_NODES), url=_RECONCILE_URL,
                            is_reusable=True)
    signals = await assess_icij_names(_quantexa_bundle(), subject_lei=LEI)
    leaks = [s for s in signals if not s.evidence.get("subject")]
    assert sorted(s.hit_id.rsplit("/", 1)[-1] for s in leaks) == ["240045474", "240105319"]
    for sig in leaks:
        assert sig.confidence == "low"
        assert sig.evidence["subject_statement_ids"] == ["ch", "oc"]
        assert sig.evidence["jurisdiction_gate"]["status"] == "not_checked"


async def test_with_a_residence_address_the_mismatch_drops_and_the_match_corroborates(
    httpx_mock, _live
) -> None:
    httpx_mock.add_callback(_fiore_responder(_FIORE_NODES), url=_RECONCILE_URL,
                            is_reusable=True)
    bods = _quantexa_bundle()
    bods[1] = _person("ch", "FIORE, Norman Benito", birth="1970-10",
                      source="companies_house", country="GB", address_type="residence")
    signals = await assess_icij_names(bods, subject_lei=LEI)
    leaks = [s for s in signals if not s.evidence.get("subject")]
    assert len(leaks) == 1
    assert leaks[0].hit_id.endswith("240045474")
    assert leaks[0].confidence == "medium"


# ---------------------------------------------------------------------
# 4. One party, one signal — and the PEP merge keeps every statement
# ---------------------------------------------------------------------


def test_pep_merge_keeps_statement_ids_it_was_handed() -> None:
    # A signal already naming two statements of one party (attach_to_party)
    # merged with a third copy of the same record keeps all three.
    ev = {"search_name": "Katherine Baicker", "matched_name": "Katherine Baicker"}
    sig = {"code": "RELATED_PEP", "confidence": "high", "summary": "x",
           "source_id": "opensanctions", "hit_id": "Q1",
           "evidence": {**ev, "subject_statement_id": "wd",
                        "subject_statement_ids": ["oc", "wd"]}}
    other = {**sig, "evidence": {**ev, "subject_statement_id": "gl"}}
    merged = merge_derived_pep([sig, other])
    peps = [m for m in merged if m["code"] == "RELATED_PEP"]
    assert len(peps) == 1
    assert peps[0]["evidence"]["subject_statement_ids"] == ["gl", "oc", "wd"]


# ---------------------------------------------------------------------
# Ranking: ties no longer depend on bundle order
# ---------------------------------------------------------------------


def _tied_bundle() -> list[dict[str, Any]]:
    """Twelve current directors in one tier — more than the limit — the Eli
    Lilly shape. Two are also named by a second source."""
    bods: list[dict[str, Any]] = [_entity("s", "ELI LILLY AND COMPANY", lei=LEI)]
    people = ["Kimberly Johnson", "Katherine Baicker", "Ralph Alvarez", "Mary Lynne Hedley",
              "Gabrielle Fitzgerald", "Juan Luciano", "Marschall Runge", "Karen Walker",
              "Jamere Jackson", "Jackson Tai", "Kathi Seifert", "Carolyn Bertozzi"]
    for i, name in enumerate(people):
        bods.append(_person(f"wd{i}", name, source="wikidata"))
        bods.append(_rel(f"rw{i}", "s", f"wd{i}", "boardMember"))
    for i in (5, 9):  # Juan Luciano, Jackson Tai
        bods.append(_person(f"oc{i}", people[i].upper(), source="opencorporates"))
        bods.append(_rel(f"ro{i}", "s", f"oc{i}", "boardMember"))
    return bods


def _chosen(bods: list[dict[str, Any]], limit: int) -> list[str]:
    sel = _select(bods, limit=limit)
    return [names.person_name_key(r["name"]) for r in sel.screened]


def test_shuffled_bundles_select_the_same_parties() -> None:
    base = _tied_bundle()
    want = _chosen(base, limit=5)
    rng = random.Random(282)
    for _ in range(25):
        shuffled = list(base)
        rng.shuffle(shuffled)
        assert _chosen(shuffled, limit=5) == want


def test_shuffled_bundles_pick_the_same_representative_spelling() -> None:
    base = _tied_bundle()
    rng = random.Random(7)
    reps = set()
    for _ in range(10):
        shuffled = list(base)
        rng.shuffle(shuffled)
        sel = _select(shuffled, limit=25)
        reps.add(tuple(r["name"] for r in sel.screened))
    assert len(reps) == 1


def test_a_party_more_sources_name_ranks_first_in_its_tier() -> None:
    chosen = _chosen(_tied_bundle(), limit=2)
    assert chosen == [names.person_name_key("Jackson Tai"),
                      names.person_name_key("Juan Luciano")]


def test_then_by_name() -> None:
    chosen = _chosen(_tied_bundle(), limit=4)
    # The order-invariant name key: sorted tokens, so "alvarez ralph".
    assert chosen[2:] == [names.person_name_key("Ralph Alvarez"),
                          names.person_name_key("Katherine Baicker")]


def test_board_chair_then_board_member_then_senior_managing_official() -> None:
    bods = [
        _entity("s", "SUBJECT CO", lei=LEI),
        _person("smo", "Aaron Officer"), _rel("r1", "s", "smo", "seniorManagingOfficial"),
        _person("bm", "Bella Member"), _rel("r2", "s", "bm", "boardMember"),
        _person("ch", "Zed Chair"), _rel("r3", "s", "ch", "boardChair"),
    ]
    sel = _select(bods)
    assert [r["statement_id"] for r in sel.screened] == ["ch", "bm", "smo"]


def test_directness_still_outranks_the_officer_order() -> None:
    # A company's own senior managing official before the chair of a parent.
    bods = [
        _entity("s", "SUBJECT CO", lei=LEI),
        _entity("mid", "MIDDLE HOLDINGS"), _rel("r0", "s", "mid", "shareholding"),
        _person("chair", "Zed Chair"), _rel("r1", "mid", "chair", "boardChair"),
        _person("smo", "Aaron Officer"), _rel("r2", "s", "smo", "seniorManagingOfficial"),
    ]
    sel = _select(bods)
    officers = [r["statement_id"] for r in sel.screened if r["statement_id"] != "mid"]
    assert officers == ["smo", "chair"]
