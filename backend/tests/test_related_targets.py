"""Phase 279 — related-party screens dedupe, rank and report their cap.

Before, ``cross_check`` and ``icij_check`` read the first 25/30 targets in
bundle order and dropped the rest silently: Eli Lilly's ``RELATED_PEP`` came
and went between runs as OpenCorporates' officers pushed Katherine Baicker past
the cap, and Rosneft screened 25 of 106 parties.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from opencheck import related_targets
from opencheck.config import get_settings
from opencheck.cross_check import (
    RELATED_PEP,
    _collect_targets,
    assess_cross_source_names,
)
from opencheck.icij_check import _RECONCILE_URL, assess_icij_names
from opencheck.risk import DEGRADED_TRUNCATED, DegradedSource, RiskSignal
from opencheck.sources import REGISTRY, SearchKind, SourceHit
from opencheck.verdict import build_verdict

LEI = "5493001KJTIIGC8Y1R12"


# ---------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------


def _person(sid: str, name: str, *, birth: str | None = None,
            identifiers: list[dict[str, str]] | None = None) -> dict[str, Any]:
    rd: dict[str, Any] = {
        "personType": "knownPerson",
        "names": [{"type": "legal", "fullName": name}],
    }
    if birth:
        rd["birthDate"] = birth
    if identifiers:
        rd["identifiers"] = identifiers
    return {"statementId": sid, "recordId": sid, "recordType": "person", "recordDetails": rd}


def _entity(sid: str, name: str, *, lei: str | None = None,
            juris: str | None = None, founded: str | None = None,
            identifiers: list[dict[str, str]] | None = None) -> dict[str, Any]:
    rd: dict[str, Any] = {"entityType": {"type": "registeredEntity"}, "name": name}
    ids = list(identifiers or [])
    if lei:
        ids.append({"scheme": "XI-LEI", "id": lei})
    if ids:
        rd["identifiers"] = ids
    if juris:
        rd["jurisdiction"] = {"code": juris}
    if founded:
        rd["foundingDate"] = founded
    return {"statementId": sid, "recordId": sid, "recordType": "entity", "recordDetails": rd}


def _rel(sid: str, subject: str, ip: str, interest: str | None,
         *, ended: bool = False, bo: bool | None = None) -> dict[str, Any]:
    interests: list[dict[str, Any]] = []
    if interest:
        i: dict[str, Any] = {"type": interest, "directOrIndirect": "direct"}
        if bo is not None:
            i["beneficialOwnershipOrControl"] = bo
        if ended:
            i["endDate"] = "2001-01-01"
        interests.append(i)
    return {
        "statementId": sid,
        "recordId": sid,
        "recordType": "relationship",
        "recordDetails": {
            "isComponent": False,
            "subject": subject,
            "interestedParty": ip,
            "interests": interests,
        },
    }


def _select(bods: list[dict[str, Any]], limit: int = 25, subject_ids=frozenset()):
    return related_targets.select(
        _collect_targets(bods, exclude=subject_ids), bods,
        limit=limit, subject_ids=subject_ids,
    )


# ---------------------------------------------------------------------
# Dedupe
# ---------------------------------------------------------------------


def test_same_name_one_year_missing_merges_and_pools_the_year() -> None:
    bods = [_person("wd", "Katherine Baicker", birth="1971"),
            _person("oc", "KATHERINE BAICKER")]
    sel = _select(bods)
    assert sel.total == 1 and sel.statements == 2
    rep = sel.screened[0]
    assert rep["statement_ids"] == ["wd", "oc"]
    assert rep["birth_year"] == 1971


def test_year_missing_on_the_first_copy_is_taken_from_the_second() -> None:
    sel = _select([_person("oc", "Jane Roe"), _person("wd", "Jane Roe", birth="1960-04")])
    assert sel.screened[0]["statement_id"] == "oc"
    assert sel.screened[0]["birth_year"] == 1960


def test_conflicting_birth_years_do_not_merge() -> None:
    sel = _select([_person("a", "John Smith", birth="1950"),
                   _person("b", "John Smith", birth="1980")])
    assert sel.total == 2


def test_a_year_less_copy_cannot_chain_two_dated_namesakes() -> None:
    sel = _select([_person("a", "John Smith", birth="1950"),
                   _person("b", "John Smith"),
                   _person("c", "John Smith", birth="1980")])
    assert sel.total == 2
    assert [r["statement_ids"] for r in sel.screened] == [["a", "b"], ["c"]]


def test_shared_identifier_merges_despite_different_names() -> None:
    sel = _select([_entity("g", "Acme Holdings Limited", lei=LEI),
                   _entity("o", "ACME HLDGS", lei=LEI)])
    assert sel.total == 1


def test_entities_with_conflicting_jurisdictions_do_not_merge() -> None:
    sel = _select([_entity("a", "Acme Holdings", juris="GB"),
                   _entity("b", "Acme Holdings", juris="US-DE")])
    assert sel.total == 2


def test_a_person_and_an_entity_never_merge() -> None:
    sel = _select([_person("p", "Morgan Stanley"), _entity("e", "Morgan Stanley")])
    assert sel.total == 2


def test_a_cluster_is_former_only_when_every_copy_is() -> None:
    bods = [
        _entity("s", "Subject Co", lei=LEI),
        _person("p1", "Ann Lee"), _person("p2", "Ann Lee"),
        _rel("r1", "s", "p1", "boardMember", ended=True),
        _rel("r2", "s", "p2", "boardMember"),
    ]
    sel = _select(bods, subject_ids=frozenset({"s"}))
    ann = next(r for r in sel.screened if r["name"] == "Ann Lee")
    assert ann["former"] is False


# ---------------------------------------------------------------------
# Ranking
# ---------------------------------------------------------------------


def _ranked_bundle() -> list[dict[str, Any]]:
    """Bundle order deliberately the reverse of the intended ranking."""
    return [
        _entity("s", "Subject Co", lei=LEI),
        _person("former", "Fay Former"),
        _person("indirect", "Ian Indirect"),
        _entity("mid", "Middle Holdings"),
        _person("other", "Otto Other"),
        _person("officer", "Olive Officer"),
        _person("owner", "Owen Owner"),
        _rel("r1", "s", "former", "boardMember", ended=True),
        _rel("r2", "mid", "indirect", "boardMember"),
        _rel("r3", "s", "mid", "shareholding"),
        _rel("r4", "s", "other", "unknownInterest"),
        _rel("r5", "s", "officer", "seniorManagingOfficial"),
        _rel("r6", "s", "owner", "shareholding"),
    ]


def test_ranking_current_owner_officer_other_then_former() -> None:
    sel = _select(_ranked_bundle(), subject_ids=frozenset({"s"}))
    assert [r["statement_id"] for r in sel.screened] == [
        "mid",       # current, owner, direct (first seen of the owner tier)
        "owner",     # current, owner, direct
        "officer",   # current, officer, direct
        "indirect",  # current, officer, indirect
        "other",     # current, other
        "former",    # former last
    ]


def test_beneficial_ownership_flag_ranks_as_owner_whatever_the_type() -> None:
    bods = [
        _entity("s", "Subject Co", lei=LEI),
        _person("a", "Alpha Person"), _person("b", "Beta Person"),
        _rel("r1", "s", "a", "seniorManagingOfficial"),
        _rel("r2", "s", "b", "otherInfluenceOrControl", bo=True),
    ]
    sel = _select(bods, subject_ids=frozenset({"s"}))
    assert [r["statement_id"] for r in sel.screened] == ["b", "a"]


def test_without_an_anchor_bundle_order_breaks_ties() -> None:
    sel = _select([_person(f"p{i}", f"Person Number{i}") for i in range(5)])
    assert [r["statement_id"] for r in sel.screened] == [f"p{i}" for i in range(5)]


def test_ranking_is_deterministic() -> None:
    a = _select(_ranked_bundle(), subject_ids=frozenset({"s"}))
    b = _select(_ranked_bundle(), subject_ids=frozenset({"s"}))
    assert [r["statement_id"] for r in a.screened] == [r["statement_id"] for r in b.screened]


# ---------------------------------------------------------------------
# Cap and its report
# ---------------------------------------------------------------------


def test_cap_counts_what_it_left_out_by_reason() -> None:
    sel = _select(_ranked_bundle(), limit=2, subject_ids=frozenset({"s"}))
    assert len(sel.screened) == 2 and sel.total == 6 and sel.truncated
    assert sel.not_screened == 4
    assert sel.not_screened_former == 1
    assert sel.not_screened_current_other == 1
    assert sel.not_screened_current_role_or_owner == 2
    detail = sel.detail("Some screening")
    assert "2 highest-ranked of 6 related parties; 4 were not screened" in detail
    for name in ("Fay", "Former", "Owen", "Otto", "Olive", "Ian"):
        assert name not in detail


def test_no_truncation_under_the_cap() -> None:
    sel = _select(_ranked_bundle(), limit=25, subject_ids=frozenset({"s"}))
    assert not sel.truncated and sel.not_screened == 0


def test_dedupe_happens_before_the_cap() -> None:
    # Three statements, one party: a cap of one still reads everyone.
    bods = [_person("a", "Ann Lee"), _person("b", "Ann Lee"), _person("c", "Ann Lee")]
    sel = _select(bods, limit=1)
    assert sel.total == 1 and not sel.truncated


# ---------------------------------------------------------------------
# Fan-out
# ---------------------------------------------------------------------


def _sig(sub: str, **extra: Any) -> RiskSignal:
    return RiskSignal(code=RELATED_PEP, confidence="high", summary="x",
                      source_id="opensanctions", hit_id="h1",
                      evidence={"subject_statement_id": sub, **extra})


def test_fan_out_gives_each_statement_its_own_copy() -> None:
    reps = [{"statement_id": "wd", "statement_ids": ["wd", "oc"]}]
    out = related_targets.fan_out([_sig("wd", score=1.0)], reps)
    assert [s.evidence["subject_statement_id"] for s in out] == ["wd", "oc"]
    assert out[1].evidence["score"] == 1.0
    out[1].evidence["score"] = 0
    assert out[0].evidence["score"] == 1.0  # not shared


def test_fan_out_leaves_subject_signals_alone() -> None:
    sig = RiskSignal(code="OFFSHORE_LEAKS", confidence="low", summary="x",
                     source_id="icij", hit_id="n", evidence={"statement_id": "s", "subject": True})
    reps = [{"statement_id": "s", "statement_ids": ["s", "t"]}]
    assert related_targets.fan_out([sig], reps) == [sig]


# ---------------------------------------------------------------------
# cross_check integration
# ---------------------------------------------------------------------


@pytest.fixture
def _live(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENSANCTIONS_API_KEY", "test-key")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class _Recorder:
    def __init__(self, pep_name: str | None = None) -> None:
        self.queries: list[str] = []
        self.pep_name = pep_name

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        self.queries.append(query)
        if self.pep_name and query.lower() == self.pep_name.lower():
            return [SourceHit(source_id="opensanctions", hit_id="OS-1", kind=kind,
                              name=self.pep_name, summary="", identifiers={},
                              raw={"topics": ["role.pep"]}, is_stub=False)]
        return []


class _Empty:
    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return []


def _big_bundle(n_former: int = 30) -> list[dict[str, Any]]:
    """Former directors first in bundle order, a current one last — the
    Eli Lilly shape: bundle order alone would never reach her."""
    bods: list[dict[str, Any]] = [_entity("s", "Subject Co", lei=LEI)]
    for i in range(n_former):
        bods.append(_person(f"f{i}", f"Former Director{i} Smithson"))
        bods.append(_rel(f"rf{i}", "s", f"f{i}", "boardMember", ended=True))
    bods.append(_person("wd", "Katherine Baicker", birth="1971"))
    bods.append(_person("oc", "Katherine Baicker"))
    bods.append(_rel("rw", "s", "wd", "boardMember"))
    bods.append(_rel("ro", "s", "oc", "boardMember"))
    return bods


async def test_cross_check_screens_the_current_director_and_reports_the_cap(
    monkeypatch, _live
) -> None:
    os_adapter = _Recorder(pep_name="Katherine Baicker")
    monkeypatch.setitem(REGISTRY, "opensanctions", os_adapter)
    monkeypatch.setitem(REGISTRY, "everypolitician", _Empty())
    degraded: list[DegradedSource] = []
    signals = await assess_cross_source_names(
        _big_bundle(), degraded=degraded, subject_lei=LEI
    )
    # Screened once, ranked first, despite sitting last in bundle order.
    assert os_adapter.queries[0] == "Katherine Baicker"
    assert os_adapter.queries.count("Katherine Baicker") == 1
    assert len(os_adapter.queries) == 25
    # The signal is on both statements that name her.
    peps = [s for s in signals if s.code == RELATED_PEP]
    assert sorted(s.evidence["subject_statement_id"] for s in peps) == ["oc", "wd"]
    # And the cap is said out loud, counts only.
    capped = [d for d in degraded if d.reason == DEGRADED_TRUNCATED]
    assert len(capped) == 1
    rec = capped[0]
    assert rec.source_id == "opencheck" and rec.check == "cross_source_names"
    assert RELATED_PEP in rec.affected_signals
    assert "25 highest-ranked of 31 related parties; 6 were not screened (6 former)" in rec.detail
    payload = json.dumps(rec.to_dict())
    assert "Baicker" not in payload and "Smithson" not in payload


async def test_cross_check_under_the_cap_emits_no_truncation(monkeypatch, _live) -> None:
    monkeypatch.setitem(REGISTRY, "opensanctions", _Empty())
    monkeypatch.setitem(REGISTRY, "everypolitician", _Empty())
    degraded: list[DegradedSource] = []
    await assess_cross_source_names(_big_bundle(n_former=3), degraded=degraded, subject_lei=LEI)
    assert degraded == []


async def test_cross_check_cap_counts_distinct_parties_not_statements(monkeypatch, _live) -> None:
    # 26 statements, 13 people: under a cap of 25, nothing is truncated.
    bods = []
    for i in range(13):
        bods += [_person(f"a{i}", f"Person Number{i}"), _person(f"b{i}", f"Person Number{i}")]
    monkeypatch.setitem(REGISTRY, "opensanctions", _Empty())
    monkeypatch.setitem(REGISTRY, "everypolitician", _Empty())
    degraded: list[DegradedSource] = []
    await assess_cross_source_names(bods, degraded=degraded)
    assert degraded == []


# ---------------------------------------------------------------------
# icij_check integration
# ---------------------------------------------------------------------


async def test_icij_reports_its_cap(httpx_mock, _live) -> None:
    httpx_mock.add_callback(
        lambda request: httpx.Response(
            200,
            json={k: {"result": []} for k in json.loads(
                dict(httpx.QueryParams(request.content.decode())).get("queries") or "{}"
            ) or {}},
        ),
        url=_RECONCILE_URL,
        is_reusable=True,
    )
    degraded: list[DegradedSource] = []
    await assess_icij_names(_big_bundle(), degraded=degraded, subject_lei=LEI, max_targets=10)
    capped = [d for d in degraded if d.reason == DEGRADED_TRUNCATED]
    assert len(capped) == 1
    assert capped[0].check == "icij_offshore_leaks"
    assert capped[0].affected_signals == ["OFFSHORE_LEAKS"]
    assert "10 highest-ranked of 31 related parties; 21 were not screened" in capped[0].detail
    assert "Smithson" not in capped[0].detail


# ---------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------


def _capped(check: str = "cross_source_names") -> dict[str, Any]:
    return {"source_id": "opencheck", "check": check, "affected_signals": [],
            "detail": "…", "reason": "truncated"}


def test_verdict_says_a_capped_screen_answered_in_part_not_that_it_did_not_run() -> None:
    v = build_verdict([], [_capped()])
    assert "one check answered only in part" in v
    assert "did not run" not in v


def test_verdict_counts_capped_checks_by_check() -> None:
    v = build_verdict([], [_capped(), _capped("icij_offshore_leaks")])
    assert "2 checks answered only in part" in v


def test_verdict_keeps_both_claims_when_one_screen_failed_and_one_was_capped() -> None:
    failed = {"source_id": "opensanctions", "check": "cross_source_names",
              "affected_signals": [], "detail": "…", "reason": "timeout"}
    v = build_verdict([], [failed, _capped("icij_offshore_leaks")])
    assert "one check did not run" in v and "one check answered only in part" in v


def test_an_identifier_merged_party_is_read_under_each_spelling() -> None:
    """GLEIF's Cyrillic legal name and OpenSanctions' English one are one
    company, but a name screen matches on the spelling: both are read, and
    the cluster still counts once against the cap."""
    bods = [_entity("g", "ПАО НК Роснефть", lei=LEI),
            _entity("o", "Rosneft Oil Company", lei=LEI),
            _entity("x", "Rosneft Oil Company")]
    sel = _select(bods, limit=1)
    assert sel.total == 1 and sel.parties_screened == 1 and not sel.truncated
    assert [t["name"] for t in sel.screened] == ["ПАО НК Роснефть", "Rosneft Oil Company"]
    assert all(t["statement_ids"] == ["g", "o", "x"] for t in sel.screened)
