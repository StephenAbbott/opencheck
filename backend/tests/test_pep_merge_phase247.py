"""Phase 247 — PEP signals: merged per upstream record, own-role PEPs labelled,
one confidence rule for person matches, and a verdict that says "possible".

The fixtures are the Equinor shapes read live on 25 Sept 2026: OpenSanctions'
entity ids, OpenAleph's signed copies of them, and the position occupancies
OpenSanctions' ``no_brreg`` dataset (Norway State-Owned Enterprises Leadership)
publishes for Equinor's board.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from opencheck import pep_merge
from opencheck.app import app
from opencheck.cross_check import (
    RELATED_PEP,
    RELATED_PEP_SUBJECT_ROLE,
    _signal_from_ep,
)
from opencheck.sources import REGISTRY, SearchKind, SourceHit
from opencheck.verdict import build_verdict

OPEDAL = "Q28047991"
ROTH = "no-nbim-3b4c0e0dab4c0fdbe5c7a1c2d9d40a97b0d24516"
BRUUN = "Q11973582"
EQUINOR_ORG = "NK-3GuDnMThNPbTqcf4r7Nomm"  # OpenSanctions' "EQUINOR ASA" Organization
EQUINOR_CO = "NK-jsAdAZqk3fG7KumbmDM5LW"  # OpenSanctions' "Equinor" Company (has the LEI)


def _sig(
    source_id: str,
    hit_id: str,
    name: str,
    statement: str,
    confidence: str = "high",
    code: str = RELATED_PEP,
) -> dict[str, Any]:
    return {
        "code": code,
        "confidence": confidence,
        "summary": f"Related party '{name}' matches a record on {source_id}: PEP.",
        "source_id": source_id,
        "hit_id": hit_id,
        "evidence": {
            "subject_statement_id": statement,
            "search_name": name,
            "matched_name": name,
            "kind": "person",
            "name_match_only": confidence != "high",
        },
        "kind": "risk",
    }


def _aleph(ftm_id: str, sig: str = "d63cd77012733205ecbc1b123d4f4f4db8ab34f0") -> str:
    return f"{ftm_id}.{sig}"


# ---------------------------------------------------------------------------
# upstream_record_id
# ---------------------------------------------------------------------------


def test_upstream_id_is_the_opensanctions_id_for_every_mirror() -> None:
    assert pep_merge.upstream_record_id(_sig("opensanctions", OPEDAL, "A", "s")) == OPEDAL
    assert pep_merge.upstream_record_id(_sig("everypolitician", OPEDAL, "A", "s")) == OPEDAL
    assert pep_merge.upstream_record_id(_sig("openaleph", _aleph(OPEDAL), "A", "s")) == OPEDAL
    assert pep_merge.upstream_record_id(_sig("openaleph", _aleph(ROTH), "A", "s")) == ROTH


def test_a_source_that_does_not_mirror_opensanctions_has_no_upstream_id() -> None:
    assert pep_merge.upstream_record_id(_sig("icij", "12345", "A", "s")) is None
    assert pep_merge.upstream_record_id(_sig("opensanctions", "", "A", "s")) is None


# ---------------------------------------------------------------------------
# merge_derived_pep — the Anders Opedal shape: 3 sources × 2 statements
# ---------------------------------------------------------------------------


def _opedal_six() -> list[dict[str, Any]]:
    return [
        _sig("opensanctions", OPEDAL, "Anders Opedal", "st-no-dob", "medium"),
        _sig("everypolitician", OPEDAL, "Anders Opedal", "st-no-dob", "medium"),
        _sig("opensanctions", OPEDAL, "Anders Opedal", "st-dob", "high"),
        _sig("everypolitician", OPEDAL, "Anders Opedal", "st-dob", "high"),
        _sig("openaleph", _aleph(OPEDAL), "Anders Opedal", "st-no-dob", "medium"),
        _sig("openaleph", _aleph(OPEDAL), "Anders Opedal", "st-dob", "high"),
    ]


def test_one_chip_per_person_per_upstream_record() -> None:
    out = pep_merge.merge_derived_pep(_opedal_six())
    assert len(out) == 1
    sig = out[0]
    # Highest confidence; the tie between three high signals goes upstream.
    assert sig["confidence"] == "high"
    assert sig["source_id"] == "opensanctions"
    ev = sig["evidence"]
    assert ev["upstream_record_id"] == OPEDAL
    # Both person statements keep the badge.
    assert ev["subject_statement_ids"] == ["st-dob", "st-no-dob"]
    assert ev["subject_statement_id"] == "st-dob"
    assert {o["source_id"] for o in ev["also_reported_by"]} == {
        "opensanctions",
        "everypolitician",
        "openaleph",
    }
    assert "also reported by EveryPolitician and OpenAleph" in sig["summary"]


def test_a_derived_source_alone_keeps_its_own_attribution() -> None:
    only_aleph = [_sig("openaleph", _aleph(BRUUN), "Haakon Stephen Bruun-Hanssen", "s1")]
    out = pep_merge.merge_derived_pep(only_aleph)
    assert out[0]["source_id"] == "openaleph"
    assert "also_reported_by" not in out[0]["evidence"]


def test_different_upstream_records_stay_separate_chips() -> None:
    two = [
        _sig("opensanctions", "Q1", "John Smith", "s1"),
        _sig("opensanctions", "Q2", "John Smith", "s1"),
    ]
    assert len(pep_merge.merge_derived_pep(two)) == 2


def test_other_codes_and_other_names_are_left_alone() -> None:
    signals = [
        _sig("opensanctions", OPEDAL, "Anders Opedal", "s1", code="RELATED_SANCTIONED"),
        _sig("openaleph", _aleph(OPEDAL), "Anders Opedal", "s1", code="RELATED_SANCTIONED"),
        _sig("opensanctions", OPEDAL, "Anders Opedal", "s1"),
        _sig("opensanctions", OPEDAL, "A. Opedal", "s2"),
    ]
    out = pep_merge.merge_derived_pep(signals)
    assert len(out) == 4


def test_the_equinor_shape_collapses_to_one_row_per_person() -> None:
    """24 → one per (person, record): the ticket's 'done when'."""
    people = {
        ROTH: "Jarle Kjell Roth",
        BRUUN: "Haakon Stephen Bruun-Hanssen",
        "Q-hansen": "Terje Werner Hansen",
    }
    signals = _opedal_six()
    for uid, name in people.items():
        signals += [
            _sig("opensanctions", uid, name, f"st-{uid}"),
            _sig("everypolitician", uid, name, f"st-{uid}"),
            _sig("openaleph", _aleph(uid), name, f"st-{uid}"),
        ]
    out = pep_merge.merge_derived_pep(signals)
    assert len(out) == 4
    assert {s["evidence"]["search_name"] for s in out} == {
        "Anders Opedal",
        *people.values(),
    }


# ---------------------------------------------------------------------------
# Positions — as OpenSanctions publishes them (nested occupancies)
# ---------------------------------------------------------------------------


def _occupancy(name: str, org: str | None, topics: list[str]) -> dict[str, Any]:
    post: dict[str, Any] = {"name": [name], "topics": topics, "country": ["no"]}
    if org:
        post["organization"] = [org]
    return {
        "schema": "Occupancy",
        "properties": {"post": [{"schema": "Position", "caption": name, "properties": post}]},
    }


def _person(uid: str, occupancies: list[dict[str, Any]], positions: list[str] = ()) -> dict:
    return {
        "id": uid,
        "schema": "Person",
        "properties": {
            "topics": ["role.pep"],
            "positionOccupancies": occupancies,
            "position": list(positions),
        },
    }


RECORDS = {
    OPEDAL: _person(OPEDAL, [_occupancy("Managing Director, EQUINOR ASA", EQUINOR_ORG, ["gov.soe"])]),
    ROTH: _person(ROTH, [_occupancy("Chairman, EQUINOR ASA", EQUINOR_ORG, ["gov.soe"])]),
    BRUUN: _person(
        BRUUN,
        [
            _occupancy("Chief of Defence of Norway", None, ["gov.national", "gov.security"]),
            _occupancy("Board Member, EQUINOR ASA", EQUINOR_ORG, ["gov.soe"]),
        ],
        positions=[
            "Inspector General of the Royal Norwegian Navy (2008-2011)",
            "Chief of Defence of Norway (2013-2020)",
        ],
    ),
}


def test_record_positions_reads_occupancies_and_drops_flat_duplicates() -> None:
    names = [p["name"] for p in pep_merge.record_positions(RECORDS[BRUUN])]
    assert names == [
        "Chief of Defence of Norway",
        "Board Member, EQUINOR ASA",
        "Inspector General of the Royal Norwegian Navy (2008-2011)",
    ]


def test_position_at_subject_by_name_after_the_comma() -> None:
    pos = {"name": "Chairman, EQUINOR ASA", "organizations": [EQUINOR_ORG]}
    # The subject's own OpenSanctions entity is the *Company*, not the
    # Organization the position points at — the name does the work.
    assert pep_merge.position_at_subject(pos, ["EQUINOR ASA"], [EQUINOR_CO])
    assert not pep_merge.position_at_subject(pos, ["SHELL PLC"], [EQUINOR_CO])
    assert not pep_merge.position_at_subject(
        {"name": "Chief of Defence of Norway", "organizations": []}, ["EQUINOR ASA"], []
    )


def test_position_at_subject_reads_the_danish_order_too() -> None:
    """Denmark's dataset puts the organisation first: "Ørsted A/S, board of
    directors (vice chairman)" (Ørsted's board, read live 25 Sept 2026)."""
    for name in (
        "Ørsted A/S, board of directors (vice chairman)",
        "Ørsted A/S, Executive Board",
    ):
        assert pep_merge.position_at_subject({"name": name}, ["ØRSTED A/S"], [])
    # A House of Lords seat is not a role at Ørsted.
    assert not pep_merge.position_at_subject(
        {"name": "Member of the House of Lords"}, ["ØRSTED A/S"], []
    )


def test_position_at_subject_by_organisation_id() -> None:
    pos = {"name": "Board member", "organizations": [EQUINOR_ORG]}
    assert pep_merge.position_at_subject(pos, ["Something else"], [EQUINOR_ORG])


# ---------------------------------------------------------------------------
# label_subject_role_peps
# ---------------------------------------------------------------------------


class _FakeOS:
    def __init__(self, records: dict[str, dict], fail: set[str] = frozenset()) -> None:
        self.records = records
        self.fail = fail
        self.calls: list[str] = []

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        self.calls.append(hit_id)
        if hit_id in self.fail:
            raise RuntimeError("boom")
        if hit_id not in self.records:
            return {"source_id": "opensanctions", "hit_id": hit_id, "is_stub": True}
        return {"source_id": "opensanctions", "entity_id": hit_id, "entity": self.records[hit_id]}


@pytest.fixture
def fake_os(monkeypatch: pytest.MonkeyPatch) -> _FakeOS:
    fake = _FakeOS(RECORDS)
    monkeypatch.setitem(REGISTRY, "opensanctions", fake)
    return fake


async def test_a_pep_only_through_the_subject_is_context(fake_os: _FakeOS) -> None:
    out = await pep_merge.consolidate_pep_signals(
        [
            _sig("opensanctions", ROTH, "Jarle Kjell Roth", "st-roth"),
            _sig("openaleph", _aleph(ROTH), "Jarle Kjell Roth", "st-roth"),
        ],
        subject_names=["EQUINOR ASA"],
        subject_record_ids=[EQUINOR_CO],
    )
    assert len(out) == 1
    sig = out[0]
    assert sig["code"] == RELATED_PEP_SUBJECT_ROLE
    assert sig["kind"] == "context"
    assert sig["evidence"]["pep_by_subject_role"] is True
    assert sig["evidence"]["subject_role_positions"] == ["Chairman, EQUINOR ASA"]
    assert "only by virtue of a role at this company (Chairman, EQUINOR ASA)" in sig["summary"]
    assert "Also reported by OpenAleph." in sig["summary"]
    # The badge still lands on the person.
    assert sig["evidence"]["subject_statement_id"] == "st-roth"
    # One record read per upstream record.
    assert fake_os.calls == [ROTH]


async def test_a_pep_with_another_position_stays_a_finding(fake_os: _FakeOS) -> None:
    out = await pep_merge.consolidate_pep_signals(
        [_sig("openaleph", _aleph(BRUUN), "Haakon Stephen Bruun-Hanssen", "st-b")],
        subject_names=["EQUINOR ASA"],
    )
    sig = out[0]
    assert sig["code"] == RELATED_PEP
    assert sig["kind"] == "risk"
    assert sig["evidence"]["pep_by_subject_role"] is False
    assert sig["evidence"]["subject_role_positions"] == ["Board Member, EQUINOR ASA"]
    assert "also lists a role at this company (Board Member, EQUINOR ASA)" in sig["summary"]


async def test_an_unreadable_record_changes_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(REGISTRY, "opensanctions", _FakeOS({}, fail={OPEDAL}))
    out = await pep_merge.consolidate_pep_signals(
        [_sig("opensanctions", OPEDAL, "Anders Opedal", "s"), _sig("opensanctions", "Q9", "X Y", "t")],
        subject_names=["EQUINOR ASA"],
    )
    assert [s["code"] for s in out] == [RELATED_PEP, RELATED_PEP]
    assert all(s["evidence"]["pep_role_checked"] is False for s in out)


async def test_without_subject_names_nothing_is_read(fake_os: _FakeOS) -> None:
    out = await pep_merge.consolidate_pep_signals(
        [_sig("opensanctions", ROTH, "Jarle Kjell Roth", "s")], subject_names=[]
    )
    assert out[0]["code"] == RELATED_PEP
    assert fake_os.calls == []


def test_subject_names_from_reads_the_subject_statements() -> None:
    bods = [
        {"statementId": "e1", "recordType": "entity", "recordDetails": {"name": "EQUINOR ASA"}},
        {"statementId": "e2", "recordType": "entity", "recordDetails": {"name": "Equinor"}},
        {"statementId": "e3", "recordType": "entity", "recordDetails": {"name": "Other AS"}},
    ]
    assert pep_merge.subject_names_from(bods, {"e1", "e2"}, "EQUINOR ASA") == [
        "EQUINOR ASA",
        "Equinor",
    ]


# ---------------------------------------------------------------------------
# The verdict says "possible" when every name match behind a clause is
# medium or lower — and only for the name-match clauses.
# ---------------------------------------------------------------------------


def _v(code: str, confidence: str, kind: str = "risk") -> dict[str, Any]:
    return {"code": code, "confidence": confidence, "kind": kind, "evidence": {}}


def test_shell_verdict_says_possible() -> None:
    signals = [_v("RELATED_PEP", "medium"), _v("RELATED_PEP", "low")]
    assert build_verdict(signals) == (
        "The records show a possible politically exposed person among the parties named."
    )


def test_one_high_pep_keeps_the_plain_clause() -> None:
    signals = [_v("RELATED_PEP", "medium"), _v("RELATED_PEP", "high")]
    assert build_verdict(signals) == (
        "The records show a politically exposed person among the parties named."
    )


def test_state_ownership_is_not_hedged() -> None:
    signals = [_v("RELATED_PEP", "high"), _v("STATE_CONTROLLED", "medium")]
    assert build_verdict(signals) == (
        "The records show a politically exposed person among the parties named "
        "and state ownership recorded on the company."
    )


def test_offshore_leaks_medium_is_possible() -> None:
    assert build_verdict([_v("OFFSHORE_LEAKS", "medium")]) == (
        "The records show a possible appearance in offshore-leaks data."
    )


def test_own_role_peps_do_not_make_the_pep_clause() -> None:
    signals = [
        _v(RELATED_PEP_SUBJECT_ROLE, "high", kind="context"),
        _v("STATE_CONTROLLED", "medium"),
    ]
    assert build_verdict(signals) == "The records show state ownership recorded on the company."


# ---------------------------------------------------------------------------
# EveryPolitician wording, and one confidence rule for person_check
# ---------------------------------------------------------------------------


def _hit(source_id: str, name: str, *, hit_id: str = "h1", raw: dict | None = None) -> SourceHit:
    return SourceHit(
        source_id=source_id,
        hit_id=hit_id,
        kind=SearchKind.PERSON,
        name=name,
        summary="",
        identifiers={},
        raw=raw or {},
        is_stub=False,
    )


def test_everypolitician_never_calls_a_director_a_political_office_holder() -> None:
    target = {"name": "Jarle Kjell Roth", "kind": "person", "statement_id": "s", "birth_year": 1960}
    hit = _hit("everypolitician", "Jarle Kjell Roth", raw={"properties": {"birthDate": ["1960-04-26"]}})
    sig = _signal_from_ep(hit, target, min_score=0.88)
    assert sig is not None
    assert "office-holder" not in sig.summary
    assert sig.summary.endswith(": PEP.")
    hit2 = _hit(
        "everypolitician",
        "Jarle Kjell Roth",
        raw={"properties": {"birthDate": ["1960"], "position": ["Member of the Storting"]}},
    )
    assert _signal_from_ep(hit2, target, min_score=0.88).summary.endswith(
        ": PEP (Member of the Storting)."
    )


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _run_with(monkeypatch: pytest.MonkeyPatch, hit: SourceHit) -> None:
    async def fake_run_adapters(q, kind):
        return {hit.source_id: [hit]}, {}

    monkeypatch.setattr("opencheck.routers.person_check._run_adapters", fake_run_adapters)


def test_person_check_rates_a_name_only_match_medium(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Jane Holl Lute: high here, medium on the company report — now one rule."""
    hit = _hit(
        "opensanctions",
        "Jane Holl Lute",
        raw={"properties": {"topics": ["role.pep"]}, "topics": ["role.pep"]},
    )
    _run_with(monkeypatch, hit)
    body = client.get("/person-check", params={"name": "Jane Holl Lute"}).json()
    pep = [s for s in body["risk_signals"] if s["code"] == "PEP"]
    assert pep, body["risk_signals"]
    assert all(s["confidence"] == "medium" for s in pep)
    assert all(s["evidence"]["match"]["name_match_only"] is True for s in pep)
    assert all(s["summary"].startswith("Possible name match only") for s in pep)


def test_person_check_keeps_high_when_the_birth_year_agrees(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    hit = _hit(
        "opensanctions",
        "Jane Holl Lute",
        raw={
            "properties": {"topics": ["role.pep"], "birthDate": ["1956-12-10"]},
            "topics": ["role.pep"],
        },
    )
    _run_with(monkeypatch, hit)
    body = client.get(
        "/person-check", params={"name": "Jane Holl Lute", "birth_year": 1956}
    ).json()
    pep = [s for s in body["risk_signals"] if s["code"] == "PEP"]
    assert pep and all(s["confidence"] == "high" for s in pep)
    assert pep[0]["evidence"]["match"]["corroboration"] == ["birth_year"]
