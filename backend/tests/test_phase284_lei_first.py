"""Phase 284 — among parties still tied, one carrying an LEI first.

Phase 282 made related-party selection deterministic, breaking ties by the
number of sources and then by name. On Taqa Bratani (3 Oct 2026) all 93
related parties shared the bottom tier, and alphabetical order pushed
Taweelah Asia Power Company — the group member carrying the subject's
sanctions-controlled and export-control-linked findings, and one of its 8
LEI-bearing parties — past the limit of 25.
"""

from __future__ import annotations

import random
from typing import Any

from opencheck import related_targets
from opencheck.cross_check import _collect_targets

SUBJECT_LEI = "213800E11LI1SCETU492"


def _entity(sid: str, name: str, *, lei: str | None = None) -> dict[str, Any]:
    rd: dict[str, Any] = {"entityType": {"type": "registeredEntity"}, "name": name}
    if lei:
        rd["identifiers"] = [{"scheme": "XI-LEI", "id": lei}]
    return {"statementId": sid, "recordId": sid, "recordType": "entity", "recordDetails": rd}


def _person(sid: str, name: str) -> dict[str, Any]:
    return {"statementId": sid, "recordId": sid, "recordType": "person",
            "recordDetails": {"personType": "knownPerson",
                              "names": [{"type": "legal", "fullName": name}]}}


def _rel(sid: str, subject: str, ip: str, interest: str = "otherInfluenceOrControl",
         *, boc: bool = False) -> dict[str, Any]:
    return {"statementId": sid, "recordId": sid, "recordType": "relationship",
            "recordDetails": {"isComponent": False, "subject": subject, "interestedParty": ip,
                              "interests": [{"type": interest, "directOrIndirect": "indirect",
                                             "beneficialOwnershipOrControl": boc}]}}


#: Real, checksum-valid LEIs (ISO 17442) — the identifier parser rejects
#: made-up ones, so a fake LEI would silently not count as one.
_LEIS = ["549300D2K6PKKKXVNN73", "W9NG6WMZIYEU8VEDOG48", "FRDRIPF3EKNDJ2CQJL29",
         "5493005044RTLQ5RZU70"]


def _lei(n: int) -> str:
    return _LEIS[n % len(_LEIS)]


def _taqa_shape(n_plain: int = 40) -> list[dict[str, Any]]:
    """A subject whose related parties all tie in the bottom tier: many
    register entities with no LEI, and a few group members carrying one,
    named to sort last alphabetically."""
    bods: list[dict[str, Any]] = [_entity("s", "TAQA BRATANI LIMITED", lei=SUBJECT_LEI)]
    for i in range(n_plain):
        bods.append(_entity(f"ch{i}", f"BP SUBSIDIARY {i:03d} LIMITED"))
        bods.append(_rel(f"rc{i}", f"ch{i}", "s"))
    for i, name in enumerate(["Taweelah Asia Power Company - P S C", "TAQA Gas Storage B.V."]):
        bods.append(_entity(f"gl{i}", name, lei=_lei(i)))
        bods.append(_rel(f"rg{i}", f"gl{i}", "s"))
    return bods


def _screened(bods: list[dict[str, Any]], limit: int = 25) -> list[str]:
    sel = related_targets.select(_collect_targets(bods, exclude={"s"}), bods,
                                 limit=limit, subject_ids={"s"})
    return [r["name"] for r in sel.screened]


def test_lei_bearing_group_members_are_screened_before_the_rest_of_a_tie() -> None:
    names = _screened(_taqa_shape())
    assert "Taweelah Asia Power Company - P S C" in names
    assert "TAQA Gas Storage B.V." in names
    # They lead the tied block; name order resumes after them.
    assert names[:2] == ["TAQA Gas Storage B.V.", "Taweelah Asia Power Company - P S C"]


def test_without_an_lei_they_would_have_fallen_past_the_limit() -> None:
    # The Phase 282 failure, pinned: same bundle, LEIs removed.
    bods = _taqa_shape()
    for st in bods:
        if st["statementId"].startswith("gl"):
            st["recordDetails"].pop("identifiers", None)
    assert "Taweelah Asia Power Company - P S C" not in _screened(bods)


def test_still_deterministic_under_shuffling() -> None:
    base = _taqa_shape()
    want = _screened(base)
    rng = random.Random(284)
    for _ in range(20):
        shuffled = list(base)
        rng.shuffle(shuffled)
        assert _screened(shuffled) == want


def test_an_lei_never_outranks_a_higher_tier() -> None:
    # An owner without an LEI still comes before an LEI-bearing "other" party.
    bods = [
        _entity("s", "SUBJECT CO", lei=SUBJECT_LEI),
        _entity("gl", "AAA GROUP MEMBER", lei=_lei(2)), _rel("r1", "gl", "s"),
        _person("own", "Zed Owner"), _rel("r2", "s", "own", "shareholding", boc=True),
    ]
    assert _screened(bods) == ["Zed Owner", "AAA GROUP MEMBER"]


def test_an_lei_never_outranks_directness() -> None:
    # A party linked straight to the subject comes before an LEI-bearing
    # party reached through an intermediate.
    bods = [
        _entity("s", "SUBJECT CO", lei=SUBJECT_LEI),
        _entity("mid", "MMM INTERMEDIATE"), _rel("r0", "mid", "s"),
        _entity("gl", "AAA INDIRECT GROUP MEMBER", lei=_lei(3)), _rel("r1", "gl", "mid"),
        _entity("dir", "ZZZ DIRECT PARTY"), _rel("r2", "dir", "s"),
    ]
    names = _screened(bods)
    assert names.index("ZZZ DIRECT PARTY") < names.index("AAA INDIRECT GROUP MEMBER")
