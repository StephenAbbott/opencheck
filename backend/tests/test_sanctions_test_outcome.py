"""The sanctions 50% test always says what it found (MCP testing, 7 Nov 2026,
issue 7).

Rosneft's ``opencheck_owners`` answered ``sanctions_ownership_test: null``.
The test had run: the one designated shareholder in any source, National
Settlement Depository, holds 10.7% in GEM's data and is a name-only match, so
it is not counted — and Rosneft is itself designated. Neither was said. The
rules are unchanged; only the no-signal outcome is now reported.
"""

from __future__ import annotations

import asyncio
from typing import Any

from opencheck import owners as O
from opencheck import sanctions_ownership as so

AS_OF = "2026-11-07"
ALL3 = ["us_ofac_sdn", "eu_fsf", "gb_hmt_sanctions"]


def ent(sid: str, name: str) -> dict[str, Any]:
    return {"statementId": sid, "recordType": "entity", "recordDetails": {"name": name}}


def own(sid: str, owner: str, share: float, source: str = "climatetrace") -> dict[str, Any]:
    return {"statementId": sid, "recordType": "relationship",
            "source": {"opencheckSourceId": source, "description": "Global Energy Monitor / Climate TRACE"},
            "recordDetails": {"subject": "T", "interestedParty": owner,
                              "interests": [{"type": "shareholding", "share": {"exact": share}}]}}


def related(sid: str, *, ident: bool) -> dict[str, Any]:
    ev: dict[str, Any] = {"subject_statement_id": sid, "search_name": sid, "corroboration": [],
                          "datasets": ALL3}
    if ident:
        ev["identifier_match"] = True
    return {"code": "RELATED_SANCTIONED", "source_id": "opensanctions", "evidence": ev, "kind": "risk"}


SUBJECT_LISTED = {"code": "SANCTIONED", "source_id": "opensanctions", "kind": "risk",
                  "evidence": {"datasets": ["eu_journal_sanctions", "us_ofac_sdn", "gb_fcdo_sanctions"]}}


def _rosneft() -> list[dict[str, Any]]:
    return [ent("T", "Rosneft"), ent("NSD", "National Settlement Depository JSC"), own("r", "NSD", 10.7)]


def test_a_name_only_holder_below_the_test_is_not_met_and_listed():
    out = so.outcome(_rosneft(), ["T"], [related("NSD", ident=False), SUBJECT_LISTED], AS_OF)
    assert out["status"] == so.STATUS_NOT_MET and out["finding"] is None
    assert out["not_counted"] == [{
        "name": "National Settlement Depository JSC", "stake": "10.7%",
        "source": "Global Energy Monitor / Climate TRACE", "regimes": ["OFAC", "EU", "UK"],
        "basis": "name_only",
    }]
    assert out["summary"].startswith("OFAC (US), EU and UK: not met.")
    assert "Not counted, because the match is on the name only: National Settlement Depository JSC, 10.7%" in out["summary"]
    assert out["subject_designated"] == ["OFAC", "EU", "UK"]
    assert "The company is itself designated under OFAC (US), EU and UK" in out["summary"]
    assert [r["result"] for r in out["regimes"]] == ["not_met"] * 3


def test_a_counted_holder_below_the_test_gives_its_figure():
    out = so.outcome(_rosneft(), ["T"], [related("NSD", ident=True)], AS_OF)
    assert out["status"] == so.STATUS_NOT_MET
    assert out["not_counted"] == []
    assert "10.7% held by designated parties" in out["summary"]
    assert out["subject_designated"] == []
    assert "itself designated" not in out["summary"]


def test_no_designated_holder_says_so():
    out = so.outcome(_rosneft(), ["T"], [], AS_OF)
    assert out["status"] == so.STATUS_NO_HOLDER
    assert out["summary"].startswith("No party designated under OFAC (US), the EU or the UK holds shares")
    assert out["regimes"] == []


def test_a_listed_company_with_no_designated_holder_says_both():
    out = so.outcome(_rosneft(), ["T"], [SUBJECT_LISTED], AS_OF)
    assert out["status"] == so.STATUS_NO_HOLDER
    assert out["subject_designated"] == ["OFAC", "EU", "UK"]


def test_the_owners_answer_is_never_a_bare_null():
    bods = _rosneft()
    rule = O.resolve_rule("RU")

    async def no_hop(lei, anchor):
        raise AssertionError

    net = asyncio.run(O.grow(bods, ["T"], None, rule, 1, no_hop, AS_OF, None))
    ans = O.owners_answer(lei="L", legal_name="Rosneft", jurisdiction="RU", net=net, rule=rule,
                          depth=1, as_of=AS_OF, risk_signals=[related("NSD", ident=False), SUBJECT_LISTED])
    block = ans["sanctions_ownership_test"]
    assert block is not None and block["status"] == so.STATUS_NOT_MET


def test_a_met_test_carries_its_status_and_the_subjects_own_listing():
    bods = [ent("T", "Target"), ent("P", "Designated P"), own("p", "P", 60.0, "opensanctions")]
    sig = so.build_signal([related("P", ident=True)], bods, ["T"], subject="L", as_of=AS_OF)
    block = O.sanctions_ownership_block([sig, SUBJECT_LISTED])
    assert block["status"] == so.STATUS_MEETS and block["finding"] == so.SANCTIONS_OWNERSHIP_TEST
    assert block["subject_designated"] == ["OFAC", "EU", "UK"]
