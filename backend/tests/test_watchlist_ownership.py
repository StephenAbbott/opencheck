"""``ownership_changed`` — the statement-level ownership diff on a watchlist
re-run (pKYC stage E, Route 2).

Pinned here: what the digest holds and leaves out (register-keyed people
only, no GLEIF bridge, no MEIP, no officer role — and never a name); that the
same Companies House PSC under two etags is one row (the item-id key the
prerequisite PR gave it); the diff's added / ended / changed / unchecked
rules and the Q4 escalate rule (a ≥25 % band added or ended); that a
baseline without a digest compares nothing; the words on both sides; the
counters.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

from opencheck import watchlist as wl
from opencheck import watchlist_materiality as materiality
from opencheck import watchlist_ownership as own
from opencheck.bods import map_companies_house
from opencheck.bods.statements import (
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
)
from opencheck.routers.watch import _describe, _entry_content, _entry_headline
from tests.test_watchlist import EASY, _resp, _watch, client, env  # noqa: F401, F811 — fixtures

REPO = Path(__file__).resolve().parents[2]
AS_OF = "2026-10-19"


# ---------------------------------------------------------------------------
# A small bundle
# ---------------------------------------------------------------------------


def _subject(*, lei: str | None = None) -> dict[str, Any]:
    identifiers = [{"id": "00102498", "scheme": "GB-COH"}]
    if lei:
        identifiers.append({"id": lei, "scheme": "XI-LEI"})
    return make_entity_statement(
        source_id="companies_house", local_id="00102498", name="DEMO HOLDINGS PLC",
        jurisdiction=("United Kingdom", "GB"), identifiers=identifiers,
    )


def _psc_person(item_id: str = "itemAAAAAAAAAAAAAAAAAAAAAAAA") -> dict[str, Any]:
    return make_person_statement(
        source_id="companies_house", local_id=f"00102498:psc:{item_id}", full_name="JONES, Alice",
        identifiers=[{"id": item_id, "scheme": "GB-COH-PSC", "schemeName": "Companies House PSC item id"}],
    )


def _keyless_person() -> dict[str, Any]:
    return make_person_statement(source_id="opencorporates", local_id="oc-officer-1", full_name="Keyless Kay")


def _rel(subject: dict[str, Any], party: dict[str, Any], *, source_id: str = "companies_house",
         interests: list[dict[str, Any]] | None = None, local_id: str = "r1", closed: bool = False) -> dict[str, Any]:
    return make_relationship_statement(
        source_id=source_id, local_id=local_id, subject_statement_id=subject["statementId"],
        interested_party_statement_id=party["statementId"],
        interested_party_type="person" if party["recordType"] == "person" else "entity",
        interests=interests or [_share(25, 50)], record_status="closed" if closed else "new",
    )


def _share(lo: float | None, hi: float | None, *, kind: str = "shareholding", bo: bool = True, **extra: Any) -> dict[str, Any]:
    share: dict[str, Any] = {}
    if lo is not None:
        share["minimum"] = lo
    if hi is not None:
        share["maximum"] = hi
    share.update(extra)
    out: dict[str, Any] = {"type": kind, "directOrIndirect": "direct", "beneficialOwnershipOrControl": bo}
    if share:
        out["share"] = share
    return out


def _digest(bods: list[dict[str, Any]], subject: dict[str, Any]) -> dict[str, Any]:
    return own.digest(bods, [subject["statementId"]], AS_OF)


# ---------------------------------------------------------------------------
# The digest
# ---------------------------------------------------------------------------


def test_the_digest_keys_a_person_on_a_register_key_and_counts_the_rest() -> None:
    subject, psc, kay = _subject(), _psc_person(), _keyless_person()
    bods = [subject, psc, kay, _rel(subject, psc), _rel(subject, kay, source_id="opencorporates", local_id="r2")]
    dg = _digest(bods, subject)
    assert dg["version"] == own.DIGEST_VERSION and dg["as_of"] == AS_OF
    assert list(dg["rows"]) == ["companies_house|GB-COH-PSC:itemAAAAAAAAAAAAAAAAAAAAAAAA"]
    row = dg["rows"]["companies_house|GB-COH-PSC:itemAAAAAAAAAAAAAAAAAAAAAAAA"]
    assert row == {"source_id": "companies_house", "party": "person", "interests": ["shareholding"],
                   "bands": [[25.0, 50.0, False, False]], "bo": True, "direct": "direct", "ended": False}
    assert dg["uncompared"] == {"opencorporates": 1}
    text = json.dumps(dg).lower()
    assert "jones" not in text and "alice" not in text and "kay" not in text


def test_bridges_officers_and_group_memberships_are_not_rows() -> None:
    subject = _subject()
    officer = make_person_statement(
        source_id="companies_house", local_id="officer:abc123", full_name="Director Dan",
        identifiers=[{"id": "abc123", "scheme": "GB-COH-OFFICER"}],
    )
    role = _rel(subject, officer, local_id="role", interests=[{"type": "seniorManagingOfficial", "directOrIndirect": "direct"}])
    secretary = _rel(subject, officer, local_id="sec", interests=[{"type": "otherInfluenceOrControl", "directOrIndirect": "direct",
                                                                     "opencheckOfficerPosition": "secretary"}])
    bridge = make_relationship_statement(
        source_id="gleif", local_id="bridge", subject_statement_id=subject["statementId"],
        interested_party_unspecified={"reason": "noBeneficialOwners", "description": "No parent reported"},
        interests=[{"type": "unknownInterest", "directOrIndirect": "unknown"}],
    )
    parent = make_entity_statement(source_id="meip", local_id="group-head", name="GROUP HEAD")
    meip = _rel(subject, parent, source_id="meip", local_id="meip-edge", interests=[{"type": "unknownInterest", "directOrIndirect": "unknown"}])
    dg = _digest([subject, officer, role, secretary, bridge, parent, meip], subject)
    assert dg["rows"] == {} and dg["uncompared"] == {}


def test_an_entity_owner_is_keyed_on_its_record_and_a_pooled_pair_is_one_row() -> None:
    subject = _subject()
    parent = make_entity_statement(source_id="companies_house", local_id="SC012345", name="NORTHERN PARENT LTD",
                                   identifiers=[{"id": "SC012345", "scheme": "GB-COH"}])
    ended = _rel(subject, parent, local_id="old", interests=[_share(75, 100, endDate="2024-01-01")], closed=True)
    current = _rel(subject, parent, local_id="new", interests=[_share(50, 75)])
    dg = _digest([subject, parent, ended, current], subject)
    (row,) = dg["rows"].values()
    assert row["party"] == "entity" and row["ended"] is False and row["bands"] == [[50.0, 75.0, False, False]]


def test_the_same_psc_under_two_etags_is_one_row_that_changed() -> None:
    """The prerequisite PR's key at work: an edited Companies House PSC
    filing is one party whose band moved, not one ended and one added."""

    def bundle(etag: str, band: str) -> dict[str, Any]:
        return {
            "source_id": "companies_house", "company_number": "00102498",
            "profile": {"company_name": "DEMO HOLDINGS PLC"}, "officers": {"items": []}, "related_companies": {},
            "pscs": {"items": [{
                "name": "JONES, Alice", "kind": "individual-person-with-significant-control",
                "natures_of_control": [f"ownership-of-shares-{band}-percent"], "notified_on": "2016-04-06",
                "date_of_birth": {"year": 1980, "month": 7}, "etag": etag,
                "links": {"self": "/company/00102498/persons-with-significant-control/individual/itemAAAAAAAAAAAAAAAAAAAAAAAA"},
            }]},
        }

    def dg(etag: str, band: str) -> dict[str, Any]:
        bods = list(map_companies_house(bundle(etag, band)).statements)
        subject = next(s for s in bods if s["recordType"] == "entity")
        return own.digest(bods, [subject["statementId"]], AS_OF)

    changes = own.diff(dg("e1", "25-to-50"), dg("e2", "50-to-75"), [])
    assert [(c["kind"], c["change"], c["crossed_25"]) for c in changes] == [("ownership_changed", "changed", False)]
    # The register's "25-to-50" is strictly more than 25 % (exclusiveMinimum 25, BODS v0.4).
    assert changes[0]["bands_before"] == [[25.0, 50.0, True, False]] and changes[0]["bands_after"] == [[50.0, 75.0, True, False]]
    assert own.change_sentence(changes[0], "UK Companies House") == (
        "A registered beneficial owner's shareholding changed from over 25% to 50% to over 50% to 75% (UK Companies House)."
    )


# ---------------------------------------------------------------------------
# The diff and the Q4 rule
# ---------------------------------------------------------------------------


def _rows(**rows: dict[str, Any]) -> dict[str, Any]:
    full = {
        k: {"source_id": k.split("|")[0], "party": "person", "interests": ["shareholding"], "bands": [],
            "bo": False, "direct": "direct", "ended": False, **v}
        for k, v in rows.items()
    }
    return {"version": own.DIGEST_VERSION, "as_of": AS_OF, "rows": full, "uncompared": {}}


def test_added_and_ended_bands_at_or_above_25_escalate_moves_between_bands_above_do_not() -> None:
    before = _rows(**{"companies_house|A": {"bands": [[25.0, 50.0, False, False]]},
                      "companies_house|B": {"bands": [[50.0, 75.0, False, False]]},
                      "companies_house|C": {"bands": [[0.0, 25.0, False, True]]}})
    after = _rows(**{"companies_house|B": {"bands": [[75.0, 100.0, False, False]]},
                     "companies_house|C": {"bands": [[25.0, 50.0, False, False]]},
                     "companies_house|D": {"bands": [[25.0, 50.0, False, False]], "bo": True},
                     "companies_house|E": {"bands": [[0.0, 25.0, False, True]]}})
    changes = {c["key"]: c for c in own.diff(before, after, [])}
    assert changes["A"]["change"] == "ended" and changes["A"]["crossed_25"] is True
    assert changes["B"]["change"] == "changed" and changes["B"]["crossed_25"] is False  # 50→75: degree, not registrability
    assert changes["C"]["change"] == "changed" and changes["C"]["crossed_25"] is True  # under 25 → 25–50
    assert changes["D"]["change"] == "added" and changes["D"]["crossed_25"] is True and changes["D"]["bo"] is True
    assert changes["E"]["change"] == "added" and changes["E"]["crossed_25"] is False
    assert materiality.tier_for_change(changes["A"]) == "escalate"
    assert materiality.tier_for_change(changes["B"]) == "recheck"
    assert materiality.tier_for_change(changes["E"]) == "recheck"
    # An exclusive 25 % floor is "more than 25 %" — still a registrable controller.
    assert own.band_at_or_above_threshold([25.0, 50.0, True, False]) is True
    assert own.band_at_or_above_threshold([None, 50.0, False, False]) is False
    assert own.band_at_or_above_threshold(None) is False


def test_a_degraded_source_gives_unchecked_never_ended() -> None:
    before = _rows(**{"opensanctions|X": {"bands": [[50.0, 75.0, False, False]]},
                      "companies_house|Y": {}})
    after = _rows(**{"companies_house|Y": {"ended": True}})
    changes = own.diff(before, after, ["opensanctions", "companies_house"])
    assert changes == [
        {"kind": "ownership_unchecked", "source_id": "companies_house", "parties": 1},
        {"kind": "ownership_unchecked", "source_id": "opensanctions", "parties": 1},
    ]
    assert materiality.tier_for_change(changes[0]) == "note"


def test_history_that_appears_already_ended_is_not_news_and_a_revived_pair_is_added() -> None:
    before = _rows(**{"companies_house|Z": {"ended": True}})
    after = _rows(**{"companies_house|Z": {"bands": [[25.0, 50.0, False, False]]},
                     "ariregister|H": {"ended": True}})
    changes = own.diff(before, after, [])
    assert [(c["key"], c["change"], c["crossed_25"]) for c in changes] == [("Z", "added", True)]


def test_no_baseline_or_another_version_compares_nothing() -> None:
    after = _rows(**{"companies_house|A": {}})
    assert own.diff(None, after, []) == []
    assert own.diff({"version": 0, "rows": {}}, after, []) == []


def test_the_snapshot_carries_the_digest_and_an_old_baseline_compares_nothing() -> None:
    subject, psc = _subject(lei=EASY), _psc_person()
    resp = _resp(EASY, bods=[subject, psc, _rel(subject, psc)], run_completed_at="2026-10-19T09:00:00Z")
    snap = wl.snapshot_from_response(resp)
    dg = snap["ownership_digest"]
    assert dg["as_of"] == "2026-10-19" and len(dg["rows"]) == 1
    # A baseline from before the digest: nothing compared, no change written.
    old = {k: v for k, v in snap.items() if k != "ownership_digest"}
    assert [c["kind"] for c in wl.diff_snapshots(old, snap)] == []
    # The same company a run later with the band moved: one change.
    moved = _resp(EASY, bods=[subject, psc, _rel(subject, psc, interests=[_share(75, 100)])], run_completed_at="2026-10-20T09:00:00Z")
    changes = wl.diff_snapshots(snap, wl.snapshot_from_response(moved))
    assert [(c["kind"], c["change"], c["crossed_25"]) for c in changes] == [("ownership_changed", "changed", False)]


# ---------------------------------------------------------------------------
# Words
# ---------------------------------------------------------------------------


def test_the_feed_words_each_change_without_a_name() -> None:
    label = "UK Companies House"
    assert own.change_sentence({"kind": "ownership_changed", "change": "ended", "party": "person", "interests": ["shareholding"],
                                "bands_before": [[25.0, 50.0, False, False]], "bands_after": None, "bo": False}, label) == (
        "A person's shareholding of 25–50% ended (UK Companies House)."
    )
    assert own.change_sentence({"kind": "ownership_changed", "change": "added", "party": "entity", "interests": ["shareholding", "votingRights"],
                                "bands_before": None, "bands_after": [[75.0, 100.0, False, False]], "bo": False}, label) == (
        "A company's shareholding and voting rights of 75–100% added (UK Companies House)."
    )
    assert own.change_sentence({"kind": "ownership_changed", "change": "changed", "party": "person", "interests": ["shareholding"],
                                "bands_before": [[25.0, 50.0, False, False]], "bands_after": [[50.0, 75.0, False, False]], "bo": True},
                               "e-Business Register") == (
        "A registered beneficial owner's shareholding changed from 25–50% to 50–75% (e-Business Register)."
    )
    assert own.change_sentence({"kind": "ownership_changed", "change": "added", "party": "entity", "interests": ["appointmentOfBoard"],
                                "bands_before": None, "bands_after": [], "bo": False}, label) == (
        "A company's right to appoint the board added (UK Companies House)."
    )
    assert own.change_sentence({"kind": "ownership_unchecked", "source_id": "opensanctions", "parties": 2}, "OpenSanctions") == (
        "2 parties' holdings could not be re-checked — OpenSanctions did not answer."
    )
    assert own.band_words([33.333, 33.333, False, False]) == "exactly 33.333%"
    assert own.band_words([50.0, None, True, False]) == "more than 50%"
    assert own.band_words([None, 25.0, False, True]) == "under 25%"
    assert own.uncompared_sentence([{"kind": "ownership_changed", "source_id": "opencorporates", "uncompared": 3}], lambda s: s) == [
        "3 parties could not be compared (opencorporates publishes no person key)."
    ]


def test_the_entry_reads_the_change_the_uncompared_line_and_the_headline() -> None:
    change = {"kind": "ownership_changed", "change": "added", "party": "person", "interests": ["shareholding"],
              "bands_before": None, "bands_after": [[25.0, 50.0, False, False]], "bo": True, "crossed_25": True,
              "source_id": "companies_house", "uncompared": 1, "materiality": "escalate"}
    entry = {"id": 1, "lei": EASY, "legal_name": "EASY POWER", "tier": "gleif", "trigger": {"publish": "2026-10-19"},
             "changes": [change], "checked": [], "degraded": [], "materiality": "escalate", "materiality_rules": 1}
    assert _describe(change) == "A registered beneficial owner's shareholding of 25–50% added (UK Companies House)."
    content = _entry_content(entry)
    assert " - A registered beneficial owner's shareholding of 25–50% added (UK Companies House). [escalate]" in content
    assert " - 1 party could not be compared (UK Companies House publishes no person key)." in content
    assert _entry_headline(entry) == "EASY POWER: ownership changed"


def test_every_kind_the_backend_emits_is_in_change_kinds() -> None:
    assert "ownership_changed" in wl.CHANGE_KINDS and "ownership_unchecked" in wl.CHANGE_KINDS
    assert materiality.KIND_TIER["ownership_changed"] == "recheck"
    assert materiality.KIND_TIER["ownership_unchecked"] == "note"


def test_the_page_carries_the_same_words_and_kinds() -> None:
    text = (REPO / "frontend/src/lib/watchlist.ts").read_text()

    def table(name: str) -> dict[str, str]:
        body = text[text.index(f"const {name}") :]
        body = body[body.index("{") + 1 : body.index("};")]
        return dict(re.findall(r'(\w+):\s*"([^"]*)"', body))

    assert table("INTEREST_WORDS") == own.INTEREST_WORDS
    assert table("PARTY_WORDS") == own.PARTY_WORDS
    assert table("CHANGE_WORDS") == own.CHANGE_WORDS
    kinds_test = (REPO / "frontend/src/lib/watchlist.test.ts").read_text()
    block = kinds_test[kinds_test.index("const KINDS = [") :]
    block = block[: block.index("];")]
    assert set(re.findall(r'"(\w+)"', block)) == set(wl.CHANGE_KINDS)


def test_watchstats_counts_the_diff(client: TestClient) -> None:  # noqa: F811
    _watch(client, EASY)
    r = client.get("/watchstats")
    body = r.json()["watched"]
    assert body["ownership_changes_by_change"] == {"added": 0, "ended": 0, "changed": 0}
    assert body["baselines_with_ownership_digest"] == 1
    assert "ownership_uncompared_by_source" in body and "ownership_changes_escalated" in body


def test_docs_name_the_rule() -> None:
    doc = (REPO / "docs/watchlist.md").read_text()
    assert "`ownership_changed` | recheck" in doc and "`ownership_unchecked` | note" in doc
    assert "at or above 25" in doc
