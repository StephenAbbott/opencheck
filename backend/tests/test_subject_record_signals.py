"""Crime and regulatory topics on the subject's own OpenSanctions record.

Notion ticket "Enforcement, regulatory-action and financial-crime
OpenSanctions topics on the subject should raise a signal". Stephen's
decisions (20 Oct 2026):

* **exact LEI only** — the record's own ``leiCode`` must hold the looked-up
  LEI; no name match, because a banking group has many similarly named
  entities;
* ``reg.action`` (and ``reg.warn``) are **context**; a ``crime`` topic is risk;
* a **branch's record** carrying the parent's LEI is not the parent.

The records below are cut down from what OpenSanctions published on
19 Oct 2026 for the entities named (topics, captions, LEI codes and dataset
names as published; everything else dropped).
"""

from __future__ import annotations

from typing import Any

import pytest

from opencheck.config import get_settings
from opencheck.risk import (
    CRIME_RECORD,
    REGULATORY_ACTION,
    REGULATORY_WARNING,
    assess_hits,
    assess_subject_record,
    is_branch_record,
    record_carries_lei,
)
from opencheck.sources import SearchKind, SourceHit
from opencheck.verdict import build_verdict

BARCLAYS_LEI = "G5GSEF7VJP5I7OUK5573"
TD_LEI = "PT3QB789TSUIDF371261"
DANSKE_LEI = "MAES062Z21O4RZ2U7M96"

BARCLAYS = {
    "id": "NK-5gNbpjFVDL7QAeVGFynbbo",
    "schema": "Company",
    "caption": "Barclays Bank PLC",
    "topics": ["crime.fin", "reg.action", "fin.bank"],
    "datasets": ["us_fed_enforcements", "us_cftc_enforcement_actions", "us_sec_harm"],
    # OpenSanctions merged a second LEI into this record; the subject's is
    # still on it, and that is what the rule reads.
    "properties": {"leiCode": ["2549003Q5J2G7ALNU316", BARCLAYS_LEI]},
}
TD = {
    "id": "NK-VD5PLu7kFo9RFSMhZpzWq2",
    "schema": "Company",
    "caption": "Toronto-Dominion Bank",
    "topics": ["reg.action", "export.risk", "fin.bank", "poi", "corp.public"],
    "datasets": ["us_fed_enforcements", "us_ofac_enforcement", "us_cftc_enforcement_actions"],
    "properties": {"leiCode": [TD_LEI]},
}
DANSKE_FINLAND_BRANCH = {
    "id": "NK-TvkvRi4irBMHMK7447ouX5",
    "schema": "Company",
    "caption": "DANSKE BANK A/S, FINLAND BRANCH",
    "topics": ["reg.warn", "fin.bank", "fin", "corp.public"],
    "datasets": ["eu_esma_sanctions"],
    "properties": {"leiCode": [DANSKE_LEI]},
}
#: The laundromat record: unmerged, no LEI, filed under the regulator's
#: country. Exact-LEI matching does not read it, by decision.
DANSKE_SEC = {
    "id": "us-sec-harm-danske-bank-a-s",
    "schema": "LegalEntity",
    "caption": "Danske Bank A/S",
    "topics": ["crime.fin"],
    "datasets": ["us_sec_harm"],
    "properties": {"country": ["us"]},
}


def _hit(entity: dict[str, Any], *, is_stub: bool = False) -> SourceHit:
    return SourceHit(
        source_id="opensanctions",
        hit_id=entity["id"],
        kind=SearchKind.ENTITY,
        name=entity["caption"],
        summary="",
        identifiers={"opensanctions_id": entity["id"]},
        raw=entity,
        is_stub=is_stub,
    )


def _codes(signals) -> list[str]:
    return sorted(s.code for s in signals)


# ---------------------------------------------------------------------------
# The rule
# ---------------------------------------------------------------------------


def test_a_crime_topic_on_the_lei_record_is_a_risk_signal() -> None:
    out = assess_subject_record([_hit(BARCLAYS)], BARCLAYS_LEI, ["Barclays Bank PLC"])
    assert _codes(out) == [CRIME_RECORD, REGULATORY_ACTION]
    crime = next(s for s in out if s.code == CRIME_RECORD)
    assert crime.kind == "risk"
    assert crime.evidence["topics"] == ["crime.fin"]
    assert crime.evidence["matched_on"] == "lei"
    assert crime.evidence["datasets"] == sorted(BARCLAYS["datasets"])
    assert crime.evidence["statement_id"].startswith("opencheck-")
    assert "financial crime" in crime.summary
    # The tag is stated, never the outcome of a case.
    assert "does not say whether a case ended" in crime.summary
    assert "convicted" not in crime.summary.lower()


def test_regulatory_action_is_context_not_risk() -> None:
    out = assess_subject_record([_hit(TD)], TD_LEI, ["The Toronto-Dominion Bank"])
    assert _codes(out) == [REGULATORY_ACTION]
    (sig,) = out
    assert sig.kind == "context"
    assert "Context, not a risk finding" in sig.summary


def test_regulatory_warning_is_context_not_risk() -> None:
    entity = {**DANSKE_FINLAND_BRANCH, "caption": "Danske Bank A/S"}
    out = assess_subject_record([_hit(entity)], DANSKE_LEI, ["Danske Bank A/S"])
    assert _codes(out) == [REGULATORY_WARNING]
    assert out[0].kind == "context"


def test_a_branch_record_carrying_the_parent_lei_is_not_the_parent() -> None:
    """Danske Bank A/S on 19 Oct 2026: the only record under its LEI is the
    Finnish branch's ESMA warning."""
    assert record_carries_lei(DANSKE_FINLAND_BRANCH, DANSKE_LEI)
    assert is_branch_record(DANSKE_FINLAND_BRANCH, ["Danske Bank A/S"])
    out = assess_subject_record([_hit(DANSKE_FINLAND_BRANCH)], DANSKE_LEI, ["Danske Bank A/S"])
    assert out == []


def test_a_branch_looked_up_as_itself_reads_its_own_record() -> None:
    """GLEIF registers branches with LEIs of their own; the branch word only
    disqualifies a record when the subject's name lacks it."""
    assert not is_branch_record(
        DANSKE_FINLAND_BRANCH, ["DANSKE BANK A/S, FINLAND BRANCH"]
    )
    out = assess_subject_record(
        [_hit(DANSKE_FINLAND_BRANCH)], DANSKE_LEI, ["DANSKE BANK A/S, FINLAND BRANCH"]
    )
    assert _codes(out) == [REGULATORY_WARNING]


@pytest.mark.parametrize(
    "caption",
    [
        "Deutsche Bank AG New York Branch",
        "MUFG Bank, Ltd. Zweigniederlassung Düsseldorf",
        "BNP Paribas Succursale Italia",
        "Bank X Oddział w Polsce",
        "Nordea Bank Abp, filial i Sverige",
    ],
)
def test_branch_words_in_several_languages(caption: str) -> None:
    assert is_branch_record({"caption": caption}, ["Some Bank AG"])


def test_a_record_without_the_lei_is_never_read() -> None:
    """No name match: the SEC's Danske record names the bank exactly and is
    still not read, because it carries no LEI."""
    assert assess_subject_record([_hit(DANSKE_SEC)], DANSKE_LEI, ["Danske Bank A/S"]) == []
    other_lei = {**BARCLAYS, "properties": {"leiCode": ["2549003Q5J2G7ALNU316"]}}
    assert assess_subject_record([_hit(other_lei)], BARCLAYS_LEI, ["Barclays Bank PLC"]) == []


def test_lei_comparison_is_exact_but_case_and_space_tolerant() -> None:
    entity = {**TD, "properties": {"leiCode": [f" {TD_LEI.lower()} "]}}
    assert record_carries_lei(entity, TD_LEI)
    assert not record_carries_lei(entity, TD_LEI[:-1] + "0")
    assert not record_carries_lei({"properties": {"leiCode": TD_LEI[:19]}}, TD_LEI)
    assert not record_carries_lei(TD, "")


def test_no_lei_no_stub_no_other_source() -> None:
    assert assess_subject_record([_hit(BARCLAYS)], None, ["Barclays Bank PLC"]) == []
    assert assess_subject_record([_hit(BARCLAYS, is_stub=True)], BARCLAYS_LEI) == []
    other = _hit(BARCLAYS)
    other.source_id = "openaleph"
    assert assess_subject_record([other], BARCLAYS_LEI) == []


def test_the_hit_rules_and_related_party_screens_still_ignore_these_topics() -> None:
    """Search-time rules (also used by /search, where there is no subject LEI)
    raise nothing for crime or regulatory topics; only the LEI-gated rule does."""
    codes = {s.code for s in assess_hits([_hit(BARCLAYS), _hit(DANSKE_SEC), _hit(TD)])}
    assert not codes & {CRIME_RECORD, REGULATORY_ACTION, REGULATORY_WARNING}

    from opencheck import cross_check, openaleph_check

    for mod in (cross_check, openaleph_check):
        src = open(mod.__file__, encoding="utf-8").read()
        assert "_CRIME_TOPICS" not in src and "_REG_ACTION_TOPICS" not in src
        assert "assess_subject_record" not in src


# ---------------------------------------------------------------------------
# The verdict
# ---------------------------------------------------------------------------


def test_verdict_names_a_crime_record_and_ignores_the_context_codes() -> None:
    out = [s.to_dict() for s in assess_subject_record(
        [_hit(BARCLAYS)], BARCLAYS_LEI, ["Barclays Bank PLC"]
    )]
    verdict = build_verdict(out, [])
    assert verdict and "tagged with a crime topic" in verdict
    assert "regulatory" not in verdict.lower()

    context_only = [s.to_dict() for s in assess_subject_record(
        [_hit(TD)], TD_LEI, ["The Toronto-Dominion Bank"]
    )]
    td_verdict = build_verdict(context_only, []) or ""
    assert "crime" not in td_verdict and "regulatory" not in td_verdict.lower()


# ---------------------------------------------------------------------------
# The pipeline
# ---------------------------------------------------------------------------


def test_the_lookup_raises_the_signals_from_the_lei_search(monkeypatch, tmp_path) -> None:
    """End to end: the OpenSanctions LEI search returns the subject's record,
    a branch's record and an identifier-less record; only the first counts."""
    from fastapi.testclient import TestClient

    import opencheck.sources.climatetrace as _ct
    from opencheck.app import app
    from opencheck.sources import REGISTRY
    from tests.test_lookup_pipeline import _seed_bundle, _stream_events

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    monkeypatch.setattr(_ct, "_lei_index", {})
    monkeypatch.setattr(_ct, "_entity_index", {})
    get_settings.cache_clear()
    lei = "213800LH1BZH3DI6G760"
    _seed_bundle(tmp_path, lei)
    subject = {**BARCLAYS, "properties": {"leiCode": [lei]}, "caption": "BP P.L.C."}
    branch = {**DANSKE_FINLAND_BRANCH, "properties": {"leiCode": [lei]},
              "caption": "BP P.L.C., Norway Branch", "id": "NK-branch"}

    cls = type(REGISTRY["opensanctions"])
    seen: list[str] = []

    async def _search(self, query, kind):
        seen.append(query)
        return [_hit(subject), _hit(branch), _hit(DANSKE_SEC)]

    monkeypatch.setattr(cls, "search", _search)
    try:
        client = TestClient(app)
        with client.stream(
            "GET", "/lookup-stream", params={"lei": lei, "refresh": "true"}
        ) as r:
            events = _stream_events("".join(r.iter_text()))
        assert seen == [lei]
        risk = dict(events)["risk_signals"]
        mine = [s for s in risk["signals"] if s["code"] in
                {CRIME_RECORD, REGULATORY_ACTION, REGULATORY_WARNING}]
        assert sorted((s["code"], s["kind"], s["hit_id"]) for s in mine) == [
            (CRIME_RECORD, "risk", subject["id"]),
            (REGULATORY_ACTION, "context", subject["id"]),
        ]
        assert "tagged with a crime topic" in (risk.get("verdict") or "")

        sync = client.get("/lookup", params={"lei": lei, "refresh": "true"}).json()
        assert {s["code"] for s in sync["risk_signals"]} >= {CRIME_RECORD, REGULATORY_ACTION}
        assert REGULATORY_WARNING not in {s["code"] for s in sync["risk_signals"]}
    finally:
        get_settings.cache_clear()
