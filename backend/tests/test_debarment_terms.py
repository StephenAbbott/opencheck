"""The debarment signal says who barred the party, why and until when.

Fixture: OpenSanctions' record of a company on Moldova's procurement ban list
(``md_interdictie``, read 13 Nov 2026), in the nested shape ``/entities/{id}``
serves — the company with its ``Sanction`` under ``properties.sanctions``.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from opencheck import debarment
from opencheck.risk import assess_bundle

FIXTURE = Path(__file__).parent / "fixtures" / "opensanctions" / "md_interdictie_viz_stram.json"
TODAY = "2026-11-13"


@pytest.fixture
def record() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def test_terms_are_read_from_the_nested_sanction(record):
    (term,) = debarment.debarment_terms(record, today=TODAY)
    assert term == {
        "datasets": ["md_interdictie"],
        "authority": "Ministry of Finance of the Republic of Moldova",
        "decision": "1/26",
        "reason": "prezentarea documentelor false în cadrul procedurilor de achiziții publice",
        "start_date": "2026-10-02",
        "end_date": "2029-10-02",
        "ended": False,
        "source_url": "https://tender.gov.md/ro/lista-de-interdictie",
    }


def test_the_sentence_names_authority_decision_dates_and_quotes_the_reason(record):
    terms = debarment.debarment_terms(record, today=TODAY)
    assert debarment.terms_summary(terms) == (
        "Barred by Ministry of Finance of the Republic of Moldova (decision 1/26) "
        "from 2 October 2026 until 2 October 2029; reason given: "
        '"prezentarea documentelor false în cadrul procedurilor de achiziții publice".'
    )


def test_an_ended_ban_is_worded_as_ended(record):
    (term,) = debarment.debarment_terms(record, today="2030-01-01")
    assert term["ended"] is True
    assert debarment.term_sentence(term).startswith(
        "Was barred by Ministry of Finance of the Republic of Moldova (decision 1/26) "
        "from 2 October 2026 until 2 October 2029; the ban has ended;"
    )


def test_a_search_card_has_no_terms(record):
    card = copy.deepcopy(record)
    del card["properties"]["sanctions"]
    assert debarment.debarment_terms(card) == []


def test_on_a_sanctions_list_only_debarment_datasets_count(record):
    rec = copy.deepcopy(record)
    rec["properties"]["topics"] = ["debarment", "sanction"]
    ofac = copy.deepcopy(rec["properties"]["sanctions"][0])
    ofac["datasets"] = ["us_ofac_sdn"]
    ofac["properties"]["authority"] = ["Office of Foreign Assets Control"]
    rec["properties"]["sanctions"].append(ofac)
    terms = debarment.debarment_terms(rec, today=TODAY)
    assert [t["datasets"] for t in terms] == [["md_interdictie"]]


def test_several_bans_spell_out_the_latest_and_count_the_rest(record):
    rec = copy.deepcopy(record)
    older = copy.deepcopy(rec["properties"]["sanctions"][0])
    older["properties"]["startDate"] = ["2019-01-24"]
    older["properties"]["endDate"] = ["2022-01-23"]
    rec["properties"]["sanctions"].insert(0, older)
    terms = debarment.debarment_terms(rec, today=TODAY)
    assert [t["start_date"] for t in terms] == ["2026-10-02", "2019-01-24"]
    assert debarment.terms_summary(terms).endswith("1 earlier ban on record.")


def test_the_subject_signal_from_deepen_carries_the_terms(record):
    raw = {"entity_id": record["id"], "entity": record}
    (sig,) = [s for s in assess_bundle("opensanctions", raw) if s.code == "DEBARMENT"]
    assert sig.confidence == "high"
    assert sig.summary.startswith(
        "OpenSanctions lists this record as debarred from public contracts. "
        "Barred by Ministry of Finance of the Republic of Moldova (decision 1/26)"
    )
    assert sig.evidence["debarment_terms"][0]["end_date"] == "2029-10-02"
    assert "debarment_ended" not in sig.evidence


def test_a_record_with_no_nested_sanction_keeps_the_old_sentence(record):
    card = copy.deepcopy(record)
    del card["properties"]["sanctions"]
    raw = {"entity_id": card["id"], "entity": card}
    (sig,) = [s for s in assess_bundle("opensanctions", raw) if s.code == "DEBARMENT"]
    assert sig.summary == "OpenSanctions lists this record as debarred from public contracts."
    assert "debarment_terms" not in sig.evidence


class _Adapter:
    def __init__(self, records: dict[str, dict], fail: bool = False):
        self.records = records
        self.fail = fail
        self.calls: list[str] = []

    async def fetch(self, hit_id: str) -> dict:
        self.calls.append(hit_id)
        if self.fail:
            raise RuntimeError("upstream down")
        return {"source_id": "opensanctions", "entity_id": hit_id, "entity": self.records[hit_id]}


def _related(hit_id: str, code: str = "RELATED_DEBARMENT") -> dict:
    return {
        "code": code,
        "confidence": "medium",
        "summary": "Related entity 'Viz-Stram SRL' matches a record on OpenSanctions: debarred from public contracts.",
        "source_id": "opensanctions",
        "hit_id": hit_id,
        "evidence": {"subject_statement_id": "s1", "topics": ["debarment"]},
    }


async def test_related_matches_are_read_once_and_described(record, monkeypatch):
    from opencheck.sources import REGISTRY

    adapter = _Adapter({record["id"]: record})
    monkeypatch.setitem(REGISTRY, "opensanctions", adapter)
    pep = {"code": "RELATED_PEP", "source_id": "opensanctions", "hit_id": "x", "evidence": {}}
    signals = [_related(record["id"]), _related(record["id"]), pep]
    out = await debarment.describe_debarment_signals(signals, today=TODAY)
    assert adapter.calls == [record["id"]]
    assert out[2] is pep
    for sig in out[:2]:
        assert sig["confidence"] == "medium"
        assert sig["summary"].endswith('procedurilor de achiziții publice".')
        assert sig["evidence"]["debarment_terms"][0]["decision"] == "1/26"
    # The input list is not modified.
    assert "debarment_terms" not in signals[0]["evidence"]


async def test_the_register_anchored_subject_screen_is_described_too(record, monkeypatch):
    from opencheck.sources import REGISTRY

    monkeypatch.setitem(REGISTRY, "opensanctions", _Adapter({record["id"]: record}))
    (sig,) = await debarment.describe_debarment_signals([_related(record["id"], "DEBARMENT")], today=TODAY)
    assert sig["evidence"]["debarment_terms"]


async def test_a_record_that_cannot_be_read_leaves_the_signal_as_it_was(record, monkeypatch):
    from opencheck.sources import REGISTRY

    monkeypatch.setitem(REGISTRY, "opensanctions", _Adapter({}, fail=True))
    sig = _related(record["id"])
    assert await debarment.describe_debarment_signals([sig], today=TODAY) == [sig]


async def test_signals_that_already_carry_terms_are_not_read_again(record, monkeypatch):
    from opencheck.sources import REGISTRY

    adapter = _Adapter({record["id"]: record})
    monkeypatch.setitem(REGISTRY, "opensanctions", adapter)
    sig = _related(record["id"])
    sig["evidence"]["debarment_terms"] = [{"ended": False}]
    await debarment.describe_debarment_signals([sig], today=TODAY)
    assert adapter.calls == []


def test_the_debarment_dataset_list_includes_moldova():
    assert "md_interdictie" in debarment.DEBARMENT_DATASETS
    assert len(debarment.DEBARMENT_DATASETS) == 61
