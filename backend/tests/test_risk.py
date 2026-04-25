"""Tests for the deterministic risk-signal rules."""

from __future__ import annotations

from opencheck.risk import (
    OFFSHORE_LEAKS,
    OPAQUE_OWNERSHIP,
    PEP,
    SANCTIONED,
    assess_bundle,
    assess_hit,
    assess_hits,
)
from opencheck.sources import SearchKind, SourceHit


def _hit(source_id: str, hit_id: str, *, kind=SearchKind.ENTITY, is_stub=False, **raw) -> SourceHit:
    return SourceHit(
        source_id=source_id,
        hit_id=hit_id,
        kind=kind,
        name=f"{source_id} {hit_id}",
        summary="",
        identifiers={},
        raw=raw,
        is_stub=is_stub,
    )


# ---------------------------------------------------------------------
# Search-time signals (assess_hit / assess_hits)
# ---------------------------------------------------------------------


def test_pep_signal_from_opensanctions_topic() -> None:
    hit = _hit(
        "opensanctions",
        "NK-putin",
        kind=SearchKind.PERSON,
        topics=["role.pep", "role.head-of-state"],
    )
    signals = assess_hit(hit)
    assert len(signals) == 1
    assert signals[0].code == PEP
    assert signals[0].confidence == "high"
    assert signals[0].evidence["topics"] == ["role.pep"]


def test_sanctioned_signal_from_opensanctions_topic() -> None:
    hit = _hit(
        "opensanctions",
        "NK-bp",
        topics=["sanction"],
    )
    signals = assess_hit(hit)
    assert len(signals) == 1
    assert signals[0].code == SANCTIONED
    assert signals[0].confidence == "high"


def test_pep_and_sanctioned_can_co_occur() -> None:
    hit = _hit(
        "opensanctions",
        "NK-double",
        kind=SearchKind.PERSON,
        topics=["role.pep", "sanction.linked"],
    )
    codes = {s.code for s in assess_hit(hit)}
    assert codes == {PEP, SANCTIONED}


def test_topics_can_live_under_properties() -> None:
    """OpenSanctions sometimes nests topics inside ``properties``."""
    hit = _hit(
        "opensanctions",
        "NK-nested",
        properties={"topics": ["sanction"]},
    )
    signals = assess_hit(hit)
    assert [s.code for s in signals] == [SANCTIONED]


def test_everypolitician_hit_is_pep_by_construction() -> None:
    hit = _hit(
        "everypolitician",
        "Q7747-pep",
        kind=SearchKind.PERSON,
    )
    signals = assess_hit(hit)
    assert len(signals) == 1
    assert signals[0].code == PEP
    assert signals[0].evidence == {"dataset": "peps"}


def test_everypolitician_entity_kind_is_not_signalled() -> None:
    """Entity searches against EveryPolitician shouldn't fire PEP."""
    hit = _hit(
        "everypolitician",
        "X",
        kind=SearchKind.ENTITY,
    )
    assert assess_hit(hit) == []


def test_stub_hits_never_signal() -> None:
    hit = _hit(
        "opensanctions",
        "NK-stub-0001",
        kind=SearchKind.PERSON,
        is_stub=True,
        topics=["role.pep", "sanction"],
    )
    assert assess_hit(hit) == []


def test_assess_hits_dedupes_within_source() -> None:
    """If two records on the same hit fire the same code, dedupe."""
    a = _hit("opensanctions", "NK-1", topics=["role.pep"])
    b = _hit("opensanctions", "NK-1", topics=["role.pep"])  # dup
    c = _hit("opensanctions", "NK-2", topics=["role.pep"])
    signals = assess_hits([a, b, c])
    assert len(signals) == 2
    assert {s.hit_id for s in signals} == {"NK-1", "NK-2"}


# ---------------------------------------------------------------------
# Deepen-time signals (assess_bundle)
# ---------------------------------------------------------------------


def test_assess_bundle_opensanctions_pep() -> None:
    raw = {
        "source_id": "opensanctions",
        "entity_id": "NK-putin",
        "entity": {
            "id": "NK-putin",
            "topics": ["role.pep"],
            "schema": "Person",
        },
    }
    signals = assess_bundle("opensanctions", raw)
    assert [s.code for s in signals] == [PEP]
    assert signals[0].source_id == "opensanctions"
    assert signals[0].hit_id == "NK-putin"


def test_assess_bundle_everypolitician_always_pep() -> None:
    raw = {
        "source_id": "everypolitician",
        "entity_id": "Q7747-pep",
        "entity": {"id": "Q7747-pep", "schema": "Person", "topics": []},
    }
    signals = assess_bundle("everypolitician", raw)
    assert [s.code for s in signals] == [PEP]


def test_assess_bundle_everypolitician_with_sanction() -> None:
    raw = {
        "source_id": "everypolitician",
        "entity_id": "Q7747-pep",
        "entity": {
            "id": "Q7747-pep",
            "schema": "Person",
            "topics": ["sanction"],
        },
    }
    codes = {s.code for s in assess_bundle("everypolitician", raw)}
    assert codes == {PEP, SANCTIONED}


def test_offshore_leaks_signal_from_panama_papers_collection() -> None:
    raw = {
        "source_id": "openaleph",
        "entity_id": "aleph-123",
        "entity": {"id": "aleph-123", "schema": "Company"},
        "collection": {
            "foreign_id": "panama_papers",
            "label": "Panama Papers",
        },
    }
    signals = assess_bundle("openaleph", raw)
    assert [s.code for s in signals] == [OFFSHORE_LEAKS]
    assert signals[0].confidence == "medium"
    assert "panama" in signals[0].evidence["match"]["foreign_id"]


def test_offshore_leaks_signal_from_label_when_foreign_id_missing() -> None:
    raw = {
        "source_id": "openaleph",
        "entity_id": "aleph-456",
        "entity": {"id": "aleph-456"},
        "collection": {"label": "ICIJ Offshore Leaks"},
    }
    signals = assess_bundle("openaleph", raw)
    assert [s.code for s in signals] == [OFFSHORE_LEAKS]
    assert signals[0].evidence["match"]["label"] == "icij offshore leaks"


def test_no_offshore_leaks_signal_for_unrelated_collection() -> None:
    raw = {
        "source_id": "openaleph",
        "entity_id": "aleph-789",
        "entity": {"id": "aleph-789"},
        "collection": {"foreign_id": "us_companies", "label": "US Companies"},
    }
    assert assess_bundle("openaleph", raw) == []


def test_wikidata_pep_when_position_is_currently_held() -> None:
    raw = {
        "qid": "Q7747",
        "is_person": True,
        "is_entity": False,
        "positions": [
            {"qid": "Q11696", "label": "President of Russia", "start": "2012-05-07", "end": None},
            {"qid": "Q899", "label": "Prime Minister of Russia", "start": "2008-05-08", "end": "2012-05-07"},
        ],
    }
    signals = assess_bundle("wikidata", raw)
    assert [s.code for s in signals] == [PEP]
    assert signals[0].confidence == "medium"
    assert "President of Russia" in signals[0].evidence["positions"]


def test_wikidata_no_pep_when_all_positions_have_ended() -> None:
    raw = {
        "qid": "Q1",
        "is_person": True,
        "positions": [
            {"label": "Foo", "start": "2000", "end": "2005"},
        ],
    }
    assert assess_bundle("wikidata", raw) == []


def test_wikidata_no_pep_for_non_person() -> None:
    raw = {
        "qid": "Q42",
        "is_person": False,
        "is_entity": True,
        "positions": [],
    }
    assert assess_bundle("wikidata", raw) == []


def test_opaque_ownership_unknown_person_in_bods() -> None:
    raw = {"source_id": "companies_house", "hit_id": "00000000"}
    bods = [
        {"statementType": "entityStatement", "entityType": "registeredEntity"},
        {"statementType": "personStatement", "personType": "unknownPerson"},
    ]
    signals = assess_bundle("companies_house", raw, bods)
    assert [s.code for s in signals] == [OPAQUE_OWNERSHIP]
    assert "unknown person" in signals[0].evidence["findings"][0]


def test_opaque_ownership_anonymous_entity_in_bods() -> None:
    raw = {"source_id": "openaleph", "entity_id": "aleph-anon"}
    bods = [
        {"statementType": "entityStatement", "entityType": "anonymousEntity"},
    ]
    signals = assess_bundle("openaleph", raw, bods)
    assert [s.code for s in signals] == [OPAQUE_OWNERSHIP]


def test_no_signals_for_stub_bundle() -> None:
    raw = {"source_id": "opensanctions", "hit_id": "NK-stub", "is_stub": True}
    assert assess_bundle("opensanctions", raw) == []


def test_dict_serialisation() -> None:
    raw = {
        "source_id": "opensanctions",
        "entity_id": "NK-x",
        "entity": {"id": "NK-x", "topics": ["sanction"]},
    }
    payload = assess_bundle("opensanctions", raw)[0].to_dict()
    assert payload["code"] == SANCTIONED
    assert payload["confidence"] == "high"
    assert payload["source_id"] == "opensanctions"
    assert payload["hit_id"] == "NK-x"
    assert payload["evidence"] == {"topics": ["sanction"]}
