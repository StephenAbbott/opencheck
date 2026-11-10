"""The Rosneftegaz ticket (10 Nov 2026): a related-party sanctions match must
reach OpenSanctions' own node for the party, the one that holds the stake.

Rosneft's bundle held two records of its 40% shareholder that no rule joins:
OpenSanctions' node "Rosneftegaz JSC" (identifiers: the record's own id
``NK-o8vs68XiDUdkQGccvAsVvo``, OGRN, INN; the stake) and Wikidata's node
"Rosneftegaz" (no share). The screen scored the party's name against the
record's caption alone — "Rosneftegaz" — and "Rosneftegaz JSC" scored 0.846,
under the 0.88 gate, so the sanctioned badge sat on the share-less Wikidata
node. Stephen's decision: score against every name the record files, and
accept a shared identifier on its own.
"""

from __future__ import annotations

from typing import Any

import pytest

from opencheck import cross_check, names
from opencheck.config import get_settings
from opencheck.cross_check import assess_cross_source_names
from opencheck.sources import REGISTRY, SearchKind, SourceHit

_NK = "NK-o8vs68XiDUdkQGccvAsVvo"


@pytest.fixture(autouse=True)
def _live_with_key(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENSANCTIONS_API_KEY", "test-key")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class _StubAdapter:
    def __init__(self, hits: list[SourceHit]) -> None:
        self._hits = hits

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return list(self._hits)


def _record(**props: Any) -> SourceHit:
    """The OpenSanctions record as /search returns it (7 Nov 2026), cut down:
    caption "Rosneftegaz", "Rosneftegaz JSC" among its names, Canadian and
    Ukrainian sanctions datasets."""
    properties = {
        "name": ["Rosneftegaz", 'Joint-Stock Company "Rosneftegaz"', "Rosneftegaz JSC"],
        "alias": ["OJSC Rosneftegaz"],
        "ogrnCode": ["1047796902966"],
        "innCode": ["7705630445"],
    }
    properties.update(props)
    return SourceHit(
        source_id="opensanctions",
        hit_id=_NK,
        kind=SearchKind.ENTITY,
        name="Rosneftegaz",
        summary="",
        identifiers={"opensanctions_id": _NK},
        raw={
            "id": _NK,
            "caption": "Rosneftegaz",
            "schema": "Company",
            "topics": ["sanction"],
            "datasets": ["ca_dfatd_sema_sanctions", "ua_nsdc_sanctions"],
            "properties": properties,
        },
        is_stub=False,
    )


def _entity(sid: str, name: str, identifiers: list[dict[str, str]]) -> dict[str, Any]:
    return {
        "statementId": sid,
        "recordId": sid,
        "recordType": "entity",
        "recordDetails": {
            "entityType": {"type": "registeredEntity"},
            "name": name,
            "identifiers": identifiers,
        },
    }


def _owns(sid: str, owner: str, interest: dict[str, Any]) -> dict[str, Any]:
    return {
        "statementId": sid,
        "recordId": sid,
        "recordType": "relationship",
        "recordDetails": {
            "subject": "rosneft",
            "interestedParty": owner,
            "interests": [interest],
        },
    }


def _rosneft_bundle() -> list[dict[str, Any]]:
    return [
        _entity("rosneft", "Rosneft Oil Company", [{"scheme": "XI-LEI", "id": "253400JT3MQWNDKMJE44"}]),
        _entity(
            "os-rosneftegaz",
            "Rosneftegaz JSC",
            [
                {"scheme": "OPENSANCTIONS", "id": _NK},
                {"scheme": "RU-OGRN", "id": "1047796902966"},
                {"scheme": "RU-INN", "id": "7705630445"},
            ],
        ),
        _entity("wd-rosneftegaz", "Rosneftegaz", [{"scheme": "WIKIDATA", "id": "Q4397843"}]),
        _owns(
            "os-share",
            "os-rosneftegaz",
            {"type": "shareholding", "share": {"minimum": 44.69}},
        ),
        _owns("wd-control", "wd-rosneftegaz", {"type": "otherInfluenceOrControl"}),
    ]


def _anchors(signals: list[Any]) -> dict[str, Any]:
    return {s.evidence["subject_statement_id"]: s for s in signals}


async def test_the_shareholder_node_carries_its_own_records_signal(monkeypatch) -> None:
    monkeypatch.setitem(REGISTRY, "opensanctions", _StubAdapter([_record()]))
    monkeypatch.setitem(REGISTRY, "everypolitician", _StubAdapter([]))
    signals = await assess_cross_source_names(
        _rosneft_bundle(), subject_lei="253400JT3MQWNDKMJE44"
    )
    by_node = _anchors(signals)

    holder = by_node["os-rosneftegaz"]
    assert holder.code == cross_check.RELATED_SANCTIONED
    assert holder.confidence == "high"
    assert holder.evidence["identifier_match"] is True
    assert holder.evidence["corroboration"][0] == "identifier"
    assert holder.evidence["matched_name"] == "Rosneftegaz JSC"
    assert "an identifier also agrees" in holder.summary

    # The Wikidata node keeps its own name match, as before.
    wikidata = by_node["wd-rosneftegaz"]
    assert wikidata.code == cross_check.RELATED_SANCTIONED
    assert "identifier_match" not in wikidata.evidence


async def test_a_shared_identifier_is_enough_without_a_name_match(monkeypatch) -> None:
    """The record's names drift away from the party's; the id still holds."""
    record = _record(name=["Some Other Spelling"], alias=[])
    record.name = "Some Other Spelling"
    record.raw["caption"] = "Some Other Spelling"
    monkeypatch.setitem(REGISTRY, "opensanctions", _StubAdapter([record]))
    monkeypatch.setitem(REGISTRY, "everypolitician", _StubAdapter([]))
    signals = await assess_cross_source_names(
        _rosneft_bundle(), subject_lei="253400JT3MQWNDKMJE44"
    )
    by_node = _anchors(signals)
    assert set(by_node) == {"os-rosneftegaz"}
    assert by_node["os-rosneftegaz"].evidence["identifier_match"] is True


async def test_without_an_identifier_a_low_name_score_is_still_dropped(monkeypatch) -> None:
    record = _record(name=["Some Other Spelling"], alias=[])
    record.name = "Some Other Spelling"
    record.raw["caption"] = "Some Other Spelling"
    monkeypatch.setitem(REGISTRY, "opensanctions", _StubAdapter([record]))
    monkeypatch.setitem(REGISTRY, "everypolitician", _StubAdapter([]))
    bundle = [
        s
        for s in _rosneft_bundle()
        if s["statementId"] not in {"os-rosneftegaz", "os-share"}
    ]
    assert await assess_cross_source_names(bundle, subject_lei="253400JT3MQWNDKMJE44") == []


def test_the_caption_alone_was_under_the_gate() -> None:
    """Why it missed: pinned so the fix's reason stays visible."""
    assert names.name_similarity("Rosneftegaz JSC", "Rosneftegaz") < names.NAME_MATCH_THRESHOLD
    best, score = names.best_ftm_name(
        "Rosneftegaz JSC",
        {"caption": "Rosneftegaz", "properties": {"name": ["Rosneftegaz JSC"]}},
    )
    assert (best, score) == ("Rosneftegaz JSC", 1.0)


def test_best_name_ignores_weak_aliases_and_keeps_the_caption_on_a_tie() -> None:
    best, _ = names.best_ftm_name(
        "Acme", {"caption": "Acme", "properties": {"name": ["ACME"], "weakAlias": ["Acme"]}}
    )
    assert best == "Acme"
    best, score = names.best_ftm_name(
        "Zed", {"caption": "Acme", "properties": {"weakAlias": ["Zed"]}}
    )
    assert best != "Zed" and score < 1.0
