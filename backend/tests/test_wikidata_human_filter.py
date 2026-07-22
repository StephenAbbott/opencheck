"""Wikidata person-search human filter (Phase C, feat/background-check).

wbsearchentities matches labels/aliases with no type filter, so a person
query returns paintings, songs and films named after people. The adapter
now post-filters person-kind hits to instance-of-human (P31 → Q5) with a
single batched SPARQL query — failing OPEN when SPARQL errors.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from opencheck.sources import SearchKind
from opencheck.sources.wikidata import WikidataAdapter


def _search_payload() -> dict:
    return {
        "search": [
            {
                "id": "Q9545",
                "label": "Tony Blair",
                "description": "Prime Minister of the United Kingdom 1997–2007",
                "match": {"type": "label"},
            },
            {
                "id": "Q17520903",
                "label": "Tony Blair",
                "description": "painting by Alastair Adams",
                "match": {"type": "label"},
            },
            {
                "id": "Q7821764",
                "label": "Tony Blair",
                "description": "1999 single by Chumbawamba",
                "match": {"type": "label"},
            },
        ]
    }


def _sparql_humans(*qids: str) -> dict:
    return {
        "results": {
            "bindings": [
                {"item": {"value": f"http://www.wikidata.org/entity/{q}"}}
                for q in qids
            ]
        }
    }


@pytest.fixture
def adapter(monkeypatch: pytest.MonkeyPatch) -> WikidataAdapter:
    a = WikidataAdapter()
    # Force the live search path regardless of allow_live: the cache
    # check passes and both HTTP helpers are mocked per-test.
    monkeypatch.setattr(a._cache, "has", lambda key: True)
    a._mediawiki_get = AsyncMock(return_value=_search_payload())  # type: ignore[method-assign]
    return a


async def test_person_search_keeps_only_humans(adapter: WikidataAdapter) -> None:
    adapter._sparql = AsyncMock(return_value=_sparql_humans("Q9545"))  # type: ignore[method-assign]
    hits = await adapter.search("Tony Blair", SearchKind.PERSON)
    assert [h.hit_id for h in hits] == ["Q9545"]
    # The VALUES query asked about every candidate.
    query = adapter._sparql.call_args.args[0]
    for qid in ("Q9545", "Q17520903", "Q7821764"):
        assert f"wd:{qid}" in query
    assert "wd:Q5" in query


async def test_person_search_fails_open_on_sparql_error(
    adapter: WikidataAdapter,
) -> None:
    # _sparql returns {} on HTTP errors / WDQS timeouts — every candidate
    # must survive (degrade to noisier results, never drop the person).
    adapter._sparql = AsyncMock(return_value={})  # type: ignore[method-assign]
    hits = await adapter.search("Tony Blair", SearchKind.PERSON)
    assert len(hits) == 3


async def test_entity_search_is_not_filtered(adapter: WikidataAdapter) -> None:
    adapter._sparql = AsyncMock(return_value=_sparql_humans())  # type: ignore[method-assign]
    hits = await adapter.search("Tony Blair", SearchKind.ENTITY)
    assert len(hits) == 3
    adapter._sparql.assert_not_called()


async def test_empty_human_set_filters_everything(
    adapter: WikidataAdapter,
) -> None:
    # A genuine result set with zero humans (all candidates are works of
    # art) filters to nothing — distinct from the fail-open error case.
    adapter._sparql = AsyncMock(return_value=_sparql_humans())  # type: ignore[method-assign]
    hits = await adapter.search("Tony Blair", SearchKind.PERSON)
    assert hits == []
