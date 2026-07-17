"""Typo-tolerance fallback for GLEIF name search (issue #33).

These tests exercise the zero-result trigger in ``GleifAdapter.search`` and its
``_relaxed_search`` leave-one-out fallback, with HTTP mocked via pytest-httpx.
The measurement that motivated the fallback lives in
``scripts/eval_gleif_autocompletions.py`` +
``docs/gleif-autocompletions-evaluation.md``; these tests pin the mechanism.

Mutation self-checks (run manually, counts recorded in the PR/doc):

* Disable the fallback (make ``search`` return the empty primary result) ->
  ``test_typo_triggers_relaxed_fallback`` and the consensus test fail.
* Make the fallback fire on non-zero results too (drop the ``if hits`` guard) ->
  ``test_exact_hit_does_not_trigger_fallback`` fails, because the relaxed
  sub-queries it would then issue have no registered mock response.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

import pytest
from pytest_httpx import HTTPXMock

from opencheck.config import get_settings
from opencheck.sources import SearchKind
from opencheck.sources.gleif import _RELAXED_SUMMARY_SUFFIX, GleifAdapter

_API = "https://api.gleif.org/api/v1"


@pytest.fixture(autouse=True)
def _live_settings(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> Any:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _search_url(query: str) -> str:
    return f"{_API}/lei-records?filter[fulltext]={quote(query)}&page[size]=10"


def _lei_record(lei: str, name: str) -> dict[str, Any]:
    return {
        "type": "lei-records",
        "id": lei,
        "attributes": {
            "lei": lei,
            "entity": {
                "legalName": {"name": name},
                "jurisdiction": "DE",
                "status": "ACTIVE",
            },
        },
    }


def _results(*records: dict[str, Any]) -> dict[str, Any]:
    return {"data": list(records)}


async def test_exact_hit_does_not_trigger_fallback(httpx_mock: HTTPXMock) -> None:
    """A query the fulltext filter resolves must NOT relax — no extra calls,
    no ``approximate match`` marking. (Guards against the fallback firing on
    non-zero results: only the primary URL is mocked, so any relaxed sub-query
    would raise.)"""
    httpx_mock.add_response(
        url=_search_url("Acme Corporation"),
        json=_results(_lei_record("ACME0000000000000001", "ACME CORPORATION")),
    )

    adapter = GleifAdapter()
    hits = await adapter.search("Acme Corporation", SearchKind.ENTITY)

    assert len(hits) == 1
    assert hits[0].hit_id == "ACME0000000000000001"
    assert _RELAXED_SUMMARY_SUFFIX not in hits[0].summary
    # Exactly one request — the primary fulltext query, no relaxation fan-out.
    assert len(httpx_mock.get_requests()) == 1


async def test_typo_triggers_relaxed_fallback(httpx_mock: HTTPXMock) -> None:
    """Zero primary hits -> leave-one-out retry surfaces the real entity via
    the variant that drops the typo'd token, marked ``approximate match``."""
    # Primary (typo in "Wigdet") returns nothing.
    httpx_mock.add_response(url=_search_url("Acme Wigdet Corporation"), json=_results())
    # Leave-one-out variants: only dropping the typo'd token recovers the entity.
    httpx_mock.add_response(url=_search_url("Wigdet Corporation"), json=_results())
    httpx_mock.add_response(
        url=_search_url("Acme Corporation"),
        json=_results(_lei_record("ACME0000000000000001", "ACME CORPORATION")),
    )
    httpx_mock.add_response(url=_search_url("Acme Wigdet"), json=_results())

    adapter = GleifAdapter()
    hits = await adapter.search("Acme Wigdet Corporation", SearchKind.ENTITY)

    assert [h.hit_id for h in hits] == ["ACME0000000000000001"]
    assert hits[0].summary.endswith(_RELAXED_SUMMARY_SUFFIX)
    assert hits[0].is_stub is False


async def test_relaxed_consensus_ranks_agreement_first(httpx_mock: HTTPXMock) -> None:
    """When several leave-one-out variants surface the same LEI, that consensus
    entity outranks one that appears in only a single variant."""
    real = _lei_record("REAL0000000000000001", "GLOBAL TRUE HOLDING GROUP")
    noise = _lei_record("NOISE000000000000001", "GROUP GENERIC LTD")
    httpx_mock.add_response(url=_search_url("Global Trx Holding Group"), json=_results())
    # Two variants agree on `real` (2 votes); `noise` appears in one (1 vote).
    httpx_mock.add_response(url=_search_url("Trx Holding Group"), json=_results())
    httpx_mock.add_response(url=_search_url("Global Holding Group"), json=_results(real, noise))
    httpx_mock.add_response(url=_search_url("Global Trx Group"), json=_results(real))
    httpx_mock.add_response(url=_search_url("Global Trx Holding"), json=_results())

    adapter = GleifAdapter()
    hits = await adapter.search("Global Trx Holding Group", SearchKind.ENTITY)

    # real: 2 votes, noise: 1 vote -> real ranks first on the vote count alone.
    assert [h.hit_id for h in hits] == ["REAL0000000000000001", "NOISE000000000000001"]
    assert all(h.summary.endswith(_RELAXED_SUMMARY_SUFFIX) for h in hits)


async def test_single_token_query_does_not_fan_out(httpx_mock: HTTPXMock) -> None:
    """A one-token query has nothing to drop: zero primary hits -> empty, and no
    relaxation requests are issued."""
    httpx_mock.add_response(url=_search_url("Wigdet"), json=_results())

    adapter = GleifAdapter()
    hits = await adapter.search("Wigdet", SearchKind.ENTITY)

    assert hits == []
    assert len(httpx_mock.get_requests()) == 1


async def test_relaxed_search_returns_empty_when_no_variant_matches(
    httpx_mock: HTTPXMock,
) -> None:
    """If every leave-one-out variant is also empty, the fallback yields []."""
    httpx_mock.add_response(url=_search_url("Foo Bar Baz"), json=_results())
    httpx_mock.add_response(url=_search_url("Bar Baz"), json=_results())
    httpx_mock.add_response(url=_search_url("Foo Baz"), json=_results())
    httpx_mock.add_response(url=_search_url("Foo Bar"), json=_results())

    adapter = GleifAdapter()
    hits = await adapter.search("Foo Bar Baz", SearchKind.ENTITY)

    assert hits == []
