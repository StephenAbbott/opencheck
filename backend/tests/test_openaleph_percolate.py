"""Tests for OpenAleph text-based percolation (POST /api/2/beta/percolate).

Covers the adapter's ``percolate_text()`` (OpenAleph 5.3.1 — the endpoint
requested in openaleph/openaleph#105), the ``fetch_by_name_percolate()``
subject-name strategy built on it, and the strategy-cascade ordering:
identifier strategies → FtM match → percolate name → free-text q= fallback.

The None-vs-[] contract matters throughout: ``percolate_text`` returns
``None`` when the screen could not run (no key / pre-5.3.1 404 / HTTP
failure) and ``[]`` when it ran and nothing matched — Phase 2 (graph
screening) will rely on the distinction for degradation records.
"""

from __future__ import annotations

import json

import pytest
from pytest_httpx import HTTPXMock

from opencheck.config import get_settings
from opencheck.sources import REGISTRY, SearchKind
from opencheck.sources.base import SourceHit
from opencheck.sources.openaleph import _PERCOLATE_MAX_TEXT, OpenAlephAdapter

_API = "https://search.openaleph.org/api/2"
_PERCOLATE = f"{_API}/beta/percolate"
_LEI = "213800LH1BZH3DI6G760"


@pytest.fixture(autouse=True)
def _live_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENALEPH_API_KEY", "test-key")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _percolate_result(
    name: str,
    *,
    surface_form: str | None = None,
    match: list[str] | None = None,
    schema: str = "Company",
) -> dict:
    """A result item in the live 5.3.1 response shape (verified 2026-08-13)."""
    return {
        "id": f"ent-{name.lower().replace(' ', '-').replace('.', '')}",
        "schema": schema,
        "caption": name,
        "properties": {"name": [name]},
        "collection": {"id": 7, "foreign_id": "test", "label": "Test Collection"},
        "percolator_match": match or ["name"],
        "surface_forms": [surface_form or name],
        "score": 1.5,
        "highlight": {"content": [f"<em>{surface_form or name}</em>"]},
    }


# ---------------------------------------------------------------------------
# percolate_text
# ---------------------------------------------------------------------------


async def test_percolate_text_parses_results(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        method="POST",
        url=f"{_PERCOLATE}?limit=25&highlight=true",
        json={"status": "ok", "results": [_percolate_result("BP P.L.C.")]},
    )
    adapter = OpenAlephAdapter()
    results = await adapter.percolate_text("The venture between BP p.l.c. and X.")
    assert results is not None
    assert len(results) == 1
    assert results[0]["caption"] == "BP P.L.C."
    assert results[0]["percolator_match"] == ["name"]
    assert results[0]["surface_forms"] == ["BP P.L.C."]


async def test_percolate_text_sends_body_auth_and_highlight(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        method="POST",
        url=f"{_PERCOLATE}?limit=25&highlight=true",
        json={"results": []},
    )
    adapter = OpenAlephAdapter()
    results = await adapter.percolate_text("Some screening text")
    assert results == []  # ran, nothing matched — NOT None
    request = httpx_mock.get_requests()[-1]
    assert request.headers["Authorization"] == "ApiKey test-key"
    assert request.headers["User-Agent"].startswith("openaleph/")
    assert json.loads(request.content) == {"text": "Some screening text"}


async def test_percolate_text_filters_in_query_string(httpx_mock: HTTPXMock) -> None:
    """schema → filter:schema; each topic → a repeated filter:properties.topics."""
    httpx_mock.add_response(
        method="POST",
        url=(
            f"{_PERCOLATE}?limit=10&highlight=true&filter:schema=Person"
            f"&filter:properties.topics=sanction&filter:properties.topics=role.pep"
        ),
        json={"results": []},
    )
    adapter = OpenAlephAdapter()
    results = await adapter.percolate_text(
        "Igor Sechin", schema="Person", topics=("sanction", "role.pep"), limit=10
    )
    assert results == []


async def test_percolate_text_requires_api_key(monkeypatch) -> None:
    """Flagship edge 405s anonymous POSTs — no key means the screen cannot
    run, which is None (not screened), never [] (clean screen)."""
    monkeypatch.delenv("OPENALEPH_API_KEY", raising=False)
    get_settings.cache_clear()
    try:
        adapter = OpenAlephAdapter()
        assert await adapter.percolate_text("anything") is None
    finally:
        get_settings.cache_clear()


async def test_percolate_text_degrades_to_none_on_http_error(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        method="POST", url=f"{_PERCOLATE}?limit=25&highlight=true", status_code=405
    )
    adapter = OpenAlephAdapter()
    assert await adapter.percolate_text("anything") is None


async def test_percolate_text_degrades_to_none_on_pre_531_instance(
    httpx_mock: HTTPXMock,
) -> None:
    """Instances older than 5.3.1 have no percolate route — 404 must be
    'screen could not run', not an error card and not a clean screen."""
    httpx_mock.add_response(
        method="POST", url=f"{_PERCOLATE}?limit=25&highlight=true", status_code=404
    )
    adapter = OpenAlephAdapter()
    assert await adapter.percolate_text("anything") is None


async def test_percolate_text_empty_text_short_circuits() -> None:
    """Whitespace-only text never issues a request (server would 400)."""
    adapter = OpenAlephAdapter()
    assert await adapter.percolate_text("   ") == []


async def test_percolate_text_truncates_oversized_text(httpx_mock: HTTPXMock) -> None:
    """Text beyond the server cap is truncated client-side, not 400'd."""
    httpx_mock.add_response(
        method="POST", url=f"{_PERCOLATE}?limit=25&highlight=true", json={"results": []}
    )
    adapter = OpenAlephAdapter()
    await adapter.percolate_text("x" * (_PERCOLATE_MAX_TEXT + 5000))
    request = httpx_mock.get_requests()[-1]
    assert len(json.loads(request.content)["text"]) == _PERCOLATE_MAX_TEXT


async def test_percolate_text_none_when_live_disabled(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    try:
        adapter = OpenAlephAdapter()
        assert await adapter.percolate_text("anything") is None
    finally:
        get_settings.cache_clear()


async def test_percolate_text_cache_key_includes_filters(httpx_mock: HTTPXMock) -> None:
    """Regression guard (mentions cache-key bug): the same text with
    different filters must issue a new request, never replay the cache."""
    httpx_mock.add_response(
        method="POST",
        url=f"{_PERCOLATE}?limit=25&highlight=true",
        json={"results": [_percolate_result("Broad Hit")]},
    )
    httpx_mock.add_response(
        method="POST",
        url=f"{_PERCOLATE}?limit=25&highlight=true&filter:schema=Person",
        json={"results": []},
    )
    adapter = OpenAlephAdapter()
    broad = await adapter.percolate_text("Same text")
    person = await adapter.percolate_text("Same text", schema="Person")
    assert broad is not None and len(broad) == 1
    assert person == []
    assert len(httpx_mock.get_requests()) == 2


# ---------------------------------------------------------------------------
# fetch_by_name_percolate
# ---------------------------------------------------------------------------


async def test_fetch_by_name_percolate_returns_bearing_hits(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        method="POST",
        url=f"{_PERCOLATE}?limit=5&highlight=true&filter:schema=LegalEntity",
        json={"results": [_percolate_result("Ericsson AB")]},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_name_percolate("Ericsson AB")
    assert len(hits) == 1
    assert hits[0].source_id == "openaleph"
    assert hits[0].name == "Ericsson AB"
    assert hits[0].is_stub is False


async def test_fetch_by_name_percolate_keeps_bears_name_gate(
    httpx_mock: HTTPXMock,
) -> None:
    """Percolation is necessary but not sufficient: an entity named just
    'Shell' fires on the text 'Shell Midstream Partners LP'. Only hits
    that bear the full subject name survive (issue #21 gate)."""
    httpx_mock.add_response(
        method="POST",
        url=f"{_PERCOLATE}?limit=5&highlight=true&filter:schema=LegalEntity",
        json={
            "results": [
                _percolate_result("Shell", surface_form="Shell"),
                _percolate_result("Shell Midstream Partners LP"),
            ]
        },
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_name_percolate("Shell Midstream Partners LP")
    assert [h.name for h in hits] == ["Shell Midstream Partners LP"]


async def test_fetch_by_name_percolate_sends_quoted_name_verbatim(
    httpx_mock: HTTPXMock,
) -> None:
    """The whole point: Rosneft's nested-quote legal name goes through as
    raw body text — no Lucene parser, no sanitisation, no 500."""
    raw_name = 'Публичное акционерное общество "Нефтяная компания "Роснефть"'
    httpx_mock.add_response(
        method="POST",
        url=f"{_PERCOLATE}?limit=5&highlight=true&filter:schema=LegalEntity",
        json={"results": [_percolate_result(raw_name)]},
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.fetch_by_name_percolate(raw_name)
    request = httpx_mock.get_requests()[-1]
    assert json.loads(request.content)["text"] == raw_name  # verbatim, quotes intact
    assert len(hits) == 1


async def test_fetch_by_name_percolate_empty_without_key(monkeypatch) -> None:
    """Keyless deployments fall through to the q= fallback: [] here, no
    request issued, no error."""
    monkeypatch.delenv("OPENALEPH_API_KEY", raising=False)
    get_settings.cache_clear()
    try:
        adapter = OpenAlephAdapter()
        assert await adapter.fetch_by_name_percolate("Ericsson AB") == []
    finally:
        get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Strategy cascade ordering: match → percolate → q= fallback
# ---------------------------------------------------------------------------


def _empty_identifier_strategies(monkeypatch, adapter) -> None:
    async def empty(*_a, **_kw):
        return []

    monkeypatch.setattr(adapter, "fetch_by_lei", empty)
    monkeypatch.setattr(adapter, "fetch_by_oc_url", empty, raising=False)
    monkeypatch.setattr(adapter, "fetch_by_registration", empty)


async def test_strategies_try_percolate_after_match_before_name(monkeypatch) -> None:
    from opencheck.routers import lookup as lookup_mod

    ctx = lookup_mod._LookupCtx(lei=_LEI)
    ctx.legal_name = "BP P.L.C."

    adapter = REGISTRY["openaleph"]
    _empty_identifier_strategies(monkeypatch, adapter)

    percolate_hit = SourceHit(
        source_id="openaleph", hit_id="ent-bp", kind=SearchKind.ENTITY,
        name="BP P.L.C.", summary="collection: Test", identifiers={"aleph_id": "ent-bp"},
        raw={}, is_stub=False,
    )
    calls: list[str] = []

    async def fake_match(entity, limit=5):
        calls.append("match")
        return []

    async def fake_percolate_name(name):
        calls.append("percolate")
        assert name == "BP P.L.C."
        return [percolate_hit]

    async def fake_name(name):
        calls.append("name")
        return []

    async def no_mentions(*_a, **_kw):
        return None

    monkeypatch.setattr(adapter, "match_entity", fake_match)
    monkeypatch.setattr(adapter, "fetch_by_name_percolate", fake_percolate_name)
    monkeypatch.setattr(adapter, "fetch_by_name", fake_name)
    monkeypatch.setattr(adapter, "fetch_mentions", no_mentions)

    result = await lookup_mod._openaleph_strategies(ctx)
    assert [h.hit_id for h in result] == ["ent-bp"]
    # Percolate resolved it; the q= fallback never ran.
    assert calls == ["match", "percolate"]


async def test_strategies_fall_through_percolate_to_name_fallback(
    monkeypatch,
) -> None:
    """An empty percolate result (keyless deployment or a genuine miss)
    must still reach the Lucene q= fallback — keyless behaviour is
    exactly as before this feature."""
    from opencheck.routers import lookup as lookup_mod

    ctx = lookup_mod._LookupCtx(lei=_LEI)
    ctx.legal_name = "Obscure Vehicle S.A."

    adapter = REGISTRY["openaleph"]
    _empty_identifier_strategies(monkeypatch, adapter)

    calls: list[str] = []

    async def fake_match(entity, limit=5):
        calls.append("match")
        return []

    async def fake_percolate_name(name):
        calls.append("percolate")
        return []

    async def fake_name(name):
        calls.append("name")
        return []

    monkeypatch.setattr(adapter, "match_entity", fake_match)
    monkeypatch.setattr(adapter, "fetch_by_name_percolate", fake_percolate_name)
    monkeypatch.setattr(adapter, "fetch_by_name", fake_name)

    result = await lookup_mod._openaleph_strategies(ctx)
    assert result == []
    assert calls == ["match", "percolate", "name"]
