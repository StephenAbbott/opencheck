"""Tests for the OpenAleph POST /api/2/match spike (spike/bods-ftm-api-match).

Covers the FtM subject conversion (opencheck/ftm.py), the adapter's
match_entity() method, and the strategy-cascade ordering: identifier
strategies → FtM match → free-text name fallback.
"""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from opencheck.config import get_settings
from opencheck.ftm import subject_to_ftm_entity
from opencheck.sources import REGISTRY, SearchKind
from opencheck.sources.base import SourceHit
from opencheck.sources.openaleph import OpenAlephAdapter

_API = "https://search.openaleph.org/api/2"
_LEI = "213800LH1BZH3DI6G760"


@pytest.fixture(autouse=True)
def _live_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENALEPH_API_KEY", "test-key")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# subject_to_ftm_entity
# ---------------------------------------------------------------------------


def test_subject_conversion_produces_ftm_company() -> None:
    entity = subject_to_ftm_entity(_LEI, "BP P.L.C.", "GB", "00102498")
    assert entity is not None
    assert entity["schema"] == "Company"
    props = entity["properties"]
    assert props["name"] == ["BP P.L.C."]
    assert props["leiCode"] == [_LEI]
    # FtM country values are lowercase alpha-2.
    assert [c.lower() for c in props["jurisdiction"]] == ["gb"]
    assert props["registrationNumber"] == ["00102498"]


def test_subject_conversion_without_optional_fields() -> None:
    entity = subject_to_ftm_entity(_LEI, "Acme GmbH")
    assert entity is not None
    assert entity["properties"]["name"] == ["Acme GmbH"]
    assert "jurisdiction" not in entity["properties"] or entity["properties"]["jurisdiction"]
    assert entity["properties"]["leiCode"] == [_LEI]


def test_subject_conversion_requires_name_and_lei() -> None:
    assert subject_to_ftm_entity(_LEI, "") is None
    assert subject_to_ftm_entity("", "Acme GmbH") is None


def test_builtin_and_bods_ftm_paths_agree() -> None:
    """When bods-ftm is installed, both conversion paths must carry the
    same core properties (name, leiCode, jurisdiction, registrationNumber)."""
    pytest.importorskip(
        "bods_ftm", reason="bods-ftm not installed — built-in fallback in use"
    )
    from opencheck.ftm import _subject_bods_statement, _via_bods_ftm, _via_builtin

    via_lib = _via_bods_ftm(
        _subject_bods_statement(_LEI, "BP P.L.C.", "GB", "00102498")
    )
    via_builtin = _via_builtin(_LEI, "BP P.L.C.", "GB", "00102498")
    assert via_lib is not None
    for key in ("name", "leiCode", "registrationNumber"):
        assert via_lib["properties"].get(key) == via_builtin["properties"].get(key), key
    # Jurisdiction may differ in case only.
    assert [c.lower() for c in via_lib["properties"].get("jurisdiction", [])] == [
        c.lower() for c in via_builtin["properties"].get("jurisdiction", [])
    ]


# ---------------------------------------------------------------------------
# OpenAlephAdapter.match_entity
# ---------------------------------------------------------------------------


async def test_match_entity_parses_scored_results(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        method="POST",
        url=f"{_API}/match?limit=5",
        json={
            "status": "ok",
            "results": [
                {
                    "id": "lei-213800LH1BZH3DI6G760.abc",
                    "schema": "Company",
                    "score": 98.51607,
                    "properties": {
                        "name": ["BP P.L.C."],
                        "leiCode": [_LEI],
                        "registrationNumber": ["00102498"],
                    },
                    "collection": {"label": "GLEIF", "foreign_id": "gleif"},
                }
            ],
        },
    )
    adapter = OpenAlephAdapter()
    hits = await adapter.match_entity(
        {"schema": "Company", "properties": {"name": ["BP P.L.C."]}}
    )
    assert len(hits) == 1
    assert hits[0].identifiers["lei"] == _LEI
    assert hits[0].raw["match_score"] == pytest.approx(98.51607)
    assert "FtM match score 99" in hits[0].summary


async def test_match_entity_requires_api_key(monkeypatch) -> None:
    """Flagship edge 405s anonymous POSTs — no key means skip entirely."""
    monkeypatch.delenv("OPENALEPH_API_KEY", raising=False)
    get_settings.cache_clear()
    try:
        adapter = OpenAlephAdapter()
        hits = await adapter.match_entity(
            {"schema": "Company", "properties": {"name": ["BP P.L.C."]}}
        )
        assert hits == []
    finally:
        get_settings.cache_clear()


async def test_match_entity_degrades_on_http_error(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(method="POST", url=f"{_API}/match?limit=5", status_code=405)
    adapter = OpenAlephAdapter()
    hits = await adapter.match_entity(
        {"schema": "Company", "properties": {"name": ["BP P.L.C."]}}
    )
    assert hits == []


async def test_match_entity_sends_auth_and_ua(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(method="POST", url=f"{_API}/match?limit=5", json={"results": []})
    adapter = OpenAlephAdapter()
    await adapter.match_entity({"schema": "Company", "properties": {"name": ["X Y"]}})
    request = httpx_mock.get_requests()[-1]
    assert request.headers["Authorization"] == "ApiKey test-key"
    assert request.headers["User-Agent"].startswith("openaleph/")


# ---------------------------------------------------------------------------
# Strategy cascade ordering
# ---------------------------------------------------------------------------


def _empty_strategies(monkeypatch, adapter) -> None:
    async def empty(*_a, **_kw):
        return []

    monkeypatch.setattr(adapter, "fetch_by_lei", empty)
    monkeypatch.setattr(adapter, "fetch_by_oc_url", empty, raising=False)
    monkeypatch.setattr(adapter, "fetch_by_registration", empty)


async def test_strategies_try_ftm_match_before_name_fallback(monkeypatch) -> None:
    from opencheck.routers import lookup as lookup_mod

    ctx = lookup_mod._LookupCtx(lei=_LEI)
    ctx.legal_name = "BP P.L.C."
    ctx.jurisdiction = "GB"
    ctx.registered_as = "00102498"

    adapter = REGISTRY["openaleph"]
    _empty_strategies(monkeypatch, adapter)

    match_hit = SourceHit(
        source_id="openaleph", hit_id="lei-x", kind=SearchKind.ENTITY,
        name="BP P.L.C.", summary="collection: GLEIF · FtM match score 99",
        identifiers={"aleph_id": "lei-x", "lei": _LEI}, raw={}, is_stub=False,
    )
    calls: list[str] = []

    async def fake_match(entity, limit=5):
        calls.append("match")
        assert entity["properties"]["leiCode"] == [_LEI]
        return [match_hit]

    async def fake_name(name):
        calls.append("name")
        return []

    monkeypatch.setattr(adapter, "match_entity", fake_match)
    monkeypatch.setattr(adapter, "fetch_by_name", fake_name)
    # No mentions noise in this test.
    async def no_mentions(*_a, **_kw):
        return None
    monkeypatch.setattr(adapter, "fetch_mentions", no_mentions)

    result = await lookup_mod._openaleph_strategies(ctx)
    assert [h.hit_id for h in result] == ["lei-x"]
    # Match ran; name fallback never needed.
    assert calls == ["match"]


async def test_strategies_fall_back_to_name_when_match_empty(monkeypatch) -> None:
    from opencheck.routers import lookup as lookup_mod

    ctx = lookup_mod._LookupCtx(lei=_LEI)
    ctx.legal_name = "Obscure Vehicle S.A."

    adapter = REGISTRY["openaleph"]
    _empty_strategies(monkeypatch, adapter)

    calls: list[str] = []

    async def fake_match(entity, limit=5):
        calls.append("match")
        return []

    async def fake_name(name):
        calls.append("name")
        return []

    monkeypatch.setattr(adapter, "match_entity", fake_match)
    monkeypatch.setattr(adapter, "fetch_by_name", fake_name)

    result = await lookup_mod._openaleph_strategies(ctx)
    assert result == []
    assert calls == ["match", "name"]
