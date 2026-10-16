"""GLEIF refusals that reach the reader — the "GLEIF rate limiter refusing a
third of stream/export calls" ticket (investigated 15 Oct 2026).

A refused anchor call sends the whole GLEIF bundle to the stale cache or the
Golden Copy snapshot. The snapshot carries no mapping-file ids (``ocid``,
``spglobal``, ``bic``, ``mic``, ``qcc``), and OpenCorporates is dispatched only
on ``ocid`` — so under load a lookup silently lost OpenCorporates: not in
``sources_applicable``, no degradation, no notice. Pinned here:

* the fallback copies the ids from the cached Level 1 record, any age —
  often answered in the same gather before a sibling was refused;
* without one the bundle says so (``crossrefs_available: false`` and a
  reason), and the pipeline records a ``gleif`` ``source_read`` degradation
  naming what was lost;
* one refusal cancels the anchor's other calls instead of letting them queue
  for budget the lookup is about to throw away;
* an LEI export reuses the run the reader just saw (``deepen_top`` 5, the
  stream's), REST and MCP alike.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from pathlib import Path

import httpx
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from pytest_httpx import HTTPXMock

from opencheck import degradation
from opencheck import entity_pages as ep
from opencheck.app import app
from opencheck.cache import Cache
from opencheck.config import get_settings
from opencheck.gleif_throttle import GleifRateLimitedError, reset_throttle_for_tests
from opencheck.mcp import server as mcp_server
from opencheck.routers import export as export_router
from opencheck.routers import lookup as lookup_router
from opencheck.sources import REGISTRY
from opencheck.sources.gleif import GleifAdapter
from tests.test_gleif_throttle import _LEI, _age_cache_entry, _entity_store_db

_API = "https://api.gleif.org/api/v1"


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_GLEIF_RATE_LIMIT_PER_MINUTE", "0")
    monkeypatch.setenv("OPENCHECK_ENTITY_PAGES_DB_FILE", str(_entity_store_db(tmp_path)))
    monkeypatch.delenv("OPENCHECK_GLEIF_MIRROR_FIRST", raising=False)
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    reset_throttle_for_tests()
    yield
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    reset_throttle_for_tests()


def _cache_record(*, ocid: str | None = "gb/04366849", **extra: object) -> None:
    attrs: dict = {"lei": _LEI, "entity": {"legalName": {"name": "SHELL PLC (LIVE)"}}}
    if ocid:
        attrs.update({"ocid": ocid, "spglobal": ["123"], "bic": [], "qcc": None})
    attrs.update(extra)
    Cache().put(f"gleif/lei/{_LEI}", {"data": {"id": _LEI, "attributes": attrs}})


# ---------------------------------------------------------------------------
# The fallback keeps what the cache knows
# ---------------------------------------------------------------------------


async def test_fallback_copies_the_cross_references_from_the_cached_record(
    httpx_mock: HTTPXMock, tmp_path: Path
) -> None:
    """The record answered (or was cached) and a parent call was refused: the
    snapshot serves the anchor, and the record's ids survive — stale is fine,
    they come from GLEIF's mapping files and change rarely."""
    _cache_record()
    _age_cache_entry(tmp_path, f"gleif/lei/{_LEI}", age_days=30)  # past the 7-day TTL
    httpx_mock.add_response(status_code=429, is_reusable=True)

    bundle = await GleifAdapter().fetch(_LEI)

    assert bundle["snapshot_source"] == "fallback"
    attrs = bundle["record"]["attributes"]
    assert attrs["ocid"] == "gb/04366849" and attrs["spglobal"] == ["123"]
    assert "bic" not in attrs and "qcc" not in attrs  # empty values are not ids
    # The record itself is still the snapshot's, not the cached one.
    assert attrs["entity"]["legalName"]["name"] == "SHELL PLC"
    assert bundle["crossrefs_available"] is True
    assert "crossrefs_unavailable_reason" not in bundle


async def test_fallback_without_a_cached_record_says_what_it_lacks(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(status_code=429, is_reusable=True)
    bundle = await GleifAdapter().fetch(_LEI)
    assert bundle["snapshot_source"] == "fallback"
    assert "ocid" not in bundle["record"]["attributes"]
    assert bundle["crossrefs_available"] is False
    assert bundle["crossrefs_unavailable_reason"] == degradation.REASON_RATE_LIMITED


async def test_fallback_after_the_snapshot_bound_names_a_timeout(
    httpx_mock: HTTPXMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OPENCHECK_GLEIF_SNAPSHOT_AFTER_S", "0.2")
    get_settings.cache_clear()

    async def slow(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(2)
        return httpx.Response(404)

    httpx_mock.add_callback(slow, is_reusable=True, is_optional=True)
    bundle = await GleifAdapter().fetch(_LEI)
    assert bundle["snapshot_source"] == "fallback"
    assert bundle["crossrefs_unavailable_reason"] == degradation.REASON_TIMEOUT


async def test_a_live_anchor_carries_no_crossref_flag(httpx_mock: HTTPXMock) -> None:
    """Only a store-served bundle says anything about cross-references: a
    live record carries its own ids, or none because GLEIF holds none."""
    _cache_record()
    for kind in ("direct", "ultimate"):
        httpx_mock.add_response(url=f"{_API}/lei-records/{_LEI}/{kind}-parent", status_code=404)
        httpx_mock.add_response(
            url=f"{_API}/lei-records/{_LEI}/{kind}-parent-reporting-exception", status_code=404
        )
    httpx_mock.add_response(
        url=f"{_API}/lei-records/{_LEI}/direct-children?page[size]=100&page[number]=1",
        json={"data": [], "meta": {"pagination": {"total": 0}}},
    )
    bundle = await GleifAdapter().fetch(_LEI)
    assert "snapshot_source" not in bundle
    assert "crossrefs_available" not in bundle


# ---------------------------------------------------------------------------
# One refusal stops the rest of the anchor
# ---------------------------------------------------------------------------


async def test_a_refusal_cancels_the_anchors_other_calls(
    httpx_mock: HTTPXMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plain ``gather`` let the siblings of a refused call run on, queueing
    for and spending budget whose answers the lookup then threw away."""
    monkeypatch.setenv("OPENCHECK_GLEIF_SNAPSHOT_AFTER_S", "0")  # no bound: only cancelling is fast
    get_settings.cache_clear()
    finished: list[str] = []

    async def slow_children(request: httpx.Request) -> httpx.Response:
        try:
            await asyncio.sleep(3)
        except asyncio.CancelledError:
            finished.append("cancelled")
            raise
        finished.append("answered")
        return httpx.Response(200, json={"data": [], "meta": {"pagination": {"total": 0}}})

    httpx_mock.add_callback(
        slow_children,
        url=f"{_API}/lei-records/{_LEI}/direct-children?page[size]=100&page[number]=1",
    )
    httpx_mock.add_response(status_code=429, is_reusable=True)

    start = time.monotonic()
    bundle = await GleifAdapter().fetch(_LEI)
    assert time.monotonic() - start < 1.5
    assert bundle["snapshot_source"] == "fallback"
    await asyncio.sleep(0.05)
    # Cancelled when the sibling was refused — not left running to answer
    # (or to queue for a budget slot) after the lookup had moved on.
    assert finished == ["cancelled"]


async def test_with_no_fallback_the_refusal_still_reaches_the_caller(
    httpx_mock: HTTPXMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("OPENCHECK_ENTITY_PAGES_DB_FILE")
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    httpx_mock.add_response(status_code=429, is_reusable=True)
    with pytest.raises(GleifRateLimitedError):
        await GleifAdapter().fetch(_LEI)


# ---------------------------------------------------------------------------
# The pipeline names the loss
# ---------------------------------------------------------------------------


async def _resolve_with(monkeypatch: pytest.MonkeyPatch, bundle: dict) -> tuple:
    """``_resolve_ctx`` over a given GLEIF bundle, inside a degradation scope."""

    async def fake_fetch(lei: str):
        return bundle

    async def no_qid(lei: str):
        return None

    monkeypatch.setattr(REGISTRY["gleif"], "fetch", fake_fetch)
    monkeypatch.setattr(REGISTRY["wikidata"], "find_qid_by_lei", no_qid)
    degradation.begin()
    try:
        ctx, _ = await lookup_router._resolve_ctx(bundle["lei"])
    finally:
        recorded = degradation.collect()
    return ctx, recorded


#: Not a curated Open Ownership example, so the anchor is GLEIF's own.
_PLAIN_LEI = "549300W96W2VKSMVDF81"


def _store_bundle(**flags: object) -> dict:
    return {
        "source_id": "gleif",
        "lei": _PLAIN_LEI,
        "record": {"id": _PLAIN_LEI, "attributes": {"lei": _PLAIN_LEI, "entity": {
            "legalName": {"name": "SHELL PLC"}, "jurisdiction": "GB"}}},
        "snapshot_source": "fallback",
        **flags,
    }


async def test_a_store_served_anchor_without_crossrefs_is_a_degradation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx, recorded = await _resolve_with(
        monkeypatch,
        _store_bundle(crossrefs_available=False, crossrefs_unavailable_reason="rate_limited"),
    )
    assert ctx.ocid is None
    [entry] = [d for d in recorded if d.source_id == "gleif"]
    assert entry.check == degradation.CHECK_SOURCE_READ
    assert entry.reason == "rate_limited"
    assert entry.detail == lookup_router.GLEIF_CROSSREFS_DETAIL
    assert "OpenCorporates was not consulted" in entry.detail


async def test_a_store_served_anchor_with_crossrefs_is_not(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = _store_bundle(crossrefs_available=True)
    bundle["record"]["attributes"]["ocid"] = "gb/04366849"
    ctx, recorded = await _resolve_with(monkeypatch, bundle)
    assert ctx.ocid == "gb/04366849"
    assert not [d for d in recorded if d.source_id == "gleif"]


async def test_mirror_first_with_live_mode_off_is_not_a_gap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A top-up skipped because the deployment runs offline names no reason:
    nothing about this lookup was missed."""
    _, recorded = await _resolve_with(
        monkeypatch, _store_bundle(snapshot_source="mirror", crossrefs_available=False)
    )
    assert not [d for d in recorded if d.source_id == "gleif"]


def test_the_crossrefs_detail_names_no_entity() -> None:
    """Degradation details are fixed sentences of source names only."""
    assert "SHELL" not in lookup_router.GLEIF_CROSSREFS_DETAIL
    assert lookup_router.GLEIF_CROSSREFS_DETAIL[0].islower()  # reads after "GLEIF — "


@pytest.mark.parametrize(
    ("exc", "reason"),
    [
        (GleifRateLimitedError("refused"), "rate_limited"),
        (TimeoutError(), "timeout"),
        (httpx.ReadTimeout("slow"), "timeout"),
        (RuntimeError("boom"), "upstream_error"),
    ],
)
def test_crossrefs_failure_reason(exc: BaseException, reason: str) -> None:
    assert lookup_router._crossrefs_failure_reason(exc) == reason


# ---------------------------------------------------------------------------
# An export reuses the run the reader just saw
# ---------------------------------------------------------------------------


def _capture_lookup(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    seen: list[int] = []

    async def fake_lookup_impl(*, lei: str, deepen_top: int = 5, **_: object):
        seen.append(deepen_top)
        raise HTTPException(status_code=418, detail="stop here")

    monkeypatch.setattr(export_router, "_lookup_impl", fake_lookup_impl)
    return seen


def test_an_lei_export_defaults_to_the_streams_depth(monkeypatch: pytest.MonkeyPatch) -> None:
    """The replay cache is keyed ``LEI:deepen_top``. At 3 an export after a
    QuickCheck (stream default 5) never matched, so every download ran a
    second full pipeline and exported a shallower graph than the screen."""
    seen = _capture_lookup(monkeypatch)
    client = TestClient(app)
    assert client.get("/export", params={"lei": _LEI, "format": "json"}).status_code == 418
    assert client.get(
        "/export", params={"lei": _LEI, "format": "json", "deepen_top": 2}
    ).status_code == 418
    stream_default = inspect.signature(lookup_router.lookup_stream).parameters["deepen_top"].default
    assert seen == [stream_default.default, 2] == [5, 2]
    assert export_router.LEI_EXPORT_DEEPEN_TOP == 5


def test_a_query_export_keeps_its_depth(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[int] = []

    async def fake_build_report(q, kind, deepen_top):
        seen.append(deepen_top)
        raise HTTPException(status_code=418, detail="stop here")

    monkeypatch.setattr(export_router, "_build_report", fake_build_report)
    assert TestClient(app).get("/export", params={"q": "shell", "format": "json"}).status_code == 418
    assert seen == [export_router.QUERY_EXPORT_DEEPEN_TOP] == [3]


def test_the_mcp_export_defaults_to_the_lookups_depth() -> None:
    def default(fn, name="deepen_top"):
        fn = getattr(fn, "fn", fn)
        return inspect.signature(fn).parameters[name].default

    assert default(mcp_server.opencheck_export_bods) == default(mcp_server.opencheck_lookup) == 5
