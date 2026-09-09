"""Phase 188 — the UK corporate-PSC chain is found on the local graph first.

With ``OPENCHECK_CH_GRAPH_FIRST`` on, the adapter walks the chain above a UK
subject on the Phase 186 store (one index lookup per hop, no register call)
and then asks Companies House for the subject and every company the graph
named **at once**, before the live walk runs. The live walk is unchanged: it
reads the register's own PSC lists — from the cache the prefetch filled — and
follows what *they* say. So the bundle, and every BODS statement mapped from
it, is byte-for-byte what the hop-by-hop walk produced; what changes is that
the hops no longer wait for each other.

These tests pin that equivalence (the flag must not change the answer), the
two ways the graph can be wrong and how each is counted, and the four ways
the graph can be absent or broken — every one of which falls back to the walk
OpenCheck has always done.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

import httpx
import pytest
from pytest_httpx import HTTPXMock

from opencheck import psc_graph, signalstats
from opencheck.config import get_settings
from opencheck.routers.hit_builders import _bh_companies_house
from opencheck.sources.companies_house import CompaniesHouseAdapter
from tests.test_ch_psc_recursion import _API, _CHAIN, _mock_chain, _mock_company
from tests.test_psc_graph import CORP, _snapshot_zip

# The Babcock stack as the *graph* holds it: one row per corporate PSC, the
# filed number spelled as the register's PSC list spells it (leading zero
# dropped) so the build normalises it exactly as the live walk does.
_GRAPH_CHAIN = [
    ("00070274", "Babcock Defence Systems Limited", "2999029"),
    ("02999029", "Babcock Southern Holdings Limited", "1915771"),
    ("01915771", "Babcock Overseas Investments Limited", "2669327"),
    ("02669327", "Babcock International Group PLC", "2342138"),
]

_SEQ = iter(range(10_000))


def _row(company: str, name: str, reg_number: str) -> dict:
    """One snapshot record: *company*'s corporate PSC, filed as UK-registered."""
    return {
        "company_number": company,
        "data": {
            "kind": CORP,
            "name": name,
            "etag": f"e-{company}",
            "links": {
                "self": (
                    f"/company/{company}/persons-with-significant-control"
                    f"/corporate-entity/id{next(_SEQ):04d}"
                )
            },
            "notified_on": "2016-04-06",
            "natures_of_control": ["ownership-of-shares-75-to-100-percent"],
            "identification": {
                "country_registered": "England",
                "place_registered": "Companies House",
                "registration_number": reg_number,
            },
        },
    }


def _build_graph(tmp_path: Path, rows: list[dict], *, name: str = "psc_graph.sqlite") -> Path:
    snapshot = _snapshot_zip(tmp_path / f"{name}.zip", rows)
    out = tmp_path / name
    psc_graph.build_psc_graph(out, source_url=f"file://{snapshot}", snapshot_date="2026-09-08")
    return out


@pytest.fixture
def graph(tmp_path: Path) -> Path:
    """A graph that knows the whole Babcock chain."""
    return _build_graph(tmp_path, [_row(*r) for r in _GRAPH_CHAIN])


@pytest.fixture(autouse=True)
def _clean_graph(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    # The live-lookup settings the adapter gates on (test_ch_psc_recursion's
    # autouse fixture does not reach this module), plus a clean slate for the
    # graph: no file, flag off, no store held over from another test.
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("COMPANIES_HOUSE_API_KEY", "test-key")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.delenv("OPENCHECK_CH_PSC_MAX_DEPTH", raising=False)
    monkeypatch.delenv("OPENCHECK_PSC_GRAPH_DB_FILE", raising=False)
    monkeypatch.delenv("OPENCHECK_CH_GRAPH_FIRST", raising=False)
    get_settings.cache_clear()
    psc_graph.reset_store_for_tests()
    signalstats.reset()
    yield
    get_settings.cache_clear()
    psc_graph.reset_store_for_tests()
    signalstats.reset()


def _graph_first(monkeypatch: pytest.MonkeyPatch, path: Path | None) -> None:
    monkeypatch.setenv("OPENCHECK_CH_GRAPH_FIRST", "true")
    if path is not None:
        monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_FILE", str(path))
    get_settings.cache_clear()
    psc_graph.reset_store_for_tests()


def _walks() -> dict[str, Any]:
    return signalstats.stats()["companies_house_walks"]


# ---------------------------------------------------------------------------
# The flag must not change the answer
# ---------------------------------------------------------------------------


async def test_the_flag_is_off_by_default_and_nothing_reads_the_graph(
    httpx_mock: HTTPXMock, graph: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A graph configured but the flag off: the walk is the Phase 177 one."""
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_FILE", str(graph))
    get_settings.cache_clear()
    assert get_settings().ch_graph_first is False
    _mock_chain(httpx_mock, _CHAIN)
    bundle = await CompaniesHouseAdapter().fetch("00070274")

    assert set(bundle["related_companies"]) == {"02999029", "01915771", "02669327", "02342138"}
    assert bundle["chain_source"]["source"] == "live"
    assert _walks()["chain"] == {"lookup|live": 1}
    assert _walks()["graph"] == {}


async def test_graph_first_returns_the_same_chain_from_the_registers_own_records(
    httpx_mock: HTTPXMock, graph: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The whole point: the graph proposes, the register answers, and the
    bundle is what the hop-by-hop walk produced — same companies, same
    records, same four calls each."""
    _mock_chain(httpx_mock, _CHAIN)
    live = await CompaniesHouseAdapter().fetch("00070274")

    # A cold cache for the second walk, or it would be answered from the
    # first one's and prove nothing about how the chain was found.
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path / "second"))
    _graph_first(monkeypatch, graph)
    _mock_chain(httpx_mock, _CHAIN)
    fresh = await CompaniesHouseAdapter().fetch("00070274")

    assert fresh["related_companies"] == live["related_companies"]
    assert fresh["profile"] == live["profile"]
    assert fresh["pscs"] == live["pscs"]
    assert fresh["unfollowed_pscs"] == live["unfollowed_pscs"] == []
    # Every record still came from Companies House, for every company.
    requested = {str(r.url) for r in httpx_mock.get_requests()}
    for number in _CHAIN:
        assert f"{_API}/company/{number}" in requested
        assert f"{_API}/company/{number}/persons-with-significant-control" in requested


async def test_graph_first_says_where_the_chain_came_from(
    httpx_mock: HTTPXMock, graph: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _graph_first(monkeypatch, graph)
    _mock_chain(httpx_mock, _CHAIN)
    bundle = await CompaniesHouseAdapter().fetch("00070274")

    chain = bundle["chain_source"]
    assert chain["source"] == "graph"
    assert chain["related"] == 4 and chain["predicted"] == 4
    assert chain["missed"] == 0 and chain["extra"] == 0
    assert chain["snapshot_date"] == "2026-09-08"


async def test_the_walk_spends_the_same_register_budget_not_twice(
    httpx_mock: HTTPXMock, graph: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The prefetch's calls are the walk's calls, made earlier. Twenty live
    calls for five companies — the same figure Phase 184 measured — and the
    walk that follows adds none, because a hit on a key the prefetch filled
    is that same call coming back, not a saving to claim."""
    _graph_first(monkeypatch, graph)
    _mock_chain(httpx_mock, _CHAIN)
    await CompaniesHouseAdapter().fetch("00070274")

    lookup = _walks()["by_origin"]["lookup"]
    assert lookup["calls_live"] == 20
    assert lookup["calls_cached"] == 0
    assert len(httpx_mock.get_requests()) == 20


async def test_a_second_walk_is_answered_from_the_cache_including_the_404(
    httpx_mock: HTTPXMock, graph: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Phase 188 caches "no PSC statements filed" too, so a re-walk of a
    chain costs the register nothing at all — before, the 404 was re-asked
    once per company on every walk."""
    _graph_first(monkeypatch, graph)
    _mock_chain(httpx_mock, _CHAIN)
    adapter = CompaniesHouseAdapter()
    await adapter.fetch("00070274")
    before = len(httpx_mock.get_requests())
    await adapter.fetch("00070274")

    assert len(httpx_mock.get_requests()) == before  # not one more call
    lookup = _walks()["by_origin"]["lookup"]
    assert lookup["walks"] == 2
    assert lookup["calls_live"] == 20
    assert lookup["calls_cached"] == 20


async def test_the_chain_is_fetched_concurrently_not_hop_by_hop(
    httpx_mock: HTTPXMock, graph: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The saving is wall-clock: the register is asked about the companies
    two hops apart at the same time, which a serial walk cannot do. Measured
    as overlap — more than one request in flight at once."""
    _graph_first(monkeypatch, graph)
    in_flight = 0
    peak = 0

    async def slow(request: httpx.Request) -> httpx.Response:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.01)
        in_flight -= 1
        number = str(request.url).split("/company/")[1].split("/")[0]
        if str(request.url).endswith("-statements"):
            return httpx.Response(404, json={"errors": [{"error": "not-found"}]})
        if str(request.url).endswith("/officers"):
            return httpx.Response(200, json={"items": []})
        if str(request.url).endswith("persons-with-significant-control"):
            return httpx.Response(200, json={"items": _CHAIN.get(number, [])})
        return httpx.Response(200, json={"company_number": number, "company_name": number})

    httpx_mock.add_callback(slow, is_reusable=True)
    await CompaniesHouseAdapter().fetch("00070274")
    assert peak > 1


# ---------------------------------------------------------------------------
# The two ways the graph can be wrong — the register is always the answer
# ---------------------------------------------------------------------------


async def test_a_company_the_graph_does_not_know_is_still_found_live(
    httpx_mock: HTTPXMock, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The seed is a day old and the stream may have a gap, so the graph can
    lag the register. The walk follows the register's PSC list regardless:
    the unknown company is fetched live and counted as ``missed`` — the
    number that says how far behind the local copy is running."""
    stale = _build_graph(tmp_path, [_row(*_GRAPH_CHAIN[0]), _row(*_GRAPH_CHAIN[1])])
    _graph_first(monkeypatch, stale)
    _mock_chain(httpx_mock, _CHAIN)
    bundle = await CompaniesHouseAdapter().fetch("00070274")

    # The full chain, exactly as the live walk finds it.
    assert set(bundle["related_companies"]) == {"02999029", "01915771", "02669327", "02342138"}
    chain = bundle["chain_source"]
    assert chain["predicted"] == 2 and chain["missed"] == 2 and chain["extra"] == 0
    assert _walks()["graph"]["lookup"] == {
        "walks": 1,
        "predicted": 2,
        "missed": 2,
        "extra": 0,
        "prefetch_seconds": _walks()["graph"]["lookup"]["prefetch_seconds"],
    }


async def test_a_company_the_graph_names_that_the_register_does_not_is_not_in_the_bundle(
    httpx_mock: HTTPXMock, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A PSC ceased since the snapshot: the graph still names the company,
    the register's own PSC list does not, and the bundle follows the
    register. The wasted prefetch is counted as ``extra`` and nothing else."""
    over = _build_graph(
        tmp_path,
        [_row(*r) for r in _GRAPH_CHAIN] + [_row("02342138", "Ghost Holdings Limited", "9999999")],
    )
    _graph_first(monkeypatch, over)
    _mock_chain(httpx_mock, _CHAIN)
    _mock_company(httpx_mock, "09999999", [])
    bundle = await CompaniesHouseAdapter().fetch("00070274")

    assert "09999999" not in bundle["related_companies"]
    chain = bundle["chain_source"]
    assert chain["predicted"] == 5 and chain["extra"] == 1 and chain["missed"] == 0
    assert _walks()["graph"]["lookup"]["extra"] == 1


# ---------------------------------------------------------------------------
# Every way the graph can be absent, and the walk that has always worked
# ---------------------------------------------------------------------------


async def test_the_flag_without_a_graph_falls_back_and_says_so(
    httpx_mock: HTTPXMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Flag on, no file (the boot download has not landed yet): the live walk
    runs, and the counter distinguishes this from a chain nobody asked the
    graph about — otherwise a flag that silently does nothing looks like a
    flag that works."""
    _graph_first(monkeypatch, None)
    _mock_chain(httpx_mock, _CHAIN)
    bundle = await CompaniesHouseAdapter().fetch("00070274")

    assert set(bundle["related_companies"]) == {"02999029", "01915771", "02669327", "02342138"}
    assert bundle["chain_source"]["source"] == "graph_unavailable"
    assert _walks()["chain"] == {"lookup|graph_unavailable": 1}
    assert _walks()["graph"] == {}


async def test_a_broken_graph_never_sinks_a_lookup(
    httpx_mock: HTTPXMock, graph: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The file opens but the walk raises (a truncated download, a schema
    from the future). The lookup is not the place to find out."""
    _graph_first(monkeypatch, graph)
    store = psc_graph.get_store()
    assert store is not None

    def boom(*args: Any, **kwargs: Any) -> None:
        raise sqlite3.DatabaseError("database disk image is malformed")

    monkeypatch.setattr(store, "walk", boom)
    _mock_chain(httpx_mock, _CHAIN)
    bundle = await CompaniesHouseAdapter().fetch("00070274")

    assert set(bundle["related_companies"]) == {"02999029", "01915771", "02669327", "02342138"}
    assert bundle["chain_source"]["source"] == "graph_unavailable"


async def test_a_company_the_prefetch_could_not_reach_is_left_to_the_walk(
    httpx_mock: HTTPXMock, graph: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One company's profile 500s during the prefetch. The walk asks again
    and records the gap the way it always has — a prefetch failure is never
    a lookup failure."""
    _graph_first(monkeypatch, graph)
    # Its profile 500s, once for the prefetch and once for the walk that
    # asks again; its other three records answer normally.
    for _ in range(2):
        httpx_mock.add_response(url=f"{_API}/company/02669327", status_code=500, json={})
    httpx_mock.add_response(url=f"{_API}/company/02669327/officers", json={"items": []})
    httpx_mock.add_response(
        url=f"{_API}/company/02669327/persons-with-significant-control",
        json={"items": _CHAIN["02669327"]},
    )
    httpx_mock.add_response(
        url=f"{_API}/company/02669327/persons-with-significant-control-statements",
        status_code=404,
        json={"errors": [{"error": "not-found"}]},
    )
    _mock_chain(httpx_mock, {k: v for k, v in _CHAIN.items() if k != "02669327"})
    bundle = await CompaniesHouseAdapter().fetch("00070274")

    assert "02669327" not in bundle["related_companies"]
    assert [u["reason"] for u in bundle["unfollowed_pscs"]] == ["fetch_failed"]
    # The chain below it is still there — one unreachable parent truncates
    # the chain above it and nothing else.
    assert "02999029" in bundle["related_companies"]


# ---------------------------------------------------------------------------
# What the counters and the card may carry
# ---------------------------------------------------------------------------


def test_the_chain_counters_carry_no_company_number_or_name(
    httpx_mock: HTTPXMock, graph: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``/signalstats`` is public and aggregate. The graph section is counts
    and a vocabulary of three words — nothing that names a company."""
    import json

    _graph_first(monkeypatch, graph)
    _mock_chain(httpx_mock, _CHAIN)
    asyncio.run(CompaniesHouseAdapter().fetch("00070274"))

    blob = json.dumps(_walks())
    for secret in ("Babcock", "00070274", "02999029", "2999029", "Defence"):
        assert secret not in blob
    assert set(_walks()["chain"]) == {"lookup|graph"}


def test_the_bundles_chain_source_is_counts_and_dates_only(
    httpx_mock: HTTPXMock, graph: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The bundle may name companies — it is the subject's own record — but
    ``chain_source`` is the provenance line's material, and a caption is not
    the place a company number should first appear."""
    _graph_first(monkeypatch, graph)
    _mock_chain(httpx_mock, _CHAIN)
    bundle = asyncio.run(CompaniesHouseAdapter().fetch("00070274"))

    assert set(bundle["chain_source"]) == {
        "source",
        "related",
        "predicted",
        "missed",
        "extra",
        "snapshot_date",
        "stream_published_at",
    }
    for value in bundle["chain_source"].values():
        assert not isinstance(value, (list, dict))


def test_the_hit_carries_the_chain_source_for_the_card(
    httpx_mock: HTTPXMock, graph: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The card's caption reads it off ``raw``, as KvK's coverage note does."""
    from opencheck.routers.lookup import _LookupCtx

    _graph_first(monkeypatch, graph)
    _mock_chain(httpx_mock, _CHAIN)
    bundle = asyncio.run(CompaniesHouseAdapter().fetch("00070274"))
    hit = _bh_companies_house(bundle, "00070274", _LookupCtx(lei="X", legal_name="Vosper"))

    assert hit.raw["chain_source"]["source"] == "graph"
    # The profile is still the profile.
    assert hit.raw["company_number"] == "00070274"


def test_the_graph_walk_and_the_live_walk_agree_on_the_fixture_chain(
    graph: Path,
) -> None:
    """A unit check on the proposal itself, independent of the adapter: the
    store returns the same four companies the register's chain reaches, on
    numbers filed without their leading zeros."""
    store = psc_graph.PscGraphStore(graph)
    try:
        walk = store.walk("00070274", max_depth=6, max_related=25)
        assert set(walk.companies) - {"00070274"} == {
            "02999029",
            "01915771",
            "02669327",
            "02342138",
        }
        assert walk.unfollowed == []
    finally:
        store.close()


def test_the_store_is_read_in_a_thread_not_on_the_event_loop(
    graph: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SQLite reads are blocking and a 2.2 GB file's index walk is not free.
    The connection is opened ``check_same_thread=False`` for exactly this."""
    _graph_first(monkeypatch, graph)
    store = psc_graph.get_store()
    assert store is not None

    async def run() -> list[str]:
        walk = await asyncio.to_thread(store.walk, "00070274")
        return sorted(walk.companies)

    assert asyncio.run(run())[0] == "00070274"


def test_the_graph_file_is_opened_read_only(graph: Path) -> None:
    """A lookup must never write to the file the stream owns."""
    store = psc_graph.PscGraphStore(graph)
    try:
        with pytest.raises(sqlite3.OperationalError):
            store._conn.execute("DELETE FROM psc")
    finally:
        store.close()
