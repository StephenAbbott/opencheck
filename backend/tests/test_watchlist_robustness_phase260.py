"""Phase 260 — watchlist robustness.

From the Opus 5.5 check (C-M5, C-M6): the OpenSanctions backlog is drained
oldest first and never skipped silently; a version that can no longer be read
is recorded and answered with a catch-up re-run; and the ``/watch`` routes
never hold the event loop while SQLite waits on a write lock.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from opencheck import watchlist as wl
from opencheck.app import app
from opencheck.config import get_settings
from tests.test_watchlist import EASY, PARENT, _os_line, _watch, client, env  # noqa: F401 — fixtures

VERSIONS = [f"202609{d:02d}000000-v{d:02d}" for d in range(1, 21)]  # 20, sorted


def _nothing() -> str:
    return _os_line("ADD", "Company", ["Nobody At All Ltd"]) + "\n"


def _seed(store: wl.WatchlistStore, last: str) -> None:
    store.set_meta("opensanctions_version", last)


# ---- the backlog ----------------------------------------------------------------


def test_twenty_pending_versions_are_all_read_over_two_ticks_oldest_first(
    client: TestClient, env: Path, httpx_mock  # noqa: ANN001
) -> None:
    _watch(client, EASY)
    store = wl.get_store()
    assert store is not None
    _seed(store, "20260900000000-v00")
    listed = ["20260900000000-v00", *VERSIONS]
    httpx_mock.add_response(url=wl.OS_VERSIONS_URL, json={"items": listed}, is_reusable=True)
    for v in VERSIONS:
        # The one entity naming EASY sits in the OLDEST delta — the one the
        # newest-12 slice used to skip for good.
        body = _os_line("MOD", "Company", ["EASY POWER"]) + "\n" if v == VERSIONS[0] else _nothing()
        httpx_mock.add_response(url=wl.OS_DELTA_URL.format(version=v), text=body)

    first = wl.opensanctions_tick()
    assert first["versions"] == wl.OS_MAX_VERSIONS_PER_TICK == 12
    assert first["backlog"] == 8 and first["queued"] == 1
    assert store.get_meta("opensanctions_version") == VERSIONS[11]
    assert wl.state()["os_backlog"] == 8

    second = wl.opensanctions_tick()
    assert second["versions"] == 8 and second["backlog"] == 0
    assert store.get_meta("opensanctions_version") == VERSIONS[-1]
    assert wl.state()["os_backlog"] == 0

    read = [str(r.url) for r in httpx_mock.get_requests() if "delta" in str(r.url)]
    assert read == [wl.OS_DELTA_URL.format(version=v) for v in VERSIONS]  # each once, in order
    pending = store.take_pending(10)
    assert [p["trigger"]["version"] for p in pending] == [VERSIONS[0]]
    assert wl.os_gaps(store) == []


def test_a_failed_download_does_not_move_the_watermark(client: TestClient, env: Path, httpx_mock) -> None:  # noqa: ANN001
    _watch(client, EASY)
    store = wl.get_store()
    assert store is not None
    _seed(store, VERSIONS[0])
    httpx_mock.add_response(url=wl.OS_VERSIONS_URL, json={"items": VERSIONS[:4]}, is_reusable=True)
    httpx_mock.add_response(url=wl.OS_DELTA_URL.format(version=VERSIONS[1]), text=_nothing())
    httpx_mock.add_response(url=wl.OS_DELTA_URL.format(version=VERSIONS[2]), status_code=503)

    out = wl.opensanctions_tick()
    assert out["versions"] == 1
    assert store.get_meta("opensanctions_version") == VERSIONS[1]  # retried next tick, not skipped
    assert "503" in (wl.state()["os_last_error"] or "")
    assert wl.os_gaps(store) == []


def test_versions_that_aged_out_are_a_recorded_gap_and_a_catch_up(
    client: TestClient, env: Path, httpx_mock  # noqa: ANN001
) -> None:
    data = _watch(client, EASY)
    _watch(client, PARENT, data["token"])
    store = wl.get_store()
    assert store is not None
    # Last read long ago; versions.json now starts after it.
    _seed(store, "20260801000000-old")
    httpx_mock.add_response(url=wl.OS_VERSIONS_URL, json={"items": VERSIONS[:2]})
    for v in VERSIONS[:2]:
        httpx_mock.add_response(url=wl.OS_DELTA_URL.format(version=v), text=_nothing())

    out = wl.opensanctions_tick()
    assert out["gaps"] == 1 and out["catch_up_queued"] == 2 and out["versions"] == 2
    [gap] = wl.os_gaps(store)
    assert gap["reason"] == "aged_out"
    assert gap["after"] == "20260801000000-old" and gap["before"] == VERSIONS[0]
    pending = store.take_pending(10)
    assert {p["lei"] for p in pending} == {EASY, PARENT}
    assert {p["tier"] for p in pending} == {wl.TIER_CATCHUP}
    assert wl.state()["os_gaps"] == 1


def test_a_listed_version_whose_delta_is_gone_is_a_gap_not_a_wedge(
    client: TestClient, env: Path, httpx_mock  # noqa: ANN001
) -> None:
    _watch(client, EASY)
    store = wl.get_store()
    assert store is not None
    _seed(store, VERSIONS[0])
    httpx_mock.add_response(url=wl.OS_VERSIONS_URL, json={"items": VERSIONS[:3]})
    httpx_mock.add_response(url=wl.OS_DELTA_URL.format(version=VERSIONS[1]), status_code=404)
    httpx_mock.add_response(url=wl.OS_DELTA_URL.format(version=VERSIONS[2]), text=_nothing())

    out = wl.opensanctions_tick()
    assert out["versions"] == 2 and out["gaps"] == 1
    assert store.get_meta("opensanctions_version") == VERSIONS[2]
    [gap] = wl.os_gaps(store)
    assert gap["reason"] == "delta_missing" and gap["after"] == VERSIONS[1]
    assert store.take_pending(10)[0]["tier"] == wl.TIER_CATCHUP


def test_a_catch_up_gives_way_to_a_delta_that_names_the_company(client: TestClient, env: Path) -> None:
    _watch(client, EASY)
    store = wl.get_store()
    assert store is not None
    assert store.enqueue(EASY, wl.TIER_CATCHUP, {"tier": wl.TIER_CATCHUP})
    assert store.enqueue(EASY, wl.TIER_OPENSANCTIONS, {"tier": wl.TIER_OPENSANCTIONS, "version": "v9"})
    assert not store.enqueue(EASY, wl.TIER_CATCHUP, {"tier": wl.TIER_CATCHUP})
    [row] = store.take_pending(10)
    assert row["tier"] == wl.TIER_OPENSANCTIONS and row["trigger"]["version"] == "v9"


def test_only_the_last_gaps_are_kept(env: Path, client: TestClient) -> None:
    _watch(client, EASY)
    store = wl.get_store()
    assert store is not None
    for i in range(wl.OS_GAPS_KEPT + 5):
        wl._record_os_gap(store, {"reason": "delta_missing", "after": f"v{i:03d}", "before": f"v{i:03d}"})
    gaps = wl.os_gaps(store)
    assert len(gaps) == wl.OS_GAPS_KEPT and gaps[-1]["after"] == f"v{wl.OS_GAPS_KEPT + 4:03d}"


@pytest.mark.asyncio
async def test_a_catch_up_rerun_writes_an_entry_only_when_something_changed(
    client: TestClient, env: Path  # noqa: ARG001
) -> None:
    _watch(client, EASY)
    trig = {"tier": wl.TIER_CATCHUP, "reason": "aged_out", "after": "a", "before": "b"}
    out = await wl.rerun(EASY, wl.TIER_CATCHUP, trig)
    assert out["entries"] == 0  # the fake lookup answers the same as the baseline


@pytest.mark.asyncio
async def test_a_backlog_is_read_on_the_next_tick_not_after_the_interval(
    client: TestClient, env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OPENCHECK_WATCHLIST_OPENSANCTIONS_INTERVAL_S", "10800")
    get_settings.cache_clear()
    _watch(client, EASY)
    store = wl.get_store()
    assert store is not None
    store.set_meta("opensanctions_checked_at", wl._now_iso())  # not due
    calls: list[int] = []
    monkeypatch.setattr(wl, "opensanctions_tick", lambda: calls.append(1) or {"versions": 0, "queued": 0})
    await wl.tick()
    assert calls == []
    with wl._state_lock:
        wl._state.os_backlog = 3
    await wl.tick()
    assert calls == [1]


def test_the_page_and_watchstats_report_the_backlog_and_gaps(client: TestClient, env: Path) -> None:
    data = _watch(client, EASY)
    store = wl.get_store()
    assert store is not None
    wl._record_os_gap(store, {"reason": "aged_out", "after": "a", "before": "b"})
    with wl._state_lock:
        wl._state.os_backlog = 4
    page = client.get(f"/watch/{data['token']}").json()
    assert page["tiers"]["opensanctions"]["backlog"] == 4
    assert page["tiers"]["opensanctions"]["gaps"][-1]["reason"] == "aged_out"
    stats = client.get("/watchstats").json()["watcher"]
    assert stats["os_backlog"] == 4 and stats["os_gaps"] == 1


def test_the_feed_words_a_catch_up_entry(client: TestClient, env: Path) -> None:
    from opencheck.routers.watch import _entry_content, _entry_title

    entry = {
        "id": 1, "lei": EASY, "legal_name": "EASY POWER", "created_at": "2026-09-28T00:00:00Z",
        "tier": wl.TIER_CATCHUP,
        "trigger": {"tier": wl.TIER_CATCHUP, "reason": "aged_out", "after": "v1", "before": "v9"},
        "changes": [{"kind": "signal_new", "code": "SANCTIONED", "sources": ["opensanctions"]}],
        "checked": [], "degraded": [],
    }
    body = _entry_content(entry)
    assert "no longer listed" in body and "v1" in body and "v9" in body
    assert "new risk signal" in _entry_title(entry)


# ---- the event loop ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_locked_store_does_not_freeze_the_event_loop(client: TestClient, env: Path) -> None:
    """Hold a write lock on the watchlist file from another thread, as the
    mirror-refresh hook does, and read the list meanwhile. The request waits
    for the lock; the loop keeps serving everything else."""
    data = _watch(client, EASY)
    path = Path(get_settings().watchlist_db_file or "")
    held = threading.Event()
    release = threading.Event()

    def _hold() -> None:
        conn = sqlite3.connect(path, isolation_level=None)
        conn.execute("BEGIN EXCLUSIVE")
        held.set()
        release.wait(5)
        conn.execute("COMMIT")
        conn.close()

    t = threading.Thread(target=_hold)
    t.start()
    assert held.wait(5)

    stalls: list[float] = []

    async def _ticker() -> None:
        last = time.monotonic()
        while not release.is_set():
            await asyncio.sleep(0.01)
            now = time.monotonic()
            stalls.append(now - last)
            last = now

    async def _release_later() -> None:
        await asyncio.sleep(0.6)
        release.set()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as ac:
        started = time.monotonic()
        r, _, _ = await asyncio.gather(ac.get(f"/watch/{data['token']}"), _ticker(), _release_later())
        waited = time.monotonic() - started
    t.join(5)
    assert r.status_code == 200
    assert waited >= 0.5  # the request really did wait on the lock
    assert max(stalls) < 0.3, f"event loop stalled for {max(stalls):.2f}s"


def test_the_version_list_is_read_sorted(client: TestClient, env: Path, httpx_mock) -> None:  # noqa: ANN001
    _watch(client, EASY)
    store = wl.get_store()
    assert store is not None
    _seed(store, VERSIONS[0])
    shuffled = [VERSIONS[2], VERSIONS[0], VERSIONS[1]]
    httpx_mock.add_response(url=wl.OS_VERSIONS_URL, json={"items": shuffled})
    httpx_mock.add_response(url=wl.OS_DELTA_URL.format(version=VERSIONS[1]), text=_nothing())
    httpx_mock.add_response(url=wl.OS_DELTA_URL.format(version=VERSIONS[2]), text=_nothing())
    assert wl.opensanctions_tick()["versions"] == 2
    assert store.get_meta("opensanctions_version") == VERSIONS[2]
    json.dumps(wl.state())  # the state stays serialisable
