"""Phase 187 — the PSC stream consumer keeps the UK PSC graph current.

The consumer is driven here by scripted line sources in the register's own
shape — one JSON event per line, blank lines as heartbeats, an envelope of
``event.type`` / ``timepoint`` / ``published_at`` and a ``resource_uri`` —
over the Phase 186 fixture store (the Timpson chain and friends).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck import psc_graph, psc_stream
from opencheck.app import app
from opencheck.config import get_settings
from tests.test_psc_graph import CORP, INDIV, MAJ, TRUST, _snapshot_zip

URI = "/company/{company}/persons-with-significant-control/{kind}/{pid}"


def _event(
    company: str,
    pid: str,
    *,
    kind: str = INDIV,
    name: str = "New Owner",
    timepoint: int = 1,
    published_at: str = "2026-09-08T14:00:00",
    ev_type: str = "changed",
    natures: list[str] | None = None,
    ceased_on: str | None = None,
    identification: dict | None = None,
    with_data: bool = True,
) -> dict:
    short = "individual" if kind == INDIV else "corporate-entity"
    uri = URI.format(company=company, kind=short, pid=pid)
    ev: dict = {
        "resource_kind": f"company-psc-{short}",
        "resource_uri": uri,
        "resource_id": pid,
        "event": {
            "type": ev_type,
            "timepoint": timepoint,
            "published_at": published_at,
            "fields_changed": ["name"],
        },
    }
    if with_data and ev_type != "deleted":
        data: dict = {
            "kind": kind,
            "name": name,
            "etag": f"e-{pid}",
            "links": {"self": uri},
            "notified_on": "2026-09-08",
            "natures_of_control": natures or MAJ,
        }
        if kind == INDIV:
            data["date_of_birth"] = {"month": 1, "year": 1980}
            data["nationality"] = "British"
        if identification:
            data["identification"] = identification
        if ceased_on:
            data["ceased_on"] = ceased_on
        ev["data"] = data
    return ev


def _lines(*events: dict | str) -> list[str]:
    return [e if isinstance(e, str) else json.dumps(e) for e in events]


@pytest.fixture
def store_path(tmp_path: Path) -> Path:
    snapshot = _snapshot_zip(tmp_path / "snap.zip")
    out = tmp_path / "psc_graph.sqlite"
    psc_graph.build_psc_graph(out, source_url=f"file://{snapshot}", snapshot_date="2026-09-08")
    return out


@pytest.fixture(autouse=True)
def _clean(monkeypatch: pytest.MonkeyPatch):
    for var in ("OPENCHECK_PSC_GRAPH_DB_FILE", "COMPANIES_HOUSE_STREAM_KEY"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(psc_stream, "BATCH_MAX_WAIT_S", 0.0)
    get_settings.cache_clear()
    psc_graph.reset_store_for_tests()
    psc_stream.reset_state_for_tests()
    yield
    get_settings.cache_clear()
    psc_graph.reset_store_for_tests()
    psc_stream.reset_state_for_tests()


def _rows(path: Path, company: str) -> list[tuple]:
    return (
        sqlite3.connect(path)
        .execute(
            "SELECT psc_id, name, reg_number, ceased_on FROM psc "
            "WHERE company_number = ? ORDER BY name",
            (company,),
        )
        .fetchall()
    )


def _meta(path: Path) -> dict[str, str]:
    return dict(sqlite3.connect(path).execute("SELECT key, value FROM meta"))


# ---------------------------------------------------------------------------
# Parsing the envelope
# ---------------------------------------------------------------------------


def test_company_number_and_psc_id_come_from_the_envelope():
    ev = _event("675216", "abc123")  # the register spells the number canonically; be safe anyway
    assert psc_stream.company_number_from_uri(ev["resource_uri"]) == "00675216"
    assert psc_stream.psc_id_of(ev) == "abc123"
    deleted = _event("00675216", "abc123", ev_type="deleted")
    assert "data" not in deleted
    assert psc_stream.psc_id_of(deleted) == "abc123"
    assert psc_stream.company_number_from_uri("/officers/x/appointments") is None
    assert psc_stream.psc_id_of({"resource_uri": "/company/1/x/id9"}) == "id9"
    assert psc_stream.psc_id_of({}) is None


# ---------------------------------------------------------------------------
# Applying events
# ---------------------------------------------------------------------------


def test_apply_upserts_a_new_record_in_the_seed_s_shape(store_path: Path):
    conn = sqlite3.connect(store_path)
    applied = psc_stream.apply_events(
        conn,
        [
            _event(
                "00000001",
                "newpsc",
                name="Ms New Owner",
                natures=["voting-rights-25-to-50-percent"],
            )
        ],
    )
    assert (applied.upserted, applied.ceased, applied.deleted, applied.skipped) == (1, 0, 0, 0)
    rows = _rows(store_path, "00000001")
    assert [r[1] for r in rows] == ["Ms New Owner", "Offshore Parent Limited"]
    store = psc_graph.PscGraphStore(store_path)
    new = next(r for r in store.pscs("00000001") if r.name == "Ms New Owner")
    assert new.kind == INDIV and new.dob == "1980-01" and new.nationality == "British"
    # A nature code the seed never saw was added to the table and reads back.
    assert new.natures == ("voting-rights-25-to-50-percent",)
    store.close()


def test_apply_a_corporate_psc_normalises_its_uk_number_so_the_walk_follows_it(store_path: Path):
    conn = sqlite3.connect(store_path)
    psc_stream.apply_events(
        conn,
        [
            _event(
                "00000001",
                "corp1",
                kind=CORP,
                name="New Holdco Ltd",
                identification={"country_registered": "England", "registration_number": "675216"},
            )
        ],
    )
    store = psc_graph.PscGraphStore(store_path)
    walk = store.walk("00000001")
    assert list(walk.companies)[:2] == ["00000001", "00675216"]
    store.close()


def test_apply_replays_the_same_event_idempotently(store_path: Path):
    conn = sqlite3.connect(store_path)
    ev = _event("00000001", "newpsc")
    psc_stream.apply_events(conn, [ev])
    psc_stream.apply_events(conn, [ev, ev])
    assert len(_rows(store_path, "00000001")) == 2


def test_apply_ceased_dates_the_row_and_the_walk_ignores_it(store_path: Path):
    conn = sqlite3.connect(store_path)
    holroyd = next(r for r in _rows(store_path, "02588889") if r[1] == "Mr Charles William Holroyd")
    applied = psc_stream.apply_events(
        conn,
        [
            _event(
                "02588889",
                holroyd[0],
                name="Mr Charles William Holroyd",
                natures=TRUST,
                ceased_on="2026-09-07",
            )
        ],
    )
    assert applied.ceased == 1
    assert next(r for r in _rows(store_path, "02588889") if r[0] == holroyd[0])[3] == "2026-09-07"
    store = psc_graph.PscGraphStore(store_path)
    assert [r.name for r in store.pscs("02588889")] == [
        "Mr Matthew Samuel Davies",
        "Sir William John Anthony Timpson",
    ]
    assert len(store.pscs("02588889", include_ceased=True)) == 3
    store.close()


def test_apply_deleted_removes_the_row(store_path: Path):
    conn = sqlite3.connect(store_path)
    sandymere = _rows(store_path, "00675216")[0]
    applied = psc_stream.apply_events(conn, [_event("00675216", sandymere[0], ev_type="deleted")])
    assert applied.deleted == 1
    assert _rows(store_path, "00675216") == []
    store = psc_graph.PscGraphStore(store_path)
    assert store.walk("00675216").companies == {}  # the chain is gone with its first hop
    store.close()


def test_apply_skips_what_it_cannot_key_or_read(store_path: Path):
    conn = sqlite3.connect(store_path)
    applied = psc_stream.apply_events(
        conn,
        [
            {"event": {"type": "changed"}},  # no id at all
            _event("00000001", "nodata", with_data=False),  # changed without data
            {
                **_event("00000001", "weird"),
                "data": {"kind": "something-else", "links": {"self": "/x/weird"}},
            },
        ],
    )
    assert applied.skipped == 3 and applied.upserted == 0


# ---------------------------------------------------------------------------
# The consumer over a scripted connection
# ---------------------------------------------------------------------------


def _scripted(*connections: list[str] | BaseException):
    """A line source that plays one scripted connection per connect: a list
    of lines, or an exception to raise on connect. Records the timepoints it
    was asked to resume from."""
    asked: list[int | None] = []
    plays = list(connections)

    async def source(timepoint: int | None) -> AsyncIterator[str]:
        asked.append(timepoint)
        if not plays:
            await asyncio.sleep(3600)  # nothing more scripted: hang until cancelled
        play = plays.pop(0)
        if isinstance(play, BaseException):
            raise play
        for line in play:
            yield line
            await asyncio.sleep(0)

    source.asked = asked  # type: ignore[attr-defined]
    return source


async def _run_until(consumer: psc_stream.Consumer, predicate, timeout: float = 5.0) -> None:
    task = asyncio.create_task(consumer.run())
    try:
        deadline = asyncio.get_event_loop().time() + timeout
        while not predicate():
            if task.done():
                task.result()
                return
            if asyncio.get_event_loop().time() > deadline:
                raise AssertionError("consumer did not reach the expected state")
            await asyncio.sleep(0.01)
    finally:
        consumer.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_consumer_applies_events_and_persists_the_cursor(store_path: Path, monkeypatch):
    monkeypatch.setattr(psc_stream, "RECONNECT_BACKOFF_START_S", 0.01)
    source = _scripted(
        _lines(
            _event("00000001", "p1", timepoint=101, published_at="2026-09-08T14:00:00"),
            "",  # heartbeat
            _event(
                "00000001",
                "p2",
                timepoint=102,
                published_at="2026-09-08T14:00:05",
                name="Second Owner",
            ),
        )
    )
    consumer = psc_stream.Consumer(store_path, lines=source)
    await _run_until(consumer, lambda: psc_stream.state()["upserted"] >= 2)
    assert [r[1] for r in _rows(store_path, "00000001")] == [
        "New Owner",
        "Offshore Parent Limited",
        "Second Owner",
    ]
    meta = _meta(store_path)
    assert meta[psc_graph.META_STREAM_TIMEPOINT] == "102"
    assert meta[psc_graph.META_STREAM_AT] == "2026-09-08T14:00:05"
    st = psc_stream.state()
    assert st["enabled"] and st["events"] == 2 and st["by_type"] == {"changed": 2}
    assert st["timepoint"] == 102 and st["last_published_at"] == "2026-09-08T14:00:05"
    assert st["resumed_from"] == "live" and st["gap"] is True  # a fresh seed has no cursor
    assert source.asked[0] is None


async def test_consumer_resumes_from_the_persisted_cursor(store_path: Path, monkeypatch):
    monkeypatch.setattr(psc_stream, "RECONNECT_BACKOFF_START_S", 0.01)
    conn = sqlite3.connect(store_path)
    psc_graph.write_meta(
        conn,
        **{psc_graph.META_STREAM_TIMEPOINT: "500", psc_graph.META_STREAM_AT: "2026-09-08T13:00:00"},
    )
    conn.close()
    source = _scripted(_lines(_event("00000001", "p1", timepoint=501)))
    consumer = psc_stream.Consumer(store_path, lines=source)
    await _run_until(consumer, lambda: psc_stream.state()["upserted"] >= 1)
    assert source.asked[0] == 500
    st = psc_stream.state()
    assert st["resumed_from"] == "timepoint" and st["gap"] is False


async def test_consumer_reconnects_from_the_cursor_after_a_drop(store_path: Path, monkeypatch):
    """The nightly disconnect: the connection ends, the consumer reconnects
    from the last timepoint it processed."""
    monkeypatch.setattr(psc_stream, "RECONNECT_BACKOFF_START_S", 0.01)
    source = _scripted(
        _lines(_event("00000001", "p1", timepoint=7)),
        _lines(_event("00000001", "p2", timepoint=8, name="After The Drop")),
    )
    consumer = psc_stream.Consumer(store_path, lines=source)
    await _run_until(consumer, lambda: psc_stream.state()["upserted"] >= 2)
    assert source.asked[:2] == [None, 7]
    st = psc_stream.state()
    assert st["connects"] == 2 and st["disconnects"] == 2


async def test_consumer_backs_off_after_a_network_error(store_path: Path, monkeypatch):
    monkeypatch.setattr(psc_stream, "RECONNECT_BACKOFF_START_S", 0.01)
    monkeypatch.setattr(psc_stream, "RECONNECT_BACKOFF_MAX_S", 0.02)
    source = _scripted(
        ConnectionError("reset by peer"), _lines(_event("00000001", "p1", timepoint=9))
    )
    consumer = psc_stream.Consumer(store_path, lines=source)
    await _run_until(consumer, lambda: psc_stream.state()["upserted"] >= 1)
    assert psc_stream.state()["connects"] == 2


async def test_consumer_416_resumes_live_and_records_a_gap(store_path: Path, monkeypatch):
    monkeypatch.setattr(psc_stream, "RECONNECT_BACKOFF_START_S", 0.01)
    conn = sqlite3.connect(store_path)
    psc_graph.write_meta(conn, **{psc_graph.META_STREAM_TIMEPOINT: "5"})
    conn.close()
    source = _scripted(
        psc_stream.StreamRefusedError(416), _lines(_event("00000001", "p1", timepoint=900))
    )
    consumer = psc_stream.Consumer(store_path, lines=source)
    await _run_until(consumer, lambda: psc_stream.state()["upserted"] >= 1)
    assert source.asked[:2] == [5, None]
    st = psc_stream.state()
    assert st["status_codes"] == {"416": 1}
    assert st["resumed_from"] == "live" and st["gap"] is True and st["gap_since"]


async def test_consumer_429_waits_a_minute(store_path: Path, monkeypatch):
    waits: list[float] = []

    async def _fake_sleep(self, seconds: float) -> None:
        waits.append(seconds)

    monkeypatch.setattr(psc_stream.Consumer, "_sleep", _fake_sleep)
    source = _scripted(
        psc_stream.StreamRefusedError(429), _lines(_event("00000001", "p1", timepoint=1))
    )
    consumer = psc_stream.Consumer(store_path, lines=source)
    await _run_until(consumer, lambda: psc_stream.state()["upserted"] >= 1)
    assert waits[0] == psc_stream.RECONNECT_AFTER_429_S
    assert psc_stream.state()["status_codes"] == {"429": 1}


async def test_consumer_401_stops_for_good(store_path: Path):
    source = _scripted(psc_stream.StreamRefusedError(401))
    consumer = psc_stream.Consumer(store_path, lines=source)
    task = asyncio.create_task(consumer.run())
    await asyncio.wait_for(task, timeout=5.0)
    st = psc_stream.state()
    assert st["enabled"] is False and st["status_codes"] == {"401": 1}
    assert "401" in st["last_error"]
    assert len(source.asked) == 1


async def test_consumer_notices_a_new_seed_and_replays_from_the_day_mark(
    store_path: Path, tmp_path: Path, monkeypatch
):
    """The six-hourly check replaced the file with the next day's seed. The
    consumer sees built_at change, and reconnects from the first timepoint it
    saw on the day the new snapshot was compiled — replaying onto the new
    file what the old one had already applied."""
    monkeypatch.setattr(psc_stream, "RECONNECT_BACKOFF_START_S", 0.01)
    monkeypatch.setattr(psc_stream, "BATCH_MAX_WAIT_S", 0.0)  # flush every event
    replaced = {"done": False}
    snapshot = _snapshot_zip(tmp_path / "snap2.zip")

    async def source(timepoint: int | None) -> AsyncIterator[str]:
        source.asked.append(timepoint)  # type: ignore[attr-defined]
        if len(source.asked) == 1:  # type: ignore[attr-defined]
            yield json.dumps(
                _event("00000001", "p1", timepoint=10, published_at="2026-09-08T09:00:00")
            )
            yield json.dumps(
                _event("00000001", "p2", timepoint=11, published_at="2026-09-08T10:00:00")
            )
            await asyncio.sleep(0.05)
            # A new seed lands underneath us: same path, new inode, new built_at.
            newer = tmp_path / "newer.sqlite"
            psc_graph.build_psc_graph(
                newer, source_url=f"file://{snapshot}", snapshot_date="2026-09-09"
            )
            newer.replace(store_path)
            replaced["done"] = True
            yield json.dumps(
                _event("00000001", "p3", timepoint=12, published_at="2026-09-09T11:00:00")
            )
            await asyncio.sleep(3600)
        else:
            yield json.dumps(
                _event("00000001", "p2", timepoint=11, published_at="2026-09-08T10:00:00")
            )
            yield json.dumps(
                _event("00000001", "p3", timepoint=12, published_at="2026-09-09T11:00:00")
            )
            await asyncio.sleep(3600)

    source.asked = []  # type: ignore[attr-defined]
    consumer = psc_stream.Consumer(store_path, lines=source)
    await _run_until(
        consumer, lambda: psc_stream.state()["reseeds"] >= 1 and len(source.asked) >= 2
    )  # type: ignore[attr-defined]
    # Resumed from the first timepoint seen on 2026-09-08, the day before the
    # new snapshot's date — the day it was compiled to the end of.
    assert source.asked[1] == 10  # type: ignore[attr-defined]
    st = psc_stream.state()
    assert st["resumed_from"] == "day_mark" and st["gap"] is False
    await asyncio.sleep(0.1)
    # The replay landed on the new file: p2 and p3 are there.
    names = {r[1] for r in _rows(store_path, "00000001")}
    assert {"New Owner"} <= names and _meta(store_path)[
        psc_graph.META_SNAPSHOT_DATE
    ] == "2026-09-09"


def test_resume_point_prefers_the_day_before_the_snapshot(store_path: Path):
    c = psc_stream.Consumer(store_path, lines=_scripted())
    c.day_marks = {"2026-09-06": 1, "2026-09-07": 50, "2026-09-08": 90}
    assert c.resume_point_for_new_seed("2026-09-08") == (50, "day_mark")
    assert c.resume_point_for_new_seed("2026-09-09") == (90, "day_mark")
    assert c.resume_point_for_new_seed("2026-09-20") == (1, "day_mark")  # earliest held
    c.day_marks = {}
    assert c.resume_point_for_new_seed("2026-09-09") == (None, "live")
    assert c.resume_point_for_new_seed("not-a-date") == (None, "live")


def test_day_marks_keep_only_the_last_few_days(store_path: Path):
    c = psc_stream.Consumer(store_path, lines=_scripted())
    for i, day in enumerate(
        ["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04", "2026-09-05", "2026-09-06"]
    ):
        c._note_day_mark(i * 10, f"{day}T10:00:00")
        c._note_day_mark(
            i * 10 + 5, f"{day}T11:00:00"
        )  # a later event on the same day does not move the mark
    assert c.day_marks == {"2026-09-03": 20, "2026-09-04": 30, "2026-09-05": 40, "2026-09-06": 50}


# ---------------------------------------------------------------------------
# Wiring and the endpoint
# ---------------------------------------------------------------------------


async def test_run_loop_returns_at_once_without_a_key_or_a_file(store_path: Path, monkeypatch):
    await asyncio.wait_for(psc_stream.run_loop(), timeout=1.0)
    assert psc_stream.state()["enabled"] is False
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_FILE", str(store_path))
    get_settings.cache_clear()
    await asyncio.wait_for(psc_stream.run_loop(), timeout=1.0)  # still no key
    assert psc_stream.state()["enabled"] is False


async def test_run_loop_waits_for_the_file_then_runs_the_consumer(
    tmp_path: Path, store_path: Path, monkeypatch
):
    target = tmp_path / "later.sqlite"
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_FILE", str(target))
    monkeypatch.setenv("COMPANIES_HOUSE_STREAM_KEY", "stream-key")
    monkeypatch.setattr(psc_stream, "STORE_WAIT_S", 0.01)
    get_settings.cache_clear()
    ran: list[Path] = []

    class _FakeConsumer:
        def __init__(self, path: Path, **kwargs) -> None:
            ran.append(path)

        async def run(self) -> None:
            await asyncio.sleep(3600)

        def stop(self) -> None:
            pass

    monkeypatch.setattr(psc_stream, "Consumer", _FakeConsumer)
    task = asyncio.create_task(psc_stream.run_loop())
    await asyncio.sleep(0.05)
    assert ran == []  # the file is not there yet
    target.write_bytes(store_path.read_bytes())
    await asyncio.sleep(0.05)
    assert ran == [target]
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def test_lifespan_starts_the_stream_only_with_file_and_key(store_path: Path, monkeypatch):
    from opencheck import app as app_module

    started: list[str] = []

    async def _fake_loop() -> None:
        started.append("stream")
        await asyncio.sleep(3600)

    async def _noop() -> None:
        return None

    monkeypatch.setattr(psc_stream, "run_loop", _fake_loop)
    monkeypatch.setattr(app_module, "_warm_caches_background", _noop)
    monkeypatch.setenv("OPENCHECK_MIRROR_REFRESH_INTERVAL_S", "0")
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_REFRESH_INTERVAL_S", "0")
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_FILE", str(store_path))
    get_settings.cache_clear()
    with TestClient(app):
        pass
    assert started == []
    monkeypatch.setenv("COMPANIES_HOUSE_STREAM_KEY", "stream-key")
    get_settings.cache_clear()
    with TestClient(app):
        pass
    assert started == ["stream"]


def test_pscgraph_endpoint_carries_the_stream_state(store_path: Path, monkeypatch):
    monkeypatch.setenv("OPENCHECK_PSC_GRAPH_DB_FILE", str(store_path))
    get_settings.cache_clear()
    conn = sqlite3.connect(store_path)
    psc_graph.write_meta(
        conn,
        **{
            psc_graph.META_STREAM_TIMEPOINT: "4242",
            psc_graph.META_STREAM_AT: "2026-09-08T14:00:05",
        },
    )
    conn.close()
    body = TestClient(app).get("/pscgraph").json()
    assert body["store"]["stream_timepoint"] == 4242
    assert body["store"]["stream_published_at"] == "2026-09-08T14:00:05"
    assert body["stream"]["enabled"] is False and body["stream"]["events"] == 0
    assert set(body["stream"]) >= {
        "connected",
        "connects",
        "by_type",
        "upserted",
        "ceased",
        "deleted",
        "gap",
        "timepoint",
        "status_codes",
        "reseeds",
    }
    text = json.dumps(body)
    for secret in ("Timpson", "Holroyd", "00675216", "stream-key"):
        assert secret not in text


def test_stream_key_is_read_from_the_environment(monkeypatch):
    monkeypatch.setenv("COMPANIES_HOUSE_STREAM_KEY", "abc")
    get_settings.cache_clear()
    s = get_settings()
    assert s.companies_house_stream_key == "abc"
    assert s.psc_stream_enabled is True
    assert s.psc_stream_url.startswith("https://stream.companieshouse.gov.uk/")
