"""Phase 244 — a slow Wikidata must not fail the whole lookup.

``_resolve_ctx`` resolves the anchor's Wikidata QID before the fan-out. That
call was unguarded, so when the Wikidata Query Service timed out the
``httpx.ReadTimeout`` escaped the pipeline and every fresh ``/lookup``
answered HTTP 500 after ~20 s (Equinor and Rosneft in production, 24 Sept
2026). The QID is optional enrichment: the lookup must complete with the
other sources and say that Wikidata did not answer — never that the company
has no Wikidata record.
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.config import get_settings
from opencheck.routers.lookup import _dispatch, _resolve_ctx
from opencheck.sources import REGISTRY

LEI = "213800LH1BZH3DI6G760"


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    import opencheck.sources.climatetrace as _ct

    monkeypatch.setattr(_ct, "_lei_index", {})
    monkeypatch.setattr(_ct, "_entity_index", {})
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _seed_bundle(tmp_path: Path) -> None:
    target = tmp_path / "cache" / "bods_data" / "gleif" / f"{LEI}.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(
            {
                "statementId": "e-subject",
                "recordType": "entity",
                "recordDetails": {
                    "name": "Bundle Co P.L.C.",
                    "jurisdiction": {"name": "United Kingdom", "code": "GB"},
                    "identifiers": [
                        {"id": LEI, "scheme": "XI-LEI"},
                        {"id": "12345678", "scheme": "GB-COH"},
                    ],
                },
            }
        )
        + "\n"
    )


def _wdqs_times_out(monkeypatch) -> None:
    """The QID lookup fails the way it did in production: a transport
    timeout, whose message is empty."""

    async def timing_out(_lei: str) -> str | None:
        raise httpx.ReadTimeout("")

    monkeypatch.setattr(REGISTRY["wikidata"], "find_qid_by_lei", timing_out)


def _stream_events(client: TestClient) -> list[tuple[str, dict]]:
    with client.stream("GET", "/lookup-stream", params={"lei": LEI}) as r:
        assert r.status_code == 200
        body = "".join(r.iter_text())
    events: list[tuple[str, dict]] = []
    current: str | None = None
    for line in body.splitlines():
        if line.startswith("event: "):
            current = line[len("event: "):]
        elif line.startswith("data: ") and current:
            events.append((current, json.loads(line[len("data: "):])))
            current = None
    return events


# --- the context ------------------------------------------------------------


async def test_resolve_ctx_survives_a_qid_timeout(tmp_path, monkeypatch) -> None:
    _seed_bundle(tmp_path)
    _wdqs_times_out(monkeypatch)

    ctx, _bundle = await _resolve_ctx(LEI)

    assert ctx.legal_name == "Bundle Co P.L.C."
    assert ctx.qid is None
    assert isinstance(ctx.qid_error, httpx.ReadTimeout)
    assert "wikidata_qid" not in ctx.derived


async def test_a_failed_qid_lookup_dispatches_wikidata_as_a_failure(
    tmp_path, monkeypatch
) -> None:
    """Wikidata stays applicable — we asked and it did not answer — and its
    task raises the original failure, so the dispatch loop reports it."""
    _seed_bundle(tmp_path)
    _wdqs_times_out(monkeypatch)
    ctx, _ = await _resolve_ctx(LEI)

    tasks = dict(_dispatch(ctx))
    assert "wikidata" in tasks
    with pytest.raises(httpx.ReadTimeout):
        await tasks["wikidata"]
    # The QID feeds Wikirate only as a fallback; it still runs on the LEI.
    for coro in tasks.values():
        coro.close()


async def test_no_qid_is_still_not_a_failure(tmp_path, monkeypatch) -> None:
    """Wikidata answering "no mapping" is unchanged: not applicable, no error."""
    _seed_bundle(tmp_path)

    async def no_mapping(_lei: str) -> str | None:
        return None

    monkeypatch.setattr(REGISTRY["wikidata"], "find_qid_by_lei", no_mapping)
    ctx, _ = await _resolve_ctx(LEI)

    assert ctx.qid is None and ctx.qid_error is None
    tasks = _dispatch(ctx)
    assert "wikidata" not in {sid for sid, _ in tasks}
    for _sid, coro in tasks:
        coro.close()


# --- the endpoints ----------------------------------------------------------


def test_lookup_completes_and_names_wikidata_as_failed(
    client: TestClient, tmp_path, monkeypatch
) -> None:
    _seed_bundle(tmp_path)
    _wdqs_times_out(monkeypatch)

    r = client.get("/lookup", params={"lei": LEI})

    assert r.status_code == 200, r.text
    body = r.json()
    assert any(h["source_id"] == "gleif" for h in body["hits"])
    assert body["errors"]["wikidata"].startswith("ReadTimeout")
    degraded = [d for d in body["degraded_sources"] if d["source_id"] == "wikidata"]
    assert degraded and degraded[0]["reason"] == "timeout"


def test_stream_completes_and_reports_a_wikidata_source_error(
    client: TestClient, tmp_path, monkeypatch
) -> None:
    _seed_bundle(tmp_path)
    _wdqs_times_out(monkeypatch)

    events = _stream_events(client)
    names = [n for n, _ in events]

    assert "error" not in names
    assert "done" in names
    applicable = next(p for n, p in events if n == "sources_applicable")["source_ids"]
    assert "wikidata" in applicable
    started = {p["source_id"] for n, p in events if n == "source_started"}
    assert "wikidata" in started
    errors = [p for n, p in events if n == "source_error" and p["source_id"] == "wikidata"]
    assert len(errors) == 1
    assert errors[0]["error"].startswith("ReadTimeout")


def test_per_source_retry_reports_then_recovers(
    client: TestClient, tmp_path, monkeypatch
) -> None:
    """The "Retry source" button re-resolves the QID, so a retry after WDQS
    recovers really does retry, rather than 404ing as not applicable."""
    _seed_bundle(tmp_path)
    _wdqs_times_out(monkeypatch)

    r = client.get("/lookup-source", params={"lei": LEI, "source_id": "wikidata"})
    assert r.status_code == 200
    assert r.json()["error"].startswith("ReadTimeout")

    wikidata = REGISTRY["wikidata"]

    async def answers(_lei: str) -> str | None:
        return "Q42"

    async def fetch(qid: str) -> dict:
        return {"source_id": "wikidata", "qid": qid, "bindings": [], "summary": {},
                "is_stub": True}

    monkeypatch.setattr(wikidata, "find_qid_by_lei", answers)
    monkeypatch.setattr(wikidata, "fetch", fetch)
    r = client.get("/lookup-source", params={"lei": LEI, "source_id": "wikidata"})
    assert r.status_code == 200
    assert r.json()["error"] is None


# --- the adapter ------------------------------------------------------------


async def test_class_roots_fails_open_on_a_transport_error(monkeypatch) -> None:
    """Phase 240's class-roots query promises that a failed query leaves the
    classes out. A ReadTimeout used to raise instead, and took the whole
    Wikidata card with it after the main fetch had already answered."""
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    wikidata = REGISTRY["wikidata"]

    async def timing_out(_query: str, *, cache_key: str) -> dict:
        raise httpx.ReadTimeout("")

    monkeypatch.setattr(wikidata, "_sparql", timing_out)
    assert await wikidata._class_roots({"Q987654321244"}) == {}


async def test_fetch_survives_a_class_roots_timeout(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    wikidata = REGISTRY["wikidata"]
    uri = "http://www.wikidata.org/entity/"
    owner_row = {
        "owner": {"type": "uri", "value": f"{uri}Q999244"},
        "ownerLabel": {"type": "literal", "value": "Owner Body"},
        "ownerClass": {"type": "uri", "value": f"{uri}Q987654321244"},
        "prop": {"type": "literal", "value": "P127"},
    }

    async def sparql(_query: str, *, cache_key: str) -> dict:
        if "class-roots-query" in cache_key:
            raise httpx.ReadTimeout("")
        if "/ownership-v2/" in cache_key:
            return {"results": {"bindings": [owner_row]}}
        if "/roleholders-v2/" in cache_key:
            return {"results": {"bindings": []}}
        return {"results": {"bindings": [{
            "item": {"type": "uri", "value": f"{uri}Q244"},
            "label": {"type": "literal", "value": "Example Company"},
            "instance": {"type": "uri", "value": f"{uri}Q4830453"},
            "instanceLabel": {"type": "literal", "value": "business"},
        }]}}

    monkeypatch.setattr(wikidata, "_sparql", sparql)
    result = await wikidata.fetch("Q244")

    assert not result.get("is_stub")
    assert result["summary"]["label"] == "Example Company"
    assert result["summary"]["is_entity"]
    # The owner is still read, classified on its direct class alone.
    assert [o.get("name") or o.get("label") for o in result["summary"]["controlling_owners"]]
