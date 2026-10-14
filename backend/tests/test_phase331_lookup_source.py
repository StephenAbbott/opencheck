"""Phase 331 — the per-source retry carries provenance, as the run does.

Ticket "Per-source retry (/lookup-source) drops provenance": on 13 Oct 2026
``/lookup-source?lei=5493005RZJHJT5PNHY10&source_id=jar_lithuania`` returned
the right register record with ``liveness: "stub"`` and ``retrieved_at: null``,
while the full ``/lookup`` for the same LEI said ``snapshot`` with a retrieval
time. The retry awaited the dispatched coroutines with no provenance scope and
never stamped a hit, and its response had no ``source_liveness`` for a client
to merge. It now runs each source through ``_run_source`` and reads the result
with ``_source_outcome`` — the pipeline's own two functions.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck import provenance
from opencheck.app import app
from opencheck.sources import REGISTRY
from tests.test_lookup_pipeline import _seed_bundle

LEI = "213800LH1BZH3DI6G760"
RETRIEVED = datetime(2026, 10, 13, 7, 38, 47, tzinfo=timezone.utc)
CUT = datetime(2026, 10, 13, tzinfo=timezone.utc)

_BUNDLE = {"profile": {"company_name": "Bundle Co P.L.C."}, "is_stub": False}


def _recording_fetch(kind: str):
    """A Companies House fetch that reads the way ``kind`` says."""

    async def _fetch(local_id: str, **_kw) -> dict:
        if kind == "live":
            provenance.record_live("test")
        elif kind == "cached":
            provenance.record_cached(RETRIEVED, "test")
        elif kind == "snapshot":
            provenance.record_snapshot(retrieved_at=RETRIEVED, source_as_of=CUT)
        elif kind == "stub":
            return {"is_stub": True}
        return dict(_BUNDLE)

    return _fetch


@pytest.fixture
def client(monkeypatch, tmp_path: Path) -> TestClient:
    import opencheck.sources.climatetrace as _ct
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    monkeypatch.setattr(_ct, "_lei_index", {})
    monkeypatch.setattr(_ct, "_entity_index", {})
    get_settings.cache_clear()
    _seed_bundle(tmp_path, LEI)
    yield TestClient(app)
    get_settings.cache_clear()


def _retry(client: TestClient) -> dict:
    r = client.get("/lookup-source", params={"lei": LEI, "source_id": "companies_house"})
    assert r.status_code == 200, r.text
    return r.json()


# ids avoid the bare word "live": conftest skips any test whose keywords hold it.
@pytest.mark.parametrize(
    "kind", ["live", "cached", "snapshot"], ids=["fetched", "cached", "snapshot"]
)
def test_a_retried_hit_says_what_the_retry_read(client, monkeypatch, kind) -> None:
    monkeypatch.setattr(REGISTRY["companies_house"], "fetch", _recording_fetch(kind))
    body = _retry(client)

    assert [h["source_id"] for h in body["hits"]] == ["companies_house"]
    hit = body["hits"][0]
    entry = body["source_liveness"]["companies_house"]
    assert hit["liveness"] == kind == entry["liveness"]
    assert hit["retrieved_at"] is not None
    assert entry["retrieved_at"] is not None
    if kind == "snapshot":
        assert entry["source_as_of"].startswith("2026-10-13")
    assert body["error"] is None and body["error_type"] is None


def test_a_stub_answer_is_no_hit_and_says_stub(client, monkeypatch) -> None:
    monkeypatch.setattr(REGISTRY["companies_house"], "fetch", _recording_fetch("stub"))
    body = _retry(client)
    assert body["hits"] == []
    assert body["error"] is None
    assert body["source_liveness"]["companies_house"]["liveness"] == "stub"


def test_a_failed_retry_names_its_error_type(client, monkeypatch) -> None:
    async def _boom(local_id: str, **_kw) -> dict:
        raise RuntimeError("upstream fell over")

    monkeypatch.setattr(REGISTRY["companies_house"], "fetch", _boom)
    body = _retry(client)
    assert body["hits"] == []
    assert body["error"] and body["error_type"] == "fetch_error"
    assert body["source_liveness"]["companies_house"]["liveness"] == "stub"


def test_a_failed_retry_falls_back_to_the_stored_bundle(
    client, monkeypatch, tmp_path: Path
) -> None:
    """The run serves a stored Open Ownership bundle when the live source
    fails; a retry that fails must not take the card away again."""
    uk = tmp_path / "cache" / "bods_data" / "uk" / "12345678.jsonl"
    uk.parent.mkdir(parents=True, exist_ok=True)
    uk.write_text(json.dumps({
        "statementId": "uk-subject", "recordType": "entity",
        "recordDetails": {"name": "Bundle Co P.L.C."},
        "publicationDetails": {"publicationDate": "2026-09-01"},
    }) + "\n")

    async def _boom(local_id: str, **_kw) -> dict:
        raise RuntimeError("upstream fell over")

    monkeypatch.setattr(REGISTRY["companies_house"], "fetch", _boom)
    body = _retry(client)
    assert [h["hit_id"] for h in body["hits"]] == ["12345678"]
    assert body["error"] is None
    assert body["hits"][0]["liveness"] == "snapshot"
    assert body["source_liveness"]["companies_house"]["liveness"] == "snapshot"


@pytest.mark.parametrize(
    "kind",
    ["live", "cached", "snapshot", "stub"],
    ids=["fetched", "cached", "snapshot", "stub"],
)
def test_retry_and_run_agree_on_provenance(client, monkeypatch, kind) -> None:
    """Parity: what the retry says about a source is what the run says."""
    monkeypatch.setattr(REGISTRY["companies_house"], "fetch", _recording_fetch(kind))
    run = client.get("/lookup", params={"lei": LEI, "refresh": "true"}).json()
    retry = _retry(client)

    run_entry = run["source_liveness"]["companies_house"]
    retry_entry = retry["source_liveness"]["companies_house"]
    for key in ("liveness", "label", "source_as_of", "detail"):
        assert retry_entry[key] == run_entry[key], key
    run_hits = [h for h in run["hits"] if h["source_id"] == "companies_house"]
    assert [h["liveness"] for h in retry["hits"]] == [h["liveness"] for h in run_hits]
