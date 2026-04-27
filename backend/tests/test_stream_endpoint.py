"""Smoke test for the SSE /stream endpoint."""

from __future__ import annotations

from fastapi.testclient import TestClient

from opencheck.app import app


def test_stream_emits_source_started_hits_completed_and_done() -> None:
    client = TestClient(app)

    # EventSourceResponse streams chunked SSE. We read the body once and
    # check the event markers are all present in expected order.
    with client.stream("GET", "/stream", params={"q": "acme", "kind": "entity"}) as r:
        assert r.status_code == 200
        body = "".join(chunk for chunk in r.iter_text())

    assert "event: source_started" in body
    assert "event: hit" in body
    assert "event: source_completed" in body
    assert "event: done" in body

    # Every stub adapter that supports entity should have emitted started + completed.
    for source in ("companies_house", "gleif", "opensanctions", "wikidata"):
        assert source in body


def test_stream_skips_non_matching_adapters() -> None:
    client = TestClient(app)
    with client.stream("GET", "/stream", params={"q": "alice", "kind": "person"}) as r:
        body = "".join(chunk for chunk in r.iter_text())

    # GLEIF is entity-only — should not appear when we ask for a person.
    assert '"gleif"' not in body
    # EveryPolitician is person-only — should appear.
    assert "everypolitician" in body


def test_deepen_returns_bods_for_stub_companies_house() -> None:
    # With allow_live=false the CH fetch returns a stub, so no BODS is produced.
    client = TestClient(app)
    r = client.get("/deepen", params={"source": "companies_house", "hit_id": "00000000"})
    assert r.status_code == 200
    body = r.json()
    assert body["source_id"] == "companies_house"
    assert body["raw"].get("is_stub") is True
    assert body["bods"] == []
