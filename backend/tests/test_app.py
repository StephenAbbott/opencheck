"""Smoke tests for the Phase 0 FastAPI surface."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.config import get_settings


@pytest.fixture(autouse=True)
def _isolated_data_root(monkeypatch, tmp_path):
    """Isolate from any real cache the developer's machine has under
    ``data/cache/live/``. Without this, adapters can short-circuit to
    cached real-API responses that don't match what the smoke-tests
    assume (Phase 0 stubs)."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_health_endpoint(client: TestClient) -> None:
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert "version" in body
    assert "allow_live" in body


def test_sources_endpoint_lists_all_adapters(client: TestClient) -> None:
    r = client.get("/sources")
    assert r.status_code == 200
    ids = {s["id"] for s in r.json()["sources"]}
    assert ids == {
        "ariregister",
        "bolagsverket",
        "companies_house",
        "gleif",
        "inpi",
        "opencorporates",
        "brightquery",
        "opensanctions",
        "everypolitician",
        "wikidata",
        "opentender",
        "zefix",
        "kvk",
    }


def test_search_entity_fans_out_to_entity_adapters(client: TestClient) -> None:
    r = client.get("/search", params={"q": "Rosneft", "kind": "entity"})
    assert r.status_code == 200
    body = r.json()
    assert body["query"] == "Rosneft"
    assert body["kind"] == "entity"
    source_ids = {h["source_id"] for h in body["hits"]}
    # EveryPolitician is person-only and should not appear.
    assert "everypolitician" not in source_ids
    assert "companies_house" in source_ids
    assert "gleif" in source_ids


def test_search_person_skips_entity_only_adapters(client: TestClient) -> None:
    r = client.get("/search", params={"q": "Alice Example", "kind": "person"})
    assert r.status_code == 200
    body = r.json()
    source_ids = {h["source_id"] for h in body["hits"]}
    # GLEIF is entity-only.
    assert "gleif" not in source_ids
    assert "everypolitician" in source_ids
    assert "wikidata" in source_ids
