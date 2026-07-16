"""Tests for the FullCheck network export (POST /export-network) + bods→Cypher."""

from __future__ import annotations

import io
import json
import zipfile

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.bods import (
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    to_cypher,
)
from opencheck.config import get_settings


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _network() -> list[dict]:
    e = make_entity_statement(
        source_id="gleif", local_id="acme", name="Acme Ltd",
        identifiers=[{"id": "5493001KJTIIGC8Y1R12", "scheme": "XI-LEI", "schemeName": "LEI"}],
    )
    p = make_person_statement(source_id="gleif", local_id="jane", full_name="Jane Roe")
    r = make_relationship_statement(
        source_id="gleif", local_id="r1",
        subject_statement_id=e["statementId"], interested_party_statement_id=p["statementId"],
        interests=[{"type": "shareholding", "beneficialOwnershipOrControl": True}],
    )
    return [e, p, r]


def test_to_cypher_emits_nodes_and_owner_to_owned_edge():
    cy = to_cypher(_network())
    assert "MERGE (n:Entity" in cy and "Acme Ltd" in cy
    assert "MERGE (n:Person" in cy and "Jane Roe" in cy
    assert "OWNS_OR_CONTROLS" in cy
    # The edge runs owner (interested party) → owned (subject).
    net = _network()
    person_id, entity_id = net[1]["statementId"], net[0]["statementId"]
    assert f"(a {{id: '{person_id}'}}), (b {{id: '{entity_id}'}})" in cy


def test_to_cypher_escapes_quotes():
    e = make_entity_statement(source_id="gleif", local_id="x", name="O'Brien & Sons")
    assert "O\\'Brien & Sons" in to_cypher([e])


def test_export_network_cypher(client):
    r = client.post("/export-network", json={"bods": _network(), "format": "cypher"})
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/plain")
    assert "-fullcheck-network-" in r.headers["content-disposition"]
    assert "OWNS_OR_CONTROLS" in r.text


def test_export_network_senzing(client):
    r = client.post("/export-network", json={"bods": _network(), "format": "senzing"})
    assert r.status_code == 200
    first = json.loads(r.text.splitlines()[0])
    assert first["DATA_SOURCE"] == "OPENCHECK"


def test_export_network_ftm(client):
    r = client.post("/export-network", json={"bods": _network(), "format": "ftm"})
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/x-ndjson")
    assert "-fullcheck-network-" in r.headers["content-disposition"]
    entities = [json.loads(ln) for ln in r.text.splitlines() if ln.strip()]
    assert {e["schema"] for e in entities} >= {"Company", "Person"}
    for e in entities:
        assert set(e) == {"id", "schema", "properties"}


def test_export_network_zip_bundles_everything(client):
    r = client.post("/export-network", json={"bods": _network(), "format": "zip", "slug": "shell-net"})
    assert r.status_code == 200
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        names = zf.namelist()
        prefix = names[0].split("/", 1)[0]
        for f in ("bods.json", "bods.jsonl", "bods.xml", "senzing.jsonl", "ftm.jsonl",
                  "network.cypher", "manifest.json", "LICENSES.md"):
            assert f"{prefix}/{f}" in names, f
        manifest = json.loads(zf.read(f"{prefix}/manifest.json"))
    assert manifest["kind"] == "fullcheck-network"
    assert manifest["bods_statement_count"] == 3
    assert manifest["senzing_record_count"] >= 1
    assert manifest["ftm_entity_count"] >= 1
    # GLEIF source resolved from the BODS source blocks → licensing + attribution.
    assert "gleif" in manifest["contributing_source_ids"]
