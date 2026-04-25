"""Integration tests for /export."""

from __future__ import annotations

import io
import json
import zipfile

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.config import get_settings


@pytest.fixture(autouse=True)
def _no_live(monkeypatch):
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    monkeypatch.delenv("OPENSANCTIONS_API_KEY", raising=False)
    monkeypatch.delenv("COMPANIES_HOUSE_API_KEY", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_export_json_returns_bods_array(client: TestClient) -> None:
    r = client.get("/export", params={"q": "Vladimir Putin", "kind": "person", "format": "json"})
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/json")
    cd = r.headers["content-disposition"]
    assert "attachment" in cd
    assert "vladimir-putin" in cd
    assert ".json" in cd

    body = json.loads(r.content)
    assert isinstance(body, list)
    # The Putin demo deepens 3 hits, each producing 1 BODS statement.
    assert len(body) >= 1
    for stmt in body:
        # BODS v0.4 nested shape — every statement has a recordType.
        assert "recordType" in stmt


def test_export_jsonl_emits_one_statement_per_line(client: TestClient) -> None:
    r = client.get("/export", params={"q": "Rosneft", "kind": "entity", "format": "jsonl"})
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/x-ndjson")
    text = r.content.decode("utf-8")
    lines = [ln for ln in text.split("\n") if ln.strip()]
    assert lines, "no statements in jsonl export"
    for ln in lines:
        # Each line should be a self-contained JSON object.
        obj = json.loads(ln)
        assert obj.get("recordType") in {"entity", "person", "relationship"}


def test_export_zip_contains_full_bundle(client: TestClient) -> None:
    r = client.get("/export", params={"q": "Vladimir Putin", "kind": "person", "format": "zip"})
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/zip"

    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        names = zf.namelist()
        # All four files exist under a single top-level directory.
        prefixes = {n.split("/", 1)[0] for n in names}
        assert len(prefixes) == 1, f"expected single top-level dir, got {prefixes}"
        prefix = next(iter(prefixes))
        assert f"{prefix}/bods.json" in names
        assert f"{prefix}/bods.jsonl" in names
        assert f"{prefix}/manifest.json" in names
        assert f"{prefix}/LICENSES.md" in names

        manifest = json.loads(zf.read(f"{prefix}/manifest.json"))
        assert manifest["query"] == "Vladimir Putin"
        assert manifest["kind"] == "person"
        # Putin demo bridges three person sources via Q-ID.
        assert "wikidata" in manifest["contributing_source_ids"]
        assert "opensanctions" in manifest["contributing_source_ids"]
        assert "everypolitician" in manifest["contributing_source_ids"]
        # Risk signals + cross-source links travel with the bundle.
        codes = {sig["code"] for sig in manifest["risk_signals"]}
        assert "PEP" in codes


def test_export_zip_licenses_md_flags_nc_sources(client: TestClient) -> None:
    """OS + EveryPolitician are CC BY-NC — LICENSES.md should call this out."""
    r = client.get(
        "/export", params={"q": "Vladimir Putin", "kind": "person", "format": "zip"}
    )
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        prefix = zf.namelist()[0].split("/", 1)[0]
        md = zf.read(f"{prefix}/LICENSES.md").decode("utf-8")

    assert "OpenSanctions" in md
    assert "CC-BY-NC-4.0" in md
    assert "non-commercial" in md.lower() or "Non-commercial" in md
    # Re-use guidance is always present.
    assert "Re-use guidance" in md


def test_export_unknown_format_rejected(client: TestClient) -> None:
    r = client.get(
        "/export", params={"q": "BP", "kind": "entity", "format": "yaml"}
    )
    # FastAPI Query pattern validation returns 422 before our handler runs.
    assert r.status_code == 422


def test_export_filename_slug_strips_unsafe_chars(client: TestClient) -> None:
    """Filename should be slugified (no spaces, no special chars)."""
    r = client.get(
        "/export", params={"q": "Vladimir Putin!", "kind": "person", "format": "json"}
    )
    cd = r.headers["content-disposition"]
    # "Vladimir Putin!" → "vladimir-putin" (slugified).
    assert "vladimir-putin" in cd
    assert " " not in cd.split("filename=", 1)[1]
    assert "!" not in cd
