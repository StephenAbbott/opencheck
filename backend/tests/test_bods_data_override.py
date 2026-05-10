"""Tests for the Open Ownership BODS-bundle override layer.

When ``data/cache/bods_data/<source>/<key>.jsonl`` exists for a given
LEI / company number, ``_safe_deepen`` should serve those statements
verbatim instead of running the live mapper. This covers the
visualisation + AMLA layer-counting use case where Open Ownership's
processed output is canonical.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pytest_httpx import HTTPXMock

from opencheck import bods_data
from opencheck.app import app
from opencheck.config import get_settings


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path: Path):
    """Tmp data root + live mode so the GLEIF mock fires."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.delenv("OPENSANCTIONS_API_KEY", raising=False)
    monkeypatch.delenv("COMPANIES_HOUSE_API_KEY", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _seed_bundle(
    tmp_path: Path, source: str, key: str, statements: list[dict]
) -> Path:
    target = tmp_path / "cache" / "bods_data" / source / f"{key}.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as fh:
        for stmt in statements:
            fh.write(json.dumps(stmt) + "\n")
    return target


# ---------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------


def test_load_bundle_returns_none_when_file_missing() -> None:
    assert bods_data.gleif_bundle_for_lei("ZZZZ00000000000000ZZ") is None
    assert bods_data.uk_bundle_for_company_number("00000000") is None


def test_load_bundle_parses_jsonl(tmp_path: Path) -> None:
    _seed_bundle(
        tmp_path,
        "gleif",
        "213800LH1BZH3DI6G760",
        [
            {"recordType": "entity", "statementId": "e1"},
            {"recordType": "relationship", "statementId": "r1"},
        ],
    )
    bundle = bods_data.gleif_bundle_for_lei("213800LH1BZH3DI6G760")
    assert bundle is not None
    assert [s["statementId"] for s in bundle] == ["e1", "r1"]


def test_load_bundle_skips_blank_lines(tmp_path: Path) -> None:
    target = tmp_path / "cache" / "bods_data" / "uk" / "00102498.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        '{"recordType": "entity", "statementId": "e1"}\n'
        '\n'  # blank line
        '{"recordType": "person", "statementId": "p1"}\n'
    )
    bundle = bods_data.uk_bundle_for_company_number("00102498")
    assert bundle is not None
    assert [s["statementId"] for s in bundle] == ["e1", "p1"]


def test_load_bundle_raises_on_bad_json(tmp_path: Path) -> None:
    target = tmp_path / "cache" / "bods_data" / "gleif" / "BAD0000000000000BAD0.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text('{"recordType": "entity"}\nnot-json\n')
    with pytest.raises(ValueError, match="invalid JSON"):
        bods_data.gleif_bundle_for_lei("BAD0000000000000BAD0")


def test_lei_lookup_is_case_insensitive(tmp_path: Path) -> None:
    """Bundle filename + the loader both normalise to upper case."""
    _seed_bundle(
        tmp_path,
        "gleif",
        "213800LH1BZH3DI6G760",
        [{"recordType": "entity", "statementId": "e1"}],
    )
    assert bods_data.gleif_bundle_for_lei("213800lh1bzh3di6g760") is not None


# ---------------------------------------------------------------------
# Endpoint integration — /deepen + /lookup
# ---------------------------------------------------------------------


def test_deepen_gleif_uses_override_bundle_instead_of_live_mapper(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """Even when GLEIF returns a thin live record, /deepen should
    surface the canonical Open Ownership bundle verbatim."""
    lei = "213800LH1BZH3DI6G760"
    api = "https://api.gleif.org/api/v1"

    # Live GLEIF returns a *single* entity statement when we use the
    # adapter's mapper. Mock it but expect /deepen to ignore it.
    httpx_mock.add_response(
        url=f"{api}/lei-records/{lei}",
        json={
            "data": {
                "id": lei,
                "type": "lei-records",
                "attributes": {
                    "lei": lei,
                    "entity": {
                        "legalName": {"name": "BP P.L.C."},
                        "jurisdiction": "GB",
                    },
                    "registration": {"status": "ISSUED"},
                },
            }
        },
    )
    for path in (
        "direct-parent",
        "direct-parent-reporting-exception",
        "ultimate-parent",
        "ultimate-parent-reporting-exception",
    ):
        httpx_mock.add_response(
            url=f"{api}/lei-records/{lei}/{path}", status_code=404
        )

    # Now seed a richer override bundle (subject + parent + relationship).
    _seed_bundle(
        tmp_path,
        "gleif",
        lei,
        [
            {
                "recordType": "entity",
                "statementId": "e-subject",
                "recordDetails": {"name": "BP P.L.C."},
            },
            {
                "recordType": "entity",
                "statementId": "e-parent",
                "recordDetails": {"name": "BP Holdings Ltd"},
            },
            {
                "recordType": "relationship",
                "statementId": "r-1",
                "recordDetails": {
                    "subject": "e-subject",
                    "interestedParty": "e-parent",
                },
            },
        ],
    )

    client = TestClient(app)
    r = client.get("/deepen", params={"source": "gleif", "hit_id": lei})
    assert r.status_code == 200
    body = r.json()
    statement_ids = {s["statementId"] for s in body["bods"]}
    # Override bundle's IDs win — none of the live-mapper IDs leak through.
    assert statement_ids == {"e-subject", "e-parent", "r-1"}


def test_deepen_companies_house_uses_uk_override_bundle(tmp_path: Path) -> None:
    """When a CH bundle exists for a company number, /deepen should
    return it instead of attempting a live fetch (the adapter would
    return a stub here anyway, since no API key is set)."""
    coh = "00102498"
    _seed_bundle(
        tmp_path,
        "uk",
        coh,
        [
            {
                "recordType": "entity",
                "statementId": "uk-e-1",
                "recordDetails": {"name": "BP P.L.C."},
            },
            {
                "recordType": "person",
                "statementId": "uk-p-1",
                "recordDetails": {"personType": "knownPerson"},
            },
            {
                "recordType": "relationship",
                "statementId": "uk-r-1",
                "recordDetails": {
                    "subject": "uk-e-1",
                    "interestedParty": "uk-p-1",
                },
            },
        ],
    )

    client = TestClient(app)
    r = client.get(
        "/deepen", params={"source": "companies_house", "hit_id": coh}
    )
    assert r.status_code == 200
    body = r.json()
    statement_ids = {s["statementId"] for s in body["bods"]}
    assert statement_ids == {"uk-e-1", "uk-p-1", "uk-r-1"}


def test_deepen_falls_back_to_live_when_no_override_bundle(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """No bundle on disk → live mapper produces whatever it produces."""
    lei = "ZZZZ00000000000000ZZ"
    api = "https://api.gleif.org/api/v1"
    httpx_mock.add_response(
        url=f"{api}/lei-records/{lei}",
        json={
            "data": {
                "id": lei,
                "type": "lei-records",
                "attributes": {
                    "lei": lei,
                    "entity": {
                        "legalName": {"name": "Stub Co"},
                        "jurisdiction": "GB",
                    },
                },
            }
        },
    )
    for path in (
        "direct-parent",
        "direct-parent-reporting-exception",
        "ultimate-parent",
        "ultimate-parent-reporting-exception",
    ):
        httpx_mock.add_response(
            url=f"{api}/lei-records/{lei}/{path}", status_code=404
        )

    client = TestClient(app)
    r = client.get("/deepen", params={"source": "gleif", "hit_id": lei})
    assert r.status_code == 200
    # Live mapper returns one entity statement.
    body = r.json()
    assert len(body["bods"]) >= 1
    assert all(s["recordType"] == "entity" for s in body["bods"])
