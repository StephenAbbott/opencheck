"""Google AML AI export — bods/aml_ai.py wrapper + /export?format=amlai.

The mapping itself (BODS → party / supplementary-data / account-link rows)
lives upstream in bods-aml-ai; these tests pin OpenCheck's packaging: the
ownership-signal encoding survives the round trip, the zip membership, NDJSON
validity, manifest counts, and the route wiring on /export and
/export-network.
"""

from __future__ import annotations

import io
import json
import zipfile

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.bods import aml_ai_counts, build_aml_ai_files, map_to_aml_ai
from opencheck.config import get_settings
from opencheck.routers.lookup import LookupResponse
from opencheck.sources.base import SearchKind


@pytest.fixture(autouse=True)
def _clear_settings():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _bods() -> list[dict]:
    return [
        {"statementId": "ent-1", "recordId": "ent-1", "recordType": "entity",
         "recordStatus": "new", "statementDate": "2026-01-01",
         "recordDetails": {"entityType": {"type": "registeredEntity"},
                           "name": "Northwind Logistics Ltd", "isComponent": False,
                           "identifiers": [{"scheme": "XI-LEI", "id": "2138000000000000A001"}]},
         "source": {"description": "UK Companies House"}},
        {"statementId": "per-1", "recordId": "per-1", "recordType": "person",
         "recordStatus": "new", "statementDate": "2026-01-01",
         "recordDetails": {"personType": "knownPerson", "isComponent": False,
                           "names": [{"type": "individual", "fullName": "Jane Smith"}]},
         "source": {"description": "UK Companies House"}},
        {"statementId": "rel-1", "recordId": "rel-1", "recordType": "relationship",
         "recordStatus": "new", "statementDate": "2026-01-01",
         "recordDetails": {"subject": "ent-1", "interestedParty": "per-1",
                           "isComponent": False,
                           "interests": [{"type": "shareholding",
                                          "directOrIndirect": "direct",
                                          "beneficialOwnershipOrControl": True,
                                          "share": {"exact": 51}}]},
         "source": {"description": "UK Companies House"}},
    ]


# ---- bods/aml_ai.py ------------------------------------------------------


def test_map_to_aml_ai_encodes_ownership():
    tables = map_to_aml_ai(_bods())
    parties = tables["party"]
    assert {p["type"] for p in parties} == {"CONSUMER", "COMPANY"}

    supp = tables["party_supplementary_data"]
    ids = {r.get("party_supplementary_data_id") for r in supp}
    # The owner carries a percentage signal aimed at the subject, and the BO flag.
    assert any(i and i.startswith("bo_ownership_pct_") for i in ids)
    assert "bo_is_beneficial_owner" in ids

    links = tables["account_party_link"]
    roles = {r.get("role") for r in links}
    assert {"PRIMARY_HOLDER", "SUPPLEMENTARY_HOLDER"} <= roles
    # All links hang off the synthetic ownership account for the subject.
    assert all(str(r.get("account_id", "")).startswith("bods-ownership-") for r in links)


def test_build_aml_ai_files_membership_and_ndjson():
    files = build_aml_ai_files(_bods())
    assert set(files) == {
        "party.ndjson",
        "party_supplementary_data.ndjson",
        "account_party_link.ndjson",
        "README.md",
    }
    for name, content in files.items():
        if not name.endswith(".ndjson"):
            continue
        for line in content.strip().splitlines():
            json.loads(line)  # every line valid JSON
    assert json.loads(files["party.ndjson"].splitlines()[0])["party_id"]
    assert "bods-aml-ai" in files["README.md"]


def test_empty_tables_are_skipped():
    entity_only = [_bods()[0]]
    files = build_aml_ai_files(entity_only)
    assert "party.ndjson" in files
    # No relationships → no supplementary rows, no synthetic accounts.
    assert "party_supplementary_data.ndjson" not in files
    assert "account_party_link.ndjson" not in files


def test_aml_ai_counts():
    counts = aml_ai_counts(_bods())
    assert counts["aml_ai_party_count"] == 2
    assert counts["aml_ai_supplementary_row_count"] >= 1
    assert counts["aml_ai_account_link_count"] >= 2  # primary + supplementary


# ---- route wiring --------------------------------------------------------


def _fake_lookup_response() -> LookupResponse:
    return LookupResponse(
        query="2138000000000000A001",
        kind=SearchKind.ENTITY,
        hits=[],
        errors={},
        cross_source_links=[],
        risk_signals=[],
        bods=_bods(),
        bods_issues=[],
        license_notices=[],
        lei="2138000000000000A001",
        legal_name="Northwind Logistics Ltd",
        jurisdiction="GB",
        derived_identifiers={},
    )


async def _fake_lookup(lei, deepen_top=5, refresh=False):
    return _fake_lookup_response()


def test_export_amlai_streams_the_aml_ai_zip(monkeypatch):
    monkeypatch.setattr("opencheck.routers.export._lookup_impl", _fake_lookup)
    client = TestClient(app)
    r = client.get("/export", params={"lei": "2138000000000000A001", "format": "amlai"})
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/zip"
    assert "-aml-ai.zip" in r.headers["content-disposition"]
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        names = zf.namelist()
        prefix = names[0].split("/", 1)[0]
        assert prefix.endswith("-aml-ai")
        for member in (
            "party.ndjson", "party_supplementary_data.ndjson",
            "account_party_link.ndjson", "README.md", "LICENSES.md",
        ):
            assert f"{prefix}/{member}" in names, member
        licenses = zf.read(f"{prefix}/LICENSES.md").decode("utf-8")
    assert licenses.startswith("# OpenCheck export — licence notes")


def test_export_network_amlai():
    client = TestClient(app)
    r = client.post("/export-network", json={"bods": _bods(), "format": "amlai", "slug": "net"})
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/zip"
    assert ".aml-ai.zip" in r.headers["content-disposition"]
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        names = zf.namelist()
        prefix = names[0].split("/", 1)[0]
        assert f"{prefix}/party.ndjson" in names
        assert f"{prefix}/LICENSES.md" in names


def test_zip_bundle_manifest_carries_aml_ai_counts(monkeypatch):
    monkeypatch.setattr("opencheck.routers.export._lookup_impl", _fake_lookup)
    client = TestClient(app)
    r = client.get("/export", params={"lei": "2138000000000000A001", "format": "zip"})
    assert r.status_code == 200
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        prefix = zf.namelist()[0].split("/", 1)[0]
        manifest = json.loads(zf.read(f"{prefix}/manifest.json"))
    assert manifest["aml_ai_party_count"] == 2
    assert manifest["aml_ai_supplementary_row_count"] >= 1
    assert manifest["aml_ai_account_link_count"] >= 2
