"""BigQuery GQL export — bods/gql.py mapper wrapper + /export?format=gql.

The heavy lifting (statement→node/edge mapping, DDL, the 14 GQL queries) lives
upstream in bods-gql; these tests pin OpenCheck's packaging of it: the zip
membership, CSV headers, the dataset placeholder, manifest counts, and the
route wiring on /export and /export-network.
"""

from __future__ import annotations

import io
import json
import zipfile

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.bods import build_gql_files, gql_counts
from opencheck.bods.gql import PLACEHOLDER_DATASET
from opencheck.config import get_settings
from opencheck.routers.export import _EXPORT_FORMATS, ExportNetworkRequest
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


# ---- bods/gql.py --------------------------------------------------------


def test_build_gql_files_membership_and_headers():
    files = build_gql_files(_bods())
    assert files["entity_nodes.csv"].splitlines()[0].startswith("record_id,")
    assert files["person_nodes.csv"].splitlines()[0].startswith("record_id,")
    assert files["ownership_edges.csv"].splitlines()[0].startswith("record_id,")
    # One data row each for the fixture bundle.
    assert len(files["entity_nodes.csv"].strip().splitlines()) == 2
    assert "Jane Smith" in files["person_nodes.csv"]
    assert "CREATE OR REPLACE PROPERTY GRAPH" in files["create_property_graph.sql"]
    # All 14 queries, under queries/.
    queries = [n for n in files if n.startswith("queries/") and n.endswith(".gql")]
    assert len(queries) == 14
    assert "queries/find-ubos.gql" in files
    assert "README.md" in files


def test_ddl_and_queries_use_the_placeholder_dataset():
    files = build_gql_files(_bods())
    assert PLACEHOLDER_DATASET in files["create_property_graph.sql"]
    for name in files:
        if name.startswith("queries/"):
            assert PLACEHOLDER_DATASET in files[name], name
    assert PLACEHOLDER_DATASET in files["README.md"]


def test_varying_row_shapes_share_one_stable_schema():
    """bods-gql's to_dict() drops None values, so row key sets vary — a later
    row can carry a column the first row lacked. Live BP data hit exactly
    this (first mapped entity had no jurisdiction; a later one did) and
    DictWriter raised. The CSV must use the full dataclass schema instead."""
    bods = _bods()
    # First entity: no jurisdiction. Second entity: with jurisdiction.
    bods.append({
        "statementId": "ent-2", "recordId": "ent-2", "recordType": "entity",
        "recordStatus": "new", "statementDate": "2026-01-01",
        "recordDetails": {"entityType": {"type": "registeredEntity"},
                          "name": "Nordwind GmbH", "isComponent": False,
                          "jurisdiction": {"name": "Germany", "code": "DE"}},
        "source": {"description": "UK Companies House"},
    })
    files = build_gql_files(bods)  # must not raise
    lines = files["entity_nodes.csv"].splitlines()
    header = lines[0].split(",")
    assert "jurisdiction_code" in header
    assert "jurisdiction_name" in header
    assert len(lines) == 3  # header + both entities
    # Every data row has exactly the header's column count.
    import csv as _csv
    rows = list(_csv.DictReader(io.StringIO(files["entity_nodes.csv"])))
    assert rows[0]["jurisdiction_code"] == ""
    assert rows[1]["jurisdiction_code"] == "DE"


def test_empty_tables_are_skipped_not_header_only():
    entity_only = [_bods()[0]]
    files = build_gql_files(entity_only)
    assert "entity_nodes.csv" in files
    assert "person_nodes.csv" not in files
    assert "ownership_edges.csv" not in files


def test_gql_counts():
    assert gql_counts(_bods()) == {
        "gql_entity_node_count": 1,
        "gql_person_node_count": 1,
        "gql_edge_count": 1,
    }


# ---- route wiring -------------------------------------------------------


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


def test_export_gql_streams_the_bigquery_zip(monkeypatch):
    monkeypatch.setattr("opencheck.routers.export._lookup_impl", _fake_lookup)
    client = TestClient(app)
    r = client.get("/export", params={"lei": "2138000000000000A001", "format": "gql"})
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/zip"
    assert "-bigquery.zip" in r.headers["content-disposition"]
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        names = zf.namelist()
        prefix = names[0].split("/", 1)[0]
        assert prefix.endswith("-bigquery")
        for member in (
            "entity_nodes.csv", "person_nodes.csv", "ownership_edges.csv",
            "create_property_graph.sql", "README.md", "LICENSES.md",
        ):
            assert f"{prefix}/{member}" in names, member
        assert sum(n.startswith(f"{prefix}/queries/") for n in names) == 14
        licenses = zf.read(f"{prefix}/LICENSES.md").decode("utf-8")
    assert licenses.startswith("# OpenCheck export — licence notes")


def test_export_network_gql(monkeypatch):
    client = TestClient(app)
    r = client.post("/export-network", json={"bods": _bods(), "format": "gql", "slug": "net"})
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/zip"
    assert ".bigquery.zip" in r.headers["content-disposition"]
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        names = zf.namelist()
        prefix = names[0].split("/", 1)[0]
        assert f"{prefix}/ownership_edges.csv" in names
        assert f"{prefix}/LICENSES.md" in names


def test_zip_bundle_manifest_carries_gql_counts(monkeypatch):
    monkeypatch.setattr("opencheck.routers.export._lookup_impl", _fake_lookup)
    client = TestClient(app)
    r = client.get("/export", params={"lei": "2138000000000000A001", "format": "zip"})
    assert r.status_code == 200
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        prefix = zf.namelist()[0].split("/", 1)[0]
        manifest = json.loads(zf.read(f"{prefix}/manifest.json"))
    assert manifest["gql_entity_node_count"] == 1
    assert manifest["gql_person_node_count"] == 1
    assert manifest["gql_edge_count"] == 1


def test_network_format_literal_matches_export_formats():
    """The three format declarations can't drift: the network Literal must be
    exactly the /export set plus its one extra (cypher)."""
    literal = set(
        ExportNetworkRequest.model_fields["format"].annotation.__args__
    )
    assert literal == _EXPORT_FORMATS | {"cypher"}
