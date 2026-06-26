"""SPIKE tests for progressive-discovery expansion (/expand + /expand-layer).

Expansion re-anchors a standard lookup on a node's LEI and collapses every
representation of that entity onto the existing graph node (`anchor`) so the new
owners layer stitches on by statementId — including a national-register
representation that keys on the company number rather than the LEI (the
cross-source duplicate the first spike pass left floating). The underlying lookup
is monkeypatched so the tests are deterministic and offline.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.bods.mapper import _stable_id
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


_LEI_A = "213800LH1BZH3DI6G760"
_LEI_B = "5493001KJTIIGC8Y1R12"


def _layer(lei: str) -> list[dict]:
    """A minimal owners layer: the looked-up entity (GLEIF subject) + one owner
    person + the ownership relationship between them. Owner/rel ids are derived
    from the LEI so two layers don't falsely de-dupe."""
    subj = _stable_id("gleif", "entity", lei)
    tag = lei[-4:]
    return [
        {"statementId": subj, "recordType": "entity",
         "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": "HoldCo",
                           "identifiers": [{"id": lei, "scheme": "XI-LEI", "schemeName": "LEI"}]}},
        {"statementId": f"owner-{tag}", "recordType": "person",
         "recordDetails": {"personType": "knownPerson",
                           "names": [{"type": "legal", "fullName": f"Owner {tag}"}]}},
        {"statementId": f"rel-{tag}", "recordType": "relationship",
         "recordDetails": {"subject": subj, "interestedParty": f"owner-{tag}",
                           "interests": [{"type": "shareholding"}]}},
    ]


def _patch_lookup(monkeypatch, builder=_layer):
    async def _fake_lookup(*, lei, deepen_top=3):
        return SimpleNamespace(lei=lei, bods=builder(lei), bods_issues=[])

    monkeypatch.setattr("opencheck.routers.lookup.lookup", _fake_lookup)


def test_expand_remaps_subject_onto_anchor(client, monkeypatch):
    _patch_lookup(monkeypatch)
    r = client.get("/expand", params={"lei": _LEI_A, "anchor": "ANCHOR-A"})
    assert r.status_code == 200
    bods = r.json()["bods"]
    subj = _stable_id("gleif", "entity", _LEI_A)
    ids = [s["statementId"] for s in bods]

    assert subj not in ids and all(subj not in str(s) for s in bods)
    rel = next(s for s in bods if s["recordType"] == "relationship")
    assert rel["recordDetails"]["subject"] == "ANCHOR-A"
    assert any(s["recordType"] == "person" for s in bods)  # owner came through


def test_expand_collapses_cross_source_duplicate(client, monkeypatch):
    """The fix: a national-register entity statement keyed on the company number
    (no LEI) collapses onto the anchor too, so its directors stitch on rather than
    floating as a duplicate node."""
    def _cross_source(lei: str) -> list[dict]:
        subj = _stable_id("gleif", "entity", lei)
        nat = "nat-entity-xyz"
        return [
            # GLEIF subject ties the LEI to the company number.
            {"statementId": subj, "recordType": "entity",
             "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": "HoldCo",
                               "identifiers": [{"id": lei, "scheme": "XI-LEI"},
                                               {"id": "12345678", "scheme": "GB-COH"}]}},
            # National register: SAME company number, NO LEI → must still collapse.
            {"statementId": nat, "recordType": "entity",
             "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": "HoldCo Ltd",
                               "identifiers": [{"id": "12345678", "scheme": "GB-COH"}]}},
            {"statementId": "dir-1", "recordType": "person",
             "recordDetails": {"personType": "knownPerson",
                               "names": [{"type": "legal", "fullName": "Jane Roe"}]}},
            {"statementId": "rel-nat", "recordType": "relationship",
             "recordDetails": {"subject": nat, "interestedParty": "dir-1",
                               "interests": [{"type": "seniorManagingOfficial"}]}},
        ]

    _patch_lookup(monkeypatch, _cross_source)
    r = client.get("/expand", params={"lei": _LEI_A, "anchor": "ANCHOR-A"})
    bods = r.json()["bods"]

    # Neither the GLEIF nor the national-register id survives — both collapsed.
    assert all("nat-entity-xyz" not in str(s) for s in bods)
    assert all(_stable_id("gleif", "entity", _LEI_A) not in str(s) for s in bods)
    # The national register's director now points at the anchor, not a duplicate.
    rel = next(s for s in bods if s["statementId"] == "rel-nat")
    assert rel["recordDetails"]["subject"] == "ANCHOR-A"


def test_expand_layer_batches_and_dedupes(client, monkeypatch):
    _patch_lookup(monkeypatch)
    r = client.post("/expand-layer", json={
        "items": [
            {"lei": _LEI_A, "anchor": "ANCHOR-A"},
            {"lei": _LEI_B, "anchor": "ANCHOR-B"},
        ]
    })
    assert r.status_code == 200
    data = r.json()
    assert data["count"] == 2
    assert data["expanded"] == ["ANCHOR-A", "ANCHOR-B"]
    assert data["truncated"] is False

    rels = [s for s in data["bods"] if s["recordType"] == "relationship"]
    subjects = {s["recordDetails"]["subject"] for s in rels}
    assert subjects == {"ANCHOR-A", "ANCHOR-B"}  # each owner stitched to its own anchor


def test_expand_rejects_bad_deepen(client):
    r = client.get("/expand", params={"lei": _LEI_A, "anchor": "A", "deepen_top": 99})
    assert r.status_code == 422
