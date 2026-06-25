"""SPIKE tests for the progressive-discovery /expand endpoint.

`/expand` re-anchors a standard lookup on the node's LEI and remaps the
looked-up entity's identity onto the existing graph node (`anchor`) so the new
owners layer stitches on by statementId. The underlying lookup is monkeypatched
so the test is deterministic and offline.
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


_LEI = "213800LH1BZH3DI6G760"
_ANCHOR = "opencheck-anchornode000000"


def _fake_layer(lei: str):
    """A minimal owners layer: the looked-up entity (GLEIF subject) + one owner
    person + the ownership relationship between them."""
    subj = _stable_id("gleif", "entity", lei)
    return [
        {"statementId": subj, "recordType": "entity",
         "recordDetails": {"entityType": {"type": "registeredEntity"}, "name": "HoldCo Ltd",
                           "identifiers": [{"id": lei, "scheme": "XI-LEI", "schemeName": "LEI"}]}},
        {"statementId": "owner-1", "recordType": "person",
         "recordDetails": {"personType": "knownPerson",
                           "names": [{"type": "legal", "fullName": "Jane Roe"}]}},
        {"statementId": "rel-1", "recordType": "relationship",
         "recordDetails": {"subject": subj, "interestedParty": "owner-1",
                           "interests": [{"type": "shareholding"}]}},
    ]


def test_expand_remaps_subject_onto_anchor(client, monkeypatch):
    async def _fake_lookup(*, lei, deepen_top=3):
        return SimpleNamespace(lei=lei, bods=_fake_layer(lei), bods_issues=[])

    monkeypatch.setattr("opencheck.routers.lookup.lookup", _fake_lookup)

    r = client.get("/expand", params={"lei": _LEI, "anchor": _ANCHOR})
    assert r.status_code == 200
    data = r.json()
    assert data["anchor"] == _ANCHOR

    bods = data["bods"]
    subj = _stable_id("gleif", "entity", _LEI)
    ids = [s["statementId"] for s in bods]

    # The looked-up entity's identity is collapsed onto the anchor — no leftover
    # GLEIF subject id anywhere.
    assert subj not in ids
    assert all(subj not in str(s) for s in bods)

    # The ownership relationship now points the owner at the anchor node.
    rel = next(s for s in bods if s["recordType"] == "relationship")
    assert rel["recordDetails"]["subject"] == _ANCHOR
    assert rel["recordDetails"]["interestedParty"] == "owner-1"

    # The new owner person came through.
    assert "owner-1" in ids


def test_expand_rejects_bad_deepen(client):
    r = client.get("/expand", params={"lei": _LEI, "anchor": _ANCHOR, "deepen_top": 99})
    assert r.status_code == 422
