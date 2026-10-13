"""Phase 326 — ``/deepen`` takes a legal name, the way a lookup passes GLEIF's.

KvK's open data carries no company name, and ``map_kvk`` emits nothing without
one, so the BODS quality sweep's ``/deepen`` of ASML Holding (13 Oct 2026)
answered 200 with no statements: the sweep checked no KvK statement at all
and reported it clean. ``/deepen`` now accepts ``legal_name`` and hands it to
the registers that take one in a lookup (``lookup_pass_legal_name``). KvK
remembers names for later calls, so ``/deepen`` tells it not to: a name a
caller supplies must not reach the next reader of that KvK number.
"""

from __future__ import annotations

import inspect

import pytest
from fastapi.testclient import TestClient
from pytest_httpx import HTTPXMock

from opencheck import bods_quality as bq
from opencheck.app import app
from opencheck.config import get_settings
from opencheck.sources import REGISTRY

_KVK = "https://opendata.kvk.nl/api/v1/hvds/basisbedrijfsgegevens/kvknummer"
_ASML = {
    "kvkNummer": "17085815",
    "rechtsvormCode": "NV",
    "datumAanvang": "19840401",
    "activiteiten": [{"sbiCode": "7010", "soortActiviteit": "Hoofdactiviteit"}],
}


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    names = REGISTRY["kvk"]._names
    saved = dict(names)
    names.clear()
    yield
    names.clear()
    names.update(saved)
    get_settings.cache_clear()


def _entity_names(body: dict) -> list[str]:
    return [
        s["recordDetails"]["name"]
        for s in body["bods"]
        if s.get("recordType") == "entity"
    ]


def test_kvk_maps_a_named_entity_when_deepen_is_given_the_name(httpx_mock: HTTPXMock):
    httpx_mock.add_response(url=f"{_KVK}/17085815", json=_ASML)
    client = TestClient(app)
    r = client.get(
        "/deepen",
        params={"source": "kvk", "hit_id": "17085815", "legal_name": "ASML Holding N.V."},
    )
    assert r.status_code == 200
    assert _entity_names(r.json()) == ["ASML Holding N.V."]


def test_kvk_without_a_name_still_maps_nothing(httpx_mock: HTTPXMock):
    # The behaviour the sweep tripped over, unchanged for a caller with no name.
    httpx_mock.add_response(url=f"{_KVK}/17085815", json=_ASML)
    r = TestClient(app).get("/deepen", params={"source": "kvk", "hit_id": "17085815"})
    assert r.status_code == 200
    assert r.json()["bods"] == []


def test_a_name_given_to_deepen_is_not_remembered_for_the_next_reader(httpx_mock: HTTPXMock):
    httpx_mock.add_response(url=f"{_KVK}/17085815", json=_ASML)
    client = TestClient(app)
    client.get(
        "/deepen",
        params={"source": "kvk", "hit_id": "17085815", "legal_name": "Not ASML B.V."},
    )
    assert "17085815" not in REGISTRY["kvk"]._names
    # The next caller, with no name, sees nothing rather than the planted one.
    r = client.get("/deepen", params={"source": "kvk", "hit_id": "17085815"})
    assert r.json()["bods"] == []


@pytest.mark.asyncio
async def test_a_lookup_still_remembers_the_gleif_name(httpx_mock: HTTPXMock):
    # The pipeline's dispatch fetch passes GLEIF's name and _safe_deepen then
    # fetches without one; that hand-off must keep working.
    httpx_mock.add_response(url=f"{_KVK}/17085815", json=_ASML)
    adapter = REGISTRY["kvk"]
    await adapter.fetch("17085815", legal_name="ASML Holding N.V.")
    bundle = await adapter.fetch("17085815")
    assert bundle["legal_name"] == "ASML Holding N.V."


def test_deepen_ignores_a_name_for_a_source_that_takes_none(monkeypatch):
    seen: dict = {}
    adapter = REGISTRY["opensanctions"]
    assert not adapter.lookup_pass_legal_name

    async def fake_fetch(hit_id, **kwargs):
        seen.update(kwargs)
        return {"is_stub": True}

    monkeypatch.setattr(adapter, "fetch", fake_fetch)
    r = TestClient(app).get(
        "/deepen", params={"source": "opensanctions", "hit_id": "x", "legal_name": "Anything"}
    )
    assert r.status_code == 200
    assert seen == {}


def test_deepen_refuses_an_overlong_name():
    r = TestClient(app).get(
        "/deepen", params={"source": "kvk", "hit_id": "17085815", "legal_name": "x" * 301}
    )
    assert r.status_code == 422


def test_every_adapter_that_remembers_names_can_be_told_not_to():
    for sid, adapter in REGISTRY.items():
        if not getattr(adapter, "remembers_legal_name", False):
            continue
        assert adapter.lookup_pass_legal_name, sid
        assert "remember_name" in inspect.signature(adapter.fetch).parameters, sid


def test_the_sweep_sends_each_probes_legal_name_to_deepen():
    from opencheck.sources.probes import PROBES

    subjects = {s.source_id: s for s in bq.deepen_subjects(PROBES)}
    assert subjects["kvk"].legal_name == "ASML Holding N.V."
    assert subjects["cr_hongkong"].legal_name.startswith("HONGKONG AND SHANGHAI")
    assert subjects["prh"].legal_name == ""  # a probe without one sends none
