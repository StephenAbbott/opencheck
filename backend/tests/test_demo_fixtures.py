"""Tests pinning the behaviour of the shipped demo fixtures.

These exercise the curated demos (BP, Rosneft, Vladimir Putin) end-to-
end via the FastAPI app with no API keys / no live calls. They serve
two purposes:

* Catch regressions if an adapter, the reconciler, or the risk rules
  drift away from the fixture shapes.
* Document the intended demo story for each subject — what cross-source
  bridges should fire, what risk signals each story is meant to show.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.config import get_settings


@pytest.fixture(autouse=True)
def _no_live(monkeypatch):
    """Force offline mode — fixtures should be the only data source."""
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    monkeypatch.delenv("OPENSANCTIONS_API_KEY", raising=False)
    monkeypatch.delenv("COMPANIES_HOUSE_API_KEY", raising=False)
    monkeypatch.delenv("OPENALEPH_API_KEY", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _real_hits(report: dict) -> list[dict]:
    return [h for h in report["hits"] if not h["is_stub"]]


def test_bp_demo_bridges_companies_house_and_gleif() -> None:
    """BP — clean entity story. CH ↔ GLEIF bridge via gb_coh."""
    client = TestClient(app)
    r = client.get("/report", params={"q": "BP", "kind": "entity"}).json()

    sources = {h["source_id"] for h in _real_hits(r)}
    assert {"companies_house", "gleif", "wikidata"}.issubset(sources)

    link_keys = {link["key"] for link in r["cross_source_links"]}
    assert "gb_coh" in link_keys


def test_rosneft_demo_lights_up_amla_pipeline() -> None:
    """Rosneft — sanctioned + RU jurisdiction → AMLA non-EU + sanction."""
    client = TestClient(app)
    r = client.get("/report", params={"q": "Rosneft", "kind": "entity"}).json()

    sources = {h["source_id"] for h in _real_hits(r)}
    assert {"opensanctions", "gleif", "wikidata"}.issubset(sources)

    codes = {sig["code"] for sig in r["risk_signals"]}
    assert "SANCTIONED" in codes
    assert "NON_EU_JURISDICTION" in codes

    # OpenSanctions/Wikidata bridged on Q-ID; GLEIF/OS bridged on LEI.
    link_keys = {link["key"] for link in r["cross_source_links"]}
    assert {"wikidata_qid", "lei"}.issubset(link_keys)


def test_putin_demo_shows_multi_source_pep() -> None:
    """Vladimir Putin — Q-ID bridges Wikidata, OpenSanctions, EveryPolitician."""
    client = TestClient(app)
    r = client.get(
        "/report", params={"q": "Vladimir Putin", "kind": "person"}
    ).json()

    sources = {h["source_id"] for h in _real_hits(r)}
    assert {"opensanctions", "everypolitician", "wikidata"}.issubset(sources)

    codes = {sig["code"] for sig in r["risk_signals"]}
    assert "PEP" in codes

    # Q-ID Q7747 should bridge all three person sources.
    qid_links = [link for link in r["cross_source_links"] if link["key"] == "wikidata_qid"]
    assert len(qid_links) == 1
    bridged = {h["source_id"] for h in qid_links[0]["hits"]}
    assert {"wikidata", "opensanctions", "everypolitician"}.issubset(bridged)


def test_demos_work_offline_without_any_keys() -> None:
    """No allow_live, no keys → demos still produce real (non-stub) hits.

    This is the regression test for the cache-first dispatch refactor.
    """
    client = TestClient(app)
    for q, kind in [("BP", "entity"), ("Rosneft", "entity"), ("Vladimir Putin", "person")]:
        r = client.get("/report", params={"q": q, "kind": kind}).json()
        real = _real_hits(r)
        assert real, f"{q!r} demo produced no real hits — fixtures missing?"
