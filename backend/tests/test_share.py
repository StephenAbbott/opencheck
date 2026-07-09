"""Tests for the share endpoints (/og/{lei}.png, /share/{lei}) and the
og_image renderer."""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

import opencheck.routers.lookup as lookup_router
import opencheck.routers.share as share_router
from opencheck.app import app
from opencheck.og_image import SIGNAL_STYLE, render_share_card

LEI = "253400JT3MQWNDKMJE44"

_PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


@pytest.fixture(autouse=True)
def _clean_caches(monkeypatch):
    share_router._OG_CACHE.clear()
    lookup_router._REPLAY_CACHE.clear()
    yield
    share_router._OG_CACHE.clear()
    lookup_router._REPLAY_CACHE.clear()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _seed_replay(lei: str, name: str, signals: list[dict]) -> None:
    events = [
        ("gleif_done", {"lei": lei, "legal_name": name, "jurisdiction": "RU",
                        "derived_identifiers": {}}),
        ("risk_signals", {"signals": signals}),
        ("done", {"lei": lei, "bods_issues": [], "license_notices": []}),
    ]
    lookup_router._REPLAY_CACHE[f"{lei}:5"] = (time.monotonic(), events)


def _signals(n: int) -> list[dict]:
    codes = ["SANCTIONED", "COMPLEX_OWNERSHIP_LAYERS", "NON_EU_JURISDICTION",
             "TRUST_OR_ARRANGEMENT", "FATF_GREY_LIST"]
    return [
        {"code": codes[i % len(codes)], "confidence": "high",
         "summary": "s", "source_id": "t", "hit_id": str(i), "evidence": {}}
        for i in range(n)
    ]


# ----------------------------------------------------------------------
# Renderer
# ----------------------------------------------------------------------


def test_render_full_card_is_valid_png():
    png = render_share_card("Rosneft Oil Company", LEI, _signals(7))
    assert png.startswith(_PNG_MAGIC)
    # 1200×630 is baked into the PNG IHDR chunk (big-endian dimensions).
    assert (1200).to_bytes(4, "big") in png[:33]
    assert (630).to_bytes(4, "big") in png[:33]


def test_render_zero_signals_and_teaser_variants():
    assert render_share_card("Acme", LEI, []).startswith(_PNG_MAGIC)
    assert render_share_card(None, LEI, None).startswith(_PNG_MAGIC)


def test_render_survives_unknown_code_and_long_name():
    png = render_share_card(
        "Nationale-Nederlanden Levensverzekering Maatschappij N.V.",
        LEI,
        [{"code": "BRAND_NEW_SIGNAL", "confidence": "banana"}],
    )
    assert png.startswith(_PNG_MAGIC)


def test_signal_styles_cover_frontend_inventory():
    # The codes rendered as picker-card chips / documented in CLAUDE.md.
    for code in [
        "SANCTIONED", "RELATED_SANCTIONED", "PEP", "RELATED_PEP",
        "FATF_BLACK_LIST", "FATF_GREY_LIST", "NON_EU_JURISDICTION",
        "OFFSHORE_LEAKS", "TRUST_OR_ARRANGEMENT", "COMPLEX_OWNERSHIP_LAYERS",
        "COMPLEX_CORPORATE_STRUCTURE",
    ]:
        assert code in SIGNAL_STYLE, f"missing style for {code}"


# ----------------------------------------------------------------------
# /og/{lei}.png
# ----------------------------------------------------------------------


def test_og_image_full_card_from_replay_cache(client: TestClient):
    _seed_replay(LEI, "Rosneft Oil Company", _signals(7))
    r = client.get(f"/og/{LEI}.png")
    assert r.status_code == 200
    assert r.headers["content-type"] == "image/png"
    assert r.content.startswith(_PNG_MAGIC)
    assert "max-age=3600" in r.headers["cache-control"]


def test_og_image_teaser_when_no_cached_lookup(client: TestClient, monkeypatch):
    async def fake_teaser_name(lei: str):
        return "BP p.l.c."

    monkeypatch.setattr(share_router, "_teaser_name", fake_teaser_name)
    r = client.get(f"/og/{LEI}.png")
    assert r.status_code == 200
    assert r.content.startswith(_PNG_MAGIC)
    # Teasers must not cache long — a completed lookup should upgrade them.
    assert "max-age=60" in r.headers["cache-control"]


def test_og_image_rejects_invalid_lei(client: TestClient):
    assert client.get("/og/not-a-lei.png").status_code == 404
    assert client.get("/og/253400JT3MQWNDKMJE4.png").status_code == 404


def test_og_image_uses_cache_second_time(client: TestClient):
    _seed_replay(LEI, "Rosneft Oil Company", _signals(2))
    first = client.get(f"/og/{LEI}.png").content
    lookup_router._REPLAY_CACHE.clear()  # cache hit must not re-render
    second = client.get(f"/og/{LEI}.png").content
    assert first == second


# ----------------------------------------------------------------------
# /share/{lei}
# ----------------------------------------------------------------------


def test_share_page_carries_entity_og_tags(client: TestClient):
    _seed_replay(LEI, "Rosneft Oil Company", _signals(7))
    r = client.get(f"/share/{LEI}")
    assert r.status_code == 200
    body = r.text
    assert 'property="og:title" content="Rosneft Oil Company — OpenCheck"' in body
    assert "7 risk signals" in body
    assert f"/og/{LEI}.png" in body
    assert 'name="twitter:card" content="summary_large_image"' in body
    # Humans get redirected to the frontend lookup URL.
    assert f"?lei={LEI}" in body
    assert 'http-equiv="refresh"' in body


def test_share_page_teaser_description(client: TestClient, monkeypatch):
    async def fake_teaser_name(lei: str):
        return None

    monkeypatch.setattr(share_router, "_teaser_name", fake_teaser_name)
    r = client.get(f"/share/{LEI}")
    assert r.status_code == 200
    assert f"LEI {LEI} — OpenCheck" in r.text
    assert "34 open data sources" in r.text


def test_share_page_escapes_html_in_names(client: TestClient):
    _seed_replay(LEI, 'Evil <script>alert("x")</script> Ltd', [])
    r = client.get(f"/share/{LEI}")
    assert "<script>alert" not in r.text
    assert "&lt;script&gt;" in r.text


def test_share_page_rejects_invalid_lei(client: TestClient):
    assert client.get("/share/DROP TABLE").status_code == 404
