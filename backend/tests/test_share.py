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
    lookup_router._REPLAY_CACHE[f"{lei}:5"] = lookup_router._ReplayEntry(
        stored=time.monotonic(),
        fetched_at="2026-07-16T12:00:00+00:00",
        events=events,
    )


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


class _FakeStore:
    """Stands in for entity_pages.EntityStore — only .get() is used here."""

    def __init__(self, names: dict[str, str]):
        self._names = names

    def get(self, lei: str):
        import types

        name = self._names.get(lei)
        return types.SimpleNamespace(name=name) if name else None


def test_og_teaser_name_comes_from_entity_pages_store(client: TestClient, monkeypatch):
    """With an entity-pages DB loaded, a teaser card must NOT touch GLEIF —
    crawlers fetching per-LEI og:images drove full GLEIF record builds into
    429s (observed live 2026-08-06)."""
    import opencheck.entity_pages as ep

    monkeypatch.setattr(ep, "get_store", lambda: _FakeStore({LEI: "Rosneft Oil Company"}))

    async def boom(lei: str):  # any GLEIF fallback is a regression
        raise AssertionError("teaser must not fall back to GLEIF when a store is loaded")

    monkeypatch.setattr(share_router.lookup_router, "_resolve_ctx", boom)
    r = client.get(f"/og/{LEI}.png")
    assert r.status_code == 200
    assert r.content.startswith(_PNG_MAGIC)


def test_og_teaser_unknown_lei_in_store_stays_local(client: TestClient, monkeypatch):
    """LEI absent from the Golden Copy (e.g. issued since the monthly
    refresh) → nameless teaser, still no upstream call."""
    import opencheck.entity_pages as ep

    monkeypatch.setattr(ep, "get_store", lambda: _FakeStore({}))

    async def boom(lei: str):
        raise AssertionError("unknown-LEI teaser must not fan out to GLEIF")

    monkeypatch.setattr(share_router.lookup_router, "_resolve_ctx", boom)
    r = client.get(f"/og/{LEI}.png")
    assert r.status_code == 200
    assert r.content.startswith(_PNG_MAGIC)


def test_render_gate_bounds_concurrency():
    assert share_router._RENDER_CONCURRENCY <= 4
    assert share_router._render_gate._value == share_router._RENDER_CONCURRENCY


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
    # 7 signal INSTANCES cycling 5 codes → the description counts distinct
    # codes, matching the results page's one-chip-per-code aggregation.
    assert "5 risk signals" in body
    assert f"/og/{LEI}.png" in body
    assert 'name="twitter:card" content="summary_large_image"' in body
    # Humans get redirected to the frontend lookup URL.
    assert f"?lei={LEI}" in body
    assert 'http-equiv="refresh"' in body


def test_share_meta_counts_distinct_codes_not_instances(client: TestClient):
    """Three related parties flagged for the same thing are three signal
    INSTANCES of one code — the results page shows one chip, so the meta
    description must say 1, not 3. Instance-counting made Eli Lilly's card
    claim "7 risk signals" against a page showing three chips (2026-08-20)."""
    sigs = [
        {"code": "RELATED_EXPORT_RISK", "confidence": "high", "summary": "s",
         "source_id": "t", "hit_id": str(i), "evidence": {}}
        for i in range(3)
    ] + [
        {"code": "NON_EU_JURISDICTION", "confidence": "low", "summary": "s",
         "source_id": "t", "hit_id": "x", "evidence": {}, "kind": "context"},
    ]
    _seed_replay(LEI, "Distinct Codes Ltd", sigs)
    r = client.get(f"/share/{LEI}")
    assert r.status_code == 200
    assert "1 risk signal ·" in r.text
    assert "3 risk signals" not in r.text


def test_share_page_teaser_description(client: TestClient, monkeypatch):
    async def fake_teaser_name(lei: str):
        return None

    monkeypatch.setattr(share_router, "_teaser_name", fake_teaser_name)
    r = client.get(f"/share/{LEI}")
    assert r.status_code == 200
    assert f"LEI {LEI} — OpenCheck" in r.text
    # Counted from the registry, not hard-coded: this asserted "34" while the
    # registry held 39, which is exactly how the share page drifted five
    # sources behind reality without any test noticing.
    from opencheck.sources import REGISTRY

    assert f"{len(REGISTRY)} open data sources" in r.text


def test_share_page_escapes_html_in_names(client: TestClient):
    _seed_replay(LEI, 'Evil <script>alert("x")</script> Ltd', [])
    r = client.get(f"/share/{LEI}")
    assert "<script>alert" not in r.text
    assert "&lt;script&gt;" in r.text


def test_share_page_rejects_invalid_lei(client: TestClient):
    assert client.get("/share/DROP TABLE").status_code == 404


def test_share_redirect_is_absolute_even_when_cors_origin_is_wildcard(
    client: TestClient, monkeypatch
):
    """Regression: Render sets OPENCHECK_CORS_ORIGIN='*' (a CORS policy
    value, not a URL). The redirect target must come from frontend_origin
    and never render a literal '*/?lei=...'."""
    monkeypatch.setenv("OPENCHECK_CORS_ORIGIN", "*")
    from opencheck.config import get_settings

    get_settings.cache_clear()
    try:
        _seed_replay(LEI, "Rosneft Oil Company", [])
        body = client.get(f"/share/{LEI}").text
        assert "*/?lei=" not in body
        assert f'url=https://opencheck.world/?lei={LEI}' in body
    finally:
        get_settings.cache_clear()


def test_share_redirect_falls_back_when_frontend_origin_invalid(
    client: TestClient, monkeypatch
):
    monkeypatch.setenv("OPENCHECK_FRONTEND_ORIGIN", "*")
    from opencheck.config import get_settings

    get_settings.cache_clear()
    try:
        _seed_replay(LEI, "Rosneft Oil Company", [])
        body = client.get(f"/share/{LEI}").text
        assert f'url=https://opencheck.world/?lei={LEI}' in body
    finally:
        get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Script coverage on the share card (Phase 137 follow-up)
# ---------------------------------------------------------------------------


class TestCardScriptCoverage:
    """The bundled faces do not cover every script a company name arrives in.

    Bitter carries Latin, Latin-ext and Cyrillic but **no Greek**, and Greek is
    not a subset Bitter publishes upstream, so a Greek name drawn in it comes
    out as .notdef boxes. Adding the ΓΕΜΗ adapter made that reachable.
    """

    GREEK = "ΓΚΟΛΕΜΗΣ ΕΤΑΙΡΕΙΑ ΑΕΡΟΠΟΡΙΚΩΝ"
    LEI = "635400NMLGFBATPGJD19"

    def test_renders_fully_knows_what_the_font_has(self) -> None:
        from opencheck.og_image import renders_fully

        assert renders_fully("ROSNEFT OIL COMPANY") is True
        assert renders_fully("ПАО НК РОСНЕФТЬ") is True      # Cyrillic: covered
        assert renders_fully("ØRSTED A/S · ŠKODA") is True   # Latin-ext: covered
        assert renders_fully(self.GREEK) is False            # Greek: not covered

    def test_latin_and_cyrillic_names_are_drawn_as_filed(self) -> None:
        from opencheck.og_image import card_display_name

        for name in ("Eli Lilly and Company", "ПАО НК РОСНЕФТЬ"):
            display, romanised = card_display_name(name, self.LEI)
            assert display == name
            assert romanised is False

    def test_greek_name_is_romanised_rather_than_drawn_as_tofu(self) -> None:
        from opencheck.og_image import card_display_name, renders_fully

        display, romanised = card_display_name(self.GREEK, self.LEI)
        assert romanised is True
        assert renders_fully(display)
        assert display.startswith("GKOLEMIS")

    def test_a_latin_name_the_source_published_wins(self) -> None:
        """ΓΕΜΗ supplies coNamesEn[] — the register's own romanisation."""
        from opencheck.og_image import card_display_name

        official = "GKOLEMIS ETAIREIA AEROPORIKON"
        display, romanised = card_display_name(
            self.GREEK, self.LEI, latin_name=official
        )
        assert display == official
        assert romanised is True

    def test_falls_back_to_the_lei_when_nothing_renders(self) -> None:
        from opencheck.og_image import card_display_name

        display, romanised = card_display_name("你好世界", self.LEI)
        assert display == f"LEI {self.LEI}"
        assert romanised is False

    def test_no_name_falls_back_to_the_lei(self) -> None:
        from opencheck.og_image import card_display_name

        assert card_display_name(None, self.LEI) == (f"LEI {self.LEI}", False)
        assert card_display_name("   ", self.LEI) == (f"LEI {self.LEI}", False)

    def test_alt_text_declares_a_romanised_name(self) -> None:
        """A screen-reader user must not be told a Greek company has a Latin name."""
        from opencheck.og_image import card_alt_text

        signals = [
            {"code": "OFFSHORE_LEAKS", "kind": "risk"},
            {"code": "NON_EU_JURISDICTION", "kind": "context"},
        ]
        alt = card_alt_text(self.GREEK, self.LEI, signals)
        assert "(romanised)" in alt
        assert "GKOLEMIS" in alt
        assert self.LEI in alt
        # Context signals never inflate the count, on the card or in the alt.
        assert "1 risk signal" in alt
        assert "NON_EU" not in alt

        plain = card_alt_text("Eli Lilly and Company", self.LEI, signals)
        assert "(romanised)" not in plain

    def test_alt_text_handles_the_teaser_and_the_empty_case(self) -> None:
        from opencheck.og_image import card_alt_text
        from opencheck.sources import REGISTRY

        teaser = card_alt_text("Eli Lilly and Company", self.LEI, None)
        assert f"{len(REGISTRY)} open sources" in teaser

        empty = card_alt_text("Eli Lilly and Company", self.LEI, [])
        assert "no risk signals found" in empty

    def test_source_count_is_counted_not_hard_coded(self) -> None:
        """The teaser card said "34" while the registry held 39."""
        from opencheck.og_image import _source_count
        from opencheck.sources import REGISTRY

        assert _source_count() == len(REGISTRY)

    def test_a_greek_card_actually_renders(self) -> None:
        from opencheck.og_image import render_share_card

        png = render_share_card(self.GREEK, self.LEI, [{"code": "OFFSHORE_LEAKS"}])
        assert png[:8] == b"\x89PNG\r\n\x1a\n"
