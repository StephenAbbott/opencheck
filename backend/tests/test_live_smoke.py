"""Opt-in live smoke tests — hit real, open APIs to catch API-shape drift.

These are the lightweight alternative to recorded cassettes: instead of baking
real payloads (with their PII, secrets and licence restrictions) into the repo,
a handful of tests hit the *current* live API and assert it still parses and
maps to valid BODS. Nothing is recorded or committed.

Scope is deliberately limited to **open, key-free, low-sensitivity** sources:

- **GLEIF** — public, CC0, no API key, legal-entity reference data.
- **Wikidata** — public, CC0, no API key.
- **Malta Business Registry** — public, CC BY 4.0, no API key (EU HVD).

Licence-restricted (OpenSanctions CC-BY-NC, OpenCorporates) and PII-heavy or
key-gated sources are intentionally excluded.

Skipped by default. Run with::

    pytest --run-live -m live            # just the live smoke tier
    OPENCHECK_RUN_LIVE=1 pytest -m live
"""

from __future__ import annotations

import pytest

from opencheck.bods.mapper import map_gleif, map_malta_mbr, map_wikidata
from opencheck.bods.validator import validate_shape
from opencheck.config import get_settings
from opencheck.sources import REGISTRY, SearchKind

pytestmark = pytest.mark.live


@pytest.fixture(autouse=True)
def _live_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# --- GLEIF (public, CC0, no key) ---------------------------------------------


def test_gleif_is_key_free_and_live():
    info = REGISTRY["gleif"].info
    assert info.requires_api_key is False
    assert info.live_available is True


async def test_gleif_search_then_fetch_maps_to_valid_bods():
    """Search resolves a real LEI, the Level-1/Level-2 fetch shape still
    validates, and it maps to valid BODS. No LEI is hardcoded (so the test can't
    rot if one company's record lapses) — whatever the live search returns is used."""
    adapter = REGISTRY["gleif"]
    hits = await adapter.search("Apple Inc", SearchKind.ENTITY)
    assert hits, "GLEIF search returned nothing — API shape changed?"
    leis = [h.identifiers.get("lei", "") for h in hits]
    assert any(len(x) == 20 for x in leis), f"no 20-char LEI in hits: {leis[:3]}"

    lei = next(x for x in leis if len(x) == 20)
    bundle = await adapter.fetch(lei)
    assert bundle["lei"] == lei
    assert bundle.get("record"), "GLEIF Level-1 record missing — API shape changed?"

    bods = list(map_gleif(bundle))
    assert bods, "GLEIF bundle produced no BODS statements"
    assert validate_shape(bods) == []
    assert any(s["recordType"] == "entity" for s in bods), "no entity statement"


# --- Wikidata (public, CC0, no key) ------------------------------------------


def test_wikidata_is_key_free_and_live():
    info = REGISTRY["wikidata"].info
    assert info.requires_api_key is False
    assert info.live_available is True


async def test_wikidata_search_then_fetch_maps_to_valid_bods():
    adapter = REGISTRY["wikidata"]
    hits = await adapter.search("Unilever", SearchKind.ENTITY)
    assert hits, "Wikidata search returned nothing — API shape changed?"

    qid = hits[0].identifiers.get("wikidata_qid") or hits[0].hit_id
    assert qid.startswith("Q"), f"unexpected Wikidata id shape: {qid!r}"

    bundle = await adapter.fetch(qid)
    bods = list(map_wikidata(bundle))
    # A sparse entity may yield no BODS, but whatever is produced must be valid.
    assert validate_shape(bods) == []


# --- Malta Business Registry (public, CC BY 4.0, no key) ----------------------


def test_malta_mbr_is_key_free_and_live():
    info = REGISTRY["malta_mbr"].info
    assert info.requires_api_key is False
    assert info.live_available is True


async def test_malta_mbr_live_fetch_maps_to_valid_bods():
    """Fetch a real company from the live MBR Open Data API and confirm the
    response still parses and maps to valid BODS.

    This is also the production access check: if the MBR endpoint starts
    rejecting non-browser clients (WAF / IP block) the fetch returns a stub
    and this test fails loudly. A few stable registration numbers are tried so
    one company being purged can't rot the test (struck-off companies normally
    remain queryable, so this is belt-and-braces)."""
    adapter = REGISTRY["malta_mbr"]

    bundle = None
    for reg in ("C 113927", "C 1", "C 100", "C 1000"):
        b = await adapter.fetch(reg, legal_name="")
        if not b.get("is_stub") and (b.get("company") or {}).get("name"):
            bundle = b
            break

    assert bundle is not None, (
        "Malta MBR live fetch returned no parseable record — the API shape or "
        "access policy (e.g. a WAF blocking non-browser clients) may have changed"
    )

    company = bundle["company"]
    # Fields the adapter/mapper rely on.
    assert company.get("registration_number"), "no registration_number in live record"

    bods = list(map_malta_mbr(bundle))
    assert bods, "Malta MBR bundle produced no BODS statements"
    assert validate_shape(bods) == []
    assert any(s["recordType"] == "entity" for s in bods), "no entity statement"
