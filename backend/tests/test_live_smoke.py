"""Opt-in live smoke tests — hit real, open APIs to catch API-shape drift.

These are the lightweight alternative to recorded cassettes: instead of baking
real payloads (with their PII, secrets and licence restrictions) into the repo,
a handful of tests hit the *current* live API and assert it still parses and
maps to valid BODS. Nothing is recorded or committed.

Scope is deliberately limited to **open, key-free, low-sensitivity** sources:

- **GLEIF** — public, CC0, no API key, legal-entity reference data.
- **Wikidata** — public, CC0, no API key.
- **Malta Business Registry** — public, CC BY 4.0, no API key (EU HVD).
- **Brazil CNPJ** (Receita Federal) — public open data, no API key (OpenCNPJ / BrasilAPI).
- **New Zealand Companies Register (NZBN)** — CC BY 4.0, but *key-gated*: this
  one runs only when ``NZBN_API_KEY`` is set, otherwise it skips.

Licence-restricted (OpenSanctions CC-BY-NC, OpenCorporates) and PII-heavy
sources are intentionally excluded. The NZBN smoke test is the one key-gated
exception (the key is free and the data is CC BY 4.0), and it skips cleanly
when no key is configured.

Skipped by default. Run with::

    pytest --run-live -m live            # just the live smoke tier
    OPENCHECK_RUN_LIVE=1 pytest -m live
"""

from __future__ import annotations

import pytest

from opencheck.bods.mapper import (
    map_cnpj_brazil,
    map_gleif,
    map_malta_mbr,
    map_nz_companies,
    map_wikidata,
)
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


# --- Brazil CNPJ (public open data, key-free) --------------------------------


def test_cnpj_brazil_is_key_free_and_live():
    info = REGISTRY["cnpj_brazil"].info
    assert info.requires_api_key is False
    assert info.live_available is True


async def test_cnpj_brazil_live_fetch_maps_to_valid_bods():
    """Fetch a real company from the live CNPJ providers (OpenCNPJ primary,
    BrasilAPI fallback) and confirm the response still parses, includes the QSA,
    and maps to valid BODS with ownership/control relationships. Also a
    production access check for both providers.

    Uses Petrobras (a stable, long-lived CNPJ) so the test can't rot easily."""
    adapter = REGISTRY["cnpj_brazil"]
    bundle = await adapter.fetch("33000167000101", legal_name="")

    assert not bundle.get("is_stub"), (
        "CNPJ Brazil live fetch returned a stub — both OpenCNPJ and BrasilAPI "
        "may be unreachable or have changed shape/access"
    )
    company = bundle.get("company") or {}
    assert company.get("name"), "no company name in live record"
    assert bundle.get("partners"), "no QSA partners parsed — provider shape changed?"

    bods = list(map_cnpj_brazil(bundle))
    assert bods, "CNPJ Brazil bundle produced no BODS statements"
    assert validate_shape(bods) == []
    assert any(s["recordType"] == "entity" for s in bods), "no entity statement"
    assert any(s["recordType"] == "relationship" for s in bods), (
        "no ownership/control relationship from the QSA"
    )


# --- New Zealand Companies Register / NZBN (CC BY 4.0, key-gated) -------------


def test_nz_companies_requires_a_key():
    assert REGISTRY["nz_companies"].info.requires_api_key is True


async def test_nz_companies_live_fetch_maps_to_valid_bods():
    """Resolve a real company number → NZBN, fetch the live FullEntity, and
    confirm it still parses and maps to valid BODS. Skipped unless
    ``NZBN_API_KEY`` is set. Also the production access check: if the NZBN API
    changes shape or rejects the key, the fetch returns a stub and this fails.

    Uses Fonterra Co-operative Group (company number 1166320) — a stable,
    long-lived NZ company — so the test can't rot easily."""
    if not get_settings().nzbn_api_key:
        pytest.skip("NZBN_API_KEY not set — skipping NZ live smoke test")

    adapter = REGISTRY["nz_companies"]
    bundle = await adapter.fetch("1166320", legal_name="")

    assert not bundle.get("is_stub"), (
        "NZ live fetch returned a stub — the NZBN API key, access policy or "
        "response shape may have changed"
    )
    assert bundle.get("nzbn"), "company number did not resolve to an NZBN"
    company = bundle.get("company") or {}
    assert company.get("name"), "no company name in live record"

    bods = list(map_nz_companies(bundle))
    assert bods, "NZ bundle produced no BODS statements"
    assert validate_shape(bods) == []
    assert any(s["recordType"] == "entity" for s in bods), "no entity statement"
