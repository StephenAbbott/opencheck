"""Tests for the KvK (Netherlands Chamber of Commerce) open-data adapter.

All HTTP calls are mocked via respx so no network access is needed.

The key behaviour under test is graceful handling of HTTP 404 from the KvK
``basisbedrijfsgegevens`` High-Value Dataset. That dataset only covers
companies registered as a BV or NV with registered business activity; every
other KvK number answers 404. The adapter must degrade to a coverage note
rather than surfacing the raw ``HTTPStatusError`` as a broken source card.
"""

from __future__ import annotations

import pytest
import respx
from httpx import Response

from opencheck.sources.kvk import KVK_RA_CODE, KvKAdapter, normalise_kvk
from opencheck.routers.lookup import _bh_kvk, _LookupCtx


_URL = "https://opendata.kvk.nl/api/v1/hvds/basisbedrijfsgegevens/kvknummer"


@pytest.fixture
def adapter():
    return KvKAdapter()


def test_ra_code():
    assert KVK_RA_CODE == "RA000463"


def test_normalise_kvk_zero_pads():
    assert normalise_kvk("33011433") == "33011433"
    assert normalise_kvk(" 215011 ") == "00215011"


def test_requires_no_api_key(adapter):
    assert not adapter.info.requires_api_key


@pytest.mark.asyncio
async def test_fetch_returns_company_on_200(adapter, monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    from opencheck.config import get_settings

    get_settings.cache_clear()
    with respx.mock:
        respx.get(f"{_URL}/17085815").mock(
            return_value=Response(
                200,
                json={
                    "kvkNummer": "17085815",
                    "rechtsvorm": "NV",
                    "activiteiten": [{"sbiCode": "7010", "soortActiviteit": "Hoofdactiviteit"}],
                },
            )
        )
        bundle = await adapter.fetch("17085815", legal_name="ASML Holding N.V.")

    assert bundle["company"] is not None
    assert bundle["company"]["rechtsvorm"] == "NV"
    assert "coverage_note" not in bundle
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_fetch_404_degrades_to_coverage_note(adapter, monkeypatch, tmp_path):
    """A 404 (not in the BV/NV-only open set) must not raise — it returns a
    company-less bundle carrying a coverage note."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    from opencheck.config import get_settings

    get_settings.cache_clear()
    with respx.mock:
        respx.get(f"{_URL}/33011433").mock(
            return_value=Response(404, json={"detail": "not found"})
        )
        bundle = await adapter.fetch("33011433", legal_name="Heineken N.V.")

    assert bundle["company"] is None
    assert not bundle["is_stub"]
    assert "coverage_note" in bundle
    assert "open-data set" in bundle["coverage_note"]
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_hit_builder_surfaces_coverage_note(adapter, monkeypatch, tmp_path):
    """The coverage note must reach the SourceHit raw so the frontend can show
    it instead of an empty/error card."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    from opencheck.config import get_settings

    get_settings.cache_clear()
    with respx.mock:
        respx.get(f"{_URL}/33011433").mock(return_value=Response(404))
        bundle = await adapter.fetch("33011433", legal_name="Heineken N.V.")

    ctx = _LookupCtx(lei="724500K5PTPSST86UQ23", legal_name="Heineken N.V.")
    hit = _bh_kvk(bundle, "33011433", ctx)
    assert hit.name == "Heineken N.V."
    assert "coverage_note" in hit.raw
    get_settings.cache_clear()
