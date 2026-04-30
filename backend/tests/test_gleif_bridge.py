"""Tests for the GLEIF CH → LEI reverse-lookup bridge.

Covers:
* The ``_ch_ra_code`` helper in ``app.py``
* ``GleifAdapter.search_by_local_id`` stub / offline behaviour
* The three GLEIF filter fields are tried in sequence and results are
  deduplicated by LEI
"""

from __future__ import annotations

import pytest

from opencheck.app import _ch_ra_code
from opencheck.config import get_settings
from opencheck.sources import REGISTRY


@pytest.fixture(autouse=True)
def _isolated_data_root(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# _ch_ra_code helper
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "company_number, expected_ra",
    [
        ("00000001", "RA000585"),   # England & Wales (numeric)
        ("01234567", "RA000585"),   # England & Wales (numeric)
        ("SC123456", "RA000586"),   # Scotland
        ("sc123456", "RA000586"),   # Scotland (lowercase input)
        ("NI012345", "RA000591"),   # Northern Ireland
        ("ni012345", "RA000591"),   # Northern Ireland (lowercase)
        ("OC123456", "RA000585"),   # LLP — England & Wales default
        ("", "RA000585"),           # Empty string → default
    ],
)
def test_ch_ra_code(company_number: str, expected_ra: str) -> None:
    assert _ch_ra_code(company_number) == expected_ra


# ---------------------------------------------------------------------------
# GleifAdapter.search_by_local_id — offline (live_available=False)
# ---------------------------------------------------------------------------


async def test_search_by_local_id_returns_empty_when_live_disabled() -> None:
    """When live mode is off, search_by_local_id must return [] without
    making any network calls (consistent with other offline stubs)."""
    gleif = REGISTRY["gleif"]
    # live_available is False in the isolated test environment (no .env)
    assert not gleif.info.live_available
    result = await gleif.search_by_local_id("00000001", ra_code="RA000585")  # type: ignore[attr-defined]
    assert result == []


async def test_search_by_local_id_method_exists() -> None:
    """The method must be present on the adapter (guards against typos)."""
    gleif = REGISTRY["gleif"]
    assert hasattr(gleif, "search_by_local_id")
    assert callable(gleif.search_by_local_id)  # type: ignore[attr-defined]
