"""Unit tests for the Climate TRACE / GEM adapter (offline / stub paths)."""

from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest

from opencheck.sources.climatetrace import (
    ClimateTRACEAdapter,
    _load_gleif_gem_mapping,
    _parse_emissions,
    _parse_parents,
    _stub_bundle,
)
from opencheck.sources.base import SearchKind


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_gem_zip(tmp_path: Path, rows: list[dict]) -> Path:
    """Create a minimal GEM ownership.zip under *tmp_path* and return its path."""
    gem_dir = tmp_path / "gem"
    gem_dir.mkdir(parents=True, exist_ok=True)
    zip_path = gem_dir / "ownership.zip"

    fieldnames = [
        "Entity ID",
        "Full Name",
        "Global Legal Entity Identifier Index",
        "Headquarters Country",
        "Gem parents IDs",
        "Gem parents",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow({f: row.get(f, "") for f in fieldnames})

    with zipfile.ZipFile(zip_path, "w") as zf:
        # Use a dated filename like real GEM releases (e.g. ownership_all_entities_020626.csv)
        zf.writestr("ownership_all_entities_test.csv", buf.getvalue())

    return zip_path


# ---------------------------------------------------------------------------
# _parse_emissions
# ---------------------------------------------------------------------------


def test_parse_emissions_v7_structured_response() -> None:
    """Primary path: real Climate TRACE v7 API shape."""
    payload = {
        "totals": {
            "summaries": [
                {"gas": "co2e_100yr", "emissionsQuantity": 345_183_438.27, "percentage": 100}
            ]
        },
        "sectors": {
            "summaries": [
                {"sector": "fossil-fuel-operations", "gas": "co2e_100yr", "emissionsQuantity": 333_224_762.65},
                {"sector": "power", "gas": "co2e_100yr", "emissionsQuantity": 1_386_099.9},
                {"sector": "manufacturing", "gas": "co2e_100yr", "emissionsQuantity": 10_572_575.71},
            ]
        },
    }
    result = _parse_emissions(payload)
    assert result["total_co2e_tonnes"] == pytest.approx(345_183_438.27)
    assert result["by_sector"]["fossil-fuel-operations"] == pytest.approx(333_224_762.65)
    assert result["by_sector"]["power"] == pytest.approx(1_386_099.9)
    assert result["year"] == 2024


def test_parse_emissions_v7_ignores_non_co2e_summaries() -> None:
    """Only the co2e_100yr gas summary is used for the total."""
    payload = {
        "totals": {
            "summaries": [
                {"gas": "ch4", "emissionsQuantity": 9_999_999},
                {"gas": "co2e_100yr", "emissionsQuantity": 500_000},
            ]
        },
        "sectors": {"summaries": []},
    }
    result = _parse_emissions(payload)
    assert result["total_co2e_tonnes"] == pytest.approx(500_000)


def test_parse_emissions_legacy_list_of_rows() -> None:
    """Fallback: pre-v7 flat-list shape still works."""
    rows = [
        {"emissions_quantity": "1000000", "sector": "oil-and-gas"},
        {"emissions_quantity": "500000", "sector": "coal"},
    ]
    result = _parse_emissions(rows)
    assert result["total_co2e_tonnes"] == pytest.approx(1_500_000)
    assert result["by_sector"]["oil-and-gas"] == pytest.approx(1_000_000)
    assert result["by_sector"]["coal"] == pytest.approx(500_000)
    assert result["year"] == 2024


def test_parse_emissions_legacy_dict_with_emissions_key() -> None:
    payload = {
        "emissions": [
            {"emissions_quantity": "200000", "sector": "power"},
        ]
    }
    result = _parse_emissions(payload)
    assert result["total_co2e_tonnes"] == pytest.approx(200_000)


def test_parse_emissions_empty_returns_zero() -> None:
    result = _parse_emissions([])
    assert result["total_co2e_tonnes"] == 0.0


def test_parse_emissions_bad_value_is_skipped() -> None:
    rows = [{"emissions_quantity": "not-a-number", "sector": "unknown"}]
    result = _parse_emissions(rows)
    assert result["total_co2e_tonnes"] == 0.0


# ---------------------------------------------------------------------------
# _parse_parents
# ---------------------------------------------------------------------------


def test_parse_parents_single_parent() -> None:
    row = {
        "Gem parents IDs": "E100000000001",
        "Gem parents": "Acme Corp",
    }
    parents = _parse_parents(row)
    assert len(parents) == 1
    assert parents[0]["entity_id"] == "E100000000001"
    assert parents[0]["name"] == "Acme Corp"


def test_parse_parents_multiple_parents_semicolon_delimited() -> None:
    row = {
        "Gem parents IDs": "E100000000001;E100000000002",
        "Gem parents": "Acme Corp;Beta Industries",
    }
    parents = _parse_parents(row)
    assert len(parents) == 2
    assert parents[1]["entity_id"] == "E100000000002"


def test_parse_parents_empty_returns_empty_list() -> None:
    assert _parse_parents({}) == []


# ---------------------------------------------------------------------------
# _stub_bundle
# ---------------------------------------------------------------------------


def test_stub_bundle_shape() -> None:
    bundle = _stub_bundle(
        entity_id="E100000001096",
        gem_row={"Full Name": "BP p.l.c.", "Gem parents IDs": "", "Gem parents": ""},
        lei="213800LH1BZH3DI6G760",
    )
    assert bundle["source_id"] == "climatetrace"
    assert bundle["entity_id"] == "E100000001096"
    assert bundle["entity_name"] == "BP p.l.c."
    assert bundle["is_stub"] is True
    assert bundle["emissions"] == {}


# ---------------------------------------------------------------------------
# ClimateTRACEAdapter.info
# ---------------------------------------------------------------------------


def test_adapter_info_category_is_esg(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_DISABLE_DOTENV", "1")
    from opencheck.config import get_settings

    get_settings.cache_clear()
    try:
        adapter = ClimateTRACEAdapter()
        assert adapter.info.category == "esg"
        assert adapter.info.id == "climatetrace"
        assert adapter.info.requires_api_key is False
        assert SearchKind.ENTITY in adapter.info.supports
        assert SearchKind.PERSON not in adapter.info.supports
    finally:
        get_settings.cache_clear()


# ---------------------------------------------------------------------------
# LEI → entity lookup via GEM index
# ---------------------------------------------------------------------------


def test_fetch_by_lei_returns_none_for_unknown_lei(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_DISABLE_DOTENV", "1")
    _make_gem_zip(tmp_path, [])
    from opencheck.config import get_settings
    from opencheck.sources.climatetrace import _lei_index, _entity_index
    import opencheck.sources.climatetrace as _ct_mod

    get_settings.cache_clear()
    _ct_mod._lei_index = None
    _ct_mod._entity_index = None
    try:
        import asyncio
        adapter = ClimateTRACEAdapter()
        result = asyncio.get_event_loop().run_until_complete(
            adapter.fetch_by_lei("XXXXXXXXXXXXXXXXXXXX")
        )
        assert result is None
    finally:
        get_settings.cache_clear()
        _ct_mod._lei_index = None
        _ct_mod._entity_index = None


def test_fetch_by_lei_returns_stub_when_live_disabled(tmp_path, monkeypatch) -> None:
    """When live is disabled and LEI is in the GEM index, return stub bundle."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_DISABLE_DOTENV", "1")
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")

    _make_gem_zip(
        tmp_path,
        [
            {
                "Entity ID": "E100000001096",
                "Full Name": "BP p.l.c.",
                "Global Legal Entity Identifier Index": "213800LH1BZH3DI6G760",
                "Headquarters Country": "GBR",
                "Gem parents IDs": "",
                "Gem parents": "",
            }
        ],
    )

    from opencheck.config import get_settings
    import opencheck.sources.climatetrace as _ct_mod

    get_settings.cache_clear()
    _ct_mod._lei_index = None
    _ct_mod._entity_index = None
    try:
        import asyncio
        adapter = ClimateTRACEAdapter()
        result = asyncio.get_event_loop().run_until_complete(
            adapter.fetch_by_lei("213800LH1BZH3DI6G760")
        )
        assert result is not None
        assert result["entity_id"] == "E100000001096"
        assert result["entity_name"] == "BP p.l.c."
        assert result["is_stub"] is True
    finally:
        get_settings.cache_clear()
        _ct_mod._lei_index = None
        _ct_mod._entity_index = None


def test_gem_index_maps_multiple_leis(tmp_path, monkeypatch) -> None:
    """Multiple entities in the CSV each get their own LEI index entry."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_DISABLE_DOTENV", "1")
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")

    _make_gem_zip(
        tmp_path,
        [
            {
                "Entity ID": "E100000000001",
                "Full Name": "Alpha Energy Ltd",
                "Global Legal Entity Identifier Index": "AAAABBBBCCCCDDDDEEEE",
                "Headquarters Country": "DEU",
                "Gem parents IDs": "",
                "Gem parents": "",
            },
            {
                "Entity ID": "E100000000002",
                "Full Name": "Beta Coal Inc",
                "Global Legal Entity Identifier Index": "FFFFGGGGHHHHIIIIJJJJ",
                "Headquarters Country": "USA",
                "Gem parents IDs": "",
                "Gem parents": "",
            },
        ],
    )

    from opencheck.config import get_settings
    import opencheck.sources.climatetrace as _ct_mod

    get_settings.cache_clear()
    _ct_mod._lei_index = None
    _ct_mod._entity_index = None
    try:
        import asyncio
        adapter = ClimateTRACEAdapter()

        r1 = asyncio.get_event_loop().run_until_complete(
            adapter.fetch_by_lei("AAAABBBBCCCCDDDDEEEE")
        )
        r2 = asyncio.get_event_loop().run_until_complete(
            adapter.fetch_by_lei("FFFFGGGGHHHHIIIIJJJJ")
        )
        assert r1 is not None and r1["entity_id"] == "E100000000001"
        assert r2 is not None and r2["entity_id"] == "E100000000002"
    finally:
        get_settings.cache_clear()
        _ct_mod._lei_index = None
        _ct_mod._entity_index = None


def test_gem_index_skips_not_found_lei(tmp_path, monkeypatch) -> None:
    """Rows where the LEI column says 'not found' are excluded from the index."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_DISABLE_DOTENV", "1")
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")

    _make_gem_zip(
        tmp_path,
        [
            {
                "Entity ID": "E100000000099",
                "Full Name": "Mystery Corp",
                "Global Legal Entity Identifier Index": "not found",
                "Headquarters Country": "FRA",
                "Gem parents IDs": "",
                "Gem parents": "",
            }
        ],
    )

    from opencheck.config import get_settings
    import opencheck.sources.climatetrace as _ct_mod

    get_settings.cache_clear()
    _ct_mod._lei_index = None
    _ct_mod._entity_index = None
    try:
        import asyncio
        adapter = ClimateTRACEAdapter()
        result = asyncio.get_event_loop().run_until_complete(
            adapter.fetch_by_lei("NOTFOUNDXXXXXXXXXXX0")
        )
        assert result is None
    finally:
        get_settings.cache_clear()
        _ct_mod._lei_index = None
        _ct_mod._entity_index = None


# ---------------------------------------------------------------------------
# Stub search path
# ---------------------------------------------------------------------------


def test_search_returns_stub_when_live_disabled(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_DISABLE_DOTENV", "1")
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    from opencheck.config import get_settings

    get_settings.cache_clear()
    try:
        import asyncio
        adapter = ClimateTRACEAdapter()
        hits = asyncio.get_event_loop().run_until_complete(
            adapter.search("BP", SearchKind.ENTITY)
        )
        assert len(hits) == 1
        assert hits[0].is_stub is True
    finally:
        get_settings.cache_clear()


def _make_gleif_gem_zip(tmp_path: Path, rows: list[dict]) -> Path:
    """Create a minimal GLEIF GEM-to-LEI zip under *tmp_path* and return its path."""
    gem_dir = tmp_path / "gem"
    gem_dir.mkdir(parents=True, exist_ok=True)
    zip_path = gem_dir / "gleif-gem-lei.zip"

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["LEI", "GEM"])
    writer.writeheader()
    for row in rows:
        writer.writerow(row)

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("LEI-GEM-20260520.csv", buf.getvalue())

    return zip_path


def test_search_returns_empty_for_person_kind(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_DISABLE_DOTENV", "1")
    from opencheck.config import get_settings

    get_settings.cache_clear()
    try:
        import asyncio
        adapter = ClimateTRACEAdapter()
        hits = asyncio.get_event_loop().run_until_complete(
            adapter.search("Shell", SearchKind.PERSON)
        )
        assert hits == []
    finally:
        get_settings.cache_clear()


# ---------------------------------------------------------------------------
# GLEIF-certified GEM mapping
# ---------------------------------------------------------------------------


def test_gleif_mapping_parses_two_column_csv(tmp_path, monkeypatch) -> None:
    """_load_gleif_gem_mapping() correctly parses a LEI,GEM CSV from the zip."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    import opencheck.sources.climatetrace as _ct_mod

    # Both are exactly 20-character LEIs
    _make_gleif_gem_zip(
        tmp_path,
        [
            {"LEI": "213800LH1BZH3DI6G760", "GEM": "E100000001096"},
            {"LEI": "AAAABBBBCCCCDDDDEEEE", "GEM": "E100000000001"},
        ],
    )
    mapping = _load_gleif_gem_mapping()
    assert mapping["213800LH1BZH3DI6G760"] == "E100000001096"
    assert mapping["AAAABBBBCCCCDDDDEEEE"] == "E100000000001"
    assert len(mapping) == 2


def test_gleif_mapping_overrides_gem_self_reported_lei(tmp_path, monkeypatch) -> None:
    """When GLEIF and GEM disagree on the LEI for an entity, GLEIF wins."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_DISABLE_DOTENV", "1")
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")

    # GEM CSV says entity E100000000042 has LEI "GEMREPORTED0000000001" (20 chars)
    _make_gem_zip(
        tmp_path,
        [
            {
                "Entity ID": "E100000000042",
                "Full Name": "Acme Energy Ltd",
                "Global Legal Entity Identifier Index": "GEMREPORTED000000001",
                "Headquarters Country": "GBR",
                "Gem parents IDs": "",
                "Gem parents": "",
            }
        ],
    )
    # GLEIF certified mapping assigns a different LEI "GLEIFCERTIFIED000001" (20 chars)
    _make_gleif_gem_zip(
        tmp_path,
        [{"LEI": "GLEIFCERTIFIED000001", "GEM": "E100000000042"}],
    )

    from opencheck.config import get_settings
    import opencheck.sources.climatetrace as _ct_mod

    get_settings.cache_clear()
    _ct_mod._lei_index = None
    _ct_mod._entity_index = None
    try:
        import asyncio
        adapter = ClimateTRACEAdapter()

        # The GLEIF-certified LEI resolves to the entity
        r_gleif = asyncio.get_event_loop().run_until_complete(
            adapter.fetch_by_lei("GLEIFCERTIFIED000001")
        )
        assert r_gleif is not None
        assert r_gleif["entity_id"] == "E100000000042"
        assert r_gleif["entity_name"] == "Acme Energy Ltd"
    finally:
        get_settings.cache_clear()
        _ct_mod._lei_index = None
        _ct_mod._entity_index = None


def test_gleif_mapping_adds_new_lei_not_in_gem_csv(tmp_path, monkeypatch) -> None:
    """GLEIF mapping makes an entity findable by LEI even when GEM's CSV has no LEI."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_DISABLE_DOTENV", "1")
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")

    # GEM CSV has no LEI for this entity
    _make_gem_zip(
        tmp_path,
        [
            {
                "Entity ID": "E100000000099",
                "Full Name": "Mystery Corp",
                "Global Legal Entity Identifier Index": "not found",
                "Headquarters Country": "FRA",
                "Gem parents IDs": "",
                "Gem parents": "",
            }
        ],
    )
    # GLEIF certified mapping now provides the LEI (exactly 20 chars)
    _make_gleif_gem_zip(
        tmp_path,
        [{"LEI": "NEWLEIFROMGLEIF00001", "GEM": "E100000000099"}],
    )

    from opencheck.config import get_settings
    import opencheck.sources.climatetrace as _ct_mod

    get_settings.cache_clear()
    _ct_mod._lei_index = None
    _ct_mod._entity_index = None
    try:
        import asyncio
        adapter = ClimateTRACEAdapter()
        result = asyncio.get_event_loop().run_until_complete(
            adapter.fetch_by_lei("NEWLEIFROMGLEIF00001")
        )
        assert result is not None
        assert result["entity_id"] == "E100000000099"
        assert result["entity_name"] == "Mystery Corp"
    finally:
        get_settings.cache_clear()
        _ct_mod._lei_index = None
        _ct_mod._entity_index = None


def test_gleif_mapping_missing_file_falls_back_gracefully(tmp_path, monkeypatch) -> None:
    """When the GLEIF zip doesn't exist, the index is built from GEM only — no error."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_DISABLE_DOTENV", "1")
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")

    # GEM CSV has a well-formed LEI; GLEIF zip is deliberately absent
    _make_gem_zip(
        tmp_path,
        [
            {
                "Entity ID": "E100000001096",
                "Full Name": "BP p.l.c.",
                "Global Legal Entity Identifier Index": "213800LH1BZH3DI6G760",
                "Headquarters Country": "GBR",
                "Gem parents IDs": "",
                "Gem parents": "",
            }
        ],
    )
    # Do NOT call _make_gleif_gem_zip — the file should simply be absent

    from opencheck.config import get_settings
    import opencheck.sources.climatetrace as _ct_mod

    get_settings.cache_clear()
    _ct_mod._lei_index = None
    _ct_mod._entity_index = None
    try:
        import asyncio
        adapter = ClimateTRACEAdapter()
        result = asyncio.get_event_loop().run_until_complete(
            adapter.fetch_by_lei("213800LH1BZH3DI6G760")
        )
        # GEM self-reported LEI still works
        assert result is not None
        assert result["entity_id"] == "E100000001096"
    finally:
        get_settings.cache_clear()
        _ct_mod._lei_index = None
        _ct_mod._entity_index = None
