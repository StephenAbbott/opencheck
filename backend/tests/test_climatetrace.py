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


def _make_gem_zip(
    tmp_path: Path,
    rows: list[dict],
    rel_rows: list[dict] | None = None,
    asset_rows: list[dict] | None = None,
) -> Path:
    """Create a minimal GEM ownership.zip under *tmp_path* and return its path.

    Optionally include the entity-relationship and entity-asset CSVs that real
    releases ship alongside the entities CSV.
    """
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

    def _csv_text(fields: list[str], data: list[dict]) -> str:
        b = io.StringIO()
        w = csv.DictWriter(b, fieldnames=fields)
        w.writeheader()
        for r in data:
            w.writerow({f: r.get(f, "") for f in fields})
        return b.getvalue()

    with zipfile.ZipFile(zip_path, "w") as zf:
        # Use a dated filename like real GEM releases (e.g. ownership_all_entities_020626.csv)
        zf.writestr("ownership_all_entities_test.csv", buf.getvalue())
        if rel_rows is not None:
            zf.writestr(
                "ownership_all_entity_relationships_test.csv",
                _csv_text(
                    [
                        "subject_entity_id",
                        "subject_name",
                        "owner_entity_id",
                        "owner_name",
                        "percent_of_ownership",
                        "data_source_url",
                    ],
                    rel_rows,
                ),
            )
        if asset_rows is not None:
            zf.writestr(
                "ownership_all_entity_asset_relationships_test.csv",
                _csv_text(
                    [
                        "source_id",
                        "source_name",
                        "immediate_source_owner_entity_id",
                        "immediate_source_owner",
                        "source_sector",
                        "source_subsector",
                    ],
                    asset_rows,
                ),
            )

    return zip_path


def _reset_indexes() -> None:
    import opencheck.sources.climatetrace as _ct_mod

    _ct_mod._lei_index = None
    _ct_mod._entity_index = None
    _ct_mod._rel_children = None
    _ct_mod._asset_index = None


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


def test_parse_parents_strips_share_brackets() -> None:
    """GEM embeds shares in both columns: 'E1000… [55.0%]' / 'Vivant Corp [55.0%]'."""
    row = {
        "Gem parents IDs": "E100000000817 [55.0%]",
        "Gem parents": "Vivant Corp [55.0%]",
    }
    parents = _parse_parents(row)
    assert len(parents) == 1
    assert parents[0]["entity_id"] == "E100000000817"
    assert parents[0]["name"] == "Vivant Corp"
    assert parents[0]["share"] == pytest.approx(55.0)


def test_parse_parents_share_without_decimal() -> None:
    row = {
        "Gem parents IDs": "E100002017082 [100%]",
        "Gem parents": "1 Thing Investments SA [100%]",
    }
    parents = _parse_parents(row)
    assert parents[0]["entity_id"] == "E100002017082"
    assert parents[0]["share"] == pytest.approx(100.0)


def test_parse_parents_multiple_with_shares() -> None:
    row = {
        "Gem parents IDs": "E100000000651 [50.0%]; E100000001982 [50.0%]",
        "Gem parents": "Mitsui & Co Ltd [50.0%]; The Chugoku Electric Power Co Inc [50.0%]",
    }
    parents = _parse_parents(row)
    assert len(parents) == 2
    assert parents[0]["entity_id"] == "E100000000651"
    assert parents[0]["name"] == "Mitsui & Co Ltd"
    assert parents[1]["entity_id"] == "E100000001982"
    assert parents[1]["share"] == pytest.approx(50.0)


def test_parse_parents_no_share_is_none() -> None:
    row = {"Gem parents IDs": "E100002016820", "Gem parents": "Plain Parent Ltd"}
    parents = _parse_parents(row)
    assert parents[0]["entity_id"] == "E100002016820"
    assert parents[0]["share"] is None


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
        result = asyncio.run(
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
        result = asyncio.run(
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

        r1 = asyncio.run(
            adapter.fetch_by_lei("AAAABBBBCCCCDDDDEEEE")
        )
        r2 = asyncio.run(
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
        result = asyncio.run(
            adapter.fetch_by_lei("NOTFOUNDXXXXXXXXXXX0")
        )
        assert result is None
    finally:
        get_settings.cache_clear()
        _ct_mod._lei_index = None
        _ct_mod._entity_index = None


# ---------------------------------------------------------------------------
# Ownership summary (entity-relationship + entity-asset indexes)
# ---------------------------------------------------------------------------


def test_ownership_summary_counts_direct_and_group_assets(tmp_path, monkeypatch) -> None:
    """Group counts walk the entity→entity graph; direct counts don't."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_DISABLE_DOTENV", "1")
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")

    _make_gem_zip(
        tmp_path,
        rows=[
            {"Entity ID": "E1", "Full Name": "Parent Plc"},
            {"Entity ID": "E2", "Full Name": "Sub One Ltd"},
            {"Entity ID": "E3", "Full Name": "Sub Two Ltd"},
        ],
        rel_rows=[
            # E1 owns E2 (60%), E2 owns E3 (100%) — two-hop chain.
            {"subject_entity_id": "E2", "subject_name": "Sub One Ltd",
             "owner_entity_id": "E1", "owner_name": "Parent Plc",
             "percent_of_ownership": "60.0"},
            {"subject_entity_id": "E3", "subject_name": "Sub Two Ltd",
             "owner_entity_id": "E2", "owner_name": "Sub One Ltd",
             "percent_of_ownership": "100.0"},
        ],
        asset_rows=[
            {"source_id": "A1", "source_name": "Plant One",
             "immediate_source_owner_entity_id": "E1",
             "source_sector": "power", "source_subsector": "electricity-generation"},
            {"source_id": "A2", "source_name": "Plant Two",
             "immediate_source_owner_entity_id": "E2",
             "source_sector": "power", "source_subsector": "electricity-generation"},
            {"source_id": "A3", "source_name": "Mine One",
             "immediate_source_owner_entity_id": "E3",
             "source_sector": "fossil-fuel-operations", "source_subsector": "coal-mining"},
            # Duplicate asset row for A3 — must not be double counted.
            {"source_id": "A3", "source_name": "Mine One",
             "immediate_source_owner_entity_id": "E3",
             "source_sector": "fossil-fuel-operations", "source_subsector": "coal-mining"},
        ],
    )

    from opencheck.config import get_settings
    from opencheck.sources.climatetrace import _ownership_summary

    get_settings.cache_clear()
    _reset_indexes()
    try:
        summary = _ownership_summary("E1")
        assert summary["direct_asset_count"] == 1
        assert summary["group_asset_count"] == 3   # A1 + A2 + A3, deduped
        assert summary["subsidiary_count"] == 2    # E2 + E3 (transitive)
        assert summary["group_assets_by_sector"] == {
            "power": 2,
            "fossil-fuel-operations": 1,
        }
        assert summary["direct_subsidiaries"][0]["entity_id"] == "E2"
        assert summary["direct_subsidiaries"][0]["percent"] == pytest.approx(60.0)

        leaf = _ownership_summary("E3")
        assert leaf["direct_asset_count"] == 1
        assert leaf["group_asset_count"] == 1
        assert leaf["subsidiary_count"] == 0
    finally:
        get_settings.cache_clear()
        _reset_indexes()


def test_ownership_summary_is_cycle_safe(tmp_path, monkeypatch) -> None:
    """Cross-shareholding cycles (E1 ↔ E2) must not loop forever."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_DISABLE_DOTENV", "1")

    _make_gem_zip(
        tmp_path,
        rows=[{"Entity ID": "E1", "Full Name": "A"}, {"Entity ID": "E2", "Full Name": "B"}],
        rel_rows=[
            {"subject_entity_id": "E2", "owner_entity_id": "E1",
             "percent_of_ownership": "50.0"},
            {"subject_entity_id": "E1", "owner_entity_id": "E2",
             "percent_of_ownership": "50.0"},
        ],
        asset_rows=[
            {"source_id": "A1", "immediate_source_owner_entity_id": "E2",
             "source_sector": "power"},
        ],
    )

    from opencheck.config import get_settings
    from opencheck.sources.climatetrace import _ownership_summary

    get_settings.cache_clear()
    _reset_indexes()
    try:
        summary = _ownership_summary("E1")
        assert summary["subsidiary_count"] == 1
        assert summary["group_asset_count"] == 1
    finally:
        get_settings.cache_clear()
        _reset_indexes()


def test_ownership_summary_empty_without_relationship_csvs(tmp_path, monkeypatch) -> None:
    """Older zips without the relationship CSVs degrade to zero counts."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_DISABLE_DOTENV", "1")

    _make_gem_zip(tmp_path, rows=[{"Entity ID": "E1", "Full Name": "A"}])

    from opencheck.config import get_settings
    from opencheck.sources.climatetrace import _ownership_summary

    get_settings.cache_clear()
    _reset_indexes()
    try:
        summary = _ownership_summary("E1")
        assert summary["direct_asset_count"] == 0
        assert summary["group_asset_count"] == 0
        assert summary["subsidiary_count"] == 0
    finally:
        get_settings.cache_clear()
        _reset_indexes()


def test_stub_bundle_includes_ownership_summary(tmp_path, monkeypatch) -> None:
    """fetch_by_lei stub path carries the ownership summary."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_DISABLE_DOTENV", "1")
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")

    _make_gem_zip(
        tmp_path,
        rows=[
            {
                "Entity ID": "E100000001096",
                "Full Name": "BP p.l.c.",
                "Global Legal Entity Identifier Index": "213800LH1BZH3DI6G760",
                "Headquarters Country": "GBR",
            }
        ],
        rel_rows=[],
        asset_rows=[
            {"source_id": "A9", "immediate_source_owner_entity_id": "E100000001096",
             "source_sector": "fossil-fuel-operations"},
        ],
    )

    from opencheck.config import get_settings

    get_settings.cache_clear()
    _reset_indexes()
    try:
        import asyncio
        adapter = ClimateTRACEAdapter()
        result = asyncio.run(
            adapter.fetch_by_lei("213800LH1BZH3DI6G760")
        )
        assert result is not None
        assert result["ownership"]["direct_asset_count"] == 1
        assert result["ownership"]["group_asset_count"] == 1
    finally:
        get_settings.cache_clear()
        _reset_indexes()


# ---------------------------------------------------------------------------
# GCS-first data refresh
# ---------------------------------------------------------------------------


def test_ensure_gem_data_skips_network_when_zip_present(tmp_path, monkeypatch) -> None:
    """A pre-seeded zip with no cached CSVs must not trigger any download."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_DISABLE_DOTENV", "1")
    _make_gem_zip(tmp_path, rows=[])

    from opencheck.config import get_settings
    import opencheck.sources.climatetrace as _ct_mod

    get_settings.cache_clear()
    try:
        with patch.object(_ct_mod, "_download_gem_csvs_from_gcs") as gcs_mock, \
             patch.object(_ct_mod.httpx, "Client") as client_mock:
            _ct_mod._ensure_gem_data()
            gcs_mock.assert_not_called()
            client_mock.assert_not_called()
    finally:
        get_settings.cache_clear()


def test_ensure_gem_data_prefers_gcs_when_nothing_on_disk(tmp_path, monkeypatch) -> None:
    """With no local data, the GCS bucket is tried before the GitHub zip."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_DISABLE_DOTENV", "1")

    from opencheck.config import get_settings
    import opencheck.sources.climatetrace as _ct_mod

    get_settings.cache_clear()
    try:
        with patch.object(
            _ct_mod, "_download_gem_csvs_from_gcs", return_value=True
        ) as gcs_mock, patch.object(_ct_mod.httpx, "Client") as client_mock:
            _ct_mod._ensure_gem_data()
            gcs_mock.assert_called_once()
            client_mock.assert_not_called()  # no GitHub zip fallback needed
    finally:
        get_settings.cache_clear()


def test_ensure_gem_data_fresh_csv_short_circuits(tmp_path, monkeypatch) -> None:
    """A fresh cached entities CSV means no network access at all."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_DISABLE_DOTENV", "1")

    from opencheck.config import get_settings
    import opencheck.sources.climatetrace as _ct_mod

    get_settings.cache_clear()
    try:
        csv_path = _ct_mod._gem_csv_path("entities")
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.write_text("Entity ID,Full Name\nE1,Test Co\n")

        with patch.object(_ct_mod, "_download_gem_csvs_from_gcs") as gcs_mock:
            _ct_mod._ensure_gem_data()
            gcs_mock.assert_not_called()
    finally:
        get_settings.cache_clear()


def test_cached_gcs_csv_preferred_over_zip(tmp_path, monkeypatch) -> None:
    """When both a cached CSV and a zip exist, the CSV (newer release) wins."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_DISABLE_DOTENV", "1")
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")

    # Zip says "Old Name"; cached GCS CSV says "New Name".
    _make_gem_zip(
        tmp_path,
        rows=[{"Entity ID": "E1", "Full Name": "Old Name",
               "Global Legal Entity Identifier Index": "AAAABBBBCCCCDDDDEEEE"}],
    )
    from opencheck.config import get_settings
    import opencheck.sources.climatetrace as _ct_mod

    get_settings.cache_clear()
    _reset_indexes()
    try:
        csv_path = _ct_mod._gem_csv_path("entities")
        csv_path.write_text(
            "Entity ID,Full Name,Global Legal Entity Identifier Index\n"
            "E1,New Name,AAAABBBBCCCCDDDDEEEE\n"
        )
        import asyncio
        adapter = ClimateTRACEAdapter()
        result = asyncio.run(
            adapter.fetch_by_lei("AAAABBBBCCCCDDDDEEEE")
        )
        assert result is not None
        assert result["entity_name"] == "New Name"
    finally:
        get_settings.cache_clear()
        _reset_indexes()


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
        hits = asyncio.run(
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
        hits = asyncio.run(
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
        r_gleif = asyncio.run(
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
        result = asyncio.run(
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
        result = asyncio.run(
            adapter.fetch_by_lei("213800LH1BZH3DI6G760")
        )
        # GEM self-reported LEI still works
        assert result is not None
        assert result["entity_id"] == "E100000001096"
    finally:
        get_settings.cache_clear()
        _ct_mod._lei_index = None
        _ct_mod._entity_index = None
