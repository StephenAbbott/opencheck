"""Tests for cache-first adapter dispatch.

When a demo fixture exists for a given cache key, the adapter should
serve from the cache regardless of ``allow_live`` / API-key state.
This is what lets the project demo cleanly without network access.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from opencheck.config import get_settings
from opencheck.sources import SearchKind
from opencheck.sources.opensanctions import OpenSanctionsAdapter, _slug


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path: Path):
    """Point the cache at a tmp dir + clear settings between tests."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    monkeypatch.delenv("OPENSANCTIONS_API_KEY", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _seed_demo_fixture(tmp_path: Path, cache_key: str, payload: dict) -> None:
    """Drop a JSON file under ``data/cache/demos/<cache_key>.json``."""
    target = tmp_path / "cache" / "demos" / f"{cache_key}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload))


async def test_search_serves_demo_fixture_when_not_live(tmp_path: Path) -> None:
    """allow_live=false + no key + demo fixture → demo wins, no stub."""
    cache_key = f"opensanctions/search/Person/{_slug('rosneft')}"
    _seed_demo_fixture(
        tmp_path,
        cache_key,
        {
            "results": [
                {
                    "id": "NK-rosneft",
                    "schema": "Company",
                    "caption": "Rosneft Oil Company",
                    "properties": {"name": ["Rosneft"]},
                    "topics": ["sanction"],
                    "datasets": ["us_ofac_sdn"],
                }
            ]
        },
    )

    adapter = OpenSanctionsAdapter()
    hits = await adapter.search("rosneft", SearchKind.PERSON)

    # Demo fixture flowed through — not a stub hit.
    assert len(hits) == 1
    assert hits[0].is_stub is False
    assert hits[0].name == "Rosneft Oil Company"
    assert hits[0].identifiers["opensanctions_id"] == "NK-rosneft"


async def test_search_falls_back_to_stub_when_no_demo_and_no_live(
    tmp_path: Path,
) -> None:
    """No demo fixture + no live → existing stub path."""
    adapter = OpenSanctionsAdapter()
    hits = await adapter.search("anything", SearchKind.PERSON)
    assert len(hits) == 1
    assert hits[0].is_stub is True


async def test_fetch_serves_demo_fixture_when_not_live(tmp_path: Path) -> None:
    cache_key = f"opensanctions/entity/{_slug('NK-rosneft')}"
    _seed_demo_fixture(
        tmp_path,
        cache_key,
        {
            "id": "NK-rosneft",
            "schema": "Company",
            "caption": "Rosneft Oil Company",
            "properties": {"topics": ["sanction"]},
        },
    )

    adapter = OpenSanctionsAdapter()
    bundle = await adapter.fetch("NK-rosneft")

    assert bundle.get("is_stub") is not True
    assert bundle["entity_id"] == "NK-rosneft"
    assert bundle["entity"]["caption"] == "Rosneft Oil Company"


async def test_fetch_falls_back_to_stub_when_no_demo(tmp_path: Path) -> None:
    adapter = OpenSanctionsAdapter()
    bundle = await adapter.fetch("NK-no-fixture")
    assert bundle.get("is_stub") is True
