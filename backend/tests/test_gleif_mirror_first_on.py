"""Mirror-first in production — the preconditions the GLEIF-refusals ticket
(15 Oct 2026) set before the flag could be flipped, and the flip itself.

* A store-served anchor carries the direct children to the live path's cap,
  not one page of 100 — otherwise mirror-first would make "Shell's 105 read as
  100" the everyday case rather than the rate-limited one.
* A top-up the mirror could not make names its reason, so the lookup records
  that OpenCorporates was not consulted (the degradation itself is pinned in
  ``test_gleif_fallback_crossrefs``).
* ``render.yaml`` turns the flag on; the config default stays off.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
import yaml
from pytest_httpx import HTTPXMock

from opencheck import entity_pages as ep
from opencheck.config import Settings, get_settings
from opencheck.gleif_throttle import get_throttle, reset_throttle_for_tests
from opencheck.sources import gleif as gleif_module
from opencheck.sources.gleif import GleifAdapter
from tests.test_gleif_mirror_first import _configure, _isolated, mirror_db  # noqa: F401
from tests.test_gleif_mirror_store import EASY

_REPO = Path(__file__).resolve().parents[2]
_HEAD = "HEAD0000000000000X01"


def _store_with_children(tmp_path: Path, n: int) -> Path:
    db = tmp_path / f"children-{n}.sqlite"
    conn = sqlite3.connect(db)
    conn.executescript(ep.SCHEMA)
    rows = [(_HEAD, "BIG GROUP PLC", "big-group-plc", "ACTIVE", "ISSUED", "GB", "LONDON",
             "GB", None)]
    rows += [
        (f"CHILD{i:015d}", f"BIG GROUP SUBSIDIARY {i:04d}", f"big-group-subsidiary-{i:04d}",
         "ACTIVE", "ISSUED", "NL", "AMSTERDAM", "NL", _HEAD)
        for i in range(n)
    ]
    conn.executemany(
        "INSERT INTO entities (lei, name, slug, entity_status, registration_status, "
        "jurisdiction, city, country, direct_parent_lei) VALUES (?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.execute(
        "INSERT INTO meta (key, value) VALUES ('source_publish_date', '2026-10-15 16:00:00')"
    )
    conn.commit()
    conn.close()
    return db


def _use_store(monkeypatch: pytest.MonkeyPatch, db: Path) -> None:
    monkeypatch.setenv("OPENCHECK_ENTITY_PAGES_DB_FILE", str(db))
    get_settings.cache_clear()
    ep.reset_store_for_tests()


async def test_a_store_served_anchor_carries_more_than_one_page_of_children(
    httpx_mock: HTTPXMock, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """105 direct children, as Shell has: all of them, as the live path's
    paging would give — the mirror and the rate-limit fallback build the
    bundle in the same place."""
    _use_store(monkeypatch, _store_with_children(tmp_path, 105))
    httpx_mock.add_response(status_code=429, is_reusable=True)
    bundle = await GleifAdapter().fetch(_HEAD)
    assert bundle["snapshot_source"] == "fallback"
    assert bundle["direct_children_total"] == 105
    assert len(bundle["direct_children"]) == 105


async def test_the_store_answers_to_the_live_paths_cap(
    httpx_mock: HTTPXMock, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cap = gleif_module._DIRECT_CHILDREN_PAGE_CAP * 100
    assert cap == 1000
    _use_store(monkeypatch, _store_with_children(tmp_path, cap + 1))
    httpx_mock.add_response(status_code=429, is_reusable=True)
    bundle = await GleifAdapter().fetch(_HEAD)
    assert len(bundle["direct_children"]) == cap
    assert bundle["direct_children_total"] == cap + 1  # the total still says how many


async def test_a_skipped_top_up_names_its_reason(
    httpx_mock: HTTPXMock, mirror_db: Path, monkeypatch: pytest.MonkeyPatch  # noqa: F811
) -> None:
    """No headroom: the anchor is served from the mirror without the ids, and
    the bundle says why — the pipeline turns that into the degradation."""
    _configure(monkeypatch, mirror_db, mirror_first=True)
    monkeypatch.setenv("OPENCHECK_GLEIF_RATE_LIMIT_PER_MINUTE", "3")
    get_settings.cache_clear()
    reset_throttle_for_tests()
    for _ in range(2):
        await get_throttle().acquire()
    bundle = await GleifAdapter().fetch(EASY)
    assert httpx_mock.get_requests() == []
    assert bundle["crossrefs_available"] is False
    assert bundle["crossrefs_unavailable_reason"] == "rate_limited"


async def test_a_failed_top_up_names_its_reason(
    httpx_mock: HTTPXMock, mirror_db: Path, monkeypatch: pytest.MonkeyPatch  # noqa: F811
) -> None:
    _configure(monkeypatch, mirror_db, mirror_first=True)
    httpx_mock.add_response(status_code=500)
    bundle = await GleifAdapter().fetch(EASY)
    assert bundle["snapshot_source"] == "mirror"
    assert bundle["crossrefs_unavailable_reason"] == "upstream_error"


async def test_a_top_up_that_answers_clears_the_reason(
    httpx_mock: HTTPXMock, mirror_db: Path, monkeypatch: pytest.MonkeyPatch  # noqa: F811
) -> None:
    _configure(monkeypatch, mirror_db, mirror_first=True)
    httpx_mock.add_response(
        json={"data": {"id": EASY, "attributes": {"lei": EASY, "ocid": "gr/007132601000"}}}
    )
    bundle = await GleifAdapter().fetch(EASY)
    assert bundle["crossrefs_available"] is True
    assert "crossrefs_unavailable_reason" not in bundle


def _render_env() -> dict[str, str]:
    config = yaml.safe_load((_REPO / "render.yaml").read_text())
    [service] = [s for s in config["services"] if s.get("type") == "web"]
    return {e["key"]: e.get("value") for e in service["envVars"]}


def test_production_serves_the_gleif_anchor_from_the_mirror_first() -> None:
    """The flip. Set the value to "false" to go back to live-first."""
    env = _render_env()
    assert env["OPENCHECK_GLEIF_MIRROR_FIRST"] == "true"
    assert env["OPENCHECK_GLEIF_LIVE_CONFIRM"] == "true"
    # The mirror it reads is the persistent-disk file the deltas keep fresh.
    assert env["OPENCHECK_ENTITY_PAGES_DB_FILE"] == "/var/data/entity_pages.sqlite"


def test_the_config_default_stays_live_first() -> None:
    fields = Settings.model_fields
    assert fields["gleif_mirror_first"].default is False
    assert fields["gleif_live_confirm"].default is False
