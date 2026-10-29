"""The suite is offline *and* dataless by construction (tests/_data_root.py).

A local release asset must never change a verdict: the same commit gave 40,
31 and 25 failures on one machine on 27 Oct 2026 depending on what sat in
``data/``. These tests pin the seeded data root, the removed asset paths and
the ``local_data`` opt-out.
"""

from __future__ import annotations

import os
import re
import uuid
from pathlib import Path

import pytest

from opencheck.cache import data_root
from tests import _data_root

#: Release assets and runtime files a developer's ``data/`` may hold. None of
#: them is tracked, so none may appear under the test data root.
_ASSETS = (
    "ariregister.sqlite",
    "ariregister.db",
    "meip.sqlite",
    "jar_lithuania.sqlite",
    "onrc_romania.sqlite",
    "chilecompra.sqlite",
    "form13f.sqlite",
    "entity_pages.sqlite",
    "gem",
    "estonia",
    "ariregister_raw",
    "securities",
)


def test_the_data_root_is_the_seeded_one_not_the_checkout() -> None:
    root = data_root()
    seeded = _data_root.seeded_data_root()
    assert seeded is not None, "conftest did not install the dataless data root"
    assert root == seeded
    assert root.resolve() != (_data_root.repo_root() / "data").resolve()
    assert root.resolve() != _data_root.real_data_root().resolve()


def test_every_tracked_data_file_resolves_through_the_seeded_root() -> None:
    tracked = _data_root.tracked_data_files()
    assert any(p.startswith("cache/bods_data/") for p in tracked)
    root = data_root()
    for rel in tracked:
        if not (_data_root.repo_root() / "data" / rel).is_file():
            continue  # deleted in the working tree
        assert (root / rel).is_file(), f"tracked data/{rel} is missing from the test data root"


def test_the_curated_bundles_load_through_the_seeded_root() -> None:
    from opencheck.bods_data import load_bundle

    bundle = load_bundle("gleif", "213800LH1BZH3DI6G760")
    assert bundle, "a committed GLEIF bundle did not resolve through the seeded root"


def test_no_release_asset_is_visible() -> None:
    root = data_root()
    present = [a for a in _ASSETS if (root / a).exists()]
    assert not present, f"release assets under the test data root: {present}"
    live = root / "cache" / "live"
    # The live response cache is the suite's own (tests write to it); what
    # must never be there is a developer's — it would be a symlink or a file
    # placed by the seed, and the seed places tracked files only.
    if live.exists():
        assert not any(p.is_symlink() for p in live.rglob("*"))


def test_the_estonian_index_tier_is_unreachable_by_default() -> None:
    from opencheck.sources import ariregister

    ariregister.reset_connection()
    assert ariregister.db_path() == data_root() / "ariregister.sqlite"
    assert not ariregister.index_available()


def test_a_write_under_the_data_root_never_reaches_the_checkout() -> None:
    name = f"dataless-{uuid.uuid4().hex}.json"
    target = data_root() / "cache" / "bods_data" / "gleif" / name
    target.write_text("{}", encoding="utf-8")
    try:
        assert not (_data_root.repo_root() / "data" / "cache" / "bods_data" / "gleif" / name).exists()
    finally:
        target.unlink()


def test_asset_paths_from_the_shell_are_removed() -> None:
    names = _data_root._asset_env_names()
    # conftest sets these two to files that do not exist, after the removal.
    pinned = {"OPENCHECK_ISIN_INDEX_DB_FILE", "OPENCHECK_FORM13F_DB_FILE"}
    assert {"ARIREGISTER_DB_FILE", "OPENCHECK_MEIP_DB_FILE", "EITI_BO_INDEX_PATH"} <= names
    leaked = sorted(n for n in names - pinned if os.environ.get(n))
    assert not leaked, f"asset paths set in the test environment: {leaked}"
    for name in pinned:
        assert not Path(os.environ[name]).exists()


_ENV_READ = re.compile(
    r"""os\.(?:environ\.get|getenv)\(\s*["']([A-Z0-9_]+_(?:PATH|FILE|DIR))["']"""
    r"""|os\.environ\[\s*["']([A-Z0-9_]+_(?:PATH|FILE|DIR))["']\s*\]"""
)


def test_every_asset_path_the_package_reads_is_removed() -> None:
    """A new ``os.environ.get("X_INDEX_PATH")`` in the package is a new door
    for a developer's asset; name it in ``DIRECT_ENV_ASSET_PATHS``."""
    package = Path(__file__).resolve().parents[1] / "opencheck"
    names = _data_root._asset_env_names()
    unnamed: set[str] = set()
    for path in package.rglob("*.py"):
        for m in _ENV_READ.finditer(path.read_text(encoding="utf-8")):
            name = m.group(1) or m.group(2)
            if name not in names:
                unnamed.add(f"{name} ({path.relative_to(package)})")
    assert not unnamed, f"asset paths read from the environment but not removed: {sorted(unnamed)}"


def test_the_fallback_subtrees_cover_every_tracked_file() -> None:
    tracked = _data_root.tracked_data_files()
    uncovered = [
        p
        for p in tracked
        if not any(p == sub or p.startswith(sub + "/") for sub in _data_root.TRACKED_FALLBACK)
    ]
    assert not uncovered, (
        "tracked data/ files outside TRACKED_FALLBACK (used when git cannot answer): "
        f"{uncovered}"
    )


def test_seed_mirrors_tracked_files_into_real_directories(tmp_path: Path) -> None:
    dest = tmp_path / "data"
    placed = _data_root.seed(dest)
    assert placed >= len([p for p in _data_root.tracked_data_files() if p.startswith("cache/")])
    gleif = dest / "cache" / "bods_data" / "gleif"
    assert gleif.is_dir() and not gleif.is_symlink(), "directories are real, so writes stay here"


@pytest.mark.local_data("no-such-asset-ever-shipped.sqlite")
def test_a_local_data_test_skips_when_its_asset_is_absent() -> None:
    pytest.fail("a local_data test ran although its asset is absent")


@pytest.mark.local_data("cache/bods_data")
def test_a_local_data_test_reads_the_real_root() -> None:
    assert data_root() == _data_root.real_data_root()
    assert data_root() != _data_root.seeded_data_root()
