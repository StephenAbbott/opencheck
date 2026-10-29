"""The suite is dataless by construction: a test never reads a developer's
release assets.

Phase 266 made the suite offline (``_network_guard``); it did not make it
dataless. ``cache.data_root()`` is ``$OPENCHECK_DATA_ROOT`` or, unset, the
repo-root ``data/`` — and that directory holds both the committed curated
bundles (``data/cache/bods_data``, ``data/demo``, ``data/laundromat``) and
every gitignored release asset a developer has downloaded: the Estonian index,
``meip.sqlite``, the GEM CSVs, the live response cache. On 27 Oct 2026 one
commit gave 40, 31 and 25 failures on one machine depending on what was on
disk (the "test suite reads whatever release assets a developer has on disk"
ticket). CI, which checks out tracked files and downloads nothing, saw the
third.

``install()`` runs at conftest import time, before any test module is
collected:

1. It points ``OPENCHECK_DATA_ROOT`` at a per-session temporary directory
   **seeded from the tracked ``data/`` files** — each tracked file symlinked
   (copied where the platform refuses a symlink) into real directories, so the
   curated bundles resolve and anything a test writes (the live cache,
   dispositions, a test-built index) lands in the temporary tree, never the
   checkout. A developer's own ``OPENCHECK_DATA_ROOT`` is overridden too: it
   is where their assets are.
2. It removes the environment variables that name an asset file or directory
   (every ``Settings`` alias ending ``_FILE``, ``_DB`` or ``_DIR``, plus the
   ``*_INDEX_PATH`` overrides the bulk adapters read directly). Dotenv loading
   is already off for the suite; this closes the other door, an ``export`` in
   the developer's shell. A test that wants one sets it with ``monkeypatch``.

A test that deliberately reads a real asset says so with
``@pytest.mark.local_data("meip.sqlite", ...)``: conftest points the data root
back at the developer's real one for that test, and skips it when any named
asset is absent — so it skips in CI and runs on a machine that has the file.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

DATA_ROOT_ENV = "OPENCHECK_DATA_ROOT"

#: Asset paths read straight from ``os.environ`` rather than through
#: ``Settings``. ``test_dataless_suite`` scans the package for any other
#: ``os.environ.get("…_PATH" | "…_FILE" | "…_DIR")`` and fails until it is
#: named here.
DIRECT_ENV_ASSET_PATHS: frozenset[str] = frozenset(
    {
        "CAC_NIGERIA_INDEX_PATH",
        "EITI_ASSESSMENT_INDEX_PATH",
        "EITI_BO_INDEX_PATH",
        "EITI_SOE_INDEX_PATH",
        "EITI_ZAMBIA_INDEX_PATH",
    }
)

#: The tracked subtrees of ``data/`` — used only when ``git ls-files`` cannot
#: answer (an exported tarball). ``test_dataless_suite`` checks that every
#: tracked file sits under one of them.
TRACKED_FALLBACK: tuple[str, ...] = (
    "cache/bods_data",
    "demo",
    "laundromat/seed.json",
)

_state: dict[str, Path | list[str]] = {}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def real_data_root() -> Path:
    """The data root a developer's dev server would read: their own
    ``OPENCHECK_DATA_ROOT`` if they set one, else the repo's ``data/``."""
    root = _state.get("real")
    if isinstance(root, Path):
        return root
    override = os.environ.get(DATA_ROOT_ENV)
    return Path(override) if override else repo_root() / "data"


def seeded_data_root() -> Path | None:
    root = _state.get("seeded")
    return root if isinstance(root, Path) else None


def removed_env() -> list[str]:
    names = _state.get("removed_env")
    return list(names) if isinstance(names, list) else []


def tracked_data_files(repo: Path | None = None) -> list[str]:
    """Paths under ``data/`` that git tracks, relative to ``data/``."""
    repo = repo or repo_root()
    try:
        out = subprocess.run(
            ["git", "ls-files", "-z", "--", "data"],
            cwd=repo,
            capture_output=True,
            check=True,
            timeout=30,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        out = b""
    files = [p[len("data/") :] for p in out.decode("utf-8").split("\0") if p.startswith("data/")]
    if files:
        return sorted(files)
    found: list[str] = []
    data = repo / "data"
    for sub in TRACKED_FALLBACK:
        path = data / sub
        if path.is_file():
            found.append(sub)
        elif path.is_dir():
            found.extend(
                str(p.relative_to(data)) for p in sorted(path.rglob("*")) if p.is_file()
            )
    return found


def seed(dest: Path, repo: Path | None = None) -> int:
    """Mirror the tracked ``data/`` files into ``dest``: real directories,
    one symlink per file (a copy where symlinks are refused). Returns the
    number of files placed."""
    repo = repo or repo_root()
    src_root = repo / "data"
    placed = 0
    for rel in tracked_data_files(repo):
        src = src_root / rel
        if not src.is_file():  # tracked but deleted in the working tree
            continue
        target = dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            target.symlink_to(src)
        except OSError:
            shutil.copy2(src, target)
        placed += 1
    return placed


def _asset_env_names() -> set[str]:
    from opencheck.config import Settings

    names = set(DIRECT_ENV_ASSET_PATHS)
    for field in Settings.model_fields.values():
        alias = field.alias or ""
        if alias.endswith(("_FILE", "_DB", "_DIR")):
            names.add(alias)
    return names


def install() -> Path:
    """Point the data root at a seeded temporary tree and drop asset-path
    environment variables. Idempotent."""
    seeded = seeded_data_root()
    if seeded is not None:
        return seeded
    _state["real"] = real_data_root()
    removed = sorted(n for n in _asset_env_names() if n in os.environ)
    for name in removed:
        del os.environ[name]
    _state["removed_env"] = removed
    root = Path(tempfile.mkdtemp(prefix="opencheck-test-data-")) / "data"
    root.mkdir()
    seed(root)
    os.environ[DATA_ROOT_ENV] = str(root)
    _state["seeded"] = root
    return root


#: ``(module, function)`` pairs that drop a process-wide handle on a file
#: under the data root. Called around a ``local_data`` test, so a connection
#: it opened on a real asset cannot outlive it.
_STORE_RESETS: tuple[tuple[str, str], ...] = (
    ("opencheck.sources.ariregister", "reset_connection"),
    ("opencheck.sources.jar_lithuania", "reset_connection"),
    ("opencheck.sources.onrc_romania", "reset_connection"),
    ("opencheck.sources.apr_serbia", "reset_connection"),
    ("opencheck.sources.asp_moldova", "reset_connection"),
    ("opencheck.sources.chilecompra", "reset_connection"),
    ("opencheck.form13f", "reset_connection"),
    ("opencheck.meip", "reload_store"),
    ("opencheck.entity_pages", "reload_store"),
    ("opencheck.psc_graph", "reload_store"),
)


def reset_stores() -> None:
    import importlib

    for module_name, func_name in _STORE_RESETS:
        try:
            module = importlib.import_module(module_name)
        except ImportError:  # pragma: no cover - a module removed later
            continue
        getattr(module, func_name)()
