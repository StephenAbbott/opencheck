"""Regression: the data-root resolver must not be shadowed by a stray ``data/``.

The GEM/GEOT work added ``opencheck/data`` (package assets), which the old
"first directory containing data/" heuristic latched onto — silently moving
``data_root`` off the repo root and emptying every pre-extracted GLEIF/UK-PSC
subgraph override (Newcastle, BP, … dropped from a full ownership tree to ~5
live statements). The resolver now anchors on ``data/cache/bods_data``.
"""

from __future__ import annotations

from opencheck.cache import _find_project_root


def test_resolver_skips_a_shadowing_package_data_dir(tmp_path):
    # Real repo root: has the committed bundles under data/cache/bods_data.
    root = tmp_path / "repo"
    (root / "data" / "cache" / "bods_data" / "gleif").mkdir(parents=True)
    # A nested package with its OWN data/ dir (GEM/GEOT assets) — must NOT win.
    pkg = root / "backend" / "opencheck"
    (pkg / "data" / "gem").mkdir(parents=True)
    (root / "backend" / "pyproject.toml").write_text("")
    start = pkg / "cache.py"
    start.write_text("")

    assert _find_project_root(start) == root


def test_resolver_skips_a_stray_runtime_backend_data_dir(tmp_path):
    # A runtime cache may appear at backend/data/cache — still must not win
    # over the repo-root data/cache/bods_data.
    root = tmp_path / "repo"
    (root / "data" / "cache" / "bods_data").mkdir(parents=True)
    (root / "backend" / "data" / "cache" / "live").mkdir(parents=True)
    (root / "backend" / "pyproject.toml").write_text("")
    pkg = root / "backend" / "opencheck"
    pkg.mkdir(parents=True)
    start = pkg / "cache.py"
    start.write_text("")

    assert _find_project_root(start) == root


def test_committed_curated_gleif_bundle_resolves():
    """End-to-end: the real repo's committed Newcastle subgraph must load (it
    is the bundle that vanished in production)."""
    from opencheck import bods_data

    bundle = bods_data.gleif_bundle_for_lei("213800AG2V6YE68H5N63")
    assert bundle is not None, "curated GLEIF override did not resolve — data_root regression"
    assert len(bundle) > 5, "expected the full pre-extracted subgraph, not the thin live slice"
