#!/usr/bin/env python3
"""Drift check: vendored FtM edge-schema table vs the followthemoney model.

``opencheck.bods.mapper._FTM_EDGE_SCHEMAS`` vendors every FollowTheMoney
schema declared with ``edge: true``, together with its ``source``/``target``
property names. The BODS mapper routes ALL nested edge handling through that
table (see ``_ftm_edge_relationships``), so if OpenSanctions adds, removes, or
renames an edge schema upstream the table must be updated — otherwise new
relationship shapes are silently dropped, or (worse) edge entities are
mistaken for parties again.

This script compares the vendored table against the *installed*
``followthemoney`` model (the ``ftm`` extra; needs the ICU toolchain — g++,
libicu-dev, pkg-config — at build time, same as bods-ftm). It is check-only:
on drift it prints a diff and exits 1. To fix a failure, update
``_FTM_EDGE_SCHEMAS`` in ``opencheck/bods/mapper.py`` (and decide the BODS
policy for any new edge schema — screening-context edges get ``None``).

Usage:
    python backend/scripts/check_ftm_edges.py          # same as --check
    python backend/scripts/check_ftm_edges.py --check  # CI: fail if stale

CI: the ``ftm-edges`` job in ``.github/workflows/vendored-enum-drift.yml``.
Mirrors the vendored-enum pattern (``revendor_psc_enums.py --check``).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck.bods.mapper import _FTM_EDGE_SCHEMAS  # noqa: E402


def model_edge_table() -> dict[str, tuple[str, str]]:
    """(schema name → (source prop, target prop)) from the installed model."""
    import followthemoney as ftm

    table: dict[str, tuple[str, str]] = {}
    for schema in ftm.model.schemata.values():
        if not schema.edge:
            continue
        source = schema.source_prop
        target = schema.target_prop
        if source is None or target is None:
            # An edge schema without both endpoints cannot be mapped; surface
            # it so a human decides what to do.
            table[schema.name] = ("<missing>", "<missing>")
            continue
        table[schema.name] = (source.name, target.name)
    return table


def main() -> int:
    try:
        model = model_edge_table()
    except ImportError:
        print(
            "followthemoney is not installed — install the `ftm` extra "
            "(uv sync --extra ftm) to run this check.",
            file=sys.stderr,
        )
        return 2

    vendored = {name: (src, tgt) for name, (src, tgt, _policy) in _FTM_EDGE_SCHEMAS.items()}

    problems: list[str] = []
    for name, endpoints in sorted(model.items()):
        if name not in vendored:
            problems.append(
                f"NEW upstream edge schema not vendored: {name} "
                f"(source={endpoints[0]!r}, target={endpoints[1]!r}) — add it to "
                "_FTM_EDGE_SCHEMAS with a BODS policy (or None)."
            )
        elif vendored[name] != endpoints:
            problems.append(
                f"ENDPOINT drift on {name}: vendored {vendored[name]!r} != "
                f"model {endpoints!r}."
            )
    for name in sorted(vendored):
        if name not in model:
            problems.append(
                f"STALE vendored edge schema no longer in the model: {name} — "
                "remove it from _FTM_EDGE_SCHEMAS."
            )

    if problems:
        print("Vendored FtM edge table has drifted from the followthemoney model:")
        for problem in problems:
            print(f"  - {problem}")
        return 1

    print(
        f"OK: {len(vendored)} vendored FtM edge schemata match the installed "
        "followthemoney model."
    )
    return 0


if __name__ == "__main__":
    # --check accepted for symmetry with the other revendor scripts; this
    # script is check-only either way.
    sys.exit(main())
