#!/usr/bin/env python3
"""Regenerate ``tests/fixtures/reading_pins/pins.json`` — the digest of what
each series source's mapper writes for its pinned bundles, beside the reading
version it was pinned at (``opencheck/bods/reading_versions.py``).

Run it after a deliberate change to a mapper's ``recordDetails``, having
bumped the source's reading version first::

    uv run python scripts/pin_reading_versions.py

It refuses to move a digest under an unchanged version: that is the mistake
the pin exists to catch (``--check`` reports without writing).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))

PINS_DIR = _HERE.parent / "tests" / "fixtures" / "reading_pins"


def mapped(source_id: str) -> list[dict[str, Any]]:
    """Every statement the source's mapper writes for its pinned bundles."""
    from opencheck import bods, provenance

    mapper = getattr(bods, f"map_{source_id}")
    bundles = json.loads((PINS_DIR / f"{source_id}.json").read_text(encoding="utf-8"))
    out: list[dict[str, Any]] = []
    for bundle in bundles:
        with provenance.mapping_provenance(provenance.STUB_PROVENANCE):
            result = mapper(bundle)
            out.extend(list(getattr(result, "statements", result)))
    return out


def current() -> dict[str, dict[str, Any]]:
    from opencheck.bods.reading_versions import READING_VERSIONS, reading_digest

    return {
        source_id: {"version": version, "digest": reading_digest(mapped(source_id))}
        for source_id, version in sorted(READING_VERSIONS.items())
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    path = PINS_DIR / "pins.json"
    pinned = json.loads(path.read_text()) if path.exists() else {}
    now = current()
    problems = [
        f"{source_id}: the mapper's output changed but its reading version is still "
        f"{pin['version']} — bump READING_VERSIONS[{source_id!r}] first"
        for source_id, pin in now.items()
        if source_id in pinned
        and pinned[source_id]["digest"] != pin["digest"]
        and pinned[source_id]["version"] == pin["version"]
    ]
    for line in problems:
        print(line, file=sys.stderr)
    if problems:
        return 1
    if args.check:
        stale = now != pinned
        print("pins are stale" if stale else "pins are current")
        return 1 if stale else 0
    path.write_text(json.dumps(now, indent=2, sort_keys=True) + "\n")
    print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
