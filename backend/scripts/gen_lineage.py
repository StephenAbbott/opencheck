"""Generate ``frontend/src/lib/lineage.json`` from the adapters' lineage.

The FullCheck network, the LEI-confirmation badge and the cross-source
reconciliation box all count sources as corroboration in the browser, so the
browser needs the same lineage table the backend uses
(``opencheck.sources.lineage``). Rather than hand-keep a second copy, this
script writes it; ``tests/test_lineage.py`` fails when the committed file is
stale, the same way the changelog JSON is regenerated rather than edited.

Run from ``backend/``::

    uv run python scripts/gen_lineage.py

The JSON carries three maps: ``derived_from`` (direct upstreams),
``ancestors`` (transitive) and ``descriptions`` (source id → the
``source.description`` label the BODS mapper stamps on statements, which is
the only handle the network has on a statement's origin).
"""

from __future__ import annotations

import sys
from pathlib import Path

from opencheck.bods.mapper import SOURCE_NAMES
from opencheck.sources import lineage

OUT = Path(__file__).resolve().parents[2] / "frontend" / "src" / "lib" / "lineage.json"


def render() -> str:
    return lineage.export_json(SOURCE_NAMES)


def main(argv: list[str]) -> int:
    text = render()
    if "--check" in argv:
        current = OUT.read_text(encoding="utf-8") if OUT.exists() else ""
        if current != text:
            print(f"{OUT} is stale — run scripts/gen_lineage.py", file=sys.stderr)
            return 1
        print("lineage.json is current")
        return 0
    OUT.write_text(text, encoding="utf-8")
    print(f"wrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
