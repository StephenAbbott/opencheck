"""Guard: the curated example LEIs in the frontend and the cache-builder script
must stay in sync.

`EXAMPLE_LEIS` (frontend/src/App.tsx) drives which curated cards appear on the
homepage; `CURATED_LEIS` (backend/scripts/build_curated_narratives.py) drives
which summaries get pre-baked. If they drift, a curated card ships without a
cached summary (or a stale cached file lingers). This test fails fast on drift.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
APP_TSX = REPO / "frontend" / "src" / "App.tsx"
SCRIPT = REPO / "backend" / "scripts" / "build_curated_narratives.py"

_LEI = re.compile(r'"([A-Z0-9]{20})"')  # ISO 17442 LEI: 20 alphanumeric chars


def _frontend_leis() -> set[str]:
    text = APP_TSX.read_text(encoding="utf-8")
    # Scope to the EXAMPLE_LEIS array (avoid the `[]` in the type annotation by
    # anchoring on the `= [` assignment).
    block = re.search(r"EXAMPLE_LEIS[^=]*=\s*\[(.*?)\n\];", text, re.S)
    assert block, "EXAMPLE_LEIS array not found in App.tsx"
    return set(_LEI.findall(block.group(1)))


def _script_leis() -> set[str]:
    text = SCRIPT.read_text(encoding="utf-8")
    block = re.search(r"CURATED_LEIS\s*=\s*\[(.*?)\n\]", text, re.S)
    assert block, "CURATED_LEIS list not found in build_curated_narratives.py"
    return set(_LEI.findall(block.group(1)))


def test_curated_leis_match_frontend_examples():
    frontend = _frontend_leis()
    script = _script_leis()
    assert frontend, "parsed no LEIs from EXAMPLE_LEIS — has the format changed?"
    assert frontend == script, (
        "Curated example LEIs are out of sync.\n"
        f"  In the homepage (EXAMPLE_LEIS) but not the script: {sorted(frontend - script)}\n"
        f"  In the script (CURATED_LEIS) but not the homepage: {sorted(script - frontend)}\n"
        "Update CURATED_LEIS in backend/scripts/build_curated_narratives.py to match, "
        "then regenerate the cached summaries."
    )
