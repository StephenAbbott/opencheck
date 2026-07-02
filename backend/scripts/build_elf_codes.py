#!/usr/bin/env python3
"""Build the vendored ELF (Entity Legal Form) code → name map.

GLEIF publishes the ISO 20275 Entity Legal Forms code list (the 4-character
codes that appear as ``entity.legalForm.id`` on every LEI record). OpenCheck
vendors a compact ``code → Latin-script legal-form name`` map so ``map_gleif``
can label an entity's legal form even when no national register is hit — which
is what lets the AMLA trust/arrangement risk signal catch a GLEIF-only
foundation (e.g. GLEIF itself, ELF ``2JZ4`` = "Foundation"/"Stiftung").

Usage:

    python3 backend/scripts/build_elf_codes.py <elf-code-list.csv>

Download the current CSV from GLEIF (link on the code-list page):
    https://www.gleif.org/en/lei-data/code-lists/iso-20275-entity-legal-forms-code-list

The script is idempotent — re-run it whenever GLEIF publishes a new version to
refresh ``opencheck/data/elf_codes.json``. A code that carries names in several
languages resolves to one Latin-script label (English row preferred, else the
first non-empty transliterated name, else the local name).
"""

from __future__ import annotations

import csv
import io
import json
import sys
from pathlib import Path

_OUT = Path(__file__).resolve().parent.parent / "opencheck" / "data" / "elf_codes.json"

_CODE = "ELF Code"
_LOCAL = "Entity Legal Form name Local name"
_TRANSLIT = "Entity Legal Form name Transliterated name (per ISO 01-140-10)"
_LANG = "Language Code (ISO 639-1)"
_STATUS = "ELF Status ACTV/INAC"

# Placeholder codes that carry no legal form (used for "not yet listed" / "no
# separate legal form") — skip them.
_SKIP = {"8888", "9999"}


def _rows(path: Path) -> list[dict[str, str]]:
    text = path.read_text(encoding="utf-8-sig")
    lines = text.splitlines()
    # Tolerate a web_fetch metadata preamble before the real CSV header.
    start = next(
        (i for i, line in enumerate(lines) if line.lstrip('"').startswith("ELF Code")),
        0,
    )
    return list(csv.DictReader(io.StringIO("\n".join(lines[start:]))))


def build(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for row in _rows(path):
        code = (row.get(_CODE) or "").strip()
        if not code or code in _SKIP:
            continue
        if (row.get(_STATUS) or "").strip().upper() == "INAC":
            continue
        name = (row.get(_TRANSLIT) or "").strip() or (row.get(_LOCAL) or "").strip()
        if not name:
            continue
        lang = (row.get(_LANG) or "").strip().lower()
        # Prefer the English row; otherwise keep the first non-empty name seen.
        if code not in out or lang == "en":
            out[code] = name
    return dict(sorted(out.items()))


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    data = build(Path(sys.argv[1]))
    _OUT.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(f"wrote {len(data)} ELF codes → {_OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
