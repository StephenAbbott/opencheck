"""Resolve GLEIF ISO 20275 Entity Legal Form (ELF) codes to legal-form names.

Every GLEIF LEI record carries ``entity.legalForm.id`` — a 4-character ELF code
(e.g. ``2JZ4`` = Swiss "Foundation"/"Stiftung"). GLEIF only publishes the code,
not the text, on the LEI record itself. This module vendors GLEIF's published
code list so ``map_gleif`` can attach a human ``legalFormLabel`` to each entity —
which is what lets the AMLA trust/arrangement risk signal catch a foundation or
trust even when no national register is hit (a GLEIF-only lookup).

Data: ``data/elf_codes.json`` (auto-generated from GLEIF's ISO 20275 CSV by
``scripts/build_elf_codes.py``) unioned with ``data/elf_codes_supplement.json``
(hand-curated additions resolved via the GLEIF API for codes not yet in the
generated file). See the supplement's ``_comment`` for the regeneration note.
"""

from __future__ import annotations

import json
from pathlib import Path

_DATA = Path(__file__).parent / "data"


def _load() -> dict[str, str]:
    codes: dict[str, str] = {}
    generated = json.loads((_DATA / "elf_codes.json").read_text(encoding="utf-8"))
    codes.update({k.upper(): v for k, v in generated.items()})
    supplement = json.loads(
        (_DATA / "elf_codes_supplement.json").read_text(encoding="utf-8")
    )
    # Supplement augments/overrides the generated file; skip the doc key.
    codes.update(
        {k.upper(): v for k, v in supplement.items() if not k.startswith("_")}
    )
    return codes


ELF_CODES: dict[str, str] = _load()


def resolve_elf(code: str | None) -> str | None:
    """Return the Latin-script legal-form name for an ELF code, or ``None``.

    ``None`` for unknown / placeholder / empty codes (e.g. ``9999`` "no separate
    legal form" is intentionally absent from the vendored map)."""
    if not code:
        return None
    return ELF_CODES.get(code.strip().upper())
