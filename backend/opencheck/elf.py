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
import re
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


#: GLEIF's two placeholders in ``entity.legalForm.id``: ``8888`` "no ELF code
#: yet assigned to this legal form" and ``9999`` "no ELF code applies". Neither
#: names a legal form, so neither is ever carried as one.
PLACEHOLDER_CODES: tuple[str, ...] = ("8888", "9999")

_ELF_SHAPE = re.compile(r"^[A-Z0-9]{4}$")


def elf_code(code: str | None) -> str | None:
    """GLEIF's ISO 20275 code, upper-cased, when it names a legal form.

    ``None`` for an empty value, a placeholder or anything not four
    alphanumeric characters. A well-formed code missing from the vendored list
    is still returned: the code is GLEIF's assertion, and the list is updated
    twice a year, so a newer code can reach a record before it reaches us
    (the label then falls back to ``legalForm.other``)."""
    value = str(code or "").strip().upper()
    if not _ELF_SHAPE.match(value) or value in PLACEHOLDER_CODES:
        return None
    return value
