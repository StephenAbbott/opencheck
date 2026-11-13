"""ISO 5009 Official Organizational Role codes, said on the interest.

GLEIF maintains ISO 5009: a six-character code for each official role *in one
legal form* in one jurisdiction. "Director" is ``UDU176`` for a UK private
limited company (ISO 20275 ELF ``H0PO``), ``VXSNEB`` for a PLC (``B6ES``) and
``JKPKHT`` for a company limited by guarantee (``G12F``). So a code describes
an appointment — this person, in this role, at this company — and is written on
the relationship statement's interest, in the extension object
``officialOrganizationalRole``:

    {"scheme": "ISO 5009", "code": "UDU176", "name": "Director", "legalForm": "H0PO"}

**Never on the person statement** (Stephen, 13 Nov 2026). A person's
``identifiers`` identify the person: a role code is shared by every holder of
the role, so as an identifier it would merge every director in a lookup into
one person (BackgroundCheck and the reconciler merge on a shared identifier),
``make_person_statement`` would move it to an annotation anyway (Phase 255),
and one person holds different roles at different companies.

**Coded only from a reviewed table, never guessed.** Each source that codes a
role keeps a table ``{(ELF, register role): OOR code}`` beside its mapper;
``tests/test_oor_codes.py`` checks every entry against the vendored list (the
code exists and is filed under that ELF and the source's country). A role the
table does not name, or a company whose legal form has no ELF the table knows,
gets no code — absence is never a claim.

Data: ``data/oor_codes.json``, built from GLEIF's CSV by
``scripts/build_oor_codes.py`` (the ``elf.py`` pattern).
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any

#: The extension key on a BODS interest object.
OOR_KEY = "officialOrganizationalRole"

#: What the object's ``scheme`` says.
OOR_SCHEME = "ISO 5009"

_DATA = Path(__file__).resolve().parent.parent / "data" / "oor_codes.json"


@lru_cache(maxsize=1)
def _load() -> dict[str, Any]:
    return json.loads(_DATA.read_text(encoding="utf-8"))


def oor_version() -> str:
    """The version of GLEIF's list vendored here."""
    return str(_load().get("version") or "")


def oor_entry(code: str | None) -> dict[str, Any] | None:
    """The list's entry for an OOR code, or None."""
    if not code:
        return None
    return _load()["roles"].get(code.strip().upper())


def oor_name(entry: Mapping[str, Any]) -> str:
    """The role's name as the list writes it: the English row where there is
    one, else the first listed (the local language)."""
    names = entry.get("names") or []
    for n in names:
        if n.get("lang") == "en" and n.get("name"):
            return str(n["name"])
    return str(names[0]["name"]) if names else ""


def role_object(code: str, elf: str) -> dict[str, str]:
    """The ``officialOrganizationalRole`` value for ``code`` under ``elf``.

    Raises ``KeyError`` for a code the list lacks, ``ValueError`` for one the
    list files under another legal form: a source's table is wrong, and a
    wrong code must fail loudly rather than reach a statement."""
    entry = oor_entry(code)
    if entry is None:
        raise KeyError(f"{code} is not in the ISO 5009 list {oor_version()}")
    if elf not in (entry.get("elfs") or []):
        raise ValueError(f"{code} is not filed under legal form {elf}")
    return {"scheme": OOR_SCHEME, "code": code, "name": oor_name(entry), "legalForm": elf}


def mark_role(
    interest: dict[str, Any],
    table: Mapping[tuple[str, str], str],
    elf: str | None,
    role: str | None,
) -> dict[str, Any]:
    """Set ``officialOrganizationalRole`` on ``interest`` (in place) when
    ``table`` names a code for ``(elf, role)``; otherwise leave it alone.
    Returns the interest."""
    if not elf or not role:
        return interest
    code = table.get((elf, role))
    if code:
        interest[OOR_KEY] = role_object(code, elf)
    return interest


def official_role(interest: Mapping[str, Any]) -> dict[str, Any] | None:
    """The role object on an interest, or None."""
    value = interest.get(OOR_KEY)
    return value if isinstance(value, dict) and value.get("code") else None
