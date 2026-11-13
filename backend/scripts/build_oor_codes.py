#!/usr/bin/env python3
"""Build the vendored ISO 5009 Official Organizational Roles (OOR) code list.

GLEIF maintains ISO 5009: one six-character code per official role *in one
legal form* in one jurisdiction — "Director" is ``UDU176`` for a UK private
limited company (ELF ``H0PO``) and ``VXSNEB`` for a PLC (``B6ES``). OpenCheck
vendors the list so ``opencheck.bods.oor`` can put a role's code on the
relationship interest that describes the appointment (never on the person:
a role code is shared by every holder of the role).

Usage:

    python3 backend/scripts/build_oor_codes.py <officialorganizationalroles.csv>

The CSV is linked from GLEIF's code-list page:
    https://www.gleif.org/en/lei-data/code-lists/iso-5009-official-organizational-roles-code-list

Writes ``opencheck/data/oor_codes.json``: ``{"version", "source", "roles":
{code: {"country", "subdivision", "elfs", "names": [{"lang",
"name"}], "legislation"}}}``. A code appears once however many languages
GLEIF lists it in (Switzerland, Finland and Belgium list one code in two or
three). Inactive rows are left out. Idempotent; re-run on a new version.
"""

from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

_OUT = Path(__file__).resolve().parent.parent / "opencheck" / "data" / "oor_codes.json"


def build(path: Path) -> dict:
    roles: dict[str, dict] = {}
    with path.open(encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            if (row.get("OOR Status ACTV/INAC") or "").strip() != "ACTV":
                continue
            code = (row.get("OOR Code") or "").strip().upper()
            if not re.fullmatch(r"[A-Z0-9]{6}", code):
                raise SystemExit(f"malformed OOR code {code!r}")
            name = (row.get("Official Organizational Role (OOR)") or "").strip()
            lang = (row.get("Language Code (ISO 639-1)") or "").strip().lower()
            entry = roles.setdefault(code, {
                "country": (row.get("Country Code (ISO 3166-1)") or "").strip().upper(),
                "subdivision": (row.get("Country sub-division code (ISO 3166-2)") or "").strip().upper() or None,
                "elfs": [],
                "names": [],
                "legislation": (row.get("Applicable legislation / regulation") or "").strip() or None,
            })
            # One code, one legal form — except where GLEIF's list files a
            # code under a second form (HDI4SZ, Hong Kong "Manager", under
            # both public-company ELFs in v1.0.0). Kept as listed.
            elf = (row.get("ELF Code") or "").strip().upper()
            if elf and elf not in entry["elfs"]:
                entry["elfs"].append(elf)
            if name and {"lang": lang, "name": name} not in entry["names"]:
                entry["names"].append({"lang": lang, "name": name})
    if len(roles) < 1000:
        raise SystemExit(f"only {len(roles)} roles read — is this the OOR list?")
    return {
        "version": "1.0.0",
        "source": "https://www.gleif.org/lei-data/code-lists/iso-5009-official-organizational-roles-code-list/officialorganizationalroles_v1.0.0.csv",
        "roles": dict(sorted(roles.items())),
    }


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    data = build(Path(sys.argv[1]))
    _OUT.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=False) + "\n", encoding="utf-8")
    print(f"wrote {len(data['roles'])} roles to {_OUT}")


if __name__ == "__main__":
    main()
