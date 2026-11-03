#!/usr/bin/env python3
"""Build the data behind the search filters (the advanced-search-filters ticket).

Writes three files, all generated — never edit them by hand:

* ``backend/opencheck/data/elf_forms.json`` — every **active** ISO 20275
  Entity Legal Form, with the fields the legal-form picker needs: country,
  sub-division (US states and Canadian provinces have their own lists), every
  name in every language and every abbreviation. GLEIF's API carries names
  only; the abbreviations ("GmbH", "e.V.", "Ltd") are in the downloadable
  code list alone, which is why this reads the CSV.
* ``backend/opencheck/data/gleif_jurisdictions.json`` — GLEIF's accepted
  jurisdictions (the codes ``entity.jurisdiction`` can hold) and the
  **clash table**: the country codes that GLEIF's jurisdiction filter also
  matches inside a sub-division. ``filter[entity.jurisdiction]=PA`` returns
  Panama *and* Pennsylvania (``US-PA``), because the filter matches each part
  of a code separately; the table lists, for each clashing country, the
  sub-divisions to subtract. Derived from the accepted list, never typed.
* ``frontend/src/lib/gleifJurisdictions.json`` — a copy of the second file
  for the website's search box, which queries GLEIF from the browser.
  ``tests/test_search_filters.py`` fails when the two differ.

Usage::

    python3 backend/scripts/build_search_filter_data.py            # download both
    python3 backend/scripts/build_search_filter_data.py --elf-csv path/to/elf.csv

The CSV URL changes with each version of the code list: take it from
https://www.gleif.org/en/lei-data/code-lists/iso-20275-entity-legal-forms-code-list
and pass ``--elf-url`` when a new version is published.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ELF_OUT = ROOT / "backend" / "opencheck" / "data" / "elf_forms.json"
JUR_OUT = ROOT / "backend" / "opencheck" / "data" / "gleif_jurisdictions.json"
JUR_FRONTEND = ROOT / "frontend" / "src" / "lib" / "gleifJurisdictions.json"

ELF_URL = (
    "https://www.gleif.org/lei-data/code-lists/"
    "iso-20275-entity-legal-forms-code-list/2026-02-19-elf-code-list-v1.6.csv"
)
ELF_VERSION = "1.6 (19 February 2026)"
JUR_URL = "https://api.gleif.org/api/v1/gleif-accepted-jurisdictions"

# Placeholders, not legal forms: 8888 "not yet in the list", 9999 "no ELF code".
PLACEHOLDER_CODES = ("8888", "9999")

_COL_CODE = "ELF Code"
_COL_COUNTRY = "Country Code (ISO 3166-1)"
_COL_SUB = "Country sub-division code (ISO 3166-2)"
_COL_LOCAL = "Entity Legal Form name Local name"
_COL_LANG = "Language Code (ISO 639-1)"
_COL_TRANSLIT = "Entity Legal Form name Transliterated name (per ISO 01-140-10)"
_COL_ABBR = "Abbreviations Local language"
_COL_ABBR_T = "Abbreviations transliterated"
_COL_STATUS = "ELF Status ACTV/INAC"

_UA = {"User-Agent": "OpenCheck search-filter data builder (opencheck.world)"}


def _get(url: str) -> bytes:
    req = urllib.request.Request(url, headers=_UA)
    with urllib.request.urlopen(req, timeout=60) as resp:  # noqa: S310 — fixed hosts
        return resp.read()


def _split_abbr(value: str) -> list[str]:
    return [a.strip() for a in (value or "").split(";") if a.strip()]


def build_elf(csv_text: str) -> list[dict]:
    """Group the code list's per-language rows into one entry per active code."""
    lines = csv_text.splitlines()
    start = next(
        (i for i, line in enumerate(lines) if line.lstrip('"').startswith(_COL_CODE)), 0
    )
    rows = list(csv.DictReader(io.StringIO("\n".join(lines[start:]))))
    if not rows or _COL_ABBR not in rows[0]:
        raise SystemExit("ELF CSV has no abbreviation column — wrong file?")
    forms: dict[str, dict] = {}
    for row in rows:
        code = (row.get(_COL_CODE) or "").strip().upper()
        if not code or code in PLACEHOLDER_CODES:
            continue
        if (row.get(_COL_STATUS) or "").strip().upper() != "ACTV":
            continue
        form = forms.setdefault(
            code,
            {
                "code": code,
                "country": (row.get(_COL_COUNTRY) or "").strip().upper(),
                "subdivision": (row.get(_COL_SUB) or "").strip().upper() or None,
                "names": [],
                "abbreviations": [],
            },
        )
        local = (row.get(_COL_LOCAL) or "").strip()
        translit = (row.get(_COL_TRANSLIT) or "").strip()
        lang = (row.get(_COL_LANG) or "").strip().lower()
        if local or translit:
            name = {"lang": lang, "local": local or translit}
            if translit and translit != local:
                name["translit"] = translit
            form["names"].append(name)
        for abbr in _split_abbr(row.get(_COL_ABBR) or "") + _split_abbr(row.get(_COL_ABBR_T) or ""):
            if abbr not in form["abbreviations"]:
                form["abbreviations"].append(abbr)
    out = [f for f in forms.values() if f["country"] and f["names"]]
    out.sort(key=lambda f: (f["country"], f["subdivision"] or "", f["code"]))
    if len(out) < 3000:
        raise SystemExit(f"only {len(out)} active ELF codes — refusing a partial list")
    return out


def build_jurisdictions() -> dict:
    codes: list[dict] = []
    page = 1
    while True:
        payload = json.loads(_get(f"{JUR_URL}?page[size]=200&page[number]={page}"))
        for item in payload.get("data", []):
            attrs = item.get("attributes") or {}
            code = (attrs.get("code") or "").strip().upper()
            if code:
                codes.append({"code": code, "name": (attrs.get("name") or "").strip()})
        last = payload.get("meta", {}).get("pagination", {}).get("lastPage") or page
        if page >= last:
            break
        page += 1
    if len(codes) < 250:
        raise SystemExit(f"only {len(codes)} jurisdictions — refusing a partial list")
    return {"jurisdictions": sorted(codes, key=lambda c: c["code"]), "collisions": collisions(c["code"] for c in codes)}


def collisions(codes) -> dict[str, list[str]]:
    """Country code → the sub-division codes GLEIF's filter also matches.

    GLEIF matches each hyphen-separated part of ``entity.jurisdiction``, so a
    country whose two letters are the second part of some sub-division picks
    that sub-division up. A sub-division of the country itself (``AE-AZ``
    under ``AE``) is the country's own and is not a clash.
    """
    codes = list(codes)
    countries = {c for c in codes if re.fullmatch(r"[A-Z]{2}", c)}
    out: dict[str, list[str]] = {}
    for code in codes:
        if "-" not in code:
            continue
        parent, suffix = code.split("-", 1)
        if suffix in countries and suffix != parent:
            out.setdefault(suffix, []).append(code)
    return {k: sorted(v) for k, v in sorted(out.items())}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--elf-csv", type=Path, help="a local copy of the ELF code list CSV")
    parser.add_argument("--elf-url", default=ELF_URL)
    parser.add_argument("--elf-version", default=ELF_VERSION)
    args = parser.parse_args()

    csv_text = (
        args.elf_csv.read_text(encoding="utf-8-sig")
        if args.elf_csv
        else _get(args.elf_url).decode("utf-8-sig")
    )
    forms = build_elf(csv_text)
    ELF_OUT.write_text(
        json.dumps({"version": args.elf_version, "source": args.elf_url, "forms": forms}, ensure_ascii=False, separators=(",", ":"))
        + "\n",
        encoding="utf-8",
    )
    jur = build_jurisdictions()
    text = json.dumps(jur, ensure_ascii=False, indent=1) + "\n"
    JUR_OUT.write_text(text, encoding="utf-8")
    JUR_FRONTEND.write_text(text, encoding="utf-8")
    print(f"{len(forms)} legal forms, {len(jur['jurisdictions'])} jurisdictions, {len(jur['collisions'])} clashing country codes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
