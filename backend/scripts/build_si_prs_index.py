"""Build the Slovenian Business Register (PRS) SQLite index from AJPES's open CSV.

AJPES publishes the Slovenian Business Register's basic data as one CSV on
podatki.gov.si (``opsiprs.csv``, CC BY 4.0, fortnightly). The portal answers
HTTP 407 to every client that is not a browser — datacentres and ``curl`` on
a home connection alike (12 Nov 2026) — so the file is saved from a browser
and this builder reads it from disk. ``scripts/refresh_si_prs_index.sh``
wraps it and uploads the ``si-prs-index`` release asset. Standard library
only: no venv needed.

Usage
-----
    python3 scripts/build_si_prs_index.py --csv ~/Downloads/opsiprs.csv \\
        --cut 2026-11-01 --out si_prs.sqlite --gzip

``--cut`` is the day the portal last changed the file: the "last modified"
date on the dataset page. It is the register's clock (Phase 314); the build
time is recorded separately as ``built_at``.

The format (measured 12 Nov 2026)
---------------------------------
128,455,276 bytes, **UTF-16LE with a BOM**, CRLF, every field quoted, twelve
columns (``HEADER`` — the published names carry stray spaces, compared here
with whitespace collapsed). No status, no dates, no people.

What is kept
------------
* Every row whose number has the register's shape — seven digits, a digit
  or capital letter, two digits (others are counted and skipped; the builder
  refuses past ``MAX_MALFORMED``). A repeated number keeps its first
  row and is counted (refused past ``MAX_REPEATED``).
* ``HSEID`` (a house identifier) is never stored.
* A **natural person's** street, house number and suffix are never stored:
  the settlement, postcode and post office are. ``NATURAL_PERSON_FORMS`` and
  ``LEGAL_PERSON_FORMS`` hold every form in the 12 Nov 2026 file; a business
  unit (``UNIT_FORMS``) takes its main unit's reading; a form in neither list
  is read as a natural person's and logged as new. Every form is logged with
  its count and reading.

Refusals (exit 2, nothing written): a file that is not UTF-16 (the portal's
407 page is HTML), a changed header, fewer than ``MIN_ROWS`` rows, too many
malformed or repeated numbers, a malformed ``--cut``.
"""

from __future__ import annotations

import argparse
import codecs
import csv
import gzip
import logging
import os
import re
import shutil
import sqlite3
import sys
import time
from collections import Counter
from collections.abc import Iterable, Iterator
from pathlib import Path

# Standard library only, so the refresh runs under a bare ``python3`` on a
# Mac. These two are copies of ``opencheck.sources.si_prs``'s, pinned equal
# by ``tests/test_si_prs.py``.
INDEX_SCHEMA_VERSION = "1"
CSV_URL = (
    "https://podatki.gov.si/dataset/9ee1a9aa-c224-4995-b2ad-3760d7af0748/"
    "resource/beb70929-3d0d-41c6-9af2-25d525d906d3/download/opsiprs.csv"
)

#: A registration number: seven digits, one character (a capital letter on
#: 93 rows of the 12 Nov 2026 file), a two-digit unit. Pinned to the
#: adapter's ``NUMBER_RE``.
NUMBER_PATTERN = r"\d{7}[0-9A-Z]\d{2}"

#: The forms that are a natural person's — every one in the 12 Nov 2026 file
#: (179,682 rows with their units): a sole trader and a sole trader's branch,
#: supplementary activity on a farm, the self-employed professions and
#: trades the register lists, a room renter. Their street and house number
#: are never stored and they are out of name search.
NATURAL_PERSON_FORMS: frozenset[str] = frozenset({
    "Samostojni podjetnik posameznik s.p.",
    "Podružnica s. p.",
    "Nosilec dopolnilne dejavnosti na kmetiji",
    "Samostojni delavec v kulturi",
    "Registrirani sobodajalec",
    "Del pri sobodajalcu",
    "Evidentirani odvetnik",
    "Registrirani zdravnik, zobozdravnik, zasebni zdravstveni delavec",
    "Poklicni športnik",
    "Zasebni športni delavec",
    "Notar",
    "Registrirani lekarnar",
    "Izvršitelj",
    "Samostojni novinar",
    "Morski ribič",
    "Evidentirani detektiv",
    "Registrirani zasebni izvajalec raziskovalec",
    "Druga fizična oseba",
    "Zasebni učitelj oziroma vzgojitelj",
})

#: Forms that are a part of another unit: they take their main unit's
#: (``…000``) reading — a sole trader's business unit is the sole trader's
#: address. No main unit in the file: a natural person's, the safe reading.
UNIT_FORMS: frozenset[str] = frozenset({
    "Poslovna enota", "Organizacijska enota", "Članica univerze (del)",
})

#: Every other form in the 12 Nov 2026 file: a legal person or a state body.
#: A form in none of the three sets is new; it is read as a natural
#: person's and logged, so the next refresh can place it.
LEGAL_PERSON_FORMS: frozenset[str] = frozenset({
    "Družba z omejeno odgovornostjo d.o.o.", "Društvo, zveza društev", "Zavod",
    "Sindikat", "Javni zavod", "Verska skupnost in podobne verske organizacije",
    "Krajevna skupnost, druge ožje lokalne skupnosti", "Podružnica domačega podjetja",
    "Agrarne, pašne in vaške skupnosti", "Podružnica tujega podjetja",
    "Delniška družba d.d.", "Zadruga z.o.o.", "Ustanova",
    "Družba z neomejeno odgovornostjo d.n.o.", "Lokalne skupnosti",
    "Skupnost lastnikov stanovanj", "Komanditna družba k.d.",
    "Gospodarsko interesno združenje GIZ", "Zadruga z.b.o.", "Politična stranka",
    "Zbornica", "Druge skupnosti", "Upravni organ v sestavi", "Upravna enota",
    "Gospodarska zbornica", "Članica univerze", "Mladinski svet", "Ministrstvo",
    "Sodišče (vrhovno, višja, okrožna, okrajna)", "Skupnost zavodov", "Javna agencija",
    "Javni sklad", "Tožilstvo", "Vlada, vladna služba", "Narodnostna skupnost",
    "Podružnica tujega društva", "Nevladna organizacija", "Študentska organizacija",
    "Sklad", "Druge oblike poslovnih subjektov", "Javni gospodarski zavod",
    "Samostojni in neodvisni državni organ", "Režijski obrat",
    "Evropsko gospodarsko interesno združenje EGIZ", "Javni raziskovalni zavod",
    "Organ, organizacija širše lokalne skupnosti", "Tuja pravna oseba po ZIKS-1",
    "Predstavniški organ (DZ, DS)", "Pravobranilstvo", "Centralna banka",
    "Ustavno sodišče", "Predsednik republike", "Varuh človekovih pravic",
    "Računsko sodišče", "Obrtna zadruga",
    "Pooblaščenec za dostop do informacij javnega značaja", "evropska zadruga (SCE)",
    "p. o.", "n. sol. o.", "Republika Slovenija", "Evropska delniška družba SE",
})


def legal_reading(form: str, main_unit_form: str | None) -> tuple[bool, bool]:
    """``(legal_person, known)`` for a row's form; *main_unit_form* is the
    form of its ``…000`` unit (None when absent or itself)."""
    if form in UNIT_FORMS:
        if main_unit_form is None or main_unit_form in UNIT_FORMS:
            return False, True
        return legal_reading(main_unit_form, None)[0], True
    if form in NATURAL_PERSON_FORMS:
        return False, True
    if form in LEGAL_PERSON_FORMS:
        return True, True
    return False, False


logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("build_si_prs_index")

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

#: The published header, whitespace collapsed.
HEADER = [
    "Matična številka", "Popolno ime", "HSEID", "Pravnoorganizacijska oblika",
    "Registrski organ", "Ulica", "Hišna št", "Hišna št dodatek", "Naselje",
    "Poštna št", "Pošta", "Država",
]

#: Below this the file is not the register (~295,000 rows in Nov 2026).
MIN_ROWS = 200_000
MAX_MALFORMED = 500
MAX_REPEATED = 500

SCHEMA = """
PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
CREATE TABLE entity (
  number TEXT PRIMARY KEY, name TEXT, form TEXT, organ TEXT,
  street TEXT, house TEXT, house_suffix TEXT,
  settlement TEXT, postcode TEXT, post_office TEXT, country TEXT,
  legal_person INTEGER NOT NULL
);
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
"""
INDEXES = "CREATE INDEX ix_entity_name ON entity(legal_person, name);"


class RefusedInput(Exception):
    """The input is not the register as this builder knows it."""


def _clean(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def read_rows(path: Path) -> Iterator[list[str]]:
    """Yield the CSV's rows after the header; refuse anything that is not the
    UTF-16 file with the expected header."""
    with open(path, "rb") as fh:
        head = fh.read(2)
    if head != codecs.BOM_UTF16_LE:
        raise RefusedInput(
            f"{path.name}: not UTF-16LE with a BOM — a portal error page? "
            "(save the file from a browser)"
        )
    with open(path, encoding="utf-16", newline="") as fh:
        reader = csv.reader(fh)
        header = [_clean(cell) for cell in next(reader, [])]
        if header != HEADER:
            raise RefusedInput(f"{path.name}: header {header!r} is not {HEADER!r}")
        yield from reader


def build(
    rows: Iterable[list[str]], out: Path, cut: str, *, min_rows: int | None = None
) -> dict[str, int]:
    """Build the index from data rows (header removed). ``min_rows``
    overrides ``MIN_ROWS`` (tests pass 0)."""
    floor = MIN_ROWS if min_rows is None else min_rows
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", cut or ""):
        raise RefusedInput(f"cut {cut!r} is not YYYY-MM-DD")
    t0 = time.time()
    tmp = out.with_name(out.name + ".building")
    if tmp.exists():
        tmp.unlink()
    con = sqlite3.connect(str(tmp))
    try:
        con.executescript(SCHEMA)
        stats = Counter(rows=0, malformed=0, repeated=0, legal_persons=0,
                        natural_persons=0, unknown_form=0)
        forms: Counter[str] = Counter()
        kept: dict[str, list[str]] = {}
        for row in rows:
            if len(row) != len(HEADER):
                stats["malformed"] += 1
                continue
            cells = [_clean(c) for c in row]
            number = cells[0].upper()
            if not re.fullmatch(NUMBER_PATTERN, number):
                stats["malformed"] += 1
                continue
            if number in kept:
                stats["repeated"] += 1
                continue
            kept[number] = cells
        # A unit takes its main unit's reading, so the main units' forms are
        # read after the whole file.
        batch: list[tuple] = []
        for number, cells in kept.items():
            (_n, name, _hseid, form, organ, street, house, suffix,
             settlement, postcode, post_office, country) = cells
            main = kept.get(number[:7] + "000") if not number.endswith("000") else None
            legal, known = legal_reading(form, main[3] if main else None)
            forms[form] += 1
            stats["rows"] += 1
            stats["legal_persons" if legal else "natural_persons"] += 1
            if not known:
                stats["unknown_form"] += 1
            batch.append((
                number, name or None, form or None, organ or None,
                (street or None) if legal else None,
                (house or None) if legal else None,
                (suffix or None) if legal else None,
                settlement or None, postcode or None, post_office or None,
                country or None, 1 if legal else 0,
            ))
            if len(batch) >= 50_000:
                con.executemany("INSERT INTO entity VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch)
                batch.clear()
        con.executemany("INSERT INTO entity VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", batch)

        if stats["rows"] < floor:
            raise RefusedInput(f"only {stats['rows']:,} rows — not the register")
        if stats["malformed"] > MAX_MALFORMED:
            raise RefusedInput(f"{stats['malformed']:,} malformed rows — the format has changed")
        if stats["repeated"] > MAX_REPEATED:
            raise RefusedInput(f"{stats['repeated']:,} repeated numbers — the format has changed")

        for form, count in forms.most_common():
            if form in UNIT_FORMS:
                reading = "a unit: its main unit's reading"
            elif form in NATURAL_PERSON_FORMS:
                reading = "natural person"
            elif form in LEGAL_PERSON_FORMS:
                reading = "legal person"
            else:
                reading = "NEW FORM: read as a natural person — place it in a list"
            log.info("  form %-70s %8s  %s", form or "(blank)", f"{count:,}", reading)

        con.commit()
        con.executescript(INDEXES)
        con.executemany(
            "INSERT OR REPLACE INTO meta VALUES (?,?)",
            [
                ("schema_version", INDEX_SCHEMA_VERSION),
                ("source", "AJPES Slovenian Business Register open file (opsiprs.csv)"),
                ("source_url", CSV_URL),
                ("cut", cut),
                ("built_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
                ("licence", "CC-BY-4.0 — AJPES"),
                *[(f"count_{k}", str(v)) for k, v in stats.items()],
            ],
        )
        con.commit()
        con.execute("VACUUM")
        con.close()
    except BaseException:
        con.close()
        if tmp.exists():
            tmp.unlink()
        raise
    os.replace(tmp, out)
    result = dict(stats)
    result["seconds"] = int(time.time() - t0)
    result["megabytes"] = int(os.path.getsize(out) / 1e6)
    return result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", required=True, type=Path, help="opsiprs.csv, saved from a browser")
    ap.add_argument("--cut", required=True, help="the portal's last-modified day, YYYY-MM-DD")
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--gzip", action="store_true", help="also write <out>.gz, the release asset's shape")
    args = ap.parse_args(argv)

    try:
        stats = build(read_rows(args.csv), args.out, args.cut)
    except RefusedInput as exc:
        log.error("refused: %s", exc)
        return 2
    log.info("wrote %s (%s MB, %ss, cut %s)", args.out, stats["megabytes"], stats["seconds"], args.cut)
    for key, value in stats.items():
        log.info("  %-18s %s", key, f"{value:,}")
    if args.gzip:
        gz_path = args.out.with_name(args.out.name + ".gz")
        with open(args.out, "rb") as src, gzip.open(gz_path, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
        log.info("wrote %s (%s MB)", gz_path, int(os.path.getsize(gz_path) / 1e6))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
