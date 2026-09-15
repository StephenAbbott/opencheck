"""Moldova — ASP, the State Register of Legal Entities (weekly open data).

The Agenția Servicii Publice (Public Services Agency) publishes the whole
State Register of Legal Entities (*Registrul de stat al unităților de drept*,
RSUD) on the national open-data portal as a **weekly full snapshot**: one
XLSX, roughly 38 MB and 300,000 rows, every Monday. There is no query API.
This adapter keeps a local SQLite index over the latest snapshot and answers
from it, keyed on the **IDNO** — the 13-digit state identification number
that is also the fiscal code, and what GLEIF files in ``registeredAs``.

Why the index builds itself
---------------------------
Unlike ``data.gov.ro`` and ``data.egov.bg``, ``dataset.gov.md`` does not block
datacentre networks: measured on 15 September 2026, the CKAN API answered and
the 38 MB file downloaded in under five seconds from a cloud container. So the
index is not a file somebody builds on a laptop and ships in. On a live
deployment the adapter resolves the newest resource through CKAN's
``package_show`` (resource ids change every week, so no download URL is ever
hardcoded), downloads it, and builds the index in a background thread —
at startup, and again when the snapshot it holds is more than a week old and
the portal has a newer one. ``ASP_MOLDOVA_DB_FILE`` pins a location;
``ASP_MOLDOVA_SYNC=false`` turns the download off and uses the file as found.

The XLSX is read as a stream, not with openpyxl
-----------------------------------------------
openpyxl's read-only mode loads the workbook's shared-string table whole:
1,144,245 strings, a 432 MB peak on the real file, on a host with 512 MB.
``_iter_raw_rows`` walks the sheet XML with ``iterparse`` and resolves shared
strings from a scratch SQLite table instead — 91 MB peak for the same file,
and 45 s for the whole build (measured on the 14 September 2026 export). The
columns are found by their header text, not by position: the company and
non-commercial exports put their headers on different rows in a different
order.

Scope — what Stephen decided on 15 September 2026
-------------------------------------------------
* **Sole traders and peasant farms are excluded** at build time
  (``EXCLUDED_FORMS``): natural persons trading under their own name, the
  Romanian decision applied again — 37,006 rows on the 14 September 2026
  export once liquidated ones are gone (about 120,000 counting those).
* **Liquidated companies are excluded** — any row with a *Data lichidării*.
  An IDNO that GLEIF still holds for a liquidated company therefore finds no
  record, and the card says the export omits liquidated companies rather than
  implying the company never existed.
* **Corporate founders are linked by name, with a name check.** The register
  names founders but never identifies them. A founder that reads as a legal
  entity is resolved to an IDNO only when its folded name equals the folded
  name of exactly one company in the index (see ``resolve_founder``). About
  half of domestic corporate founders resolve; the rest stay unidentified,
  which is what the register actually says about them.
* The non-commercial organisations dataset is **not** indexed. It carries
  political parties, trade unions and religious associations, and one of the
  54 Moldovan LEIs.

What the register publishes, and what it does not
-------------------------------------------------
Directors (*conducători*) with their role, and founders (*fondatori*) with
their percentage of the share capital — which, for an SRL, is its shareholder
register. Names only: no dates of birth, no nationalities, no identifiers for
people or for corporate founders. **No beneficial owners**, and joint-stock
companies (SA) publish no founders at all: their shareholders are held by the
central securities depository, not by RSUD.

Licence
-------
The dataset states none (``license_id: notspecified``, like nearly every ASP
dataset). The portal's own *Despre* page sets reuse conditions for everything
it hosts: anyone may reproduce, redistribute, adapt and "exploit commercially"
the published data. OpenCheck relies on those conditions and attributes ASP.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import posixpath
import re
import sqlite3
import tempfile
import threading
import time
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .. import degradation, provenance
from ..config import get_settings
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.asp_moldova import AspMoldovaBundle

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GLEIF registration authorities
# ---------------------------------------------------------------------------
# Every Moldovan LEI record (55 legal-address MD records, 15 September 2026)
# files the 13-digit IDNO in ``registeredAs``, whichever authority it names:
#
#   RA000451  State Register of Legal Entities (ASP)   50
#   RA000950  National Commission for Financial Markets  2
#   RA000951  National Bank of Moldova                   2
#   RA999999  (the National Bank itself, no number)      1
#
# Watch out when counting: ``filter[entity.jurisdiction]=MD`` also matches
# ``US-MD`` (Maryland) and returns 4,838 records. Filter on
# ``entity.legalAddress.country`` instead.
MD_RSUD_RA_CODE: str = "RA000451"
MD_RA_CODES: frozenset[str] = frozenset({MD_RSUD_RA_CODE, "RA000950", "RA000951"})

_DATASET_ID = (
    "11736-date-din-registrul-de-stat-al-unitatilor-de-drept-privind-"
    "intreprinderile-inregistrate-in-repu"
)
DATASET_URL = f"https://dataset.gov.md/dataset/{_DATASET_ID}"
CKAN_PACKAGE_URL = f"https://dataset.gov.md/api/3/action/package_show?id={_DATASET_ID}"
_HOMEPAGE = "https://www.asp.gov.md/"

#: Bumped whenever the index tables change shape, so an old file is rebuilt.
INDEX_SCHEMA_VERSION = "1"

#: A snapshot older than this prompts a check for a newer one.
REFRESH_AFTER_DAYS = 7

#: The portal is asked at most this often per process.
_CHECK_INTERVAL_S = 12 * 3600

#: How long a lookup waits for a cold index build before degrading.
_BUILD_WAIT_S = 45.0

_LOOKUP_TIMEOUT_S = 60.0

LICENSE_ID = "DATASET.GOV.MD-Reuse"

COVERAGE_NOT_IN_EXPORT = (
    "No company under this IDNO in the State Register of Legal Entities' "
    "weekly open-data export as indexed by OpenCheck. The index leaves out "
    "liquidated companies, sole traders and peasant farms, so a company that "
    "has been liquidated is not found here."
)


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------


def fold(text: str | None) -> str:
    """Uppercase ASCII with diacritics removed.

    Moldovan filings mix the comma-below ``Ș``/``Ț`` with the legacy cedilla
    ``Ş``/``Ţ`` in the same column, so nothing is compared before folding.
    """
    decomposed = unicodedata.normalize("NFKD", str(text or ""))
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    return " ".join(stripped.upper().split())


#: Legal forms excluded at build time: natural persons trading in their own
#: name. Compared folded, because the export spells ``ţ`` with a cedilla.
EXCLUDED_FORMS: frozenset[str] = frozenset(
    fold(f)
    for f in (
        "Întreprindere individuală",
        "Întreprindere individuală în agricultură (gospodăria ţărănească, de fermier)",
    )
)


# ---------------------------------------------------------------------------
# Identifier grammar
# ---------------------------------------------------------------------------
_IDNO_WEIGHTS = (7, 3, 1, 7, 3, 1, 7, 3, 1, 7, 3, 1)


def idno_check_digit(first_twelve: str) -> str:
    """The IDNO check digit: weights 7-3-1 over the first twelve digits."""
    return str(sum(w * int(d) for w, d in zip(_IDNO_WEIGHTS, first_twelve)) % 10)


def normalise_idno(raw: str) -> str:
    """Return a Moldovan IDNO as 13 digits, or raise ``ValueError``.

    Whitespace is removed and the check digit is verified. All 265,814
    13-digit IDNOs in the 14 September 2026 export pass it, and so do 53 of
    the 54 Moldovan LEI records; the one that fails (``16479``, filed for Mogo
    Loans SRL, whose register IDNO is ``1017600033216``) is not an IDNO at all,
    and a lookup is better skipped than sent to the wrong company.
    """
    text = re.sub(r"\s+", "", str(raw or ""))
    if not re.fullmatch(r"\d{13}", text):
        raise ValueError(f"not a Moldovan IDNO: {raw!r}")
    if idno_check_digit(text[:12]) != text[12]:
        raise ValueError(f"IDNO check digit fails: {raw!r}")
    return text


def is_idno(raw: str) -> bool:
    try:
        normalise_idno(raw)
    except ValueError:
        return False
    return True


# ---------------------------------------------------------------------------
# Names: person or entity, and the resolution key
# ---------------------------------------------------------------------------
# A name is treated as a legal entity when it carries a legal-form designator
# or an institutional word. Measured over the 14 September 2026 export: 8,117
# of 305,198 founder and director names match. Apostrophes are deliberately
# not a marker (D'ALONZO, O'SHEA), nor are two-letter tokens that are also
# surnames (SAS).
_ENTITY_MARKERS = re.compile(
    r'["“”„«»]'
    r"|\b(?:S\.?\s?R\.?\s?L|S\.?\s?A|I\.?\s?C\.?\s?S|I\.?\s?M|I\.?\s?S|I\.?\s?I|S\.?\s?C"
    r"|LTD|LIMITED|GMBH|LLC|L\.L\.C|B\.?\s?V|N\.?\s?V|S\.?\s?R\.?\s?O|AG|INC|CORP"
    r"|CORPORATION|SCS|KFT|OOO|A/S|SARL|SPA|S\.P\.A|PLC|SP\.?\s?Z\s?O\.?\s?O"
    r"|HOLDING|HOLDINGS|COMPANY|COMPANIA|GROUP|FUND|FONDUL|BANK|BANCA"
    r"|ASOCIATIA|FUNDATIA|UNIUNEA|COOPERATIVA|INTREPRINDEREA|SOCIETATEA|SOCIETATE"
    r"|FIRMA|MINISTERUL|PRIMARIA|CONSILIUL|AGENTIA|STATUL|INSTITUTUL|UNIVERSITATEA"
    r"|ACADEMIA|CANCELARIA|DIRECTIA|DEPARTAMENTUL|SERVICIUL|INSPECTORATUL|CAMERA"
    r"|CENTRUL|ORGANIZATIA|ADMINISTRATIA|BASCANATUL|UNITATEA|CONCERNUL|COMBINATUL"
    r"|CUMBINATUL|UZINA|FABRICA|TRUSTUL|ASSOCIATION|FOUNDATION|LIMITADA"
    r"|GESELLSCHAFT|ANONIM|SIRKETI|UAB|SIA|D\.O\.O|EOOD|OOD|ZAO|OAO|PAO|TOV"
    r"|ТОВ|ООО|ЗАО|ОАО|ПАО)\b"
)

#: Founder entries that name a group rather than anyone: "TOTAL 71 MEMBRI",
#: "MEMBRII ASOCIAȚIEI -PERSOANE FIZICE", "COLECTIVUL DE MUNCĂ …". Skipped.
_COLLECTIVE = re.compile(r"^(?:TOTAL\b|MEMBRII\b|COLECTIVUL\b)")

#: The state itself as a founder of a state enterprise.
_STATE_FOUNDER = re.compile(r"^STATUL\b")

#: Legal-form designators removed before two company names are compared.
_FORM_WORDS = re.compile(
    r"\b(?:SOCIETATEA|SOCIETATE) CU RASPUNDERE LIMITATA\b"
    r"|\b(?:SOCIETATEA|SOCIETATE) PE ACTIUNI(?: DE TIP (?:INCHIS|DESCHIS))?\b"
    r"|\bINTREPRINDEREA CU CAPITAL STRAIN\b|\bINTREPRINDEREA MIXTA\b"
    r"|\bS R L\b|\bSRL\b|\bS A\b|\bSA\b|\bI C S\b|\bICS\b|\bI M\b|\bIM\b"
)

_EMBEDDED_IDNO = re.compile(r"\bIDNO\s*:?\s*(\d{13})\b")


def is_entity_name(name: str) -> bool:
    return bool(_ENTITY_MARKERS.search(fold(name)))


def name_key(name: str) -> str:
    """Fold a company name to the key used to resolve a founder.

    Diacritics, quotes, punctuation and legal-form designators go; the order
    of what is left stays. ``"GRAWE CONSULTING & DEVELOPMENT INTERNATIONAL"
    S.R.L.`` and ``Societatea cu Răspundere Limitată "GRAWE CONSULTING &
    DEVELOPMENT INTERNATIONAL"`` both fold to ``GRAWE CONSULTING DEVELOPMENT
    INTERNATIONAL``.
    """
    text = re.sub(r"[^A-Z0-9 ]", " ", fold(name))
    text = " ".join(text.split())
    text = _FORM_WORDS.sub(" ", text)
    return " ".join(text.split())


# ---------------------------------------------------------------------------
# Directors and founders, as the export writes them
# ---------------------------------------------------------------------------
_DIRECTOR = re.compile(r"\s*([^\[\]]+?)\s*\[([^\[\]]*)\]\s*(?:,|$)")
_FOUNDER_PCT = re.compile(r"(.+?)\s*\((\d+(?:[.,]\d+)?)\s*%\)\s*(?:,\s*|$)", re.S)

#: The register's director roles → BODS v0.4 ``interestType``, keyed folded.
#: Checked against ``libcovebods/data/schema-0-4-0/relationship-record.json``.
#:
#: * *Administrator* is the company's managing officer →
#:   ``seniorManagingOfficial`` (the CLAUDE.md rule; never
#:   ``appointmentOfBoard``).
#: * *Direcţie de conducere* and *Comitet* are collective management organs —
#:   the banks file their management committee this way → ``boardMember``.
#: * Office-holders whose powers come from liquidation or insolvency law →
#:   ``controlByLegalFramework``: *Lichidator*, and the insolvency
#:   administrators (*al procesului de insolvabilitate*, *provizoriu*,
#:   *fiduciar*, *din oficiu*, *al procedurii planului*). A *lichidator* in a
#:   voluntary liquidation is chosen by the members, one in insolvency by the
#:   court; the export does not say which, and the liquidator's powers are
#:   statutory either way.
#: * *Comisia* (a cooperative's audit or liquidation commission) and *Datele
#:   lipsesc* ("data missing") → ``unknownInterest``: the register names a
#:   person and says nothing about what they can do.
ROLE_INTEREST: dict[str, str] = {
    "ADMINISTRATOR": "seniorManagingOfficial",
    "DIRECTIE DE CONDUCERE": "boardMember",
    "COMITET": "boardMember",
    "LICHIDATOR": "controlByLegalFramework",
    "ADMINISTRATOR AL PROCESULUI DE INSOLVABILITATE": "controlByLegalFramework",
    "ADMINISTRATOR PROVIZORIU": "controlByLegalFramework",
    "ADMINISTRATOR FIDUCIAR": "controlByLegalFramework",
    "ADMINISTRATOR DIN OFICIU": "controlByLegalFramework",
    "ADMINISTRATOR AL PROCEDURII PLANULUI": "controlByLegalFramework",
    "COMISIA": "unknownInterest",
    "DATELE LIPSESC": "unknownInterest",
}

#: Roles that mean the company is being wound up or is in insolvency.
WINDING_UP_ROLES: frozenset[str] = frozenset(
    k for k, v in ROLE_INTEREST.items() if v == "controlByLegalFramework"
)


def parse_directors(raw: str | None) -> list[dict[str, Any]]:
    """Split ``NAME [Role], NAME [Role]`` into rows. Unknown roles are kept."""
    rows: list[dict[str, Any]] = []
    for name, role in _DIRECTOR.findall(str(raw or "")):
        name = " ".join(name.split())
        if not name:
            continue
        role = " ".join(role.split())
        rows.append(
            {
                "name": name,
                "role": role or None,
                "role_key": fold(role) or None,
                "interest_type": ROLE_INTEREST.get(fold(role), "unknownInterest"),
                "is_entity": is_entity_name(name),
            }
        )
    return rows


def _pct(text: str) -> float | None:
    try:
        return float(text.replace(",", "."))
    except ValueError:
        return None


def _looks_like_person(part: str) -> bool:
    return len(part.split()) >= 2 and not is_entity_name(part)


def parse_founders(raw: str | None) -> list[dict[str, Any]]:
    """Split the founders column into rows.

    Three shapes occur:

    * ``NAME (40,00%), NAME (30,00%)`` — a share of the capital each, with a
      decimal comma.
    * ``NAME, NAME`` — no share filed (public institutions, some older SRLs).
    * ``SURNAME GIVEN, SURNAME GIVEN (50,00%)`` — two people holding one stake
      together. Split into both holders only when every part has at least two
      words, so ``BOURDIER ERIC, MARCEL, LOUIS`` stays one person with three
      given names. Joint holders carry the filed percentage as text, never as
      an exact share each: the export does not say how it divides.
    """
    text = str(raw or "").strip()
    if not text:
        return []
    rows: list[dict[str, Any]] = []
    if "%" in text:
        entries = [(n, p) for n, p in _FOUNDER_PCT.findall(text)]
    else:
        entries = [(part, None) for part in text.split(",")]
    for group, (name, pct_text) in enumerate(entries):
        name = " ".join(str(name).split()).strip(" ,")
        if not name or _COLLECTIVE.match(fold(name)):
            continue
        parts = [p.strip() for p in name.split(",") if p.strip()]
        joint = pct_text is not None and len(parts) > 1 and all(
            _looks_like_person(p) for p in parts
        )
        holders = parts if joint else [name]
        for holder in holders:
            embedded = _EMBEDDED_IDNO.search(fold(holder))
            if embedded:
                # "… SRL, IDNO 1011600007976 MD-2028, STRADA …": the name is
                # what comes before the number, the rest is an address.
                cut = re.search(r"[,\s]*\bIDNO\b", holder, flags=re.I)
                holder = (holder[: cut.start()].strip(" ,") if cut else holder) or (
                    f"IDNO {embedded.group(1)}"
                )
            kind = (
                "state"
                if _STATE_FOUNDER.match(fold(holder))
                else "entity"
                if (embedded or is_entity_name(holder))
                else "person"
            )
            rows.append(
                {
                    "name": holder,
                    "kind": kind,
                    "share_pct": None if (joint or pct_text is None) else _pct(pct_text),
                    "share_text": f"{pct_text}%" if pct_text is not None else None,
                    "joint": joint,
                    "group": group,
                    "embedded_idno": embedded.group(1) if embedded else None,
                }
            )
    return rows


# ---------------------------------------------------------------------------
# Streaming XLSX reader
# ---------------------------------------------------------------------------
_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_REL_NS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"


class IndexBuildError(RuntimeError):
    """The export's shape is not what the index builder understands."""


def _sheet_path(zf: zipfile.ZipFile, sheet_name: str | None) -> str:
    workbook = ET.fromstring(zf.read("xl/workbook.xml"))
    sheets = list(workbook.iter(f"{_NS}sheet"))
    if not sheets:
        raise IndexBuildError("workbook has no sheets")
    chosen = sheets[0]
    if sheet_name is not None:
        named = [s for s in sheets if s.get("name") == sheet_name]
        if not named:
            raise IndexBuildError(f"no sheet named {sheet_name!r}")
        chosen = named[0]
    rel_id = chosen.get(f"{_REL_NS}id")
    rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    for rel in rels:
        if rel.get("Id") == rel_id:
            target = rel.get("Target") or ""
            if target.startswith("/"):
                return target.lstrip("/")
            return posixpath.normpath(posixpath.join("xl", target))
    raise IndexBuildError(f"sheet relationship {rel_id!r} not found")


def _uses_1904(zf: zipfile.ZipFile) -> bool:
    workbook = ET.fromstring(zf.read("xl/workbook.xml"))
    props = workbook.find(f"{_NS}workbookPr")
    return props is not None and props.get("date1904") in ("1", "true")


def _col_index(ref: str) -> int:
    n = 0
    for ch in ref:
        if not ch.isalpha():
            break
        n = n * 26 + (ord(ch.upper()) - 64)
    return n - 1


class _SharedStrings:
    """The shared-string table, held in a scratch SQLite file, not in memory."""

    def __init__(self, zf: zipfile.ZipFile, scratch: Path) -> None:
        self._conn = sqlite3.connect(str(scratch))
        self._conn.execute("CREATE TABLE s (i INTEGER PRIMARY KEY, t TEXT)")
        self._cache: dict[int, str] = {}
        if "xl/sharedStrings.xml" not in zf.namelist():
            return
        batch: list[tuple[int, str]] = []
        index = 0
        root = None
        with zf.open("xl/sharedStrings.xml") as fh:
            for event, elem in ET.iterparse(fh, events=("start", "end")):
                if root is None:
                    root = elem
                if event == "end" and elem.tag == f"{_NS}si":
                    text = "".join(t.text or "" for t in elem.iter(f"{_NS}t"))
                    batch.append((index, text))
                    index += 1
                    root.clear()
                    if len(batch) >= 50_000:
                        self._conn.executemany("INSERT INTO s VALUES (?, ?)", batch)
                        batch.clear()
        self._conn.executemany("INSERT INTO s VALUES (?, ?)", batch)
        self._conn.commit()

    def get_many(self, indexes: list[int]) -> dict[int, str]:
        found = {i: self._cache[i] for i in indexes if i in self._cache}
        missing = [i for i in indexes if i not in found]
        for start in range(0, len(missing), 500):
            chunk = missing[start : start + 500]
            placeholders = ",".join("?" * len(chunk))
            for i, t in self._conn.execute(
                f"SELECT i, t FROM s WHERE i IN ({placeholders})", chunk
            ):
                found[i] = t
        return found

    def get_cached(self, index: int) -> str:
        """For low-cardinality columns (legal form): keep what was looked up."""
        if index not in self._cache:
            row = self._conn.execute("SELECT t FROM s WHERE i = ?", (index,)).fetchone()
            self._cache[index] = row[0] if row else ""
        return self._cache[index]

    def close(self) -> None:
        self._conn.close()


class _Cell:
    """A raw cell: a shared-string index, or literal text."""

    __slots__ = ("shared", "text")

    def __init__(self, shared: int | None, text: str | None) -> None:
        self.shared = shared
        self.text = text


def _iter_raw_rows(zf: zipfile.ZipFile, sheet: str) -> Iterator[dict[int, _Cell]]:
    root = None
    sheet_data = None
    with zf.open(sheet) as fh:
        for event, elem in ET.iterparse(fh, events=("start", "end")):
            if root is None:
                root = elem
            if event == "start":
                if elem.tag == f"{_NS}sheetData":
                    sheet_data = elem
                continue
            if elem.tag != f"{_NS}row":
                continue
            cells: dict[int, _Cell] = {}
            for c in elem.findall(f"{_NS}c"):
                kind = c.get("t")
                col = _col_index(c.get("r") or "A")
                if kind == "inlineStr":
                    cells[col] = _Cell(None, "".join(t.text or "" for t in c.iter(f"{_NS}t")))
                    continue
                value = c.find(f"{_NS}v")
                if value is None or value.text is None:
                    continue
                if kind == "s":
                    cells[col] = _Cell(int(value.text), None)
                else:
                    cells[col] = _Cell(None, value.text)
            yield cells
            (sheet_data if sheet_data is not None else root).clear()


# ---------------------------------------------------------------------------
# Cell values
# ---------------------------------------------------------------------------


def _excel_date(text: str | None, *, date1904: bool) -> str | None:
    """An Excel serial or a ``DD.MM.YYYY`` string → ISO date, else None."""
    value = str(text or "").strip()
    if not value:
        return None
    m = re.fullmatch(r"(\d{1,2})[./-](\d{1,2})[./-](\d{4})", value)
    if m:
        day, month, year = (int(p) for p in m.groups())
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            return None
    try:
        serial = float(value)
    except ValueError:
        return None
    epoch = date(1904, 1, 1) if date1904 else date(1899, 12, 30)
    try:
        return (epoch + timedelta(days=int(serial))).isoformat()
    except OverflowError:
        return None


def _numeric_text(text: str | None) -> str:
    """A number cell as the digits it holds (``1.003602023531E12`` too)."""
    value = str(text or "").strip()
    if not value:
        return ""
    if re.fullmatch(r"\d+", value):
        return value
    try:
        number = Decimal(value)
    except InvalidOperation:
        return value
    if number == number.to_integral_value():
        return str(int(number))
    return value


# ---------------------------------------------------------------------------
# Index build
# ---------------------------------------------------------------------------
_HEADER_RULES: tuple[tuple[str, str], ...] = (
    # (column name, substring of the folded header). First match wins, and
    # "NELICENTIATE" is tested before "LICENTIATE" because it contains it.
    ("idno", "IDNO"),
    ("registered_on", "DATA INREG"),
    ("name", "DENUMIREA"),
    ("legal_form", "FORMA"),
    ("address", "ADRESA"),
    ("cuatm", "CUATM"),
    ("directors", "CONDUCATOR"),
    ("founders", "FONDATOR"),
    ("activities", "NELICENTIATE"),
    ("licensed_activities", "LICENTIATE"),
    ("liquidated_on", "LICHIDAR"),
)

_REQUIRED_COLUMNS = ("idno", "name", "legal_form", "directors", "founders", "liquidated_on")


def _map_header(cells: dict[int, str]) -> dict[str, int]:
    mapping: dict[str, int] = {}
    for col, header in sorted(cells.items()):
        folded = fold(header)
        for name, needle in _HEADER_RULES:
            if name in mapping:
                continue
            if needle in folded:
                mapping[name] = col
                break
    missing = [c for c in _REQUIRED_COLUMNS if c not in mapping]
    if missing:
        raise IndexBuildError(f"export header is missing columns: {missing}")
    return mapping


_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE company (
    idno TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    name_key TEXT,
    legal_form TEXT,
    registered_on TEXT,
    address TEXT,
    cuatm TEXT,
    activities TEXT,
    licensed_activities TEXT,
    directors_raw TEXT,
    founders_raw TEXT
);
CREATE TABLE officer (
    idno TEXT NOT NULL,
    seq INTEGER NOT NULL,
    name TEXT NOT NULL,
    role TEXT,
    role_key TEXT,
    interest_type TEXT NOT NULL,
    is_entity INTEGER NOT NULL
);
CREATE TABLE founder (
    idno TEXT NOT NULL,
    seq INTEGER NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    share_pct REAL,
    share_text TEXT,
    joint INTEGER NOT NULL,
    grp INTEGER,
    resolved_idno TEXT,
    resolved_name TEXT,
    resolution TEXT
);
"""


def build_index(
    xlsx_path: Path | str,
    out_path: Path | str,
    *,
    snapshot_date: str | None = None,
    source_url: str | None = None,
    sheet_name: str | None = "Company",
) -> dict[str, Any]:
    """Build the SQLite index from one weekly company export.

    Written beside ``out_path`` and renamed into place at the end, so a
    lookup never reads a half-built file. Returns the counts recorded in the
    ``meta`` table.
    """
    xlsx_path = Path(xlsx_path)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_name(out_path.name + ".building")
    scratch = out_path.with_name(out_path.name + ".strings")
    for leftover in (tmp, scratch):
        if leftover.exists():
            leftover.unlink()

    started = time.monotonic()
    counts: dict[str, int] = {
        "rows_seen": 0,
        "companies": 0,
        "excluded_liquidated": 0,
        "excluded_sole_traders_and_farms": 0,
        "excluded_no_valid_idno": 0,
        "duplicate_idno": 0,
        "officers": 0,
        "founders": 0,
        "founders_skipped_collective": 0,
        "founders_resolved": 0,
    }
    unmapped_roles: dict[str, int] = {}

    conn = sqlite3.connect(str(tmp))
    conn.executescript(_SCHEMA)
    with zipfile.ZipFile(xlsx_path) as zf:
        date1904 = _uses_1904(zf)
        strings = _SharedStrings(zf, scratch)
        try:
            header: dict[str, int] | None = None
            title = ""
            for cells in _iter_raw_rows(zf, _sheet_path(zf, sheet_name)):
                if header is None:
                    texts = {
                        col: (strings.get_cached(c.shared) if c.shared is not None else (c.text or ""))
                        for col, c in cells.items()
                    }
                    if any(fold(t).startswith("IDNO") for t in texts.values()):
                        header = _map_header(texts)
                    elif not title:
                        title = " ".join(t for t in texts.values() if t)
                    continue
                if not cells:
                    continue
                counts["rows_seen"] += 1

                liquidated = cells.get(header["liquidated_on"])
                if liquidated is not None and (liquidated.shared is not None or (liquidated.text or "").strip()):
                    counts["excluded_liquidated"] += 1
                    continue
                form_cell = cells.get(header["legal_form"])
                form = ""
                if form_cell is not None:
                    form = (
                        strings.get_cached(form_cell.shared)
                        if form_cell.shared is not None
                        else (form_cell.text or "")
                    )
                if fold(form) in EXCLUDED_FORMS:
                    counts["excluded_sole_traders_and_farms"] += 1
                    continue

                wanted = [c.shared for c in cells.values() if c.shared is not None]
                resolved = strings.get_many(wanted) if wanted else {}

                def value(column: str) -> str:
                    col = header.get(column) if header else None
                    cell = cells.get(col) if col is not None else None
                    if cell is None:
                        return ""
                    if cell.shared is not None:
                        return resolved.get(cell.shared, "")
                    return cell.text or ""

                try:
                    idno = normalise_idno(_numeric_text(value("idno")))
                except ValueError:
                    counts["excluded_no_valid_idno"] += 1
                    continue
                name = " ".join(value("name").split())
                if not name:
                    counts["excluded_no_valid_idno"] += 1
                    continue

                directors_raw = value("directors")
                founders_raw = value("founders")
                try:
                    conn.execute(
                        "INSERT INTO company VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            idno,
                            name,
                            name_key(name),
                            " ".join(form.split()) or None,
                            _excel_date(value("registered_on"), date1904=date1904),
                            " ".join(value("address").split()) or None,
                            _numeric_text(value("cuatm")) or None,
                            value("activities").strip() or None,
                            value("licensed_activities").strip() or None,
                            directors_raw or None,
                            founders_raw or None,
                        ),
                    )
                except sqlite3.IntegrityError:
                    counts["duplicate_idno"] += 1
                    continue
                counts["companies"] += 1

                for seq, officer in enumerate(parse_directors(directors_raw)):
                    if officer["role_key"] and officer["role_key"] not in ROLE_INTEREST:
                        unmapped_roles[officer["role"]] = unmapped_roles.get(officer["role"], 0) + 1
                    conn.execute(
                        "INSERT INTO officer VALUES (?,?,?,?,?,?,?)",
                        (
                            idno,
                            seq,
                            officer["name"],
                            officer["role"],
                            officer["role_key"],
                            officer["interest_type"],
                            int(officer["is_entity"]),
                        ),
                    )
                    counts["officers"] += 1

                founders = parse_founders(founders_raw)
                counts["founders_skipped_collective"] += _count_collectives(founders_raw)
                for seq, founder in enumerate(founders):
                    conn.execute(
                        "INSERT INTO founder VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            idno,
                            seq,
                            founder["name"],
                            founder["kind"],
                            founder["share_pct"],
                            founder["share_text"],
                            int(founder["joint"]),
                            founder["group"],
                            founder["embedded_idno"],
                            None,
                            "embedded" if founder["embedded_idno"] else None,
                        ),
                    )
                    counts["founders"] += 1
            if header is None:
                raise IndexBuildError("no header row starting with IDNO was found")
        finally:
            strings.close()
            if scratch.exists():
                scratch.unlink()

    conn.execute("CREATE INDEX company_name_key ON company(name_key)")
    conn.execute("CREATE INDEX officer_idno ON officer(idno)")
    conn.execute("CREATE INDEX founder_idno ON founder(idno)")
    counts["founders_resolved"] = _resolve_founders(conn)

    snapshot = snapshot_date or _snapshot_from_title(title)
    meta = {
        "schema_version": INDEX_SCHEMA_VERSION,
        "snapshot_date": snapshot or "",
        "source_url": source_url or "",
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "build_seconds": f"{time.monotonic() - started:.1f}",
        "unmapped_roles": json.dumps(unmapped_roles, ensure_ascii=False),
        **{k: str(v) for k, v in counts.items()},
    }
    conn.executemany("INSERT INTO meta VALUES (?, ?)", list(meta.items()))
    conn.commit()
    conn.close()
    os.replace(tmp, out_path)
    if unmapped_roles:
        logger.warning("asp_moldova: director roles with no BODS mapping: %s", unmapped_roles)
    logger.info("asp_moldova: index built at %s: %s", out_path, counts)
    return meta


def _count_collectives(raw: str | None) -> int:
    text = str(raw or "")
    if not text:
        return 0
    names = [n for n, _ in _FOUNDER_PCT.findall(text)] if "%" in text else text.split(",")
    return sum(1 for n in names if _COLLECTIVE.match(fold(n.strip())))


def _snapshot_from_title(title: str) -> str | None:
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", title or "")
    if not m:
        return None
    day, month, year = (int(p) for p in m.groups())
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def resolve_founder(
    founder_name: str,
    candidates: list[tuple[str, str]],
    *,
    own_idno: str,
) -> tuple[str, str] | None:
    """Pick the company a corporate founder names, or None.

    ``candidates`` are the ``(idno, name)`` pairs whose ``name_key`` equals the
    founder's. The name check is the whole rule: the folded names must be
    equal (not similar), the key must keep at least four characters once
    legal-form words are gone, exactly one company may carry it, and a
    company is never its own founder. Anything else stays unidentified —
    that is what the register says about it.
    """
    key = name_key(founder_name)
    if len(key.replace(" ", "")) < 4:
        return None
    others = [(i, n) for i, n in candidates if i != own_idno and name_key(n) == key]
    if len(others) != 1:
        return None
    return others[0]


def _resolve_founders(conn: sqlite3.Connection) -> int:
    resolved = 0
    rows = conn.execute(
        "SELECT rowid, idno, name, resolved_idno FROM founder WHERE kind = 'entity'"
    ).fetchall()
    for rowid, own, name, embedded in rows:
        if embedded:
            # The register's own text names the founder's IDNO ("… SRL, IDNO
            # 1011600007976"). That is the register identifying it, so it is
            # kept whether or not the founder is in the index — a liquidated
            # founder is still that company — provided it is a real IDNO and
            # not the company itself.
            if embedded == own or not is_idno(embedded):
                conn.execute(
                    "UPDATE founder SET resolved_idno = NULL, resolution = NULL WHERE rowid = ?",
                    (rowid,),
                )
                continue
            hit = conn.execute(
                "SELECT name FROM company WHERE idno = ?", (embedded,)
            ).fetchone()
            conn.execute(
                "UPDATE founder SET resolved_name = ?, resolution = 'embedded_idno' WHERE rowid = ?",
                (hit[0] if hit else None, rowid),
            )
            resolved += 1
            continue
        key = name_key(name)
        if not key:
            continue
        candidates = conn.execute(
            "SELECT idno, name FROM company WHERE name_key = ? LIMIT 3", (key,)
        ).fetchall()
        match = resolve_founder(name, [(i, n) for i, n in candidates], own_idno=own)
        if match is None:
            continue
        conn.execute(
            "UPDATE founder SET resolved_idno = ?, resolved_name = ?, resolution = 'name_match' WHERE rowid = ?",
            (match[0], match[1], rowid),
        )
        resolved += 1
    return resolved


# ---------------------------------------------------------------------------
# Where the index lives, and keeping it current
# ---------------------------------------------------------------------------


def index_path() -> Path:
    configured = getattr(get_settings(), "asp_moldova_db_file", None)
    if configured:
        return Path(configured)
    return Path(tempfile.gettempdir()) / "opencheck" / "asp_moldova.sqlite"


def sync_enabled() -> bool:
    settings = get_settings()
    return bool(settings.allow_live and getattr(settings, "asp_moldova_sync", True))


_SHARED: dict[str, sqlite3.Connection | None] = {}
_STATE: dict[str, Any] = {"last_check": None, "last_outcome": None, "thread": None}
_BUILD_LOCK = threading.Lock()


def read_meta(path: Path | None = None) -> dict[str, str]:
    target = path or index_path()
    if not target.exists():
        return {}
    try:
        conn = sqlite3.connect(f"file:{target}?mode=ro", uri=True)
        try:
            return dict(conn.execute("SELECT key, value FROM meta").fetchall())
        finally:
            conn.close()
    except sqlite3.Error:
        return {}


def _shared_conn() -> sqlite3.Connection | None:
    if _SHARED.get("conn") is None:
        path = index_path()
        if not path.exists():
            return None
        meta = read_meta(path)
        if meta.get("schema_version") != INDEX_SCHEMA_VERSION:
            logger.warning("asp_moldova: index at %s has schema %r; ignoring", path, meta.get("schema_version"))
            return None
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        _SHARED["conn"] = conn
    return _SHARED.get("conn")


def reset_connection() -> None:
    """Forget the cached connection.

    Deliberately not closed: a rebuild runs in a background thread while a
    lookup may be reading, and the old connection keeps reading the file it
    opened after ``os.replace`` swaps in the new one.
    """
    _SHARED.pop("conn", None)


def index_available() -> bool:
    return _shared_conn() is not None


def snapshot_age_days(meta: dict[str, str]) -> int | None:
    try:
        snapshot = date.fromisoformat(meta.get("snapshot_date") or "")
    except ValueError:
        return None
    return (date.today() - snapshot).days


def latest_resource(package: dict[str, Any] | None = None) -> dict[str, str]:
    """The newest XLSX resource on the dataset, as ``{url, snapshot_date, name}``.

    The snapshot date is read from the resource name (``Informații la data de
    14.09.2026``), then from the file name (``company_2026.09.14.xlsx``), then
    from the upload date. The page lists resources oldest-first and cuts off,
    which is how the ticket came to think publication stopped in 2020.
    """
    if package is None:
        import httpx

        response = httpx.get(CKAN_PACKAGE_URL, timeout=60.0, follow_redirects=True)
        response.raise_for_status()
        package = response.json()
    result = (package or {}).get("result") or {}
    best: dict[str, str] | None = None
    for resource in result.get("resources") or []:
        url = str(resource.get("url") or "")
        fmt = str(resource.get("format") or "").lower()
        if fmt != "xlsx" and not url.lower().endswith(".xlsx"):
            continue
        snapshot = None
        m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", str(resource.get("name") or ""))
        if m:
            snapshot = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        if snapshot is None:
            m = re.search(r"(\d{4})\.(\d{2})\.(\d{2})\.xlsx$", url)
            if m:
                snapshot = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        if snapshot is None:
            snapshot = str(resource.get("created") or "")[:10]
        try:
            date.fromisoformat(snapshot)
        except ValueError:
            continue
        if best is None or snapshot > best["snapshot_date"]:
            best = {"url": url, "snapshot_date": snapshot, "name": str(resource.get("name") or "")}
    if best is None:
        raise IndexBuildError("the dataset lists no XLSX resource with a date")
    return best


def _download(url: str, target: Path) -> int:
    import httpx

    target.parent.mkdir(parents=True, exist_ok=True)
    size = 0
    with httpx.stream("GET", url, timeout=300.0, follow_redirects=True) as response:
        response.raise_for_status()
        with open(target, "wb") as fh:
            for chunk in response.iter_bytes():
                size += len(chunk)
                fh.write(chunk)
    return size


def sync_index(*, force: bool = False) -> dict[str, Any]:
    """Make sure the index holds the newest weekly snapshot. Blocking.

    Keeps the file when it is younger than ``REFRESH_AFTER_DAYS``, or when the
    portal has nothing newer; otherwise downloads the newest export and
    rebuilds. Never raises: a week-old index beats none, and the outcome is
    returned and logged.
    """
    path = index_path()
    with _BUILD_LOCK:
        meta = read_meta(path)
        current = meta.get("schema_version") == INDEX_SCHEMA_VERSION
        age = snapshot_age_days(meta) if current else None
        if not force and current and age is not None and age <= REFRESH_AFTER_DAYS:
            return _outcome("kept", path, meta)
        if not sync_enabled():
            return _outcome("kept" if current else "not_configured", path, meta)
        now = time.monotonic()
        last = _STATE["last_check"]
        if not force and current and last is not None and now - last < _CHECK_INTERVAL_S:
            return _outcome("kept", path, meta)
        _STATE["last_check"] = now
        xlsx = path.with_name(path.name + ".xlsx")
        try:
            resource = latest_resource()
            if not force and current and resource["snapshot_date"] <= (meta.get("snapshot_date") or ""):
                return _outcome("kept", path, meta)
            _download(resource["url"], xlsx)
            new_meta = build_index(
                xlsx,
                path,
                snapshot_date=resource["snapshot_date"],
                source_url=resource["url"],
            )
            reset_connection()
            return _outcome("built", path, new_meta)
        except Exception as exc:  # noqa: BLE001 - reported, never raised
            logger.warning("asp_moldova: index sync failed: %s", exc)
            return _outcome("failed", path, meta, error=f"{type(exc).__name__}: {exc}"[:300])
        finally:
            if xlsx.exists():
                try:
                    xlsx.unlink()
                except OSError:  # pragma: no cover - best effort
                    pass


def _outcome(kind: str, path: Path, meta: dict[str, str], *, error: str | None = None) -> dict[str, Any]:
    result = {
        "asp_moldova": kind,
        "path": str(path),
        "snapshot_date": meta.get("snapshot_date") or None,
        "companies": meta.get("companies") or None,
    }
    if error:
        result["error"] = error
    _STATE["last_outcome"] = result
    return result


def warm_index() -> dict[str, Any]:
    """Startup hook: build or refresh the index. Synchronous; run in a thread."""
    if not sync_enabled() and not index_path().exists():
        return _outcome("not_configured", index_path(), {})
    return sync_index()


def _start_background_sync() -> threading.Thread | None:
    thread = _STATE.get("thread")
    if thread is not None and thread.is_alive():
        return thread
    if not sync_enabled():
        return None
    thread = threading.Thread(target=sync_index, name="asp_moldova-sync", daemon=True)
    _STATE["thread"] = thread
    thread.start()
    return thread


def state() -> dict[str, Any]:
    """What the last sync did, for diagnostics."""
    return dict(_STATE.get("last_outcome") or {})


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


def _rows(conn: sqlite3.Connection, table: str, idno: str) -> list[dict[str, Any]]:
    cur = conn.execute(f"SELECT * FROM {table} WHERE idno = ? ORDER BY seq", (idno,))
    return [dict(r) for r in cur.fetchall()]


class AspMoldovaAdapter(SourceAdapter):
    """Source adapter for Moldova's State Register of Legal Entities."""

    id = "asp_moldova"

    lookup_derivers = (LookupDeriver(MD_RA_CODES, "md_idno", normalise_idno),)
    lookup_pass_legal_name = True
    lookup_timeout_s = _LOOKUP_TIMEOUT_S

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="ASP — State Register of Legal Entities (Moldova)",
            homepage=_HOMEPAGE,
            description=(
                "Moldova's State Register of Legal Entities (Registrul de stat "
                "al unităților de drept), published every week by the Agenția "
                "Servicii Publice as a full open-data snapshot on "
                "dataset.gov.md. Company name, IDNO, legal form, registration "
                "date, registered address and activity codes, directors with "
                "their filed role, and founders with their share of the "
                "capital. No beneficial owners; joint-stock companies publish "
                "no founders. Liquidated companies, sole traders and peasant "
                "farms are not indexed."
            ),
            license=LICENSE_ID,
            attribution=(
                "Contains information from the State Register of Legal Entities "
                "published by the Agenția Servicii Publice (Public Services "
                "Agency) of the Republic of Moldova on dataset.gov.md, reused "
                "under the portal's conditions for the reuse of public-sector "
                "information."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=bool(settings.allow_live or index_available()),
            is_national_register=True,
            country="MD",
        )

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Name search over the index. Empty when no index is present."""
        if kind != SearchKind.ENTITY:
            return []
        conn = _shared_conn()
        key = name_key(query)
        if conn is None or len(key) < 3:
            return []
        cur = conn.execute(
            "SELECT idno, name, legal_form FROM company WHERE name_key LIKE ? LIMIT 10",
            (f"%{key}%",),
        )
        return [
            SourceHit(
                source_id=self.id,
                hit_id=row["idno"],
                kind=SearchKind.ENTITY,
                name=row["name"],
                summary=f"MD-IDNO {row['idno']}",
                identifiers={"md_idno": row["idno"]},
                raw={"idno": row["idno"], "legal_form": row["legal_form"]},
                is_stub=False,
                liveness="snapshot",
            )
            for row in cur.fetchall()
        ]

    def _bundle(self, idno: str, legal_name: str, **extra: Any) -> dict[str, Any]:
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "idno": idno,
            "name": legal_name or "",
            "company": None,
            "officers": [],
            "founders": [],
            "snapshot_date": None,
            "legal_name": legal_name,
            "link": DATASET_URL,
            "not_found": False,
            "coverage_note": None,
            "is_stub": True,
        }
        bundle.update(extra)
        return bundle

    async def _wait_for_index(self) -> sqlite3.Connection | None:
        thread = _start_background_sync()
        if thread is None:
            return None
        deadline = time.monotonic() + _BUILD_WAIT_S
        while thread.is_alive() and time.monotonic() < deadline:
            await asyncio.sleep(0.5)
        return _shared_conn()

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the register's record for one IDNO."""
        try:
            idno = normalise_idno(hit_id)
        except ValueError:
            return self._bundle(str(hit_id or ""), legal_name)

        conn = _shared_conn()
        if conn is None:
            conn = await self._wait_for_index()
        if conn is None:
            if sync_enabled():
                outcome = state()
                degradation.record(
                    self.id,
                    (
                        "The State Register index was not available: "
                        + (
                            "the weekly export could not be downloaded or built"
                            if outcome.get("asp_moldova") == "failed"
                            else "the weekly export is still being downloaded and indexed"
                        )
                    ),
                    reason=degradation.REASON_UPSTREAM_ERROR,
                )
            return self._bundle(idno, legal_name)

        meta = read_meta()
        age = snapshot_age_days(meta)
        if age is None or age > REFRESH_AFTER_DAYS:
            _start_background_sync()
        snapshot = meta.get("snapshot_date") or None
        built = None
        if snapshot:
            built = datetime.fromisoformat(snapshot).replace(tzinfo=timezone.utc)
        provenance.record_snapshot(
            built,
            f"ASP weekly open-data export of {snapshot}" if snapshot else "ASP weekly open-data export",
        )

        row = conn.execute("SELECT * FROM company WHERE idno = ?", (idno,)).fetchone()
        if row is None:
            return self._bundle(
                idno,
                legal_name,
                snapshot_date=snapshot,
                not_found=True,
                coverage_note=COVERAGE_NOT_IN_EXPORT,
                is_stub=False,
            )
        company = dict(row)
        bundle = self._bundle(
            idno,
            legal_name,
            name=company.get("name") or legal_name,
            company=company,
            officers=_rows(conn, "officer", idno),
            founders=_rows(conn, "founder", idno),
            snapshot_date=snapshot,
            is_stub=False,
        )
        validate_raw(self.id, AspMoldovaBundle, bundle)
        return bundle
