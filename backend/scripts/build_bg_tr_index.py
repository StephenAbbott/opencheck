#!/usr/bin/env python3
"""Build the Bulgarian Commercial Register index from the daily open-data files.

The Registry Agency (Агенция по вписванията) publishes the Commercial Register
and Register of Non-Profit Legal Entities on data.egov.bg as one dataset,
"Търговски регистър - списък набори данни"
(``2df0c2af-e769-4397-be33-fcbe269806f3``), under **CC0**: one XML file per
publication day since 1 January 2021. The entries for 2008–2020 are listed on
the portal but hold no file, so the index starts on 1 January 2021.

    python3 scripts/build_bg_tr_index.py --input-dir data/bg/bg_tr/<dataset> \
        --out /tmp/bg_tr.sqlite --gzip

``scripts/refresh_bg_tr_index.sh`` runs ``data/bg/fetch_bg_tr.py`` (which must
run on a Bulgarian connection: data.egov.bg refuses every address outside
Bulgaria), then this, then replaces the ``bg-tr-index`` release asset.
Standard library only, so it runs with a bare ``python3``.

What a daily file is
--------------------

``Message/Body/Deeds/Deed`` (one company: ``UIC``, ``CompanyName``,
``LegalForm``, ``DeedStatus``) → ``SubDeed`` (a part of the company's file:
main circumstances, procura, liquidation, beneficial owners, bankruptcy…) →
one element per **field entry** made that day, with ``FieldOperation`` Add or
Erase and ``FieldEntryNumber`` (the entry's timestamp, ``yyyymmddhhmmss``).

* A list field (managers, partners, board, beneficial owners…) is entered
  **whole**: an Add carries the full current list, including people entered
  years earlier; an Erase (often empty) clears it.
* A single-valued field (name, seat, capital, sole owner…) is replaced by an
  Add; an Erase names the value it removes by ``RecordID``.

So the state of a company is the latest entry of each field, replayed in entry
order, Erase before Add on the same timestamp. This build keeps that state and
nothing else. A field not touched since 1 January 2021 is **not in the files**:
the index cannot say what it holds, and the adapter never reads its absence as
"none".

Files are ordered by the header's ``CreateDate``, never by name: some file
names carry impossible dates (``31.11.2024``). A file republished under a
second name (the same header ``ID``) is read once.

Personal data, dropped here
---------------------------

The register publishes beneficial owners with their home address, and people
with a salted hash of their personal number (ЕГН/ЛНЧ) or, for foreigners,
sometimes a raw birth date. OpenCheck republishes a beneficial owner's name,
country of nationality and residence, and the nature and extent of the
control, and never (Stephen, 18 Nov 2026):

* an address of a natural person (beneficial owners, procurators, partners);
* the ``Indent`` value — neither the hash nor a raw birth date. The index
  keeps a company-scoped key derived from the hash (so one person on two lists
  of one company is one party), never the hash itself, and no key at all from
  a raw birth date (a hash of six digits is reversible by enumeration);
* passport fields (procurators);
* free text that can carry a personal number or an address (the way of
  representation, a beneficial owner's description of control): only the
  structured parts are kept, and a text is kept only when it carries neither.

A sole trader's (ET) seat is the trader's own address: only the settlement and
country are kept for it.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import logging
import re
import shutil
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from pathlib import Path
from typing import Any

log = logging.getLogger("build_bg_tr_index")

#: Bump when the table layout changes; the adapter refuses any other value
#: (``bg_tr.INDEX_SCHEMA_VERSION``, pinned equal by a test).
SCHEMA_VERSION = "1"

DATASET_ID = "2df0c2af-e769-4397-be33-fcbe269806f3"
DATASET_URL = f"https://data.egov.bg/data/view/{DATASET_ID}"
LICENSE = "CC0-1.0"

#: Fewer files than this is not the whole 2021+ series (1,784 on 18 Nov 2026).
#: ``--min-files 0`` lowers it for fixtures.
MIN_FILES = 1_000

#: Fields read, by element name. Everything else (pledges, distraints, the
#: accounts announcements, branches' own files…) is not read in v1.
LIST_FIELDS = frozenset({
    "Managers", "Representatives", "Representatives101", "Representatives103",
    "BoardOfDirectors", "BoardOfManagers", "BoardOfManagers2", "SupervisingBoard",
    "ControllingBoard", "ManagementBodies12d", "Partners",
    "UnlimitedLiabilityPartners", "Liquidators", "Procurators",
})
PARTY_SCALARS = frozenset({"SoleCapitalOwner", "PhysicalPersonTrader", "ChairMan"})
FLAG_FIELDS = frozenset({"CessationOfTrade", "AddemptionOfTrader", "ResumeOfLiquidation"})
COURT_ACT_FIELDS = frozenset({"OpenProccedings", "DeclareBankrupt"})
TEXT_FIELDS = frozenset({"Company", "Transliteration", "TermsOfLiquidation"})
OTHER_FIELDS = frozenset({
    "UIC", "LegalForm", "Seat", "Funds", "WayOfRepresentation", "ForeignTraders",
    "ActualOwners",
})
READ_FIELDS = LIST_FIELDS | PARTY_SCALARS | FLAG_FIELDS | COURT_ACT_FIELDS | TEXT_FIELDS | OTHER_FIELDS

#: Address parts kept for a legal person's seat; a sole trader's keeps only
#: ``_PLACE_PARTS``. A natural person's address is never read.
_PLACE_PARTS = ("CountryCode", "Country", "District", "Municipality", "Settlement",
                "PostCode", "ForeignPlace")
_STREET_PARTS = ("HousingEstate", "Street", "StreetNumber", "Block", "Entrance",
                 "Floor", "Apartment")

_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_EIK = re.compile(r"^\d{9}(\d{4})?$")
_CREATE = re.compile(rb"<CreateDate>([^<]+)</CreateDate>")
_MSG_ID = re.compile(rb"<ID>([^<]+)</ID>")

#: A text that may carry a personal number or an address is not kept.
_UNSAFE_TEXT = re.compile(
    r"\d{10}|ЕГН|ЛНЧ|роден|родена|дата на раждане|паспорт|"
    r"\bул\.|\bбул\.|\bж\.к\.|\bжк\b|\bет\.|\bап\.|\bвх\.|\bбл\.|\bп\.к\.|адрес|"
    r"street|\bapt\b|passport|born",
    re.IGNORECASE,
)
_PERCENT = re.compile(
    r"(?P<pre>повече от|над|не по-малко от|more than|over|at least)?\s*"
    r"(?P<num>\d{1,3}(?:[.,]\d+)?)\s*(?:%|на сто|процент|per cent|percent)",
    re.IGNORECASE,
)

_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE company (
    uic TEXT PRIMARY KEY,           -- the 9-digit ЕИК (13 for a branch unit)
    name TEXT,
    name_fold TEXT,
    transliteration TEXT,
    legal_form TEXT,                -- the Deed's code: EOOD, AD, ET, ASSOC…
    deed_status TEXT,               -- N, E, C, L
    status_class TEXT,              -- live | pending | terminal
    status_text TEXT,
    status_since TEXT,
    registered_on TEXT,             -- only when the files hold the registration
    first_entry TEXT,               -- the first entry the files hold
    last_entry TEXT,
    has_bo INTEGER NOT NULL,
    data TEXT NOT NULL              -- JSON: the current fields, reduced
);
CREATE INDEX company_name ON company(name_fold);
"""

_WORK = """
CREATE TABLE state (
    uic TEXT, subuic TEXT, subtype TEXT, tag TEXT,
    entry_key TEXT, op TEXT, entry_date TEXT, record_id TEXT, value TEXT,
    PRIMARY KEY (uic, subuic, tag)
) WITHOUT ROWID;
CREATE TABLE subdeed (
    uic TEXT, subuic TEXT, subtype TEXT, status TEXT, seen TEXT,
    PRIMARY KEY (uic, subuic)
) WITHOUT ROWID;
CREATE TABLE deed (
    uic TEXT PRIMARY KEY, name TEXT, legal_form TEXT, deed_status TEXT,
    seen TEXT, first_entry TEXT, last_entry TEXT
) WITHOUT ROWID;
"""

# The newer entry wins. An Erase that names a RecordID removes only that value:
# erasing a value an earlier Add already replaced changes nothing.
_UPSERT = """
INSERT INTO state VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (uic, subuic, tag) DO UPDATE SET
    subtype = excluded.subtype, entry_key = excluded.entry_key, op = excluded.op,
    entry_date = excluded.entry_date, record_id = excluded.record_id,
    value = excluded.value
WHERE excluded.entry_key > state.entry_key
  AND (excluded.op = 'Add' OR excluded.record_id IS NULL OR state.record_id IS NULL
       OR state.record_id = excluded.record_id OR state.op = 'Erase')
"""


class BuildRefused(Exception):
    """The input is not what the build expects; nothing is written."""


# ---------------------------------------------------------------------------
# Reading the files
# ---------------------------------------------------------------------------


def _open(path: Path):
    return gzip.open(path, "rb") if path.suffix == ".gz" else open(path, "rb")


def file_header(path: Path) -> tuple[str, str]:
    """``(CreateDate, message ID)`` from a file's header; refuses a file that
    is not one of the register's messages (an HTML page saved by mistake)."""
    try:
        with _open(path) as fh:
            head = fh.read(4096)
    except (OSError, EOFError) as exc:
        raise BuildRefused(f"{path.name}: cannot read ({exc})") from exc
    if b"<Message" not in head:
        raise BuildRefused(f"{path.name}: not a register message (an HTML page?)")
    created = _CREATE.search(head)
    if not created:
        raise BuildRefused(f"{path.name}: no CreateDate in the header")
    msg_id = _MSG_ID.search(head)
    return created.group(1).decode(), (msg_id.group(1).decode() if msg_id else path.name)


def ordered_files(input_dir: Path) -> list[tuple[str, Path]]:
    """Every daily file in ``input_dir`` (not its subfolders), ordered by the
    header's CreateDate, one per message ID."""
    files = sorted(p for p in input_dir.iterdir()
                   if p.is_file() and (p.name.endswith(".xml.gz") or p.name.endswith(".xml")))
    seen: dict[str, Path] = {}
    out: list[tuple[str, Path]] = []
    for path in files:
        created, msg_id = file_header(path)
        if msg_id in seen:
            log.info("skipping %s: the same message as %s", path.name, seen[msg_id].name)
            continue
        seen[msg_id] = path
        out.append((created, path))
    out.sort()
    return out


def _text(el: ET.Element | None, tag: str) -> str:
    if el is None:
        return ""
    child = el.find(tag)
    return (child.text or "").strip() if child is not None and child.text else ""


def safe_text(text: str | None, limit: int = 300) -> str | None:
    """A free text kept only when it can carry neither a personal number nor
    an address; whitespace collapsed, cut at ``limit``."""
    t = " ".join((text or "").split())
    if not t or _UNSAFE_TEXT.search(t):
        return None
    return t if len(t) <= limit else t[: limit - 1].rstrip() + "…"


def parse_percent(*texts: str | None) -> dict[str, float] | None:
    """The one percentage a beneficial owner's texts state, as
    ``{"exact": x}`` or ``{"more_than": x}``; None when they state none or
    several different ones."""
    found: set[tuple[str, float]] = set()
    for text in texts:
        for m in _PERCENT.finditer(text or ""):
            try:
                num = float(m.group("num").replace(",", "."))
            except ValueError:
                continue
            if not 0 < num <= 100:
                continue
            found.add(("more_than" if m.group("pre") else "exact", num))
    if len(found) != 1:
        return None
    kind, num = found.pop()
    return {kind: num}


def person_key(uic: str, indent: str) -> str | None:
    """A company-scoped key for a person, from the register's salted hash —
    never the hash itself, and nothing from a raw birth date."""
    indent = (indent or "").strip().lower()
    if not _HEX64.match(indent):
        return None
    return hashlib.sha256(f"bg_tr|{uic}|{indent}".encode()).hexdigest()[:20]


_ORG_TOKENS = frozenset({
    "ЕООД", "ООД", "АД", "ЕАД", "ЕТ", "КД", "СД", "КДА", "ДЗЗД", "АДСИЦ", "ЛТД",
    "LTD", "LTD.", "LIMITED", "GMBH", "AG", "SA", "S.A.", "SRL", "S.R.L.", "BV", "B.V.",
    "NV", "N.V.", "LLC", "INC", "INC.", "CORP", "PLC", "AB", "AS", "A/S", "OY", "SPA",
    "S.P.A.", "SE", "KG", "OOD", "EOOD", "EAD", "AD", "ФОНДАЦИЯ", "СДРУЖЕНИЕ",
    "КООПЕРАЦИЯ", "ОБЩИНА", "ДРУЖЕСТВО", "HOLDING", "HOLDINGS", "FOUNDATION", "TRUST",
})


def _looks_like_organisation(name: str) -> bool:
    words = {w.strip(".,„“\"”()«»") for w in name.upper().replace("/", " ").split()}
    return bool(words & _ORG_TOKENS)


def reduce_party(uic: str, el: ET.Element | None) -> dict[str, Any] | None:
    """One ``Subject`` / ``Person`` element, reduced to what may be published."""
    if el is None:
        return None
    name = _text(el, "Name")
    if not name:
        return None
    itype = _text(el, "IndentType")
    indent = _text(el, "Indent")
    foreign = {
        k: (el.get(a) or "").strip()
        for k, a in (("register", "ForeignRegisterCode"), ("number", "RegistrationNumber"),
                     ("authority", "CompetentAuthorityForRegistration"),
                     ("legal_form", "ForeignLegalFormCode"))
        if (el.get(a) or "").strip()
    }
    is_entity = (
        itype == "UIC"
        or bool(foreign.get("register") or foreign.get("number"))
        or bool((el.get("LegalForm") or "").strip())
        or (itype in ("", "Undefined") and _looks_like_organisation(name))
    )
    party: dict[str, Any] = {"name": " ".join(name.split()), "kind": "entity" if is_entity else "person"}
    country = (el.get("CountryCode") or "").strip()
    country_name = _text(el, "CountryName")
    if country:
        party["country"] = country
    if country_name:
        party["country_name"] = country_name
    if is_entity:
        if itype == "UIC" and _EIK.match(indent):
            party["eik"] = indent
        if foreign:
            party["foreign"] = foreign
        if (el.get("LegalForm") or "").strip():
            party["legal_form"] = el.get("LegalForm").strip()
    else:
        key = person_key(uic, indent)
        if key:
            party["key"] = key
    position = (el.get("Position") or "").strip()
    if position:
        party["position"] = position
    return party


def _first_party(uic: str, item: ET.Element) -> dict[str, Any] | None:
    for child in item:
        if child.tag in ("Subject", "Person"):
            party = reduce_party(uic, child)
            if party:
                return party
    return None


def reduce_actual_owner(uic: str, item: ET.Element) -> dict[str, Any] | None:
    party = _first_party(uic, item)
    if not party:
        return None
    residence = item.find("CountryOfResidence")
    res_code = _text(residence, "CountryCode")
    if res_code:
        party["residence"] = res_code
        res_name = _text(residence, "Country")
        if res_name:
            party["residence_name"] = res_name
    rights_text = _text(item, "OwnedRights")
    rights: list[dict[str, Any]] = []
    texts = [rights_text]
    details = item.find("OwnedRightsDetails")
    for detail in (details if details is not None else []):
        code = _text(detail, "OwnedRightCode")
        size = _text(detail, "OwnedRightSize")
        desc = _text(detail, "OwnedRightDescription")
        texts += [size, desc]
        entry: dict[str, Any] = {}
        if code:
            entry["code"] = code
        name = safe_text(_text(detail, "OwnedRightName"), 200)
        if name:
            entry["name"] = name
        kept = safe_text(size)
        if kept:
            entry["size"] = kept
        if entry:
            rights.append(entry)
    if rights:
        party["rights"] = rights
    kept_rights = safe_text(rights_text)
    if kept_rights:
        party["owned_rights"] = kept_rights
    percent = parse_percent(*texts)
    if percent:
        party["percent"] = percent
    return party


def reduce_address(el: ET.Element | None) -> dict[str, str] | None:
    if el is None:
        return None
    out = {k: _text(el, k) for k in _PLACE_PARTS + _STREET_PARTS}
    out = {k: v for k, v in out.items() if v and v != "-"}
    return out or None


def reduce_field(uic: str, field: ET.Element) -> Any:
    """The publishable value of one Add entry, or None when it holds nothing."""
    tag = field.tag
    if tag in LIST_FIELDS:
        items = []
        for item in field:
            party = _first_party(uic, item)
            if not party:
                continue
            party["role"] = item.tag
            share = (item.get("share") or "").strip()
            if share:
                party["share"] = share
            items.append(party)
        return items or None
    if tag in PARTY_SCALARS:
        for child in field:
            if child.tag in ("Subject", "Person"):
                party = reduce_party(uic, child)
                if party:
                    party["role"] = tag
                    return [party]
        return None
    if tag == "ActualOwners":
        owners = [o for o in (reduce_actual_owner(uic, item) for item in field
                              if item.tag == "ActualOwner") if o]
        return owners or None
    if tag in FLAG_FIELDS:
        return True
    if tag in COURT_ACT_FIELDS:
        act = field.find("ActData")
        return {"act_date": _text(act, "ActDate")[:10] or None}
    if tag in TEXT_FIELDS:
        text = " ".join((field.text or "").split())
        return text or None
    if tag == "UIC":
        return {"is_new": (field.get("IsNew") or "").lower() == "true"}
    if tag == "LegalForm":
        return {"code": field.get("Code") or "", "text": field.get("Text") or ""}
    if tag == "Seat":
        return reduce_address(field.find("Address"))
    if tag == "Funds":
        amount = (field.text or "").strip()
        return {"amount": amount, "euro": field.get("euro") == "1"} if amount else None
    if tag == "WayOfRepresentation":
        flags = {k: field.get(k) == "1" for k in ("jointly", "severally", "otherWay")}
        return flags if any(flags.values()) else None
    if tag == "ForeignTraders":
        out: dict[str, str] = {}
        for item in field:
            if item.tag == "ForeignTraderRegistration":
                for key, attr in (("name", "Name"), ("register", "Register"),
                                  ("number", "EntryNumber"), ("legal_form", "LegalForm")):
                    if (item.get(attr) or "").strip():
                        out[key] = item.get(attr).strip()
            elif item.tag == "ForeignTraderCountry":
                for key, attr in (("country", "CountryCode"), ("country_name", "Country")):
                    if (item.get(attr) or "").strip():
                        out[key] = item.get(attr).strip()
        return out or None
    return None


def iter_deeds(path: Path) -> Iterator[ET.Element]:
    with _open(path) as fh:
        for _event, el in ET.iterparse(fh, events=("end",)):
            if el.tag == "Deed":
                yield el
                el.clear()


# ---------------------------------------------------------------------------
# The build
# ---------------------------------------------------------------------------


def _replay_file(conn: sqlite3.Connection, seq: int, created: str, path: Path) -> int:
    rows: list[tuple] = []
    subdeeds: list[tuple] = []
    deeds: list[tuple] = []
    entries = 0
    for deed in iter_deeds(path):
        uic = (deed.get("UIC") or "").strip()
        if not uic:
            continue
        first = last = None
        for sub in deed.findall("SubDeed"):
            subuic = sub.get("SubUIC") or ""
            subtype = sub.get("SubUICType") or ""
            subdeeds.append((uic, subuic, subtype, sub.get("SubDeedStatus") or "", created))
            for field in sub:
                op = field.get("FieldOperation")
                if op not in ("Add", "Erase"):
                    continue
                entry_no = (field.get("FieldEntryNumber") or "").strip()
                entry_date = (field.get("FieldEntryDate") or "")[:10]
                if entry_date:
                    first = entry_date if first is None or entry_date < first else first
                    last = entry_date if last is None or entry_date > last else last
                if field.tag not in READ_FIELDS:
                    continue
                entries += 1
                value = reduce_field(uic, field) if op == "Add" else None
                key = f"{entry_no:0>14}{'1' if op == 'Add' else '0'}{seq:06d}"
                rows.append((uic, subuic, subtype, field.tag, key, op, entry_date or None,
                             (field.get("RecordID") or None),
                             json.dumps(value, ensure_ascii=False) if value is not None else None))
        deeds.append((uic, (deed.get("CompanyName") or "").strip() or None,
                      (deed.get("LegalForm") or "").strip() or None,
                      (deed.get("DeedStatus") or "").strip() or None, created, first, last))
    conn.executemany(_UPSERT, rows)
    conn.executemany(
        "INSERT INTO subdeed VALUES (?, ?, ?, ?, ?) ON CONFLICT (uic, subuic) DO UPDATE SET"
        " subtype = excluded.subtype, status = excluded.status, seen = excluded.seen"
        " WHERE excluded.seen >= subdeed.seen",
        subdeeds,
    )
    conn.executemany(
        "INSERT INTO deed VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (uic) DO UPDATE SET"
        " name = CASE WHEN excluded.seen >= deed.seen THEN coalesce(excluded.name, deed.name) ELSE deed.name END,"
        " legal_form = CASE WHEN excluded.seen >= deed.seen THEN coalesce(excluded.legal_form, deed.legal_form) ELSE deed.legal_form END,"
        " deed_status = CASE WHEN excluded.seen >= deed.seen THEN coalesce(excluded.deed_status, deed.deed_status) ELSE deed.deed_status END,"
        " seen = max(excluded.seen, deed.seen),"
        " first_entry = CASE WHEN deed.first_entry IS NULL OR excluded.first_entry < deed.first_entry"
        "   THEN coalesce(excluded.first_entry, deed.first_entry) ELSE deed.first_entry END,"
        " last_entry = CASE WHEN deed.last_entry IS NULL OR excluded.last_entry > deed.last_entry"
        "   THEN coalesce(excluded.last_entry, deed.last_entry) ELSE deed.last_entry END",
        deeds,
    )
    return entries


def derive_status(deed_status: str | None, fields: dict[str, dict[str, Any]]) -> tuple[str, str, str | None]:
    """``(class, words, since)`` from the replayed fields. The order is the
    register's: a deletion ends everything; insolvency and liquidation are
    under way until a resumption; a decision to dissolve is pending."""
    def since(tag: str) -> str | None:
        return (fields.get(tag) or {}).get("entry_date")

    if "AddemptionOfTrader" in fields:
        return "terminal", "deleted from the register", since("AddemptionOfTrader")
    if deed_status in ("C", "L"):
        return "terminal", "file closed", None
    if "DeclareBankrupt" in fields:
        act = (fields["DeclareBankrupt"].get("value") or {}).get("act_date")
        return "pending", "declared bankrupt", act or since("DeclareBankrupt")
    if "OpenProccedings" in fields:
        act = (fields["OpenProccedings"].get("value") or {}).get("act_date")
        return "pending", "insolvency proceedings opened", act or since("OpenProccedings")
    in_liquidation = "Liquidators" in fields or "TermsOfLiquidation" in fields
    if in_liquidation and "ResumeOfLiquidation" not in fields:
        tag = "Liquidators" if "Liquidators" in fields else "TermsOfLiquidation"
        return "pending", "in liquidation", since(tag)
    if "CessationOfTrade" in fields:
        return "pending", "dissolution entered", since("CessationOfTrade")
    return "live", "no deletion, liquidation or insolvency entered since 2021", None


def _finalise(conn: sqlite3.Connection) -> dict[str, int]:
    """Fold the replayed state into one row per company."""
    counts = {"companies": 0, "with_bo": 0, "terminal": 0, "pending": 0}
    out = conn.cursor()
    deeds = conn.execute("SELECT uic, name, legal_form, deed_status, first_entry, last_entry FROM deed")
    state = conn.cursor()
    batch: list[tuple] = []
    for uic, name, legal_form, deed_status, first_entry, last_entry in deeds.fetchall():
        fields: dict[str, dict[str, Any]] = {}
        for subuic, subtype, tag, entry_date, value, sub_status in state.execute(
            "SELECT s.subuic, s.subtype, s.tag, s.entry_date, s.value, d.status FROM state s"
            " LEFT JOIN subdeed d ON d.uic = s.uic AND d.subuic = s.subuic"
            " WHERE s.uic = ? AND s.value IS NOT NULL ORDER BY s.subuic", (uic,)
        ):
            if sub_status == "C":
                continue
            parsed = json.loads(value)
            if tag in fields and isinstance(parsed, list) and isinstance(fields[tag]["value"], list):
                fields[tag]["value"] = fields[tag]["value"] + parsed
                fields[tag]["entry_date"] = max(fields[tag]["entry_date"] or "", entry_date or "") or None
            else:
                fields[tag] = {"value": parsed, "entry_date": entry_date}
        if legal_form == "ET" and isinstance((fields.get("Seat") or {}).get("value"), dict):
            seat = fields["Seat"]["value"]
            fields["Seat"]["value"] = {k: v for k, v in seat.items() if k in _PLACE_PARTS}
        status_class, status_text, status_since = derive_status(deed_status, fields)
        company_name = (fields.get("Company") or {}).get("value") or name
        registered_on = None
        uic_field = fields.get("UIC") or {}
        if (uic_field.get("value") or {}).get("is_new"):
            registered_on = uic_field.get("entry_date")
        has_bo = 1 if fields.get("ActualOwners") else 0
        counts["companies"] += 1
        counts["with_bo"] += has_bo
        if status_class in ("terminal", "pending"):
            counts[status_class] += 1
        batch.append((
            uic, company_name, (company_name or "").casefold() or None,
            (fields.get("Transliteration") or {}).get("value"), legal_form, deed_status,
            status_class, status_text, status_since, registered_on, first_entry, last_entry,
            has_bo, json.dumps(fields, ensure_ascii=False, sort_keys=True),
        ))
        if len(batch) >= 5_000:
            out.executemany("INSERT INTO company VALUES (" + ",".join("?" * 14) + ")", batch)
            batch.clear()
    if batch:
        out.executemany("INSERT INTO company VALUES (" + ",".join("?" * 14) + ")", batch)
    return counts


def _stamp(moment: dt.datetime) -> str:
    return moment.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_index(input_dir: Path, out_path: Path, *, min_files: int = MIN_FILES) -> dict[str, str]:
    """Replay every daily file in ``input_dir`` into ``out_path``. Refuses
    (``BuildRefused``) and writes nothing when the input is not the series."""
    started = dt.datetime.now(dt.timezone.utc)
    files = ordered_files(input_dir)
    if len(files) < min_files:
        raise BuildRefused(f"{len(files)} daily files in {input_dir}; expected at least {min_files}")
    if not files:
        raise BuildRefused(f"no daily files in {input_dir}")
    work = out_path.with_name(out_path.name + ".building")
    if work.exists():
        work.unlink()
    conn = sqlite3.connect(work)
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA cache_size = -400000")
    conn.executescript(_SCHEMA + _WORK)
    entries = 0
    gaps: list[tuple[str, str]] = []
    previous: dt.date | None = None
    t0 = time.monotonic()
    for seq, (created, path) in enumerate(files):
        day = dt.date.fromisoformat(created[:10])
        if previous is not None and (day - previous).days > 7:
            gaps.append((previous.isoformat(), day.isoformat()))
        previous = day
        try:
            entries += _replay_file(conn, seq, created, path)
        except ET.ParseError as exc:
            conn.close()
            work.unlink(missing_ok=True)
            raise BuildRefused(f"{path.name}: not well-formed XML ({exc})") from exc
        if seq % 50 == 0:
            conn.commit()
            log.info("%d/%d files, %d entries read, %.0fs", seq + 1, len(files), entries,
                     time.monotonic() - t0)
    conn.commit()
    counts = _finalise(conn)
    # The cut is the last day an entry was made, not the file's CreateDate
    # (written just after midnight, the next day).
    cut, first_day = conn.execute("SELECT max(last_entry), min(first_entry) FROM deed").fetchone()
    cut = cut or files[-1][0][:10]
    conn.executescript("DROP TABLE state; DROP TABLE subdeed; DROP TABLE deed;")
    meta = {
        "schema_version": SCHEMA_VERSION,
        "cut": cut,
        "first_entry": first_day or files[0][0][:10],
        "files": str(len(files)),
        "entries_read": str(entries),
        "companies": str(counts["companies"]),
        "with_beneficial_owners": str(counts["with_bo"]),
        "terminal": str(counts["terminal"]),
        "pending": str(counts["pending"]),
        "gaps": json.dumps(gaps),
        "built_at": _stamp(started),
        "license": LICENSE,
        "source_page": DATASET_URL,
    }
    conn.executemany("INSERT INTO meta VALUES (?, ?)", sorted(meta.items()))
    conn.commit()
    conn.execute("VACUUM")
    conn.close()
    work.replace(out_path)
    log.info("wrote %s: %s companies (%s with beneficial owners), cut %s",
             out_path, meta["companies"], meta["with_beneficial_owners"], meta["cut"])
    return meta


def gzip_file(path: Path) -> Path:
    out = path.with_name(path.name + ".gz")
    with open(path, "rb") as src, gzip.open(out, "wb", compresslevel=9) as dst:
        shutil.copyfileobj(src, dst, 1 << 20)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--input-dir", type=Path, required=True,
                        help="the folder fetch_bg_tr.py filled (one .xml.gz per day)")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--gzip", action="store_true", help="also write <out>.gz")
    parser.add_argument("--min-files", type=int, default=MIN_FILES)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    try:
        meta = build_index(args.input_dir, args.out, min_files=args.min_files)
    except BuildRefused as exc:
        log.error("refused: %s", exc)
        return 2
    if args.gzip:
        gz = gzip_file(args.out)
        log.info("gzipped: %s (%d bytes)", gz, gz.stat().st_size)
    print(" ".join(f"{k}={v}" for k, v in meta.items() if k != "source_page"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
