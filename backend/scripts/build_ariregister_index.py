#!/usr/bin/env python3
"""Build the Estonian e-Business Register (e-Äriregister) index (Phase 330).

The Centre of Registers and Information Systems (RIK) publishes the whole
register — every entity that is entered, in liquidation or in bankruptcy —
as daily open-data files under CC BY 4.0:

    https://avaandmed.ariregister.rik.ee/en/downloading-open-data

Seven of them make the index: the company "simple data" CSV, the registry
cards (every dated entry on every card), the general data, and four JSON files that put the
same people on each company the printable page shows — the persons
entered on the registry card (board members, procurists, liquidators,
partners…), the register of shareholders, the declared beneficial owners
— and, since the fifth file was added, the persons the register knows
about who are *not* on the card: supervisory board members, the shareholders
of a public limited company (AS) the register itself records, founders
with their founding contribution, auditors. The files are regenerated every
morning (~09:30 GMT) and carry no personal identification codes: since
November 2024 a natural person is identified across the files only by
``isikukood_hash``, a pseudonymous UUID.

    python3 scripts/build_ariregister_index.py --out /tmp/ariregister.sqlite --gzip

``.github/workflows/refresh-ariregister-index.yml`` runs this daily and
replaces the ``ariregister-index`` release asset, which production downloads
at boot (``ariregister.warm_index``) — the ONRC / ChileCompra arrangement.
Standard library only, on purpose: it must also run with a bare ``python3``
on a laptop when the workflow cannot.

What it reads, and what it refuses
----------------------------------

* ``ettevotja_rekvisiidid__lihtandmed.csv.zip`` — one row per entity:
  name, registry code, legal form (+ subtype), VAT number, status, first
  entry date, address.
* ``ettevotja_rekvisiidid__kaardile_kantud_isikud.json.zip`` — the persons
  on the registry card, per entity (≈525,000 rows; 1 GB unzipped).
* ``ettevotja_rekvisiidid__osanikud.json.zip`` — shareholders (≈392,000).
* ``ettevotja_rekvisiidid__kasusaajad.json.zip`` — beneficial owners
  (≈410,000).
* ``ettevotja_rekvisiidid__kandevalised_isikud.json.zip`` — the persons
  outside the registry card (≈434,000 rows on ≈296,000 entities; 16 Oct
  2026: founders A/B 410,221, supervisory board N/E 12,943, AS
  shareholders S 3,323, auditors D/J/M 11,283, bankruptcy committee P 374,
  authorised persons V 282, audit committee R 228, congregations L 457).
  The rows have the shareholder file's shape (share size and percentage,
  ownership kind, entry source) plus the role.
* ``ettevotja_rekvisiidid__registrikaardid.json.zip`` — every entry on every
  registry card (≈2.16 million on 379,829 entities, 620 MB unzipped; 16 Oct
  2026): card type and number, entry number, date and entry type
  (``kandeliik``: first entry, change entry, merger, deletion…). It needs
  no credentials, and for Eesti Energia AS and Bolt Technology OÜ it holds
  exactly the entries the credentialed SOAP card log returns. Packed one
  row per company into ``card_entry``.
* ``ettevotja_rekvisiidid__maarused.json.zip`` — the registration
  department's rulings (2.9 million on 16 Oct 2026, 1.8 GB unzipped). Only
  the compliance types in ``RULING_TYPES`` with status ``J`` (in force) are
  kept, into ``ruling`` (one JSON row per company): type, date, number,
  extra deadline. On 16 Oct 2026 that was ~150,000 rulings on ~41,000
  companies; ``L`` (deficiencies remedied) is the answered majority — 95,760
  of 106,757 annual-report deletion warnings.
* ``ettevotja_rekvisiidid__kommertspandid.json.zip`` — commercial pledges
  (4,945 companies, 8,068 pledges, all current): amount, currency (EEK on
  pledges from before 2011, as filed), rank, date and the pledgee. A
  natural-person pledgee (35) is kept as a kind only — the file carries the
  person's code. Into ``pledge``.
* ``ettevotja_rekvisiidid__yldandmed.json.zip`` — the general data, one
  object per entity (230 MB zip, 1.5 GB unzipped; current rows only, no
  history): share capital, whether the company must file beneficial owners,
  legal succession (merger, division, transformation — the text names the
  other company's registry code), dated status changes, activity
  suspensions, a foreign company's branch and its parent, the main activity
  (EMTAK) and the annual reports filed. Into ``general``; contact details,
  real estate and contact persons' addresses are not carried.

Every JSON file is a pretty-printed array of one object per entity, so it
is streamed with ``json.JSONDecoder.raw_decode`` straight out of the zip —
never loaded whole (the officers file would need several gigabytes). Each
list becomes a table (``officer``, ``shareholder``, ``beneficial_owner``,
``off_card_person``)
keyed on the registry code, holding only the columns the adapter and mapper
read, with the Estonian wording of every code stored once in ``vocab``;
street addresses and e-mail addresses of people are **not** carried.

The build refuses, rather than writes a plausible-looking index, when:

* a download is not a zip — Cloudflare answers HTTP 429 (``Error code:
  1015``) to back-to-back requests from one address, and the status alone is
  not trusted either way: the body must start with ``PK``;
* the CSV header is not the expected one, or its parsed records ≠ physical
  lines − 1 (a stray quote merging rows — the ONRC lesson, Phase 211);
* a file is implausibly short (truncated transfer, or the files swapped);
* an entity code is not eight digits, or appears twice;
* a JSON file names an entity the CSV does not hold — the nine files are
  one snapshot, and on 13 Oct 2026 each held exactly the CSV's 379,664
  entities (16 Oct 2026: 379,829, the off-card file included);
* the files' cut dates (``Last-Modified``) fall on different days.

Deleted companies are absent from every file: a code the index lacks means
"not in the current register", never "never existed". The adapter says so.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import email.utils
import gzip
import io
import json
import logging
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
import zipfile
from pathlib import Path
from typing import Any, Iterator

log = logging.getLogger("build_ariregister_index")

#: Bump when the table layout changes; the adapter refuses any other value
#: (``ariregister.INDEX_SCHEMA_VERSION``, pinned equal by a test).
SCHEMA_VERSION = "2"  # 2: the off_card_person table (kandevalised_isikud)

BASE_URL = "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/"
PAGE_URL = "https://avaandmed.ariregister.rik.ee/en/downloading-open-data"
LICENSE = "CC-BY-4.0"

CSV_FILE = "ettevotja_rekvisiidid__lihtandmed.csv.zip"
OFFICERS_FILE = "ettevotja_rekvisiidid__kaardile_kantud_isikud.json.zip"
SHAREHOLDERS_FILE = "ettevotja_rekvisiidid__osanikud.json.zip"
BO_FILE = "ettevotja_rekvisiidid__kasusaajad.json.zip"
OFF_CARD_FILE = "ettevotja_rekvisiidid__kandevalised_isikud.json.zip"
#: Every entry on every registry card: number, date and entry type — the
#: dated event spine the History tab draws from (the card-entry phase).
CARDS_FILE = "ettevotja_rekvisiidid__registrikaardid.json.zip"
#: The general data (üldandmed): capital, the beneficial-owner filing
#: obligation, legal succession, dated status changes, activity
#: suspensions, a foreign company's branch, the main activity and the
#: annual reports filed — current rows only (the üldandmed phase).
GENERAL_FILE = "ettevotja_rekvisiidid__yldandmed.json.zip"
#: The registration department's rulings (määrused) — 2.9 million on 16 Oct
#: 2026, of which only the compliance types in ``RULING_TYPES`` that are in
#: force are kept (the rulings phase).
RULINGS_FILE = "ettevotja_rekvisiidid__maarused.json.zip"
#: Commercial pledges (kommertspandid): amount, rank, date and pledgee.
PLEDGES_FILE = "ettevotja_rekvisiidid__kommertspandid.json.zip"
FILES = (
    CSV_FILE, OFFICERS_FILE, SHAREHOLDERS_FILE, BO_FILE, OFF_CARD_FILE, CARDS_FILE,
    GENERAL_FILE, RULINGS_FILE, PLEDGES_FILE,
)

#: The ruling types kept (Stephen, 16 Oct 2026): the annual-report warnings
#: and fines, the deletion warning published in Ametlikud Teadaanded, and
#: every compulsory-dissolution warning. The rest — entry rulings (1.9
#: million), deficiency rulings on an application, refusals, fee notices —
#: are procedural. ``ariregister.RULING_TYPES`` reads the same set (pinned).
RULING_TYPES = frozenset({
    "X1", "X2", "HM", "HP", "H", "H2", "KAA", "M3",
    "Y3", "Y2O", "Y2A", "GY", "KI", "G1", "G2T", "I2", "Z1",
})
#: A ruling's status: ``J`` in force. ``L`` (deficiencies remedied), ``D``
#: (signed, not yet in force), ``E`` (contested) and ``K`` are not kept —
#: a warning the company answered is not a warning.
RULING_IN_FORCE = "J"

#: The JSON list each people file carries per entity, and the index table
#: its rows go to. The off-card file spells its list ``kaardivalised`` —
#: RIK's own spelling, not a typo here.
PEOPLE_FILES: dict[str, tuple[str, str]] = {
    OFFICERS_FILE: ("kaardile_kantud_isikud", "officer"),
    SHAREHOLDERS_FILE: ("osanikud", "shareholder"),
    BO_FILE: ("kasusaajad", "beneficial_owner"),
    OFF_CARD_FILE: ("kaardivalised_isikud", "off_card_person"),
}

CSV_HEADER = [
    "nimi", "ariregistri_kood", "ettevotja_oiguslik_vorm",
    "ettevotja_oigusliku_vormi_alaliik", "kmkr_nr", "ettevotja_staatus",
    "ettevotja_staatus_tekstina", "ettevotja_esmakande_kpv", "ettevotja_aadress",
    "asukoht_ettevotja_aadressis", "asukoha_ehak_kood", "asukoha_ehak_tekstina",
    "indeks_ettevotja_aadressis", "ads_adr_id", "ads_ads_oid",
    "ads_normaliseeritud_taisaadress", "teabesysteemi_link",
]

#: Columns kept per person row — everything the adapter's bundle and the
#: mapper read, nothing more. Street addresses, EHAK codes, ADS ids and
#: e-mail addresses stay in RIK's files. The ``*_tekstina`` texts beside
#: each code go once into ``vocab`` rather than once per row; ``isikukood_hash``
#: is stored as the UUID's 16 bytes. Order matters: ``ariregister.py`` reads
#: the tables by these names, and ``INDEX_SCHEMA_VERSION`` pins the layout.
TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "officer": (
        "kirje_id", "kaardi_tyyp", "isiku_tyyp", "isiku_roll", "eesnimi",
        "nimi_arinimi", "isikukood_hash", "isikukood_registrikood", "valis_kood",
        "valis_kood_riik", "synniaeg", "osamaks", "osamaksu_valuuta", "aadress_riik",
        "algus_kpv", "lopp_kpv",
    ),
    "shareholder": (
        "kirje_id", "isiku_tyyp", "isiku_roll", "eesnimi", "nimi_arinimi",
        "isikukood_hash", "isikukood_registrikood", "valis_kood", "valis_kood_riik",
        "synniaeg", "aadress_riik", "osaluse_protsent", "osaluse_suurus",
        "osaluse_valuuta", "osaluse_omandiliik", "osaluse_murdosa_lugeja",
        "osaluse_murdosa_nimetaja", "kontrolli_allikas", "kontrolli_allika_kpv",
        "algus_kpv", "lopp_kpv", "grupp",
    ),
    "beneficial_owner": (
        "kirje_id", "algus_kpv", "lopp_kpv", "eesnimi", "nimi", "isikukood_hash",
        "valis_kood", "valis_kood_riik", "synniaeg", "aadress_riik",
        "kontrolli_teostamise_viis", "lahknevusteade_esitatud",
    ),
    # The shareholder row's columns plus nothing: the off-card file carries
    # the role in ``isiku_roll`` as the shareholder file does.
    "off_card_person": (
        "kirje_id", "isiku_tyyp", "isiku_roll", "eesnimi", "nimi_arinimi",
        "isikukood_hash", "isikukood_registrikood", "valis_kood", "valis_kood_riik",
        "synniaeg", "aadress_riik", "osaluse_protsent", "osaluse_suurus",
        "osaluse_valuuta", "osaluse_omandiliik", "osaluse_murdosa_lugeja",
        "osaluse_murdosa_nimetaja", "kontrolli_allikas", "kontrolli_allika_kpv",
        "algus_kpv", "lopp_kpv", "grupp",
    ),
}

#: ``(vocab kind, code field, text field)`` pairs read off every row, so the
#: Estonian wording of each code is stored once. Country codes come from both
#: the foreign-identifier country and the address country.
VOCAB_PAIRS: dict[str, tuple[tuple[str, str, str], ...]] = {
    "officer": (
        ("role", "isiku_roll", "isiku_roll_tekstina"),
        ("country", "valis_kood_riik", "valis_kood_riik_tekstina"),
        ("country", "aadress_riik", "aadress_riik_tekstina"),
        ("currency", "osamaksu_valuuta", "osamaksu_valuuta_tekstina"),
    ),
    "shareholder": (
        ("role", "isiku_roll", "isiku_roll_tekstina"),
        ("country", "valis_kood_riik", "valis_kood_riik_tekstina"),
        ("country", "aadress_riik", "aadress_riik_tekstina"),
        ("ownership_kind", "osaluse_omandiliik", "osaluse_omandiliik_tekstina"),
        ("control_source", "kontrolli_allikas", "kontrolli_allikas_tekstina"),
        ("currency", "osaluse_valuuta", "osamaksu_valuuta_tekstina"),
    ),
    "beneficial_owner": (
        ("country", "valis_kood_riik", "valis_kood_riik_tekstina"),
        ("country", "aadress_riik", "aadress_riik_tekstina"),
        ("control", "kontrolli_teostamise_viis", "kontrolli_teostamise_viis_tekstina"),
    ),
    "off_card_person": (
        ("role", "isiku_roll", "isiku_roll_tekstina"),
        ("country", "valis_kood_riik", "valis_kood_riik_tekstina"),
        ("country", "aadress_riik", "aadress_riik_tekstina"),
        ("ownership_kind", "osaluse_omandiliik", "osaluse_omandiliik_tekstina"),
        ("control_source", "kontrolli_allikas", "kontrolli_allikas_tekstina"),
        ("currency", "osaluse_valuuta", "osamaksu_valuuta_tekstina"),
    ),
}

#: Columns that hold a register date (``DD.MM.YYYY`` in the files), stored
#: as an ISO day.
DATE_COLUMNS = frozenset({"algus_kpv", "lopp_kpv", "synniaeg", "kontrolli_allika_kpv"})

#: Floors well under the 13 Oct 2026 counts (379,664 entities; 525,454 /
#: 392,472 / 410,543 people rows; 434,083 off-card rows on 16 Oct 2026): a
#: file shorter than this was cut short, or is not the file it claims to be.
#: ``--min-rows 0`` lowers them for fixtures.
MIN_ROWS = {
    CSV_FILE: 300_000, OFFICERS_FILE: 400_000,
    SHAREHOLDERS_FILE: 300_000, BO_FILE: 300_000, OFF_CARD_FILE: 300_000,
    # 2,161,627 entries on 16 Oct 2026.
    CARDS_FILE: 1_500_000,
    # 379,829 entities on 16 Oct 2026.
    GENERAL_FILE: 300_000,
    # Companies in the file (it lists all 379,925 on 16 Oct 2026).
    RULINGS_FILE: 300_000,
    # 4,945 pledged companies on 16 Oct 2026.
    PLEDGES_FILE: 3_000,
}

USER_AGENT = "OpenCheck ariregister index builder (+https://opencheck.world)"

#: Seconds between the nine downloads. Cloudflare rate-limits back-to-back
#: requests to this host from one address (``Error code: 1015``, HTTP 429).
DOWNLOAD_SPACING_S = 15

_CODE_RE = re.compile(r"^\d{8}$")
_EE_DATE_RE = re.compile(r"^(\d{2})\.(\d{2})\.(\d{4})$")

_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE vocab (kind TEXT NOT NULL, code TEXT NOT NULL, text TEXT, PRIMARY KEY (kind, code));
CREATE TABLE entity (
    code TEXT PRIMARY KEY,          -- the 8-digit registry code
    name TEXT NOT NULL,
    name_key TEXT NOT NULL,         -- casefolded, for name search (a LIKE scan; no index)
    legal_form TEXT,
    legal_form_subtype TEXT,
    vat_number TEXT,
    status_code TEXT,               -- R entered / L in liquidation / N bankrupt (vocab 'status')
    first_entry_on TEXT,            -- ISO day
    address TEXT
) WITHOUT ROWID;
-- One row per person on the registry card (kaardile_kantud_isikud).
CREATE TABLE officer (
    code TEXT NOT NULL, kirje_id INTEGER, kaardi_tyyp TEXT, isiku_tyyp TEXT,
    isiku_roll TEXT, eesnimi TEXT, nimi_arinimi TEXT, isikukood_hash BLOB,
    isikukood_registrikood TEXT, valis_kood TEXT, valis_kood_riik TEXT,
    synniaeg TEXT, osamaks TEXT, osamaksu_valuuta TEXT, aadress_riik TEXT,
    algus_kpv TEXT, lopp_kpv TEXT
);
CREATE INDEX officer_code ON officer (code);
CREATE INDEX officer_holder ON officer (isikukood_registrikood) WHERE isikukood_registrikood IS NOT NULL;
-- One row per entry in the register of shareholders (osanikud).
CREATE TABLE shareholder (
    code TEXT NOT NULL, kirje_id INTEGER, isiku_tyyp TEXT, isiku_roll TEXT,
    eesnimi TEXT, nimi_arinimi TEXT, isikukood_hash BLOB, isikukood_registrikood TEXT,
    valis_kood TEXT, valis_kood_riik TEXT, synniaeg TEXT, aadress_riik TEXT,
    osaluse_protsent TEXT, osaluse_suurus TEXT, osaluse_valuuta TEXT,
    osaluse_omandiliik TEXT, osaluse_murdosa_lugeja TEXT, osaluse_murdosa_nimetaja TEXT,
    kontrolli_allikas TEXT, kontrolli_allika_kpv TEXT, algus_kpv TEXT, lopp_kpv TEXT,
    grupp INTEGER
);
CREATE INDEX shareholder_code ON shareholder (code);
-- Reverse holdings: which companies a legal person holds shares in.
CREATE INDEX shareholder_holder ON shareholder (isikukood_registrikood) WHERE isikukood_registrikood IS NOT NULL;
-- One row per declared beneficial owner (kasusaajad).
CREATE TABLE beneficial_owner (
    code TEXT NOT NULL, kirje_id INTEGER, algus_kpv TEXT, lopp_kpv TEXT,
    eesnimi TEXT, nimi TEXT, isikukood_hash BLOB, valis_kood TEXT,
    valis_kood_riik TEXT, synniaeg TEXT, aadress_riik TEXT,
    kontrolli_teostamise_viis TEXT, lahknevusteade_esitatud INTEGER
);
CREATE INDEX beneficial_owner_code ON beneficial_owner (code);
-- One row per person outside the registry card (kaardivalised_isikud):
-- founders, supervisory board, AS shareholders, auditors and the rest.
CREATE TABLE off_card_person (
    code TEXT NOT NULL, kirje_id INTEGER, isiku_tyyp TEXT, isiku_roll TEXT,
    eesnimi TEXT, nimi_arinimi TEXT, isikukood_hash BLOB, isikukood_registrikood TEXT,
    valis_kood TEXT, valis_kood_riik TEXT, synniaeg TEXT, aadress_riik TEXT,
    osaluse_protsent TEXT, osaluse_suurus TEXT, osaluse_valuuta TEXT,
    osaluse_omandiliik TEXT, osaluse_murdosa_lugeja TEXT, osaluse_murdosa_nimetaja TEXT,
    kontrolli_allikas TEXT, kontrolli_allika_kpv TEXT, algus_kpv TEXT, lopp_kpv TEXT,
    grupp INTEGER
);
CREATE INDEX off_card_person_code ON off_card_person (code);
CREATE INDEX off_card_person_holder ON off_card_person (isikukood_registrikood) WHERE isikukood_registrikood IS NOT NULL;
-- A natural person's other companies: the register's pseudonymous key is the
-- same UUID for the same person on every list of every company, so these four
-- indexes answer "where else is this person" (companies_for_hash). An index
-- built before them still answers, by scan, until the daily rebuild.
-- Every entry on the company's registry cards (registrikaardid), one row per
-- company, packed as "card type,card no,entry no,ISO day,entry type" joined
-- by ";" in the register's order — 2.2 million entries, which as one row
-- each would be the biggest table in the file. The entry type's Estonian
-- wording is in vocab ('card_entry').
CREATE TABLE card_entry (code TEXT PRIMARY KEY, entries TEXT NOT NULL) WITHOUT ROWID;
-- The general data (üldandmed), one row per company: the fields most
-- companies carry as columns, the rare lists (succession, status changes,
-- suspensions, a foreign branch's parent, predecessor references) as one
-- JSON object in ``extra``. The main activity's wording is in vocab
-- ('emtak'). Contact details, real estate and contact persons' addresses
-- are in the file and deliberately not here.
CREATE TABLE general (
    code TEXT PRIMARY KEY,
    capital TEXT, capital_currency TEXT, capital_since TEXT,
    bo_obliged INTEGER,             -- esitab_kasusaajad: obliged to file beneficial owners
    bo_absence_reported INTEGER,    -- lahknevusteade_puudumisest: a report that none were filed
    activity TEXT, activity_nace TEXT,
    last_report_end TEXT, last_report_employees TEXT, reports_filed INTEGER,
    extra TEXT
) WITHOUT ROWID;
-- The compliance rulings in force (määrused, the rulings phase), one JSON
-- array per company: [{type, day, number, deadline}], newest first. The
-- type's Estonian wording is in vocab ('ruling').
CREATE TABLE ruling (code TEXT PRIMARY KEY, rulings TEXT NOT NULL) WITHOUT ROWID;
-- Commercial pledges (kommertspandid), one JSON array per company:
-- [{number, rank, amount, currency, since, pledgees: [{kind, name, code, country}]}].
-- A natural-person pledgee is {kind: "F"} only.
CREATE TABLE pledge (code TEXT PRIMARY KEY, pledges TEXT NOT NULL) WITHOUT ROWID;
CREATE INDEX officer_person ON officer (isikukood_hash) WHERE isikukood_hash IS NOT NULL;
CREATE INDEX shareholder_person ON shareholder (isikukood_hash) WHERE isikukood_hash IS NOT NULL;
CREATE INDEX beneficial_owner_person ON beneficial_owner (isikukood_hash) WHERE isikukood_hash IS NOT NULL;
CREATE INDEX off_card_person_person ON off_card_person (isikukood_hash) WHERE isikukood_hash IS NOT NULL;
"""

class BuildRefused(RuntimeError):
    """The input is not what the index may be built from."""


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


class _CutShort(Exception):
    """A download that ended early — retried."""


def download(name: str, dest: Path, *, attempts: int = 4) -> str | None:
    """Fetch one open-data zip to ``dest``; return its ``Last-Modified``.

    Cloudflare answers HTTP 429 with a 17-byte ``Error code: 1015`` body to
    requests that follow one another too closely, and a page rather than the
    file for a wrong name. Both are refused: the body must be a zip.
    """
    url = BASE_URL + name
    last: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=600) as resp:
                ctype = resp.headers.get("Content-Type", "")
                if not ctype.lower().startswith("application/zip"):
                    raise BuildRefused(
                        f"{name}: expected application/zip, got {ctype!r} — the "
                        "site answered with a page, not the file"
                    )
                tmp = dest.with_suffix(dest.suffix + ".part")
                with open(tmp, "wb") as fh:
                    shutil.copyfileobj(resp, fh, length=1 << 20)
                with open(tmp, "rb") as fh:
                    magic = fh.read(2)
                if magic != b"PK":
                    raise BuildRefused(f"{name}: the body is not a zip file")
                # A transfer cut short still starts with "PK" (seen 16 Oct
                # 2026: 73,269,248 of 74,262,843 bytes of the rulings file).
                # It is retried, not refused: the next attempt is usually whole.
                expected = resp.headers.get("Content-Length")
                got = tmp.stat().st_size
                if expected and expected.isdigit() and int(expected) != got:
                    raise _CutShort(f"{got} of {expected} bytes")
                if not zipfile.is_zipfile(tmp):
                    raise _CutShort("no zip directory at the end")
                tmp.replace(dest)
                last_modified = resp.headers.get("Last-Modified")
                log.info("downloaded %s: %d bytes, Last-Modified %s", name, dest.stat().st_size, last_modified)
                return last_modified
        except BuildRefused:
            raise
        except urllib.error.HTTPError as exc:
            last = exc
            wait = 60 * attempt if exc.code == 429 else 10 * attempt
            log.warning("download %s: HTTP %s (attempt %d/%d); waiting %ds", name, exc.code, attempt, attempts, wait)
            if attempt < attempts:
                time.sleep(wait)
        except Exception as exc:  # noqa: BLE001 — retried, then reported
            last = exc
            log.warning("download %s failed (attempt %d/%d): %s", name, attempt, attempts, exc)
            if attempt < attempts:
                time.sleep(10 * attempt)
    raise BuildRefused(f"{name}: could not download ({last})")


def download_all(dest_dir: Path) -> dict[str, str | None]:
    """Download the nine files, spaced out, and record each ``Last-Modified``
    beside it (``<name>.last-modified``) so ``--from-dir`` can read the cut."""
    stamps: dict[str, str | None] = {}
    for i, name in enumerate(FILES):
        if i:
            time.sleep(DOWNLOAD_SPACING_S)
        stamps[name] = download(name, dest_dir / name)
        if stamps[name]:
            (dest_dir / (name + ".last-modified")).write_text(stamps[name] or "")
    return stamps


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


def _open_member(path: Path) -> tuple[zipfile.ZipFile, io.TextIOWrapper, dt.date | None]:
    """Open the single member of a zip as UTF-8 text; also its stamped day."""
    if not path.is_file():
        raise BuildRefused(f"{path}: no such file")
    try:
        zf = zipfile.ZipFile(path)
    except zipfile.BadZipFile as exc:
        raise BuildRefused(f"{path.name}: not a zip file ({exc})") from exc
    members = [m for m in zf.infolist() if not m.is_dir()]
    if len(members) != 1:
        raise BuildRefused(f"{path.name}: expected one member, found {len(members)}")
    member = members[0]
    member_day = dt.date(*member.date_time[:3]) if member.date_time[0] > 1980 else None
    raw = zf.open(member, "r")
    return zf, io.TextIOWrapper(raw, encoding="utf-8-sig", newline=""), member_day


def read_csv_rows(path: Path, *, min_rows: int) -> list[dict[str, str]]:
    """The entity CSV as dicts, after checking its shape (header, records ==
    physical lines − 1, floor)."""
    zf, text, _ = _open_member(path)
    with zf:
        data = text.read()
    physical = data.count("\n") + (0 if data.endswith("\n") or not data else 1)
    reader = csv.reader(io.StringIO(data, newline=""), delimiter=";", quotechar='"')
    first = next(reader, None)
    if first != CSV_HEADER:
        raise BuildRefused(f"{path.name}: header is {first!r}, expected {CSV_HEADER!r}")
    rows = list(reader)
    if len(rows) != physical - 1:
        raise BuildRefused(
            f"{path.name}: {len(rows)} records from {physical - 1} lines — a "
            "quote is merging or splitting rows"
        )
    if len(rows) < min_rows:
        raise BuildRefused(f"{path.name}: {len(rows)} records, fewer than {min_rows}")
    width = len(CSV_HEADER)
    out: list[dict[str, str]] = []
    for number, row in enumerate(rows, start=2):
        if len(row) != width:
            raise BuildRefused(f"{path.name}: line {number} has {len(row)} fields, not {width}")
        out.append(dict(zip(CSV_HEADER, row)))
    return out


def stream_json_array(text: io.TextIOBase, *, chunk: int = 1 << 20) -> Iterator[Any]:
    """Yield the elements of a JSON array from a text stream, one at a time,
    never holding more than a chunk or so of it (``raw_decode`` over a rolling
    buffer). Anything that is not a well-formed array is refused."""
    dec = json.JSONDecoder()
    skip = re.compile(r"[\s,]*")
    buf = text.read(chunk)
    stripped = buf.lstrip()
    if not stripped.startswith("["):
        raise BuildRefused("JSON file does not start with an array")
    buf = stripped[1:]
    pos = 0  # a cursor, not a slice: slicing a 1 MB buffer per object is O(n²)
    while True:
        pos = skip.match(buf, pos).end()
        if pos >= len(buf):
            more = text.read(chunk)
            if not more:
                raise BuildRefused("JSON array ended without a closing bracket")
            buf, pos = more, 0
            continue
        if buf[pos] == "]":
            return
        try:
            obj, end = dec.raw_decode(buf, pos)
        except json.JSONDecodeError:
            more = text.read(chunk)
            if not more:
                raise BuildRefused("JSON array is truncated or malformed") from None
            buf = buf[pos:] + more
            pos = 0
            continue
        yield obj
        pos = end
        if len(buf) - pos < chunk // 8:
            buf = buf[pos:] + text.read(chunk)
            pos = 0


def _iso_day(value: Any) -> str | None:
    m = _EE_DATE_RE.match(str(value or "").strip())
    if not m:
        return None
    d, mo, y = m.groups()
    return f"{y}-{mo}-{d}"


def _hash_bytes(value: Any, where: str) -> bytes | None:
    """``isikukood_hash`` as the UUID's 16 bytes; a value that is not a UUID
    is refused rather than stored as something else."""
    if value in (None, ""):
        return None
    try:
        return uuid.UUID(str(value)).bytes
    except ValueError:
        raise BuildRefused(f"{where}: isikukood_hash {value!r} is not a UUID") from None


def row_values(table: str, row: dict[str, Any], where: str) -> tuple[Any, ...]:
    """One people row as the tuple ``TABLE_COLUMNS[table]`` orders, with
    empties as NULL, dates as ISO days, the hash as bytes and the discrepancy
    flag as 0/1."""
    out: list[Any] = []
    for column in TABLE_COLUMNS[table]:
        v = row.get(column)
        if v is None or v == "":
            out.append(None)
        elif column == "isikukood_hash":
            out.append(_hash_bytes(v, where))
        elif column in DATE_COLUMNS:
            day = _iso_day(v)
            if day is None:
                raise BuildRefused(f"{where}: {column} {v!r} is not DD.MM.YYYY")
            out.append(day)
        elif column == "lahknevusteade_esitatud":
            out.append(1 if v is True else 0)
        elif isinstance(v, bool):
            out.append(int(v))
        elif isinstance(v, (int, float)):
            out.append(v if column in ("kirje_id", "grupp") else str(v))
        else:
            out.append(" ".join(str(v).split()) or None)
    return tuple(out)


# ---------------------------------------------------------------------------
# Cut date
# ---------------------------------------------------------------------------


def _day_of(last_modified: str | None) -> dt.date | None:
    if not last_modified:
        return None
    try:
        parsed = email.utils.parsedate_to_datetime(last_modified)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc).date()


def resolve_cut(
    src: Path, stamps: dict[str, str | None], member_days: dict[str, dt.date | None],
    override: str | None,
) -> str:
    """One cut day for the whole build, or a refusal.

    ``--cut`` wins; else each file's ``Last-Modified`` (from the download,
    or the ``.last-modified`` sidecar a download left); else the zip
    member's own stamp. The nine days must agree — the files are regenerated
    together each morning, and a download that straddled a regeneration
    would otherwise pair yesterday's shareholders with today's companies.
    """
    if override:
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", override):
            raise BuildRefused(f"--cut {override!r} is not YYYY-MM-DD")
        return override
    days: dict[str, dt.date | None] = {}
    for name in FILES:
        stamp = stamps.get(name)
        if stamp is None:
            sidecar = src / (name + ".last-modified")
            stamp = sidecar.read_text().strip() if sidecar.is_file() else None
        days[name] = _day_of(stamp) or member_days.get(name)
    missing = [n for n, d in days.items() if d is None]
    if missing:
        raise BuildRefused(f"no cut date for {', '.join(missing)} (pass --cut YYYY-MM-DD)")
    distinct = sorted({d.isoformat() for d in days.values() if d})
    if len(distinct) != 1:
        raise BuildRefused(f"the files carry different cut days: {distinct}")
    return distinct[0]


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def _stamp(moment: dt.datetime) -> str:
    """``meta.built_at``: when OpenCheck read the register, to the second, in
    UTC — it becomes ``source.retrievedAt`` (Phase 318)."""
    return moment.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat()


def build_index(
    src: Path,
    out_path: Path,
    *,
    stamps: dict[str, str | None] | None = None,
    cut_override: str | None = None,
    min_rows: dict[str, int] | None = None,
    started: dt.datetime | None = None,
) -> dict[str, str]:
    """Build the SQLite index at ``out_path`` from the nine zips in ``src``.

    Written to a temporary file beside ``out_path`` and renamed at the end,
    so a failed build never leaves a half-written index where one was.
    Returns the ``meta`` table.
    """
    floors = MIN_ROWS if min_rows is None else min_rows
    started = started or dt.datetime.now(dt.timezone.utc)
    stamps = dict(stamps or {})
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_name(out_path.name + ".building")
    if tmp.exists():
        tmp.unlink()

    member_days: dict[str, dt.date | None] = {}
    for name in FILES:
        zf, _text, day = _open_member(src / name)
        zf.close()
        member_days[name] = day
    cut = resolve_cut(src, stamps, member_days, cut_override)

    conn = sqlite3.connect(tmp)
    conn.executescript("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;" + _SCHEMA)
    vocab: dict[tuple[str, str], str] = {}
    counts: dict[str, int] = {"entities": 0, "vat": 0}
    seen: set[str] = set()

    try:
        for row in read_csv_rows(src / CSV_FILE, min_rows=floors.get(CSV_FILE, 0)):
            code = row["ariregistri_kood"].strip()
            if not _CODE_RE.match(code):
                raise BuildRefused(f"{CSV_FILE}: registry code {code!r} is not eight digits")
            if code in seen:
                raise BuildRefused(f"{CSV_FILE}: code {code} appears twice")
            seen.add(code)
            name = " ".join(row["nimi"].split())
            if not name:
                raise BuildRefused(f"{CSV_FILE}: code {code} has no name")
            status_code = row["ettevotja_staatus"].strip() or None
            status_text = " ".join(row["ettevotja_staatus_tekstina"].split()) or None
            if status_code:
                vocab.setdefault(("status", status_code), status_text or "")
            form = " ".join(row["ettevotja_oiguslik_vorm"].split()) or None
            address = (
                " ".join(row["ads_normaliseeritud_taisaadress"].split())
                or " ".join(
                    p for p in (
                        row["ettevotja_aadress"], row["asukoht_ettevotja_aadressis"],
                        row["asukoha_ehak_tekstina"], row["indeks_ettevotja_aadressis"],
                    ) if p.strip()
                )
                or None
            )
            vat = row["kmkr_nr"].strip() or None
            counts["vat"] += vat is not None
            conn.execute(
                "INSERT INTO entity (code, name, name_key, legal_form, legal_form_subtype,"
                " vat_number, status_code, first_entry_on, address)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    code, name, name.casefold(), form,
                    " ".join(row["ettevotja_oigusliku_vormi_alaliik"].split()) or None,
                    vat, status_code, _iso_day(row["ettevotja_esmakande_kpv"]), address,
                ),
            )
            counts["entities"] += 1

        for name, (list_key, table) in PEOPLE_FILES.items():
            columns = TABLE_COLUMNS[table]
            insert = (
                f"INSERT INTO {table} (code, {', '.join(columns)}) "
                f"VALUES (?, {', '.join('?' for _ in columns)})"
            )
            zf, text, _day = _open_member(src / name)
            entities = 0
            rows = 0
            file_seen: set[str] = set()
            batch: list[tuple[Any, ...]] = []
            with zf:
                try:
                    for ent in stream_json_array(text):
                        if not isinstance(ent, dict) or "ariregistri_kood" not in ent:
                            raise BuildRefused("an element is not an entity object")
                        code = str(ent["ariregistri_kood"]).strip().zfill(8)
                        if code not in seen:
                            raise BuildRefused(f"code {code} is not in {CSV_FILE}")
                        if code in file_seen:
                            raise BuildRefused(f"code {code} appears twice")
                        file_seen.add(code)
                        entities += 1
                        people = ent.get(list_key) or []
                        if not isinstance(people, list):
                            raise BuildRefused(f"{list_key} of {code} is not a list")
                        for p in people:
                            if not isinstance(p, dict):
                                raise BuildRefused(f"a {list_key} row of {code} is not an object")
                            _collect_vocab(vocab, table, p)
                            batch.append((code, *row_values(table, p, f"{code}/{list_key}")))
                            rows += 1
                        if len(batch) >= 5000:
                            conn.executemany(insert, batch)
                            batch = []
                except BuildRefused as exc:
                    raise BuildRefused(f"{name}: {exc}") from None
            if batch:
                conn.executemany(insert, batch)
            if rows < floors.get(name, 0):
                raise BuildRefused(f"{name}: {rows} rows, fewer than {floors.get(name, 0)}")
            counts[table] = rows
            log.info("%s: %d entities, %d rows", name, entities, rows)

        counts["card_entries"] = _load_card_entries(
            conn, src / CARDS_FILE, seen, vocab, floor=floors.get(CARDS_FILE, 0),
        )

        counts["general"] = _load_general(
            conn, src / GENERAL_FILE, seen, vocab, floor=floors.get(GENERAL_FILE, 0),
        )
        counts["rulings"], counts["ruled_companies"] = _load_rulings(
            conn, src / RULINGS_FILE, seen, vocab, floor=floors.get(RULINGS_FILE, 0),
        )
        counts["pledges"], counts["pledged_companies"] = _load_pledges(
            conn, src / PLEDGES_FILE, seen, floor=floors.get(PLEDGES_FILE, 0),
        )
        # The companies obliged to file beneficial owners that have none on
        # file — counted here, said per company in the finding.
        counts["bo_obliged_none_filed"] = conn.execute(
            "SELECT COUNT(*) FROM general g WHERE g.bo_obliged = 1 AND NOT EXISTS"
            " (SELECT 1 FROM beneficial_owner b WHERE b.code = g.code)"
        ).fetchone()[0]

        conn.executemany(
            "INSERT INTO vocab VALUES (?, ?, ?)",
            sorted((k[0], k[1], v) for k, v in vocab.items()),
        )
        meta = {
            "schema_version": SCHEMA_VERSION,
            "cut": cut,
            "built_at": _stamp(started),
            "entities": str(counts["entities"]),
            "with_vat_number": str(counts["vat"]),
            "officers": str(counts.get("officer", 0)),
            "shareholders": str(counts.get("shareholder", 0)),
            "beneficial_owners": str(counts.get("beneficial_owner", 0)),
            "off_card_persons": str(counts.get("off_card_person", 0)),
            "card_entries": str(counts.get("card_entries", 0)),
            "general": str(counts.get("general", 0)),
            "bo_obliged_none_filed": str(counts.get("bo_obliged_none_filed", 0)),
            "rulings": str(counts.get("rulings", 0)),
            "ruled_companies": str(counts.get("ruled_companies", 0)),
            "pledges": str(counts.get("pledges", 0)),
            "pledged_companies": str(counts.get("pledged_companies", 0)),
            "source_page": PAGE_URL,
            "source_files": ",".join(FILES),
            "license": LICENSE,
        }
        conn.executemany("INSERT INTO meta VALUES (?, ?)", sorted(meta.items()))
        conn.commit()
        conn.execute("VACUUM")
    except BaseException:
        conn.close()
        if tmp.exists():
            tmp.unlink()
        raise
    conn.close()
    os.replace(tmp, out_path)
    log.info(
        "built %s: %s entities, %s officers, %s shareholders, %s beneficial owners,"
        " %s persons off the card, cut %s",
        out_path, meta["entities"], meta["officers"], meta["shareholders"],
        meta["beneficial_owners"], meta["off_card_persons"], cut,
    )
    return meta


#: A card entry's fields, packed in this order (``ariregister.card_entries``
#: reads them back in the same order).
CARD_ENTRY_FIELDS = ("card_type", "card_no", "entry_no", "day", "entry_type")


def _load_card_entries(
    conn: sqlite3.Connection, path: Path, seen: set[str],
    vocab: dict[tuple[str, str], str], *, floor: int,
) -> int:
    """The registry cards file into ``card_entry``, one packed row per
    company. Refuses a company the CSV lacks, a repeated company, an entry
    without a number, an undated entry and a field holding the separators."""
    zf, text, _day = _open_member(path)
    total = 0
    batch: list[tuple[str, str]] = []
    file_seen: set[str] = set()
    with zf:
        try:
            for ent in stream_json_array(text):
                if not isinstance(ent, dict) or "ariregistri_kood" not in ent:
                    raise BuildRefused("an element is not an entity object")
                code = str(ent["ariregistri_kood"]).strip().zfill(8)
                if code not in seen:
                    raise BuildRefused(f"code {code} is not in {CSV_FILE}")
                if code in file_seen:
                    raise BuildRefused(f"code {code} appears twice")
                file_seen.add(code)
                packed: list[str] = []
                for card in ent.get("registrikaardid") or []:
                    if not isinstance(card, dict):
                        raise BuildRefused(f"a card of {code} is not an object")
                    card_type = str(card.get("kaardi_tyyp") or "").strip()
                    card_no = str(card.get("kaardi_nr") or "").strip()
                    for entry in card.get("kanded") or []:
                        if not isinstance(entry, dict):
                            raise BuildRefused(f"an entry of {code} is not an object")
                        number = str(entry.get("kande_nr") or "").strip()
                        day = _iso_day(entry.get("kpv"))
                        kind = str(entry.get("kandeliik") or "").strip()
                        if not number.isdigit() or day is None:
                            raise BuildRefused(f"an entry of {code} has no number or date")
                        fields = (card_type, card_no, number, day, kind)
                        if any("," in f or ";" in f for f in fields):
                            raise BuildRefused(f"an entry of {code} holds a separator")
                        packed.append(",".join(fields))
                        if kind:
                            text_ = " ".join(str(entry.get("kandeliik_tekstina") or "").split())
                            if text_ or ("card_entry", kind) not in vocab:
                                vocab[("card_entry", kind)] = text_
                if packed:
                    batch.append((code, ";".join(packed)))
                    total += len(packed)
                if len(batch) >= 5000:
                    conn.executemany("INSERT INTO card_entry VALUES (?, ?)", batch)
                    batch = []
        except BuildRefused as exc:
            raise BuildRefused(f"{path.name}: {exc}") from None
    if batch:
        conn.executemany("INSERT INTO card_entry VALUES (?, ?)", batch)
    if total < floor:
        raise BuildRefused(f"{path.name}: {total} entries, fewer than {floor}")
    log.info("%s: %d entities, %d entries", path.name, len(file_seen), total)
    return total


#: A registry code named in a succession entry's text: after "registrikood"
#: (and its inflections) or "reg. nr" — 76% of divisions and 97% of mergers
#: on 16 Oct 2026. Any other eight-digit number in the text is taken only
#: when it is a company in the index.
_SUCCESSION_CODE = re.compile(r"(?:registrikood\w*|reg\.?\s*(?:nr|kood)\.?)\D{0,6}?(\d{8})(?!\d)", re.IGNORECASE)
_ANY_CODE = re.compile(r"(?<!\d)(\d{8})(?!\d)")


#: An eleven-digit number that could be an Estonian personal identification
#: code (first digit 1–6). The general data's free texts name people now and
#: then — a bankruptcy ruling against a sole trader names the person "and
#: personal code …" — so any such number is removed from a kept text, unless
#: the few characters before it call it a registry code (a Latvian company
#: is eleven digits too, and a merger names it so).
_PERSONAL_CODE = re.compile(r"(?<!\d)[1-6]\d{10}(?!\d)")
_REGISTRY_WORD = re.compile(r"(registrikood\w*|reg\.?\s*(nr|kood)\.?|registreerimisnumb\w*)\W{0,6}$", re.IGNORECASE)
PERSONAL_CODE_REMOVED = "[personal code removed]"


def scrub_text(text: Any) -> str:
    """A register text with whitespace collapsed and every number shaped
    like a personal identification code removed (``_PERSONAL_CODE``)."""
    text = " ".join(str(text or "").split())

    def keep(m: re.Match[str]) -> str:
        before = text[max(0, m.start() - 24):m.start()]
        return m.group(0) if _REGISTRY_WORD.search(before) else PERSONAL_CODE_REMOVED

    return _PERSONAL_CODE.sub(keep, text)


def _current(rows: Any) -> list[dict[str, Any]]:
    """A list's rows that are dicts and carry no end date (the file holds
    current rows only, but a stray ended row must not be read as current)."""
    return [r for r in rows or [] if isinstance(r, dict) and not r.get("lopp_kpv")]


def _general_values(
    code: str, y: dict[str, Any], vocab: dict[tuple[str, str], str], known: set[str],
) -> tuple[Any, ...]:
    """One ``general`` row from an entity's ``yldandmed`` object."""
    capital = sorted(_current(y.get("kapitalid")), key=lambda r: _iso_day(r.get("algus_kpv")) or "")
    cap = capital[-1] if capital else {}
    activities = _current(y.get("teatatud_tegevusalad"))
    main = next((a for a in activities if a.get("on_pohitegevusala")), activities[0] if activities else {})
    emtak = str(main.get("emtak_kood") or "").strip() or None
    if emtak:
        text = " ".join(str(main.get("emtak_tekstina") or "").split())
        if text or ("emtak", emtak) not in vocab:
            vocab[("emtak", emtak)] = text
    reports = [r for r in y.get("info_majandusaasta_aruannetest") or [] if isinstance(r, dict)]
    last = max(reports, key=lambda r: _iso_day(r.get("majandusaasta_perioodi_lopp_kpv")) or "", default={})
    absence = y.get("lahknevusteade_puudumisest")

    extra: dict[str, Any] = {}
    succession = []
    for r in _current(y.get("oigusjargsused")):
        text = scrub_text(r.get("sisu"))
        codes = [c for c in _SUCCESSION_CODE.findall(text) if c != code]
        codes += [c for c in _ANY_CODE.findall(text) if c != code and c in known and c not in codes]
        succession.append({
            "type": r.get("tyyp"), "day": _iso_day(r.get("algus_kpv")), "entry_no": r.get("kande_nr"),
            "text": text, "codes": list(dict.fromkeys(codes)),
        })
    if succession:
        extra["succession"] = succession
    changes = [
        {"type": r.get("tyyp"), "type_text": r.get("tyyp_tekstina"), "day": _iso_day(r.get("algus_kpv")),
         "entry_no": r.get("kande_nr"), "text": scrub_text(r.get("sisu"))}
        for r in _current(y.get("seisundi_muutused"))
    ]
    if changes:
        extra["status_changes"] = changes
    periods = [
        {"type": r.get("tyyp"), "type_text": r.get("tyyp_tekstina"),
         "start": r.get("periood_algus_kpv"), "end": r.get("periood_lopp_kpv"),
         "recurring": r.get("periood") == "K", "day": _iso_day(r.get("algus_kpv")),
         "text": scrub_text(r.get("sisu"))}
        for r in _current(y.get("ettevotja_tegevuse_aeg"))
    ]
    if periods:
        extra["activity_periods"] = periods
    for branch in y.get("valismaa_ariyhing") or []:
        if not isinstance(branch, dict):
            continue
        def first(key: str, field: str = "sisu") -> Any:
            rows = _current(branch.get(key))
            return rows[0].get(field) if rows else None
        seat = _current(branch.get("asukoht"))
        extra["branch_of"] = {
            "name": first("arinimi"),
            "country": seat[0].get("aadress_riik") if seat else None,
            "country_text": seat[0].get("aadress_riik_tekstina") if seat else None,
            "register": first("register"),
            "number": first("registreerimisnumber"),
            "form": first("oiguslik_vorm"),
            "like_estonian_form": first("sarnane_eesti_oigvorm", "sisu_tekstina"),
        }
        break
    predecessors = [
        scrub_text(r.get("sisu"))
        for r in _current(y.get("markused_kaardil")) if r.get("tyyp") == "EVREG"
    ]
    if predecessors:
        extra["predecessor_notes"] = predecessors
    no_par = _current(y.get("nimivaartuseta_aktsiad"))
    if no_par:
        extra["no_par_shares"] = no_par[0].get("nimivaartuseta_aktsiate_arv")

    return (
        code,
        str(cap.get("kapitali_suurus") or "").strip() or None,
        str(cap.get("kapitali_valuuta") or "").strip() or None,
        _iso_day(cap.get("algus_kpv")),
        1 if y.get("esitab_kasusaajad") else 0,
        None if absence is None else (1 if absence else 0),
        emtak,
        str(main.get("nace_kood") or "").strip() or None,
        _iso_day(last.get("majandusaasta_perioodi_lopp_kpv")),
        str(last.get("tootajate_arv") or "").strip() or None,
        len(reports),
        json.dumps(extra, ensure_ascii=False, separators=(",", ":")) if extra else None,
    )


def _load_general(
    conn: sqlite3.Connection, path: Path, seen: set[str],
    vocab: dict[tuple[str, str], str], *, floor: int,
) -> int:
    """The general-data file into ``general``. Its entity set must be the
    CSV's, as the people files' must."""
    zf, text, _day = _open_member(path)
    file_seen: set[str] = set()
    batch: list[tuple[Any, ...]] = []
    insert = "INSERT INTO general VALUES (" + ", ".join("?" for _ in range(12)) + ")"
    with zf:
        try:
            for ent in stream_json_array(text):
                if not isinstance(ent, dict) or "ariregistri_kood" not in ent:
                    raise BuildRefused("an element is not an entity object")
                code = str(ent["ariregistri_kood"]).strip().zfill(8)
                if code not in seen:
                    raise BuildRefused(f"code {code} is not in {CSV_FILE}")
                if code in file_seen:
                    raise BuildRefused(f"code {code} appears twice")
                file_seen.add(code)
                y = ent.get("yldandmed")
                if not isinstance(y, dict):
                    raise BuildRefused(f"yldandmed of {code} is not an object")
                batch.append(_general_values(code, y, vocab, seen))
                if len(batch) >= 5000:
                    conn.executemany(insert, batch)
                    batch = []
        except BuildRefused as exc:
            raise BuildRefused(f"{path.name}: {exc}") from None
    if batch:
        conn.executemany(insert, batch)
    if len(file_seen) < floor:
        raise BuildRefused(f"{path.name}: {len(file_seen)} entities, fewer than {floor}")
    log.info("%s: %d entities", path.name, len(file_seen))
    return len(file_seen)


def _entities(
    path: Path, seen: set[str], list_key: str,
) -> Iterator[tuple[str, list[Any]]]:
    """``(code, list)`` for each entity object in a JSON file, refusing an
    element that is not one, a code the CSV lacks, a repeated code and a list
    that is not a list."""
    zf, text, _day = _open_member(path)
    file_seen: set[str] = set()
    with zf:
        try:
            for ent in stream_json_array(text):
                if not isinstance(ent, dict) or "ariregistri_kood" not in ent:
                    raise BuildRefused("an element is not an entity object")
                code = str(ent["ariregistri_kood"]).strip().zfill(8)
                if code not in seen:
                    raise BuildRefused(f"code {code} is not in {CSV_FILE}")
                if code in file_seen:
                    raise BuildRefused(f"code {code} appears twice")
                file_seen.add(code)
                rows = ent.get(list_key) or []
                if not isinstance(rows, list):
                    raise BuildRefused(f"{list_key} of {code} is not a list")
                yield code, rows
        except BuildRefused as exc:
            raise BuildRefused(f"{path.name}: {exc}") from None


def _load_rulings(
    conn: sqlite3.Connection, path: Path, seen: set[str],
    vocab: dict[tuple[str, str], str], *, floor: int,
) -> tuple[int, int]:
    """The rulings file into ``ruling``: the ``RULING_TYPES`` in force, one
    JSON array per company, newest first. Returns (rulings, companies). The
    floor is on the companies the file lists — almost every company has an
    entry ruling, so a short file is a cut-short file."""
    total = companies = listed = 0
    batch: list[tuple[str, str]] = []
    for code, rows in _entities(path, seen, "maarused"):
        listed += 1
        kept: list[dict[str, Any]] = []
        for r in rows:
            if not isinstance(r, dict):
                raise BuildRefused(f"{path.name}: a ruling of {code} is not an object")
            kind = str(r.get("maaruse_liik") or "").strip()
            if kind not in RULING_TYPES or str(r.get("maaruse_olek") or "").strip() != RULING_IN_FORCE:
                continue
            day = _iso_day(r.get("maaruse_kpv"))
            if day is None:
                raise BuildRefused(f"{path.name}: a ruling of {code} is undated")
            text_ = " ".join(str(r.get("maaruse_liik_tekstina") or "").split())
            if text_ or ("ruling", kind) not in vocab:
                vocab[("ruling", kind)] = text_
            kept.append({
                "type": kind, "day": day,
                "number": " ".join(str(r.get("maaruse_nr") or "").split()) or None,
                "deadline": _iso_day(r.get("lisatahtaeg")),
            })
        if kept:
            kept.sort(key=lambda k: (k["day"], k["number"] or ""), reverse=True)
            batch.append((code, json.dumps(kept, ensure_ascii=False, separators=(",", ":"))))
            total += len(kept)
            companies += 1
        if len(batch) >= 5000:
            conn.executemany("INSERT INTO ruling VALUES (?, ?)", batch)
            batch = []
    if batch:
        conn.executemany("INSERT INTO ruling VALUES (?, ?)", batch)
    if listed < floor:
        raise BuildRefused(f"{path.name}: {listed} entities, fewer than {floor}")
    log.info("%s: %d entities, %d compliance rulings in force on %d", path.name, listed, total, companies)
    return total, companies


def _pledgee(h: dict[str, Any]) -> dict[str, Any]:
    """A pledgee as the index keeps it: a legal person by name, registry
    code (eight digits, Estonian) and country; a natural person as its kind
    alone — the file carries the person's code, which never leaves here."""
    kind = str(h.get("pidaja_isikuliik") or "").strip()
    if kind != "J":
        return {"kind": "F"}
    country = str(h.get("pidaja_riik") or "").strip() or None
    code = str(h.get("pidaja_kood") or "").strip()
    out: dict[str, Any] = {"kind": "J", "name": " ".join(str(h.get("pidaja_nimi") or "").split()) or None}
    if _CODE_RE.match(code) and country in (None, "EST"):
        out["code"] = code
    if country and country != "EST":
        out["country"] = country
        out["country_text"] = h.get("pidaja_riik_tekstina")
    return out


def _load_pledges(
    conn: sqlite3.Connection, path: Path, seen: set[str], *, floor: int,
) -> tuple[int, int]:
    """The commercial pledges file into ``pledge``: the current pledges of
    each company (status ``R``, no end date). Returns (pledges, companies)."""
    total = 0
    listed = 0
    batch: list[tuple[str, str]] = []
    for code, rows in _entities(path, seen, "kommertspandiandmed"):
        listed += 1
        kept: list[dict[str, Any]] = []
        for p in rows:
            if not isinstance(p, dict):
                raise BuildRefused(f"{path.name}: a pledge of {code} is not an object")
            if p.get("lopp_kpv") or str(p.get("pandi_olek") or "R").strip() != "R":
                continue
            pledgees = [_pledgee(h) for h in _current(p.get("pandipidajad"))]
            kept.append({
                "number": p.get("pandi_number"),
                "rank": p.get("pandi_jarjekoht"),
                "amount": str(p.get("pandi_summa") or "").strip() or None,
                "currency": str(p.get("pandi_valuuta") or "").strip() or None,
                "since": _iso_day(p.get("algus_kpv")),
                "pledgees": pledgees,
            })
        if kept:
            kept.sort(key=lambda k: (k["rank"] or 0, k["since"] or ""))
            batch.append((code, json.dumps(kept, ensure_ascii=False, separators=(",", ":"))))
            total += len(kept)
    if batch:
        conn.executemany("INSERT INTO pledge VALUES (?, ?)", batch)
    if listed < floor:
        raise BuildRefused(f"{path.name}: {listed} entities, fewer than {floor}")
    log.info("%s: %d pledged companies, %d current pledges", path.name, listed, total)
    return total, len(batch)


def _collect_vocab(vocab: dict[tuple[str, str], str], table: str, p: dict[str, Any]) -> None:
    """Record every code → text pair the files use, once."""
    for kind, code_key, text_key in VOCAB_PAIRS[table]:
        code = p.get(code_key)
        if code in (None, ""):
            continue
        text = " ".join(str(p.get(text_key) or "").split())
        key = (kind, str(code))
        if text or key not in vocab:
            vocab[key] = text


def gzip_file(path: Path) -> Path:
    out = path.with_name(path.name + ".gz")
    with open(path, "rb") as src, gzip.open(out, "wb", compresslevel=9) as dst:
        shutil.copyfileobj(src, dst, length=1 << 20)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out", required=True, type=Path, help="the SQLite file to write")
    parser.add_argument(
        "--from-dir", type=Path,
        help="build from the nine zips already in this directory instead of downloading",
    )
    parser.add_argument(
        "--keep-dir", type=Path,
        help="download into this directory and keep the zips (default: a temp dir)",
    )
    parser.add_argument("--cut", help="the files' cut day, YYYY-MM-DD (overrides Last-Modified)")
    parser.add_argument("--gzip", action="store_true", help="also write <out>.gz")
    parser.add_argument(
        "--min-rows", type=int, default=None,
        help="override every row floor (fixtures only)",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    started = dt.datetime.now(dt.timezone.utc)
    floors = None if args.min_rows is None else {name: args.min_rows for name in FILES}
    scratch: tempfile.TemporaryDirectory | None = None
    stamps: dict[str, str | None] = {}
    try:
        if args.from_dir:
            src = args.from_dir
        else:
            if args.keep_dir:
                src = args.keep_dir
                src.mkdir(parents=True, exist_ok=True)
            else:
                scratch = tempfile.TemporaryDirectory(prefix="ariregister-")
                src = Path(scratch.name)
            stamps = download_all(src)
        meta = build_index(
            src, args.out, stamps=stamps, cut_override=args.cut,
            min_rows=floors, started=started,
        )
    except BuildRefused as exc:
        log.error("refused: %s", exc)
        return 2
    finally:
        if scratch is not None:
            scratch.cleanup()
    if args.gzip:
        gz = gzip_file(args.out)
        log.info("gzipped: %s (%d bytes)", gz, gz.stat().st_size)
    print(" ".join(f"{k}={v}" for k, v in meta.items() if k not in {"source_page", "source_files"}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
