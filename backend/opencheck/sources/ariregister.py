"""Estonian e-Business Register (e-Äriregister) adapter — index first, page second.

Two tiers, one source id (Phase 330, Stephen's decision of 13 Oct 2026):

1. **The index** — a SQLite file packed daily by
   ``scripts/build_ariregister_index.py`` from RIK's open-data files
   (``avaandmed.ariregister.rik.ee``, CC BY 4.0): every company that is
   entered, in liquidation or in bankruptcy (≈380,000), with the persons on
   its registry card, its register of shareholders and its declared
   beneficial owners. Shipped as the ``ariregister-index`` release asset and
   downloaded at boot by :func:`warm_index`. It answers whenever it holds the
   code, as a ``snapshot`` dated by the files' cut.
2. **The printable page** — ``ariregister.rik.ee/eng/company/{code}/company_print_json``,
   a server-rendered HTML page with the same data, read without
   authentication. Used only when the index does not hold the code (a company
   newer than the cut, or one deleted from the register — the page can say
   "Deleted from the register"; the files hold no deleted companies) or when
   there is no index at all. RIK neither documents nor supports it, and it
   rate-limits datacentre callers (HTTP 429, Cloudflare ``Error code: 1015``).

Why the index leads: the page is not richer — it lists shareholders only for
an OÜ, exactly as the files do (Eesti Energia AS reads 0 shareholders on
either) — and it republishes 11-digit personal identification codes that the
open data dropped in November 2024 for a pseudonymous ``isikukood_hash``.
The page path now strips those codes from the bundle it returns; the hash
is a cross-role join key (director = shareholder = owner within a company)
and never an identifier (the Phase 255 rule moves it to an annotation).

Both tiers return the bundle shape ``map_ariregister`` has read since the
SOAP days: ``registry_code, name, legal_form, vat_number, status,
registration_date, address, link, shareholders, officers, beneficial_owners,
is_stub`` — plus ``cut`` / ``tier`` / ``index_miss`` and, for a note card,
``not_found`` / ``coverage_note``.

GLEIF RA code: RA000181. Nothing here touches the response
:class:`~opencheck.cache.Cache`: a miss on the index is an answer about one
build of it and must not outlive it (the Phase 325 rule).
"""

from __future__ import annotations

import asyncio
import logging
import json
import re
import sqlite3
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from .. import degradation, provenance
from ..config import get_settings
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.ariregister import AriregisterBundle

logger = logging.getLogger(__name__)

# GLEIF Registration Authority code for the Estonian e-Business Register.
EE_RA_CODE: str = "RA000181"

_BASE_URL = "https://ariregister.rik.ee"
_PRINT_URL = _BASE_URL + "/eng/company/{reg_code}/company_print_json"
_AUTO_URL  = _BASE_URL + "/eng/api/autocomplete"
_TIMEOUT   = 20.0

# --- RIK X-Road SOAP open-data API (Time Machine history only) --------------
# The public scraper above drives the live /lookup. For *historical* data the
# only machine-readable source is the credentialed SOAP service: detailandmed_v2
# returns the full registry-card history (ainult_kehtivad=0), tegelikudKasusaajad_v2
# the beneficial-owner history. Read-only, off the main lookup path. Credentials:
# ARIREGISTER_USERNAME / ARIREGISTER_PASSWORD (open-data API contract).
# NOTE: Estonia's switch to legitimate-interest BO access was postponed on its
# 2026-07-10 start date (no new date announced) — BO stays available until the
# revised framework lands. Issues #22/#28 track the eventual removal.
_SOAP_URL = "https://ariregxmlv6.rik.ee/"
_SOAP_NS = "http://arireg.x-road.eu/producer/"
_SOAP_TIMEOUT = 30.0
_SOAP_HEADERS = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": '""',
    "User-Agent": "OpenCheck/1.0 (https://opencheck.world; beneficialownership.co.uk)",
}


def _soap_escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _detail_envelope(user: str, pw: str, reg_code: str) -> str:
    """detailandmed_v2 with general + personnel data and history on."""
    return (
        f'<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
        f'xmlns:prod="{_SOAP_NS}"><soapenv:Body><prod:detailandmed_v2><prod:keha>'
        f"<prod:ariregister_kasutajanimi>{_soap_escape(user)}</prod:ariregister_kasutajanimi>"
        f"<prod:ariregister_parool>{_soap_escape(pw)}</prod:ariregister_parool>"
        f"<prod:ariregistri_kood>{_soap_escape(reg_code)}</prod:ariregistri_kood>"
        "<prod:yandmed>1</prod:yandmed><prod:iandmed>1</prod:iandmed>"
        "<prod:kandmed>0</prod:kandmed><prod:dandmed>0</prod:dandmed>"
        "<prod:maarused>0</prod:maarused><prod:ainult_kehtivad>0</prod:ainult_kehtivad>"
        "<prod:keel>eng</prod:keel>"
        "</prod:keha></prod:detailandmed_v2></soapenv:Body></soapenv:Envelope>"
    )


def _bo_envelope(user: str, pw: str, reg_code: str) -> str:
    """tegelikudKasusaajad_v2 with history on (ainult_kehtivad=0)."""
    return (
        f'<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
        f'xmlns:prod="{_SOAP_NS}"><soapenv:Body><prod:tegelikudKasusaajad_v2><prod:keha>'
        f"<prod:ariregister_kasutajanimi>{_soap_escape(user)}</prod:ariregister_kasutajanimi>"
        f"<prod:ariregister_parool>{_soap_escape(pw)}</prod:ariregister_parool>"
        f"<prod:ariregistri_kood>{_soap_escape(reg_code)}</prod:ariregistri_kood>"
        "<prod:ainult_kehtivad>0</prod:ainult_kehtivad><prod:keel>eng</prod:keel>"
        "</prod:keha></prod:tegelikudKasusaajad_v2></soapenv:Body></soapenv:Envelope>"
    )

_HEADERS = {
    "User-Agent": "OpenCheck/1.0 (https://opencheck.world; beneficialownership.co.uk)",
    "Accept": "text/html,application/json,*/*",
}

# Maps English role labels from the HTML page → internal Estonian role code
# used by the existing map_ariregister() mapper in bods/mapper.py.
_ROLE_LABEL_MAP: dict[str, str] = {
    "management board member":   "JUHL",
    "board member":              "JUHL",
    "procurist":                 "PROK",
    "liquidator (board member)": "LIKVJ",
    "liquidator":                "LIKV",
    "general partner":           "TOSAN",
    "limited partner":           "UOSAN",
    "authorised representative": "ASES",
    "legal representative":      "SJESI",
    "branch manager":            "VFILJ",
    "fund manager":              "FV",
}

# Maps English manner-of-control text → Estonian BO control code
# used by map_ariregister().
_BO_MANNER_MAP: dict[str, str] = {
    "direct ownership":        "O",
    "direct participation":    "O",
    "indirect ownership":      "K",
    "indirect participation":  "K",
    "through voting rights":   "H",
    "via voting rights":       "H",
    "voting rights":           "H",
}


# ---------------------------------------------------------------------------
# HTML parsing helpers
# ---------------------------------------------------------------------------

def _clean(html_fragment: str) -> str:
    """Strip HTML tags and collapse whitespace."""
    text = re.sub(r"<[^>]+>", " ", html_fragment)
    return re.sub(r"\s+", " ", text).strip()


def _parse_info(html: str) -> dict[str, str]:
    """Extract label → value pairs from Bootstrap row structure.

    The page uses:
        <div class="col-md-4 text-muted">Label</div>
        <div class="col font-weight-bold">Value … </div>
    """
    pattern = (
        r'class="col-md-4 text-muted"[^>]*>(.*?)</div>'
        r'\s*<div[^>]*class="col[^"]*">(.*?)</div>'
    )
    result: dict[str, str] = {}
    for label_html, val_html in re.findall(pattern, html, re.DOTALL):
        label = _clean(label_html)
        value = _clean(val_html)
        # Drop "Open map" suffix from address values
        value = re.sub(r"\s*Open map.*$", "", value, flags=re.DOTALL).strip()
        if label and value:
            result[label] = value
    return result


def _find_table(html: str, header_keywords: list[str]) -> list[list[str]]:
    """Find the first <table> whose header row contains all given keywords.

    Returns a list of data rows (each row is a list of cell text values).
    """
    for table_html in re.findall(r"<table[^>]*>(.*?)</table>", html, re.DOTALL):
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.DOTALL)
        if not rows:
            continue
        header_cells = [
            _clean(c)
            for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", rows[0], re.DOTALL)
        ]
        header_text = " | ".join(header_cells).lower()
        if all(kw.lower() in header_text for kw in header_keywords):
            data_rows: list[list[str]] = []
            for row in rows[1:]:
                cells = [
                    _clean(c)
                    for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.DOTALL)
                ]
                cells = [c for c in cells if c]  # drop blank cells
                if cells:
                    data_rows.append(cells)
            return data_rows
    return []


def _is_estonian_personal_code(code: str) -> bool:
    """Return True if code looks like an Estonian personal identification code.

    Estonian personal codes are exactly 11 digits and the first digit encodes
    gender + birth century (valid values: 1-6 for 20th/21st century).
    """
    return bool(re.fullmatch(r"[1-6]\d{10}", code))


def _is_registry_code(code: str) -> bool:
    """Return True if code looks like an Estonian commercial registry code.

    Estonian registry codes are 8 digits.
    """
    return bool(re.fullmatch(r"\d{8}", code))


def _parse_date(raw: str) -> str | None:
    """Convert DD.MM.YYYY date string to ISO YYYY-MM-DD, or return None."""
    m = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", raw.strip())
    if m:
        d, mo, y = m.groups()
        return f"{y}-{mo.zfill(2)}-{d.zfill(2)}"
    return None


def _parse_contribution(contribution_str: str) -> tuple[str | None, str | None]:
    """Extract (amount, currency) from strings like '834.00 EUR' or '1515363.00 EUR Sole ownership'."""
    m = re.match(r"([\d\s,\.]+)\s+([A-Z]{3})", contribution_str.strip())
    if m:
        amount = m.group(1).replace(" ", "").replace(",", "")
        currency = m.group(2)
        return amount, currency
    return None, None


def _split_name(full_name: str) -> tuple[str, str]:
    """Split 'Firstname Lastname' → (first, last). Last word is the surname."""
    parts = full_name.strip().split()
    if len(parts) >= 2:
        return " ".join(parts[:-1]), parts[-1]
    return "", full_name.strip()


# ---------------------------------------------------------------------------
# Page-level parsers
# ---------------------------------------------------------------------------

def _parse_officers(html: str) -> list[dict[str, Any]]:
    """Parse the officers/board members table.

    Expected headers: Name | Personal identification code | Role | Start - end
    """
    rows = _find_table(html, ["Name", "Personal identification code", "Role"])
    officers: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        if len(row) < 3:
            continue
        name = row[0]
        id_code = row[1] if len(row) > 1 else ""
        role_label = row[2].lower() if len(row) > 2 else ""
        start_raw = row[3] if len(row) > 3 else ""
        end_raw = row[4] if len(row) > 4 else ""

        role_code = _ROLE_LABEL_MAP.get(role_label)
        if not role_code:
            # Try partial match
            for label, code in _ROLE_LABEL_MAP.items():
                if label in role_label:
                    role_code = code
                    break
        if not role_code:
            logger.debug("ariregister: unknown officer role %r — skipping", row[2])
            continue

        # The code column says what kind of person holds the role: an
        # 11-digit personal code for a human, the 8-digit registry code for a
        # legal person — "2C Ventures Fund 1 GP OÜ (16769949), General
        # partner" of 2C Ventures Fund I usaldusfond, live 6 Oct 2026. The
        # same rule the shareholders table already uses. Anything else (a
        # foreign person's date of birth, a blank) stays a natural person:
        # the column does not say otherwise.
        # A natural person's 11-digit personal code is used only to tell a
        # person from a company and is then dropped (Phase 330): the open
        # data stopped publishing it in November 2024, and nothing downstream
        # reads it. The 8-digit registry code of a legal person is kept.
        id_code = id_code.strip()
        if _is_registry_code(id_code):
            isiku_tyyp, first, last = "J", "", name.strip()
            code_field = id_code
        else:
            isiku_tyyp = "F"
            first, last = _split_name(name)
            code_field = ""
        officers.append({
            "kirje_id": str(idx + 1),
            "eesnimi": first,
            "nimi_arinimi": last,
            "isiku_roll": role_code,
            "isiku_tyyp": isiku_tyyp,
            "isikukood_registrikood": code_field,
            "algus_kpv": _parse_date(start_raw) or start_raw or None,
            "lopp_kpv": _parse_date(end_raw) or end_raw or None,
            "synniaeg": None,
            "valis_kood_riik": None,
            "isikukood_hash": None,
        })
    return officers


def _parse_shareholders(html: str) -> list[dict[str, Any]]:
    """Parse the shareholders/members table.

    Expected headers: Participation | Contribution | Name | Code | Start - End
    """
    rows = _find_table(html, ["Participation", "Contribution", "Name", "Code"])
    shareholders: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        if len(row) < 4:
            continue
        pct_str = re.sub(r"[^\d\.]", "", row[0])  # strip "%" etc.
        contribution_str = row[1]
        name = row[2]
        code = row[3].strip()
        start_raw = row[4] if len(row) > 4 else ""
        end_raw = row[5] if len(row) > 5 else ""

        amount, currency = _parse_contribution(contribution_str)

        # Determine if legal entity or natural person. The personal code is
        # dropped once it has said "person" (Phase 330, see _parse_officers).
        if _is_estonian_personal_code(code):
            isiku_tyyp = "F"
            reg_code_field = ""
            personal_code = ""
            first, last = _split_name(name)
        else:
            isiku_tyyp = "J"
            reg_code_field = code if _is_registry_code(code) else ""
            personal_code = ""
            first = ""
            last = name.strip()

        shareholders.append({
            "kirje_id": str(idx + 1),
            "isiku_tyyp": isiku_tyyp,
            "eesnimi": first,
            "nimi_arinimi": last,
            "isikukood_registrikood": reg_code_field or personal_code,
            "osaluse_protsent": pct_str or None,
            "osaluse_suurus": amount,
            "osaluse_valuuta": currency,
            "algus_kpv": _parse_date(start_raw) or start_raw or None,
            "lopp_kpv": _parse_date(end_raw) or end_raw or None,
            "synniaeg": None,
            "valis_kood_riik": None,
        })
    return shareholders


def _parse_beneficial_owners(html: str) -> list[dict[str, Any]]:
    """Parse the beneficial owners table.

    Expected headers: Name | Personal identification code … | Manner … | Start - end
    """
    rows = _find_table(html, ["Name", "Manner"])
    bos: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        if len(row) < 3:
            continue
        full_name = row[0]
        id_code = row[1] if len(row) > 1 else ""
        manner_label = (row[2] if len(row) > 2 else "").lower()
        start_raw = row[3] if len(row) > 3 else ""
        end_raw = row[4] if len(row) > 4 else ""

        control_code = _BO_MANNER_MAP.get(manner_label)
        if not control_code:
            for label, code in _BO_MANNER_MAP.items():
                if label in manner_label:
                    control_code = code
                    break
        control_code = control_code or "M"

        first, last = _split_name(full_name)
        # The personal code column is never carried (Phase 330).
        bos.append({
            "kirje_id": str(idx + 1),
            "eesnimi": first,
            "nimi": last,
            "isikukood": "",
            "kontrolli_teostamise_viis": control_code,
            "algus_kpv": _parse_date(start_raw) or start_raw or None,
            "lopp_kpv": _parse_date(end_raw) or end_raw or None,
            "synniaeg": None,
            "aadress_riik": None,
            "valis_kood_riik": None,
            "isikukood_hash": None,
        })
    return bos


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


def normalise_registry_code(raw: str) -> str:
    """Estonian registry codes are 8 digits; GLEIF may drop leading zeros."""
    return raw.strip().zfill(8)


# ---------------------------------------------------------------------------
# The index (Phase 330)
# ---------------------------------------------------------------------------

#: The table layout this adapter reads. ``scripts/build_ariregister_index.py``
#: writes the same value into ``meta``; a test pins the two equal, and an
#: index carrying any other value is ignored rather than misread.
INDEX_SCHEMA_VERSION = "2"  # 2: the off_card_person table (kandevalised_isikud)

#: The layouts this adapter can read. A schema-1 index (built before the
#: fifth file) is read with an empty off-card list rather than ignored, so
#: a deploy that lands before the next daily rebuild does not send every
#: Estonian lookup to the rate-limited page.
INDEX_SCHEMA_VERSIONS_READ = frozenset({"1", INDEX_SCHEMA_VERSION})

#: Where the open data is published — the page a reader can follow.
DATASET_URL = "https://avaandmed.ariregister.rik.ee/en/downloading-open-data"

#: ``ettevotja_staatus`` → the English status label the printable page shows,
#: so ``map_ariregister``'s liveness classification reads both tiers alike.
STATUS_LABELS: dict[str, str] = {
    "R": "Entered into the register",
    "L": "In liquidation",
    "N": "Bankrupt",
}

COVERAGE_NOT_IN_REGISTER = (
    "Not in the e-Business Register's open data as of {cut} (which holds "
    "every company entered, in liquidation or in bankruptcy, and no deleted "
    "company), and the register's page holds no record under this code."
)
COVERAGE_PAGE_UNREACHABLE = (
    "Not in the e-Business Register's open data as of {cut} (which holds "
    "every company entered, in liquidation or in bankruptcy, and no deleted "
    "company); the register's page could not be read ({why}). This is not a "
    "confirmation that the register holds nothing — a company newer than "
    "the cut, or deleted, is answered by the page."
)

_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
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
    "off_card_person": (
        "kirje_id", "isiku_tyyp", "isiku_roll", "eesnimi", "nimi_arinimi",
        "isikukood_hash", "isikukood_registrikood", "valis_kood", "valis_kood_riik",
        "synniaeg", "aadress_riik", "osaluse_protsent", "osaluse_suurus",
        "osaluse_valuuta", "osaluse_omandiliik", "osaluse_murdosa_lugeja",
        "osaluse_murdosa_nimetaja", "kontrolli_allikas", "kontrolli_allika_kpv",
        "algus_kpv", "lopp_kpv", "grupp",
    ),
}

#: ``(code column, vocab kind, text column the mapper reads)`` — the Estonian
#: wording stored once in ``vocab`` is put back beside each code, as the
#: JSON files carry it.
_TEXT_COLUMNS: dict[str, tuple[tuple[str, str, str], ...]] = {
    "officer": (
        ("isiku_roll", "role", "isiku_roll_tekstina"),
        ("valis_kood_riik", "country", "valis_kood_riik_tekstina"),
        ("aadress_riik", "country", "aadress_riik_tekstina"),
    ),
    "shareholder": (
        ("isiku_roll", "role", "isiku_roll_tekstina"),
        ("valis_kood_riik", "country", "valis_kood_riik_tekstina"),
        ("aadress_riik", "country", "aadress_riik_tekstina"),
        ("osaluse_omandiliik", "ownership_kind", "osaluse_omandiliik_tekstina"),
        ("kontrolli_allikas", "control_source", "kontrolli_allikas_tekstina"),
    ),
    "beneficial_owner": (
        ("valis_kood_riik", "country", "valis_kood_riik_tekstina"),
        ("aadress_riik", "country", "aadress_riik_tekstina"),
        ("kontrolli_teostamise_viis", "control", "kontrolli_teostamise_viis_tekstina"),
    ),
    "off_card_person": (
        ("isiku_roll", "role", "isiku_roll_tekstina"),
        ("valis_kood_riik", "country", "valis_kood_riik_tekstina"),
        ("aadress_riik", "country", "aadress_riik_tekstina"),
        ("osaluse_omandiliik", "ownership_kind", "osaluse_omandiliik_tekstina"),
        ("kontrolli_allikas", "control_source", "kontrolli_allikas_tekstina"),
    ),
}


def db_path() -> Path:
    """``ARIREGISTER_DB_FILE`` or the data-root default — one function, so
    the reader, the boot download and the sweep's ``requires_files`` agree
    (the ONRC lesson)."""
    from ..cache import data_root

    configured = get_settings().ariregister_db_file
    return Path(configured) if configured else data_root() / "ariregister.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}
_VOCAB: dict[str, dict[tuple[str, str], str]] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().ariregister_db_file:
            logger.warning("ariregister: index not found at %s", path)
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        version = conn.execute(
            "SELECT value FROM meta WHERE key = 'schema_version'"
        ).fetchone()
    except sqlite3.Error:
        version = None
    if not version or str(version[0]) not in INDEX_SCHEMA_VERSIONS_READ:
        logger.warning("ariregister: index at %s has an unexpected schema; ignoring", path)
        conn.close()
        return None
    return conn


def _shared_conn() -> sqlite3.Connection | None:
    if _SHARED.get("conn") is None:
        _SHARED["conn"] = _connect()
    return _SHARED.get("conn")


def reset_connection(*, close: bool = True) -> None:
    """Forget the cached connection (after a download, or in tests).
    ``close=False`` leaves closing to the garbage collector, for a file
    replaced while lookups may be reading it."""
    _VOCAB.clear()
    conn = _SHARED.pop("conn", None)
    if conn is not None and close:
        try:
            conn.close()
        except sqlite3.Error:  # pragma: no cover - best effort
            pass


def index_available() -> bool:
    """True when a readable index of the expected schema is present."""
    return _shared_conn() is not None


def read_meta() -> dict[str, str]:
    """The index's ``meta`` table, or ``{}`` when there is no index."""
    conn = _shared_conn()
    if conn is None:
        return {}
    try:
        return {str(k): str(v) for k, v in conn.execute("SELECT key, value FROM meta")}
    except sqlite3.Error:  # pragma: no cover
        return {}


def _vocab() -> dict[tuple[str, str], str]:
    """``(kind, code) → text``, read once per connection."""
    if "v" not in _VOCAB:
        conn = _shared_conn()
        table: dict[tuple[str, str], str] = {}
        if conn is not None:
            try:
                for kind, code, text in conn.execute("SELECT kind, code, text FROM vocab"):
                    table[(str(kind), str(code))] = str(text or "")
            except sqlite3.Error:  # pragma: no cover
                pass
        _VOCAB["v"] = table
    return _VOCAB["v"]


def vocab_text(kind: str, code: str | None) -> str | None:
    """The register's own wording for a code, or None."""
    if not code:
        return None
    return _vocab().get((kind, str(code))) or None


def cut_date(meta: dict[str, str] | None = None) -> datetime | None:
    """The day the files were generated (their ``Last-Modified``) — the day
    the data is "as of". Never the build time."""
    meta = read_meta() if meta is None else meta
    try:
        return datetime.strptime(meta.get("cut") or "", "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        return None


def declare_snapshot(meta: dict[str, str] | None = None) -> None:
    """Declare an index read as a snapshot of the register's open data.

    Without this the provenance recorder collects nothing and resolves to its
    ``stub`` default, and real register rows badge as "Placeholder data" —
    this adapter's own Phase 45 bug. The cut is the register's clock;
    ``built_at`` is ours (Phase 314).
    """
    meta = read_meta() if meta is None else meta
    cut = meta.get("cut") or ""
    provenance.record_snapshot(
        retrieved_at=provenance.parse_moment(meta.get("built_at")),
        source_as_of=cut_date(meta),
        detail=(
            f"e-Business Register open data, cut {cut}"
            if cut else "e-Business Register open data"
        ),
    )


def warm_index() -> dict[str, Any]:
    """Download the index at boot when absent; replace it when the release
    asset changed; keep it otherwise. Never raises — no index is a state this
    module models (the page answers instead, as it did before Phase 330)."""
    from ..entity_pages import (
        ASSET_STAMP_KEY,
        asset_check,
        download_db,
        read_meta as read_file_meta,
        record_asset_stamp,
    )

    settings = get_settings()
    path = db_path()
    url = settings.ariregister_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {"ariregister": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"ariregister": f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        # The file was replaced under a server that may be reading it (the
        # hourly refresh): drop the old connection without closing it, so a
        # query in flight finishes on the old file and the next one opens
        # the new. The old connection closes when the last user lets go.
        reset_connection(close=False)
        return {"ariregister": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("ariregister: asset check/download failed: %s", exc)
        return {"ariregister": f"failed: {exc}"}


async def refresh_loop(interval_s: float) -> None:
    """Re-run :func:`warm_index` every ``interval_s`` seconds, so the daily
    index lands without a restart (the PSC graph's arrangement). Before this
    the server picked up a new index only when it restarted — on a busy
    instance, rarely — and the watchlist's register tier would have re-run a
    changed company against yesterday's record. The first run is one interval
    after boot, since boot already checked."""
    import asyncio

    while True:
        await asyncio.sleep(interval_s)
        try:
            result = await asyncio.to_thread(warm_index)
            if not str(result.get("ariregister", "")).startswith("already present"):
                logger.info("ariregister: refresh: %s", result)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning("ariregister: refresh loop iteration failed: %s", exc)


def _people_rows(conn: sqlite3.Connection, table: str, code: str) -> list[dict[str, Any]]:
    """One table's rows for a code, in the JSON files' shape: the hash back
    as a UUID string, the code's Estonian wording beside it, empties absent."""
    columns = _TABLE_COLUMNS[table]
    try:
        rows = conn.execute(
            f"SELECT {', '.join(columns)} FROM {table} WHERE code = ? ORDER BY kirje_id",
            (code,),
        ).fetchall()
    except sqlite3.OperationalError as exc:
        if table == "off_card_person" and "no such table" in str(exc):
            return []  # a schema-1 index, built before the fifth file
        raise
    out: list[dict[str, Any]] = []
    for row in rows:
        item: dict[str, Any] = {}
        for column in columns:
            value = row[column]
            if value is None:
                continue
            if column == "isikukood_hash":
                value = str(uuid.UUID(bytes=bytes(value)))
            elif column == "lahknevusteade_esitatud":
                value = bool(value)
            elif column in ("kirje_id", "grupp"):
                value = str(value)
            item[column] = value
        for code_column, kind, text_column in _TEXT_COLUMNS[table]:
            text = vocab_text(kind, item.get(code_column))
            if text:
                item[text_column] = text
        out.append(item)
    return out


def entity_record(code: str) -> dict[str, Any] | None:
    """The index's record for one registry code — the entity row with its
    four people lists — or None when the index does not hold it (or there
    is no index)."""
    conn = _shared_conn()
    if conn is None:
        return None
    row = conn.execute(
        "SELECT code, name, legal_form, legal_form_subtype, vat_number, status_code,"
        " first_entry_on, address FROM entity WHERE code = ?",
        (code,),
    ).fetchone()
    if row is None:
        return None
    record = dict(row)
    record["status_text"] = vocab_text("status", record.get("status_code"))
    record["officers"] = _people_rows(conn, "officer", code)
    record["shareholders"] = _people_rows(conn, "shareholder", code)
    record["beneficial_owners"] = _people_rows(conn, "beneficial_owner", code)
    record["off_card_persons"] = _people_rows(conn, "off_card_person", code)
    record["general"] = general_record(code)
    return record


#: How far up a chain of Estonian corporate shareholders one lookup
#: follows, and how many related companies it may pull in — the Companies
#: House walk's two caps (``OPENCHECK_CH_PSC_MAX_DEPTH``, ``_MAX_RELATED_COMPANIES``).
#: Every hop is an index read, so the caps bound the bundle, not a budget.
CHAIN_MAX_DEPTH = 6
MAX_RELATED_COMPANIES = 25

#: Why a corporate shareholder was not followed (``chain.unfollowed``).
UNFOLLOWED_FOREIGN = "foreign_register"       # a valis_kood, not an Estonian code
UNFOLLOWED_NOT_IN_INDEX = "not_in_index"      # deleted, or newer than the cut
UNFOLLOWED_DEPTH = "max_depth_reached"
UNFOLLOWED_CAP = "related_companies_cap_reached"

#: Off-card roles that are a shareholding of the company (``S`` aktsionär)
#: and registry-card roles that are a partner's stake in a partnership —
#: the rows the chain walk and the reverse index follow.
_CHAIN_OFF_CARD_ROLES = frozenset({"S"})
_CHAIN_OFFICER_ROLES = frozenset({"TOSAN", "UOSAN", "EUSOS", "EUSOS2"})


def _corporate_holders(record: dict[str, Any]) -> list[tuple[str | None, dict[str, Any]]]:
    """The legal persons holding a company's shares, as (registry code or
    None for a foreign one, the row): the OÜ shareholder list, the AS
    shareholders the register records (off-card ``S``) and partners."""
    out: list[tuple[str | None, dict[str, Any]]] = []
    rows = list(record.get("shareholders") or [])
    rows += [r for r in record.get("off_card_persons") or [] if r.get("isiku_roll") in _CHAIN_OFF_CARD_ROLES]
    rows += [r for r in record.get("officers") or [] if r.get("isiku_roll") in _CHAIN_OFFICER_ROLES]
    for row in rows:
        if (row.get("isiku_tyyp") or "F") != "J":
            continue
        code = (row.get("isikukood_registrikood") or "").strip()
        out.append((code or None, row))
    return out


def walk_chain(
    code: str, record: dict[str, Any], *, max_depth: int = CHAIN_MAX_DEPTH,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Follow the company's Estonian corporate shareholders up the chain, in
    the index, breadth-first: the related companies' records keyed on their
    registry code, and a ``chain`` block — depth reached, count, and every
    corporate holder that was *not* followed with the reason, so a chain cut
    short by the cap or the cut is visible rather than indistinguishable
    from the top of the structure (the Companies House rule, Phase 177).

    A company that is in the chain twice (a cycle, or a parent two
    subsidiaries share) is read once. The subject itself is never in
    ``related``.
    """
    related: dict[str, dict[str, Any]] = {}
    unfollowed: list[dict[str, Any]] = []
    depth_reached = 0
    frontier: list[tuple[str, dict[str, Any], int]] = [(code, record, 0)]
    visited = {code}
    while frontier:
        parent_code, parent, depth = frontier.pop(0)
        for holder_code, row in _corporate_holders(parent):
            name = (row.get("nimi_arinimi") or "").strip()
            if holder_code is None:
                unfollowed.append({
                    "of": parent_code, "name": name, "reason": UNFOLLOWED_FOREIGN,
                    "foreign_code": row.get("valis_kood"), "country": row.get("valis_kood_riik"),
                })
                continue
            if holder_code in visited:
                continue
            if depth >= max_depth:
                unfollowed.append({"of": parent_code, "name": name, "code": holder_code, "reason": UNFOLLOWED_DEPTH})
                continue
            if len(related) >= MAX_RELATED_COMPANIES:
                unfollowed.append({"of": parent_code, "name": name, "code": holder_code, "reason": UNFOLLOWED_CAP})
                continue
            visited.add(holder_code)
            sub = entity_record(holder_code)
            if sub is None:
                unfollowed.append({"of": parent_code, "name": name, "code": holder_code, "reason": UNFOLLOWED_NOT_IN_INDEX})
                continue
            related[holder_code] = sub
            depth_reached = max(depth_reached, depth + 1)
            frontier.append((holder_code, sub, depth + 1))
    chain = {"depth": depth_reached, "related": len(related), "max_depth": max_depth, "unfollowed": unfollowed}
    return related, chain


def holdings_of(code: str, *, limit: int = 200) -> list[dict[str, Any]]:
    """The companies a legal person holds shares in, from the index's
    reverse side: every shareholder, AS-shareholder and partner row whose
    ``isikukood_registrikood`` is ``code``, with the held company's name,
    form and status. ``[]`` without an index. The subject is a *holder*
    here — what it owns, never who owns it."""
    conn = _shared_conn()
    if conn is None:
        return []
    code = normalise_registry_code(code)
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    #: (table, relation word, roles kept, the row's share columns)
    queries = (
        ("shareholder", "shareholder", None,
         "t.osaluse_protsent, t.osaluse_suurus, t.osaluse_valuuta"),
        ("off_card_person", "as_shareholder", _CHAIN_OFF_CARD_ROLES,
         "t.osaluse_protsent, t.osaluse_suurus, t.osaluse_valuuta"),
        ("officer", "partner", _CHAIN_OFFICER_ROLES,
         "NULL, t.osamaks, t.osamaksu_valuuta"),
    )
    for table, relation, roles, share_columns in queries:
        try:
            rows = conn.execute(
                f"SELECT t.code, t.isiku_roll, {share_columns}, t.algus_kpv,"
                f" e.name, e.legal_form, e.status_code"
                f" FROM {table} t JOIN entity e ON e.code = t.code"
                f" WHERE t.isikukood_registrikood = ? ORDER BY t.code",
                (code,),
            ).fetchall()
        except sqlite3.Error:
            continue
        for held, role, percent, size, currency, since, name, form, status in rows:
            role = str(role or "")
            if roles is not None and role not in roles:
                continue
            held = str(held)
            if (held, role) in seen:
                continue
            seen.add((held, role))
            out.append({
                "code": held,
                "name": name,
                "legal_form": form,
                "status_code": status,
                "status_text": vocab_text("status", status),
                "relation": relation,
                "role": role,
                "role_text": vocab_text("role", role),
                "percent": percent,
                "share_size": size,
                "currency": currency,
                "since": since,
            })
            if len(out) >= limit:
                return out
    return out


#: The register's lists, as ``companies_for_hash`` names them — the words
#: the route and the page use, one per index table.
PERSON_LISTS: tuple[tuple[str, str, str, str], ...] = (
    # (table, list name, the column holding the row's role code, the surname column)
    ("officer", "registry_card", "isiku_roll", "nimi_arinimi"),
    ("shareholder", "shareholders", "isiku_roll", "nimi_arinimi"),
    ("beneficial_owner", "beneficial_owners", "kontrolli_teostamise_viis", "nimi"),
    ("off_card_person", "off_card", "isiku_roll", "nimi_arinimi"),
)

#: The most rows one person's list returns; ``total`` still counts all.
PERSON_COMPANIES_LIMIT = 500


def parse_person_hash(raw: str) -> bytes | None:
    """The register's ``isikukood_hash`` as the index stores it (the UUID's
    16 bytes), or None when ``raw`` is not a UUID. Never a personal code:
    an eleven-digit number is refused here, whatever its shape."""
    try:
        return uuid.UUID(str(raw).strip()).bytes
    except (ValueError, AttributeError):
        return None


def companies_for_hash(person_hash: str, *, limit: int = PERSON_COMPANIES_LIMIT) -> dict[str, Any] | None:
    """Every company on whose current lists the register names the person
    keyed by ``person_hash`` — the registry card, the shareholders, the
    beneficial owners and the persons outside the card — one row per company,
    list and role, with the fields the register publishes: company name,
    form and status, the role (in the register's words), since when, and the
    share where the list carries one. No addresses: the index holds none.

    The hash is the register's pseudonymous key, published so a person can
    be followed across companies; it is never an identifier, and the files
    hold current rows only, so a deleted company or an ended role is not
    here. None without an index or for a value that is not a UUID; an index
    that holds no row for the hash answers an empty list.
    """
    key = parse_person_hash(person_hash)
    if key is None:
        return None
    conn = _shared_conn()
    if conn is None:
        return None
    rows: list[dict[str, Any]] = []
    names: dict[str, int] = {}
    seen: set[tuple[str, str, str]] = set()
    for table, list_name, role_column, surname_column in PERSON_LISTS:
        share = (
            "t.osaluse_protsent, t.osaluse_suurus, t.osaluse_valuuta"
            if table in ("shareholder", "off_card_person")
            else ("NULL, t.osamaks, t.osamaksu_valuuta" if table == "officer" else "NULL, NULL, NULL")
        )
        try:
            found = conn.execute(
                f"SELECT t.code, t.{role_column}, {share}, t.algus_kpv,"
                f" e.name, e.legal_form, e.status_code, t.eesnimi, t.{surname_column}"
                f" FROM {table} t JOIN entity e ON e.code = t.code"
                f" WHERE t.isikukood_hash = ? ORDER BY t.code, t.kirje_id",
                (key,),
            ).fetchall()
        except sqlite3.OperationalError as exc:
            if table == "off_card_person" and "no such table" in str(exc):
                continue  # a schema-1 index, built before the fifth file
            raise
        vocab_kind = "control" if table == "beneficial_owner" else "role"
        for code, role, percent, size, currency, since, name, form, status, first, last in found:
            code, role = str(code), str(role or "")
            person = " ".join(p.strip() for p in (first, last) if p and p.strip())
            if person:
                names[person] = names.get(person, 0) + 1
            if (code, list_name, role) in seen:
                continue
            seen.add((code, list_name, role))
            rows.append({
                "code": code,
                "name": name,
                "legal_form": form,
                "status_code": status,
                "status_text": vocab_text("status", status),
                "list": list_name,
                "role": role or None,
                "role_text": vocab_text(vocab_kind, role),
                "since": since,
                "percent": percent,
                "share_size": size,
                "currency": currency,
            })
    rows.sort(key=lambda r: (r["name"] or "", r["code"], r["list"]))
    companies = sorted({r["code"] for r in rows})
    return {
        # The name the register gives the person most often across the rows
        # (it is spelled the same almost everywhere; a tie goes to the first).
        "name": max(names, key=lambda n: names[n]) if names else None,
        "rows": rows[:limit],
        "total_rows": len(rows),
        "companies": len(companies),
        "truncated": len(rows) > limit,
    }


#: The packed ``card_entry`` fields, in the builder's order
#: (``build_ariregister_index.CARD_ENTRY_FIELDS``).
CARD_ENTRY_FIELDS = ("card_type", "card_no", "entry_no", "day", "entry_type")

#: The ruling types the index keeps — ``build_ariregister_index.RULING_TYPES``
#: (pinned by a test). Of them, the ones about the annual report: a report
#: filed since answers them, whatever status the ruling keeps.
RULING_TYPES = frozenset({
    "X1", "X2", "HM", "HP", "H", "H2", "KAA", "M3",
    "Y3", "Y2O", "Y2A", "GY", "KI", "G1", "G2T", "I2", "Z1",
})
ANNUAL_REPORT_RULINGS = frozenset({"X1", "X2", "HM", "HP", "H", "H2", "KAA", "M3"})
#: A ruling about a missing or deficient annual report concerns a financial
#: year that ended at most this long before it (the report is due six months
#: after the year end; warnings and fines follow within the year).
_REPORT_RULING_REACH = timedelta(days=548)


def _json_rows(table: str, column: str, code: str) -> list[dict[str, Any]] | None:
    conn = _shared_conn()
    if conn is None:
        return None
    try:
        row = conn.execute(
            f"SELECT {column} FROM {table} WHERE code = ?", (normalise_registry_code(code),)
        ).fetchone()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc):
            return None  # an index built before the rulings phase
        raise
    return json.loads(row[0]) if row else []


def rulings(code: str) -> list[dict[str, Any]] | None:
    """The company's compliance rulings in force (the rulings phase), newest
    first, each with the register's Estonian wording: ``[]`` for a company
    with none, None without an index or the table."""
    rows = _json_rows("ruling", "rulings", code)
    if rows is None:
        return None
    for r in rows:
        r["type_text"] = vocab_text("ruling", r.get("type"))
    return rows


def rulings_in_force(
    rows: list[dict[str, Any]] | None, last_report_end: str | None,
) -> list[dict[str, Any]]:
    """The rulings to show as current: every kept ruling, except one about
    the annual report when the company has since filed a report for a year
    ending within ``_REPORT_RULING_REACH`` of the ruling — the register keeps
    such a fine or warning "in force", but the report it asked for is on
    file (the ticket's rule: a ruling older than the latest filed report is
    not current). Measured 16 Oct 2026: this sets aside 48,306 of 148,469
    in-force rulings, leaving 100,163 on 19,162 companies."""
    out: list[dict[str, Any]] = []
    try:
        filed = date.fromisoformat(last_report_end) if last_report_end else None
    except ValueError:
        filed = None
    for r in rows or []:
        try:
            day = date.fromisoformat(str(r.get("day") or ""))
        except ValueError:
            continue
        if r.get("type") in ANNUAL_REPORT_RULINGS and filed and filed >= day - _REPORT_RULING_REACH:
            continue
        out.append(r)
    return out


def pledges(code: str) -> list[dict[str, Any]] | None:
    """The company's current commercial pledges (the rulings phase), first
    rank first: ``[]`` for none, None without an index or the table."""
    return _json_rows("pledge", "pledges", code)


def card_entries(code: str) -> list[dict[str, Any]] | None:
    """Every entry on the company's registry cards, oldest first as the
    register numbers them — card type and number, entry number, ISO day,
    entry type and its Estonian wording. ``[]`` when the index holds no
    entry for the code; None without an index, or with one built before
    the registry cards were read (a missing table)."""
    conn = _shared_conn()
    if conn is None:
        return None
    try:
        row = conn.execute(
            "SELECT entries FROM card_entry WHERE code = ?", (normalise_registry_code(code),),
        ).fetchone()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc):
            return None
        raise
    if row is None:
        return []
    out: list[dict[str, Any]] = []
    for packed in str(row[0]).split(";"):
        parts = packed.split(",")
        if len(parts) != len(CARD_ENTRY_FIELDS):
            continue
        item: dict[str, Any] = dict(zip(CARD_ENTRY_FIELDS, parts))
        item["entry_no"] = int(item["entry_no"])
        item["entry_text"] = vocab_text("card_entry", item["entry_type"]) or None
        out.append(item)
    return out


def general_record(code: str) -> dict[str, Any] | None:
    """The company's general data (üldandmed) from the index: capital, the
    beneficial-owner filing obligation, main activity, the last annual
    report filed, and the rare lists (succession, status changes, activity
    periods, a foreign branch's parent, predecessor references) — with the
    name of every company a succession entry names that the index holds.
    None without an index, without the table (an index built before the
    general data was read) or for a code it lacks."""
    conn = _shared_conn()
    if conn is None:
        return None
    try:
        row = conn.execute(
            "SELECT capital, capital_currency, capital_since, bo_obliged, bo_absence_reported,"
            " activity, activity_nace, last_report_end, last_report_employees, reports_filed, extra"
            " FROM general WHERE code = ?",
            (normalise_registry_code(code),),
        ).fetchone()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc):
            return None
        raise
    if row is None:
        return None
    out: dict[str, Any] = {k: row[k] for k in row.keys() if k != "extra"}
    out["bo_obliged"] = bool(row["bo_obliged"])
    out["bo_absence_reported"] = None if row["bo_absence_reported"] is None else bool(row["bo_absence_reported"])
    out["activity_text"] = vocab_text("emtak", row["activity"]) or None
    extra = json.loads(row["extra"]) if row["extra"] else {}
    for entry in extra.get("succession") or []:
        partners = []
        for partner in entry.get("codes") or []:
            hit = conn.execute("SELECT name FROM entity WHERE code = ?", (partner,)).fetchone()
            partners.append({"code": partner, "name": hit["name"] if hit else None})
        entry["partners"] = partners
    out.update(extra)
    return out


def search_index(query: str, *, limit: int = 10) -> list[dict[str, Any]]:
    """Entities whose name contains ``query`` (case-insensitive), from the
    index; ``[]`` without one."""
    conn = _shared_conn()
    if conn is None:
        return []
    needle = " ".join(query.split()).casefold()
    if not needle:
        return []
    try:
        rows = conn.execute(
            "SELECT code, name, legal_form, status_code, address FROM entity"
            " WHERE name_key LIKE ? ESCAPE '\\' LIMIT ?",
            ("%" + needle.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%", limit),
        ).fetchall()
    except sqlite3.Error:
        return []
    return [dict(r) for r in rows]

class AriregisterAdapter(SourceAdapter):
    """Source adapter for the Estonian e-Business Register (e-Äriregister).

    Uses the public printable-page endpoint — no authentication required.
    """

    id = "ariregister"

    lookup_derivers = (
        LookupDeriver(frozenset({EE_RA_CODE}), "ee_registry_code", normalise_registry_code),
    )
    lookup_pass_legal_name = True


    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="Estonian e-Business Register (e-Äriregister)",
            homepage="https://ariregister.rik.ee/eng",
            description=(
                "Estonian company data — entity details, the persons on the "
                "registry card (board members, procurists, liquidators, "
                "partners), the register of shareholders with ownership "
                "percentages, declared beneficial owners with their manner "
                "of control, and the persons outside the registry card "
                "(supervisory board members, the shareholders of a public "
                "limited company the register records, founders) — read from a "
                "daily index of RIK's open-data files, with the public "
                "e-Business Register page as the fallback for a company the "
                "files do not hold. An Estonian corporate owner's own record "
                "is read from the same index up the chain (six hops, "
                "twenty-five companies), and a foreign owner's register code "
                "becomes a register-hop identifier where it has the shape of "
                "that country's company number. Former shareholders and former "
                "beneficial owners come from the register's own history "
                "(the credentialed X-Road service, cached a day), each a "
                "closed record dated by the register."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Data from the Estonian e-Business Register (e-Äriregister), "
                "published by the Centre of Registers and Information Systems "
                "(RIK), CC BY 4.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=True,
            is_national_register=True,
            country="EE",
        )

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Name search — from the index when there is one, else the
        autocomplete JSON endpoint (live mode only)."""
        if kind != SearchKind.ENTITY or not (query or "").strip():
            return []
        if index_available():
            rows = search_index(query)
            if not rows:
                return []
            declare_snapshot()
            return [self._index_search_hit(row) for row in rows]
        # Every other adapter's search answers nothing unless live mode is on;
        # this one called ariregister.rik.ee regardless, which is how the
        # search fan-out tests reached the network (Phase 266).
        if not get_settings().allow_live:
            return []
        # This adapter talks to ariregister.rik.ee directly rather than through
        # the shared http.build_client() (it needs a bare HTML Accept header
        # and a longer timeout — see module docstring), so build_client()'s
        # automatic provenance.record_live() never fires. Record it here at
        # the same moment build_client() would: when committing to the
        # network call, regardless of whether it ultimately succeeds. A
        # failed search still resolves to "stub" because it returns no hits.
        provenance.record_live("ariregister autocomplete")
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            try:
                r = await client.get(
                    _AUTO_URL,
                    params={"q": query},
                    headers=_HEADERS,
                )
                r.raise_for_status()
            except Exception as exc:
                logger.warning("ariregister: search error for %r: %s", query, exc)
                return []

        data = r.json()
        hits: list[SourceHit] = []
        for item in (data.get("data") or []):
            reg = str(item.get("reg_code") or "")
            name = item.get("name") or ""
            status = item.get("status") or ""
            address = item.get("legal_address") or ""
            url = item.get("url") or f"{_BASE_URL}/eng/company/{reg}"
            if not reg or not name:
                continue
            hits.append(SourceHit(
                source_id=self.id,
                hit_id=reg,
                kind=SearchKind.ENTITY,
                name=name,
                summary=f"EE-ARIREGISTER {reg}",
                identifiers={"ee_registry_code": reg},
                raw={"reg_code": reg, "name": name, "status": status,
                     "address": address, "url": url},
                is_stub=False,
            ))
        return hits

    @staticmethod
    def _index_search_hit(row: dict[str, Any]) -> SourceHit:
        code = row["code"]
        segments = [f"EE-ARIREGISTER {code}"]
        form = (row.get("legal_form") or "").strip()
        if form:
            segments.append(form)
        status = str(row.get("status_code") or "")
        if status == "L":
            segments.append("in liquidation")
        elif status == "N":
            segments.append("bankrupt")
        return SourceHit(
            source_id="ariregister",
            hit_id=code,
            kind=SearchKind.ENTITY,
            name=row.get("name") or f"EE {code}",
            summary=" · ".join(segments),
            identifiers={"ee_registry_code": code},
            raw={"reg_code": code, "name": row.get("name"), "status": status,
                 "address": row.get("address"), "url": f"{_BASE_URL}/eng/company/{code}"},
            is_stub=False,
            liveness="snapshot",
        )

    def _stub(self, registry_code: str, legal_name: str) -> dict[str, Any]:
        """Nothing was read. Dropped by the pipeline."""
        return {
            "source_id": self.id,
            "registry_code": registry_code,
            "name": legal_name,
            "legal_form": None,
            "vat_number": None,
            "status": None,
            "registration_date": None,
            "address": None,
            "link": f"{_BASE_URL}/eng/company/{registry_code}",
            "shareholders": [],
            "officers": [],
            "beneficial_owners": [],
            "off_card_persons": [],
            "is_stub": True,
        }

    def _note_card(
        self, registry_code: str, legal_name: str, meta: dict[str, str], note: str,
    ) -> dict[str, Any]:
        """The index answered without the code and the page could not add to
        it — a note card, the INPI / JAR shape: ``is_stub`` False and
        ``not_found`` True, so the source is announced and the row says why
        it is empty. Not cached: a miss belongs to one build of the index."""
        bundle = self._stub(registry_code, legal_name)
        bundle.update({
            "is_stub": False,
            "not_found": True,
            "coverage_note": note,
            "cut": meta.get("cut"),
            "tier": "index",
        })
        return bundle

    @staticmethod
    def _index_bundle(code: str, record: dict[str, Any], meta: dict[str, str]) -> dict[str, Any]:
        status_code = str(record.get("status_code") or "") or None
        return {
            "source_id": "ariregister",
            "registry_code": code,
            "name": record.get("name") or "",
            "legal_form": record.get("legal_form"),
            "legal_form_subtype": record.get("legal_form_subtype"),
            "vat_number": record.get("vat_number"),
            "status": STATUS_LABELS.get(status_code or "", record.get("status_text")),
            "status_code": status_code,
            "status_text": record.get("status_text"),
            "registration_date": record.get("first_entry_on"),
            "address": record.get("address"),
            "link": f"{_BASE_URL}/eng/company/{code}",
            "shareholders": record.get("shareholders") or [],
            "officers": record.get("officers") or [],
            "beneficial_owners": record.get("beneficial_owners") or [],
            # The persons outside the registry card: supervisory board, AS
            # shareholders, founders, auditors. The page path has no such
            # list, so a page bundle carries an empty one.
            "off_card_persons": record.get("off_card_persons") or [],
            # The general data (üldandmed), when the index holds it.
            "general": record.get("general"),
            # The compliance rulings in force — those a filed annual report
            # has not answered — and the current commercial pledges. None
            # from an index built before them.
            "rulings": (
                None if (raw_rulings := rulings(code)) is None
                else rulings_in_force(raw_rulings, (record.get("general") or {}).get("last_report_end"))
            ),
            "pledges": pledges(code),
            "cut": meta.get("cut"),
            "tier": "index",
            "is_stub": False,
        }

    async def fetch(
        self,
        hit_id: str,
        *,
        legal_name: str = "",
    ) -> dict[str, Any]:
        """Return the company's record: from the index when it holds the
        code, else from the printable page.

        ``hit_id`` is the 8-digit Estonian registry code (GLEIF's
        ``registeredAs``, or a register-hop / ``opencheck_register_lookup``
        caller's).
        """
        registry_code = normalise_registry_code(hit_id)
        index_present = index_available()
        meta = read_meta() if index_present else {}
        if index_present:
            try:
                record = entity_record(registry_code)
            except sqlite3.Error as exc:
                degradation.record(
                    self.id,
                    "The e-Business Register index could not be read; the page "
                    "was asked instead.",
                    reason=degradation.reason_for_failure(type(exc).__name__),
                )
                record = None
            if record is not None:
                # Declared only once a row is in hand: a miss is not a read.
                declare_snapshot(meta)
                bundle = self._index_bundle(registry_code, record, meta)
                # The chain above the company, from the same index: every
                # Estonian corporate shareholder's own record, up to the
                # caps, so the owners' owners arrive in the first lookup
                # (the Companies House walk, with no network to spend).
                related, chain = walk_chain(registry_code, record)
                bundle["related_companies"] = {
                    code: self._index_bundle(code, sub, meta) for code, sub in related.items()
                }
                bundle["chain"] = chain
                # The ended holdings and former beneficial owners, from the
                # register's own history (SOAP, credentialed; cached a day).
                # The subject only — an intermediary's past owners are not
                # the subject's network. Never on the page path: the page
                # already shows the ended holdings it lists, with no
                # kirje_id to join on.
                from .ariregister_history import read_history

                bundle["history"] = await read_history(self, registry_code, record)
                validate_raw(self.id, AriregisterBundle, bundle)
                return bundle

        page, outcome = await self._read_page(registry_code, legal_name)
        if page is not None:
            page["tier"] = "page"
            if index_present:
                page["index_miss"] = True
                page["cut"] = meta.get("cut")
            validate_raw(self.id, AriregisterBundle, page)
            return page
        if not index_present:
            return self._stub(registry_code, legal_name)
        cut = meta.get("cut") or "the last build"
        if outcome == "not_found":
            return self._note_card(
                registry_code, legal_name, meta, COVERAGE_NOT_IN_REGISTER.format(cut=cut),
            )
        why = outcome or "no answer"
        degradation.record(
            self.id,
            f"The e-Business Register page did not answer ({why}) for a code "
            "the open-data index does not hold; the card shows nothing from "
            "the register. This is not a confirmation that the register "
            "holds nothing.",
            reason=degradation.reason_for_failure(why),
        )
        return self._note_card(
            registry_code, legal_name, meta,
            COVERAGE_PAGE_UNREACHABLE.format(cut=cut, why=why),
        )

    async def _read_page(
        self, registry_code: str, legal_name: str,
    ) -> tuple[dict[str, Any] | None, str]:
        """The printable page as a bundle, or ``(None, outcome)`` — outcome
        ``not_found`` when the site redirected away from the company, else
        the failure (``HTTP 429``, ``timeout``…)."""
        url = _PRINT_URL.format(reg_code=registry_code.lstrip("0") or registry_code)
        # See the comment in search() — this bypasses build_client(), so the
        # live observation has to be recorded by hand. Recorded unconditionally
        # before the request, same as build_client(): a bundle that ends up
        # is_stub=True still resolves to "stub" regardless (provenance.resolve
        # short-circuits on is_stub), so this only affects genuinely-fetched
        # bundles.
        provenance.record_live("ariregister company printable page")
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                r = await client.get(url, headers=_HEADERS, follow_redirects=True)
        except Exception as exc:
            logger.error("ariregister: fetch error for %s: %s", registry_code, exc)
            return None, type(exc).__name__

        if r.status_code != 200:
            logger.info("ariregister: HTTP %s for %s", r.status_code, registry_code)
            return None, f"HTTP {r.status_code}"

        # If the page redirected to a "not found" search page, bail out.
        final_url = str(r.url)
        if "/eng/company/" not in final_url:
            logger.info("ariregister: no company found for %s", registry_code)
            return None, "not_found"

        html = r.text

        # ── Extract label/value info ─────────────────────────────────────────
        info = _parse_info(html)
        name = info.get("Name") or legal_name
        legal_form = info.get("Legal form") or None
        status_raw = info.get("Status") or ""
        status = status_raw if status_raw else None
        reg_date = _parse_date(info.get("Registered") or "") or info.get("Registered")
        address_raw = info.get("Address") or ""
        # Address sometimes ends with postal code — keep as-is.
        address = address_raw if address_raw else None

        # ── Parse the company name from H2 heading if label lookup missed ────
        if not name:
            m = re.search(r"<h2[^>]*>([^<]+?)\s*\(\d+\)\s*</h2>", html)
            if m:
                name = m.group(1).strip()

        if not name:
            logger.info("ariregister: could not determine company name for %s", registry_code)
            return None, "unparseable page"

        officers           = _parse_officers(html)
        shareholders       = _parse_shareholders(html)
        beneficial_owners  = _parse_beneficial_owners(html)

        return {
            "source_id": self.id,
            "registry_code": registry_code,
            "name": name,
            "legal_form": legal_form,
            "vat_number": None,
            "status": status,
            "registration_date": reg_date,
            "address": address,
            "link": f"{_BASE_URL}/eng/company/{registry_code}",
            "shareholders": shareholders,
            "officers": officers,
            "beneficial_owners": beneficial_owners,
            "is_stub": False,
        }, "ok"

    # ------------------------------------------------------------------
    # Time Machine — historical data via the credentialed SOAP API
    # ------------------------------------------------------------------

    async def fetch_timeline_data(self, registry_code: str) -> dict[str, Any] | None:
        """Raw registry-card + beneficial-owner *history* for the Time Machine.

        Calls the RIK X-Road SOAP open-data API with history on
        (``ainult_kehtivad=0``): ``detailandmed_v2`` for the dated registry-card
        blocks and ``tegelikudKasusaajad_v2`` for beneficial owners. Read-only,
        lazy, off the main lookup path — the live ``/lookup`` still uses the
        no-auth public scraper in ``fetch()``.

        Returns ``None`` when not live or when credentials are unset (the
        feature then simply doesn't contribute Estonian events). The two calls
        run concurrently and a failure of either is tolerated.
        """
        settings = get_settings()
        if not settings.allow_live:
            return None
        user = settings.ariregister_username
        pw = settings.ariregister_password
        if not user or not pw:
            return None

        code = re.sub(r"\D", "", registry_code or "") or (registry_code or "").strip()
        if not code:
            return None

        async def _post(envelope: str) -> str | None:
            # Same reasoning as search() and fetch(): the X-Road SOAP call needs
            # its own client and timeout, so build_client()'s automatic
            # provenance.record_live() never fires. Record it here, at the
            # moment we commit to the network. Nothing opens a provenance scope
            # around the Time Machine path today, so this was latent rather
            # than user-visible — but it is the same defect PR #153 fixed on
            # the lookup path, and the AST guard in tests/test_source_probes.py
            # now keeps both closed.
            provenance.record_live("ariregister X-Road timeline")
            try:
                async with httpx.AsyncClient(timeout=_SOAP_TIMEOUT) as client:
                    r = await client.post(_SOAP_URL, content=envelope, headers=_SOAP_HEADERS)
            except Exception as exc:  # noqa: BLE001
                logger.warning("ariregister: timeline SOAP error for %s: %s", code, exc)
                return None
            if r.status_code != 200:
                logger.info("ariregister: timeline HTTP %s for %s", r.status_code, code)
                return None
            return r.text

        detail_xml, bo_xml = await asyncio.gather(
            _post(_detail_envelope(user, pw, code)),
            _post(_bo_envelope(user, pw, code)),
        )
        if not detail_xml and not bo_xml:
            return None
        return {"registry_code": code, "detail_xml": detail_xml, "bo_xml": bo_xml}
