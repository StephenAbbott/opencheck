"""Florida — Department of State, Division of Corporations (Sunbiz).

An **index-tier** register source, the ``jar_lithuania`` / ``cyprus_drcor``
shape: the lookup reads a SQLite index of the Division's quarterly corporate
data files, downloaded at boot from the ``sunbiz-florida-index`` release asset
(``SUNBIZ_FLORIDA_DB_URL``) and replaced when the asset changes. Dispatch is
gated on the index being present (``covers_lei``); a document number the
index does not hold is a note card, never a failure. The index is rebuilt
quarterly by ``scripts/build_sunbiz_florida_index.py`` — see that script for
the files, their layout and what was measured on them.

What the register publishes
---------------------------
The Division's free bulk files (``sftp.floridados.gov``, published public
credentials): for every corporation, LLC, limited partnership and foreign
qualification on file — active and inactive, 12,987,601 records in the
October 2026 quarterly file — the name, status (``A`` / ``I``), filing type,
file date, FEI (EIN) number, principal and mailing address, registered agent,
the last three annual reports, and **up to six officers** (title, name,
address). A flag says when a company has more than six; the full list is only
on the live site. The events file dates every status change (dissolutions,
revocations, withdrawals, reinstatements), name change and merger. **No
shareholders and no beneficial owners** — Florida has no BO register.

What the index holds (Stephen, 30 Oct 2026 — decisions D1–D9 on the ticket)
-------------------------------------------------------------------------
* **Every active record, plus every record a GLEIF LEI names** under
  RA000603 (or by FEI, for the few LEIs that filed an EIN instead) — about
  3.8 million of the 13 million (D1). An inactive company no LEI names is
  not in the index; a number outside it is a note card that says so.
* **No officer's or registered agent's street address or ZIP** — city and
  state only (D4). Most are people's homes.
* Officers are **managing officials, never owners** (D5): an LLC's
  "authorized member" is listed because it is authorised, not because the
  filing says what it holds.

Status
------
``A`` is live. ``I`` for a Florida entity is terminal, dated and worded by
the latest terminal event not undone by a reinstatement (administrative
dissolution for no annual report, voluntary dissolution, merger…). ``I`` for
a **foreign** filing (``FORP``, ``FORL``, ``FORNP``, ``FORLP``) means its
authority to transact business in Florida ended — the company may be alive at
home — so it is not terminal (the ``ny_dos`` rule).

Licence: no licence is stated. Florida public records (Fla. Const. art. I
§24, Fla. Stat. ch. 119); an agency cannot assert copyright in them without
statutory authority (*Microdecisions, Inc. v. Skinner*, 889 So.2d 871, Fla.
2d DCA 2004). Recorded as ``FL-Public-Records`` (D3).
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pycountry

from .. import degradation, names, provenance
from ..config import get_settings
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.sunbiz_florida import SunbizFloridaBundle

logger = logging.getLogger(__name__)

#: GLEIF Registration Authority code for the Florida Division of Corporations.
#: 12,885 LEI records file under it (29 Oct 2026; 3,079 ISSUED) — 12,842 with
#: jurisdiction ``US-FL`` and 43 elsewhere (foreign companies authorised in
#: Florida), so the deriver keys on the RA code, never the jurisdiction.
FL_RA_CODE: str = "RA000603"

#: BODS identifier scheme: the ISO 3166-2 subdivision code the GLEIF mapper
#: already stamps on a Florida entity (``_US_STATE_REGISTRY_NAMES``) — the
#: DC / NY precedent, so the two sources corroborate.
FL_SCHEME = "US-FL"
FL_SCHEME_NAME = "Florida Department of State, Division of Corporations"

#: org-id.guide's scheme for a US Employer Identification Number (D9).
EIN_SCHEME = "US-EIN"
EIN_SCHEME_NAME = "US Employer Identification Number (FEI number filed with the Florida Division of Corporations)"

#: ``meta.schema_version`` the reader accepts.
INDEX_SCHEMA_VERSION = "1"

DATA_DOWNLOADS_URL = "https://dos.fl.gov/sunbiz/other-services/data-downloads/"
#: The register's own record page, by document number.

#: The filing types the record layout documents, glossed. A type not listed
#: (``COOP`` once in Oct 2026) is shown as filed.
FILING_TYPES: dict[str, str] = {
    "DOMP": "Florida profit corporation",
    "DOMNP": "Florida not-for-profit corporation",
    "FORP": "foreign profit corporation",
    "FORNP": "foreign not-for-profit corporation",
    "DOMLP": "Florida limited partnership",
    "FORLP": "foreign limited partnership",
    "FLAL": "Florida limited liability company",
    "FORL": "foreign limited liability company",
    "NPREG": "non-profit registration",
    "TRUST": "declaration of trust",
    "AGENT": "designation of registered agent",
}

#: Filing types for an entity formed elsewhere and authorised in Florida.
FOREIGN_TYPES: frozenset[str] = frozenset({"FORP", "FORNP", "FORL", "FORLP"})

#: Officer titles as the Division's filers write them — a four-character free
#: field, not the six codes the layout documents (21.1M officer slots in Oct
#: 2026: MGR 4.53M, P 2.19M, D 2.06M, AMBR 1.92M, MGRM 1.30M, VP 0.91M, PD
#: 0.82M, "Pres" 0.35M …). Exact matches first (case-insensitive); a title
#: made only of the layout's single letters is read letter by letter (PSTD =
#: president, secretary, treasurer, director); anything else is shown as filed.
TITLE_WORDS: dict[str, str] = {
    "MGR": "manager",
    "MGRM": "managing member",
    "AMBR": "authorized member",
    "MBR": "member",
    "MEMB": "member",
    "MANA": "manager",
    "MGMR": "managing member",
    "AP": "authorized person",
    "AUTH": "authorized person",
    "AR": "authorized representative",
    "CEO": "chief executive officer",
    "CFO": "chief financial officer",
    "COO": "chief operating officer",
    "CTO": "chief technology officer",
    "PRES": "president",
    "DIR": "director",
    "DIRE": "director",
    "SEC": "secretary",
    "SECR": "secretary",
    "TREA": "treasurer",
    "TRES": "treasurer",
    "VP": "vice president",
    "VICE": "vice president",
    "TRE": "treasurer",
    "MEM": "member",
    "OFFI": "officer",
    "SVP": "senior vice president",
    "AVP": "assistant vice president",
    "EVP": "executive vice president",
    "CHAI": "chair",
    "CHAIR": "chair",
    "OWNE": "owner",
    "G": "general partner",
    "GP": "general partner",
    "TRUS": "trustee",
    "TR": "trustee",
    "EXEC": "executive",
    "PART": "partner",
}

#: The record layout's single-letter officer codes.
LETTER_TITLES: dict[str, str] = {
    "P": "president",
    "T": "treasurer",
    "C": "chair",
    "V": "vice president",
    "S": "secretary",
    "D": "director",
}

# ---------------------------------------------------------------------------
# Events (``corevent``): what the index keeps and how it reads them
# ---------------------------------------------------------------------------

#: Events that end a Florida entity (or a foreign one's authority in Florida),
#: with the words the finding and the status annotation use. Measured on the
#: 15,083,889 events of the October 2026 file.
TERMINAL_EVENTS: dict[str, str] = {
    "CORAMADMAR": "administratively dissolved for not filing an annual report",
    "CORAMADMRA": "administratively dissolved for want of a registered agent",
    "CORAMADMTX": "administratively dissolved for a tax warrant",
    "CORAMADDIS": "administratively dissolved",
    "CORAMINVOL": "involuntarily dissolved",
    "CORAMINVAR": "involuntarily dissolved for not filing an annual report",
    "CORAMINVRA": "involuntarily dissolved for want of a registered agent",
    "CORAMDSPRC": "dissolved by proclamation",
    "CORAMDCO": "dissolved by court order",
    "CORAPVOLDS": "voluntarily dissolved",
    "CORAPVDWN": "voluntarily dissolved",
    "CORAPVLDSI": "voluntarily dissolved",
    "CORLCVOLDS": "voluntarily dissolved",
    "CORLCVLDSI": "voluntarily dissolved",
    "CORALPCD": "dissolved",
    "CORALPCDWN": "dissolved",
    "CORALPST": "terminated",
    "CORLCSTERM": "terminated",
    "CORAPVOLCN": "voluntarily cancelled",
    "CORALPCAN": "cancelled",
    "CORAMCANNP": "cancelled for non-payment",
    "CORAMADVCN": "cancelled",
    "CORAMEXP": "expired",
    "CORAMREVAR": "authority revoked for not filing an annual report",
    "CORAMREVRA": "authority revoked for want of a registered agent",
    "CORAMREV": "authority revoked",
    "CORAMADREV": "authority revoked",
    "CORAPWITH": "withdrawn from Florida",
    "CORLCWITH": "withdrawn from Florida",
    "CORLCSWITH": "withdrawn from Florida",
    "CORALPSW": "withdrawn from Florida",
    "CORAPMER": "merged",
    "CORAPCMER": "merged",
    "CORMERNC": "merged",
    "GENACEMER": "merged",
}

#: Events that undo a dissolution or revocation.
REVERSAL_EVENTS: frozenset[str] = frozenset({
    "CORAPREIN", "CORAPREIWP", "CORLCREVDS", "CORAPREVDS", "CORLPREVDS", "CORAPAREIN",
})

#: Events whose name field is the entity's name *after* the change (checked
#: against the corporate file on 30 Oct 2026: 894 of 975 GLEIF-named entities'
#: latest name-change event carries their current name).
NAME_EVENTS: frozenset[str] = frozenset({
    "CORAPNC", "CORLCNC", "CORLPNC", "CORAPAMDNC", "CORLCAMDNC", "CORLPAMDNC",
    "CORAPCORNC", "CORLCCORNC", "CORAPARNAM", "CORLCARNAM", "CORMERNC",
    "CORLCSTCNC", "CORAPRNC", "CORAPAMNSN", "CORAPARNC", "CORLCRNC",
    "CORAPRCRNC", "LTDAPRCRNC", "CORALPCCNC",
})

#: Events that name another entity by document number (``other_doc``).
LINK_EVENTS: dict[str, str] = {
    "CORAPMER": "merger",
    "CORAPCMER": "merger",
    "CORMERNC": "merger",
    "CORAPMNEW": "merger",
    "GENACEMER": "merger",
    "GENACENEW": "merger",
    "CORAPCONV": "conversion",
    "CORAPCCONV": "conversion",
    "GENACECONV": "conversion",
    "CORAPCONS": "consolidation",
    "CORAPCCONS": "consolidation",
    "CORAPDOM": "domestication",
}

#: Every event code the builder keeps — the rest (amendments, agent changes,
#: annual-report notes) are not read by anything here.
KEPT_EVENTS: frozenset[str] = (
    frozenset(TERMINAL_EVENTS) | REVERSAL_EVENTS | NAME_EVENTS | frozenset(LINK_EVENTS)
)

COVERAGE_NOT_IN_INDEX = (
    "This document number is not in the Florida Division of Corporations' "
    "quarterly data as indexed — every active record and every record an LEI "
    "names. General partnerships and LLPs are filed separately, and a company "
    "formed after the quarterly cut is not yet in it."
)
COVERAGE_NAME_MISMATCH = (
    "The Florida Division of Corporations holds this document number for a "
    "different company, under a name that matches neither GLEIF's nor any "
    "name the register records for it. The number GLEIF files may be wrong."
)


# ---------------------------------------------------------------------------
# Small readers, shared by the builder, the mapper and the finding
# ---------------------------------------------------------------------------

_DOC_RE = re.compile(r"^[A-Z0-9]{6}$|^[A-Z0-9]{12}$")
_EIN_RE = re.compile(r"^(\d{2})-?(\d{7})$")


def normalise_doc(raw: str) -> str:
    """The register's document number (``L22000385605``, ``M70748``) from
    GLEIF's ``registeredAs`` or a caller.

    Upper-cased, spaces removed; 6 or 12 letters and digits — the Data Usage
    Guide's two lengths. An EIN-shaped value (``59-1234567``: five ISSUED
    LEIs filed the company's EIN instead) becomes ``FEI:591234567``, which
    ``fetch`` looks up on the FEI column. Anything else raises ``ValueError``.
    """
    text = re.sub(r"\s+", "", str(raw or "")).upper()
    ein = _EIN_RE.match(text)
    if ein and "-" in text:
        return f"FEI:{ein.group(1)}{ein.group(2)}"
    if text.startswith("FEI:") and re.fullmatch(r"\d{9}", text[4:]):
        return text
    if not _DOC_RE.match(text):
        raise ValueError(f"not a Florida document number: {raw!r}")
    return text


def clean_fei(raw: str | None) -> str | None:
    """A nine-digit FEI (EIN) or None — the field also holds ``APPLIED FOR``,
    ``NONE`` and zeros."""
    digits = re.sub(r"\D", "", str(raw or ""))
    if len(digits) != 9 or digits == "000000000":
        return None
    return digits


def format_ein(fei: str) -> str:
    """``591234567`` → ``59-1234567``, the IRS's spelling."""
    return f"{fei[:2]}-{fei[2:]}" if len(fei) == 9 else fei


def filing_type_label(code: str | None) -> str | None:
    text = (code or "").strip()
    if not text:
        return None
    return FILING_TYPES.get(text.upper(), text)


def is_foreign(code: str | None) -> bool:
    return (code or "").strip().upper() in FOREIGN_TYPES


def title_label(title: str | None) -> str:
    """An officer title in words; the filed title when it cannot be read."""
    text = (title or "").strip()
    if not text:
        return "officer"
    # "VP," / "MGR." / "MGR/": a code with a stray separator (measured on
    # the October 2026 file, 6.25M kept officer rows).
    key = text.upper().rstrip(".,/&- ")
    if not key:
        return text
    if key in TITLE_WORDS:
        return TITLE_WORDS[key]
    if key in LETTER_TITLES:
        return LETTER_TITLES[key]
    tokens = [t for t in re.split(r"[\s,/&]+", key) if t]
    if len(tokens) > 1 and all(t in TITLE_WORDS or t in LETTER_TITLES for t in tokens):
        # "P, C" / "P/D": several codes the filer separated.
        words = [TITLE_WORDS.get(t) or LETTER_TITLES[t] for t in tokens]
        return ", ".join(dict.fromkeys(words))
    if key.isalpha() and all(ch in LETTER_TITLES for ch in key):
        if key.startswith("VP"):
            rest = [LETTER_TITLES[ch] for ch in key[2:]]
            words = ["vice president", *rest]
        else:
            words = [LETTER_TITLES[ch] for ch in key]
        seen: list[str] = []
        for word in words:
            if word not in seen:
                seen.append(word)
        return ", ".join(seen)
    return text


def person_name(surname: str | None, given: str | None, middle: str | None) -> str:
    """``GARY S THOUBBORON`` from the layout's surname / first / middle split."""
    parts = [p.strip() for p in (given, middle, surname) if p and p.strip()]
    return re.sub(r"\s+", " ", " ".join(parts))


def jurisdiction_of(state_country: str | None) -> tuple[str, str] | None:
    """``(country name, ISO 3166-1 code)`` for the record's state/country of
    incorporation. US states → United States; a Canadian province → Canada;
    a code that is a country's alpha-2 → that country; anything else None
    (the risk engine's contract forbids a stand-in code)."""
    code = (state_country or "").strip().upper()
    if len(code) != 2:
        return None
    if pycountry.subdivisions.get(code=f"US-{code}") is not None:
        return ("United States", "US")
    if pycountry.subdivisions.get(code=f"CA-{code}") is not None:
        return ("Canada", "CA")
    country = pycountry.countries.get(alpha_2=code)
    if country is not None:
        return (country.name, country.alpha_2)
    return None


def event_day(event: dict[str, Any]) -> str | None:
    """An event's date: effective where the filer set one, else filed."""
    return event.get("effective") or event.get("filed") or None


def status_reading(
    status: str | None, filing_type: str | None, events: list[dict[str, Any]]
) -> dict[str, Any]:
    """What the register's status means, with its words and date.

    Returns ``{"class": live|terminal|inactive_foreign|unknown, "words": str,
    "since": ISO day | None, "event": code | None}``.
    """
    code = (status or "").strip().upper()
    if code == "A":
        return {"class": "live", "words": "active", "since": None, "event": None}
    if code != "I":
        return {"class": "unknown", "words": code or "unknown", "since": None, "event": None}
    terminal: dict[str, Any] | None = None
    for event in sorted(events, key=lambda e: int(e.get("seq") or 0)):
        ev = (event.get("code") or "").strip()
        if ev in TERMINAL_EVENTS:
            terminal = event
        elif ev in REVERSAL_EVENTS:
            terminal = None
    if terminal is not None:
        words = TERMINAL_EVENTS[terminal["code"]]
        since = event_day(terminal)
        ev_code = terminal["code"]
    else:
        words, since, ev_code = "inactive", None, None
    if is_foreign(filing_type):
        # Its authority in Florida ended; the company itself may be alive.
        return {
            "class": "inactive_foreign",
            "words": words if ev_code else "inactive in Florida",
            "since": since,
            "event": ev_code,
        }
    return {"class": "terminal", "words": words, "since": since, "event": ev_code}


def former_names(current: str | None, events: list[dict[str, Any]]) -> list[tuple[str, str | None]]:
    """Names the register recorded on name-change events, other than the
    current one, oldest first, each with the day it took effect. The name an
    entity was formed under is not in the events file (an event carries the
    name *after* the change)."""
    cur = names.org_comparable_name(current or "") if current else ""
    out: list[tuple[str, str | None]] = []
    seen: set[str] = set()
    for event in sorted(events, key=lambda e: int(e.get("seq") or 0)):
        if (event.get("code") or "") not in NAME_EVENTS:
            continue
        name = (event.get("name") or "").strip()
        if not name:
            continue
        key = names.org_comparable_name(name)
        if not key or key == cur or key in seen:
            continue
        seen.add(key)
        out.append((name, event_day(event)))
    return out


def renamed(events: list[dict[str, Any]]) -> bool:
    return any((e.get("code") or "") in NAME_EVENTS for e in events)


_NAME_SIMILARITY_THRESHOLD = names.NAME_MATCH_THRESHOLD


def _latin_agree(a: str, b: str) -> bool:
    ca, cb = names.org_comparable_name(a), names.org_comparable_name(b)
    if ca and ca == cb:
        return True
    if ca and names.despace(ca) == names.despace(cb):
        return True
    if names.distinctive_token_agreement(a, b):
        return True
    return names.name_similarity(a, b) >= _NAME_SIMILARITY_THRESHOLD


def name_check(legal_name: str | None, current: str | None, events: list[dict[str, Any]]) -> str:
    """How GLEIF's name relates to the register's record.

    ``agrees`` — the current name or a name on a name-change event;
    ``renamed`` — neither, but the register records that the entity was
    renamed, and the name it was formed under is the one name the events file
    never carries (the case GLEIF most often holds);
    ``differs`` — neither, and no rename on record: the number is probably
    wrong. An empty ``legal_name`` cannot contradict the number.
    """
    wanted = (legal_name or "").strip()
    if not wanted:
        return "agrees"
    candidates = [current or ""] + [n for n, _ in former_names(current, events)]
    if any(c and _latin_agree(wanted, c) for c in candidates):
        return "agrees"
    if renamed(events):
        return "renamed"
    return "differs"


def record_url(doc: str) -> str:
    """Where a record's statements say they were read from.

    The Division's record pages (search.sunbiz.org) are reached through a
    search form, sit behind a Cloudflare challenge and address a record by a
    per-result aggregate id, so there is no stable per-document address to
    link — never a guessed one (the Phase 296 rule). Statements cite the bulk
    data page the index was built from instead.
    """
    return DATA_DOWNLOADS_URL


# ---------------------------------------------------------------------------
# Index access
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``SUNBIZ_FLORIDA_DB_FILE`` or the data-root default — one function, so
    the reader, the boot download and the sweep's ``requires_files`` agree."""
    from ..cache import data_root

    configured = get_settings().sunbiz_florida_db_file
    return Path(configured) if configured else data_root() / "sunbiz_florida.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().sunbiz_florida_db_file:
            logger.warning("sunbiz_florida: index not found at %s", path)
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        version = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    except sqlite3.Error:
        version = None
    if not version or version[0] != INDEX_SCHEMA_VERSION:
        logger.warning(
            "sunbiz_florida: index at %s has an unexpected schema (want %s); ignoring",
            path, INDEX_SCHEMA_VERSION,
        )
        conn.close()
        return None
    return conn


def _shared_conn() -> sqlite3.Connection | None:
    if _SHARED.get("conn") is None:
        _SHARED["conn"] = _connect()
    return _SHARED.get("conn")


def reset_connection() -> None:
    """Forget the cached connection (after a download, or in tests)."""
    conn = _SHARED.pop("conn", None)
    if conn is not None:
        try:
            conn.close()
        except sqlite3.Error:  # pragma: no cover - best effort
            pass


def index_available() -> bool:
    return _shared_conn() is not None


def read_meta() -> dict[str, str]:
    conn = _shared_conn()
    if conn is None:
        return {}
    try:
        return {str(k): str(v) for k, v in conn.execute("SELECT key, value FROM meta")}
    except sqlite3.Error:  # pragma: no cover
        return {}


def cut_date(meta: dict[str, str] | None = None) -> datetime | None:
    """The day the Division generated the quarterly files the index read."""
    meta = read_meta() if meta is None else meta
    try:
        return datetime.strptime(meta.get("cut") or "", "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        return None


def declare_snapshot(meta: dict[str, str] | None = None) -> None:
    """Declare an index read as a snapshot: the quarterly files' date is the
    register's clock, ``built_at`` is ours (Phase 314)."""
    meta = read_meta() if meta is None else meta
    cut = meta.get("cut") or ""
    provenance.record_snapshot(
        retrieved_at=provenance.parse_moment(meta.get("built_at")),
        source_as_of=cut_date(meta),
        detail=(
            f"Florida Division of Corporations quarterly data, cut {cut}"
            if cut else "Florida Division of Corporations quarterly data"
        ),
    )


def warm_index() -> dict[str, Any]:
    """Download the index at boot when absent; replace it when the release
    asset changed; keep it otherwise. Never raises."""
    from ..entity_pages import (
        ASSET_STAMP_KEY,
        asset_check,
        download_db,
        record_asset_stamp,
    )
    from ..entity_pages import (
        read_meta as read_file_meta,
    )

    settings = get_settings()
    path = db_path()
    url = settings.sunbiz_florida_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {"sunbiz_florida": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"sunbiz_florida": f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {"sunbiz_florida": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("sunbiz_florida: asset check/download failed: %s", exc)
        return {"sunbiz_florida": f"failed: {exc}"}


_ENTITY_COLUMNS = (
    "doc", "name", "status", "filing_type", "file_date", "last_tx", "fei",
    "state_country", "p_addr1", "p_addr2", "p_city", "p_state", "p_zip",
    "p_country", "m_addr1", "m_addr2", "m_city", "m_state", "m_zip",
    "m_country", "agent_name", "agent_type", "agent_city", "agent_state",
    "more_than_six", "reports",
)
_ENTITY_SQL = "SELECT " + ", ".join(_ENTITY_COLUMNS) + " FROM entity"


def entity_record(doc: str) -> dict[str, Any] | None:
    conn = _shared_conn()
    if conn is None:
        return None
    row = conn.execute(_ENTITY_SQL + " WHERE doc = ?", (doc,)).fetchone()
    return dict(row) if row is not None else None


def entity_by_fei(fei: str) -> dict[str, Any] | None:
    """The one record filing this FEI; None when none or several do."""
    conn = _shared_conn()
    if conn is None:
        return None
    rows = conn.execute(_ENTITY_SQL + " WHERE fei = ? LIMIT 2", (fei,)).fetchall()
    return dict(rows[0]) if len(rows) == 1 else None


def officers_of(doc: str) -> list[dict[str, Any]]:
    conn = _shared_conn()
    if conn is None:
        return []
    rows = conn.execute(
        "SELECT seq, title, type, surname, given, middle, name, city, state"
        " FROM officer WHERE doc = ? ORDER BY seq",
        (doc,),
    ).fetchall()
    return [dict(r) for r in rows]


def events_of(doc: str) -> list[dict[str, Any]]:
    conn = _shared_conn()
    if conn is None:
        return []
    rows = conn.execute(
        "SELECT seq, code, description, effective, filed, other_doc, name"
        " FROM event WHERE doc = ? ORDER BY seq",
        (doc,),
    ).fetchall()
    return [dict(r) for r in rows]


def names_of(docs: list[str]) -> dict[str, str]:
    """Names of other records the index holds (a merger partner)."""
    conn = _shared_conn()
    if conn is None or not docs:
        return {}
    marks = ",".join("?" for _ in docs)
    return {
        r["doc"]: r["name"]
        for r in conn.execute(f"SELECT doc, name FROM entity WHERE doc IN ({marks})", docs)
    }


def parse_reports(raw: str | None) -> list[dict[str, str]]:
    """``"2026:2026-03-02;2025:2025-04-11"`` → newest first."""
    out: list[dict[str, str]] = []
    for part in (raw or "").split(";"):
        year, _, day = part.partition(":")
        if year.strip():
            out.append({"year": year.strip(), "filed": day.strip()})
    return sorted(out, key=lambda r: r["year"], reverse=True)


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class SunbizFloridaAdapter(SourceAdapter):
    """Source adapter for the Florida Division of Corporations (local index)."""

    id = "sunbiz_florida"

    lookup_derivers = (
        LookupDeriver(frozenset({FL_RA_CODE}), "us_fl_doc", normalise_doc),
    )
    lookup_pass_legal_name = True

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="Florida Division of Corporations (Sunbiz)",
            homepage="https://dos.fl.gov/sunbiz/",
            description=(
                "Florida's register of corporations, LLCs, limited "
                "partnerships and foreign qualifications, read from a "
                "quarterly index of the Division of Corporations' bulk data: "
                "name, status with the dated event behind it, filing type, "
                "file date, FEI (EIN) number, principal address, registered "
                "agent, annual reports, former names and up to six officers. "
                "No shareholders or beneficial owners are published."
            ),
            license="FL-Public-Records",
            attribution=(
                "Contains information from the Florida Department of State, "
                "Division of Corporations (sunbiz.org) bulk data downloads — "
                "Florida public records."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=index_available(),
            is_national_register=True,
            country="US",
        )

    def covers_lei(self, lei: str) -> bool:
        """Gate dispatch on the index file, not on the LEI (the JAR gate)."""
        return index_available()

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY or not (query or "").strip():
            return []
        conn = _shared_conn()
        if conn is None:
            return []
        terms = [t for t in re.findall(r"\w+", query.upper()) if t]
        if not terms:
            return []
        match = " ".join(f'"{t}"' for t in terms)
        try:
            rows = conn.execute(
                _ENTITY_SQL
                + " WHERE rowid IN (SELECT rowid FROM entity_fts WHERE entity_fts MATCH ? LIMIT 200)"
                " ORDER BY name = ? DESC, status = 'A' DESC, name LIMIT 10",
                (match, query.strip().upper()),
            ).fetchall()
        except sqlite3.Error:
            return []
        if not rows:
            return []
        declare_snapshot()
        return [self._search_hit(dict(row)) for row in rows]

    @staticmethod
    def _search_hit(row: dict[str, Any]) -> SourceHit:
        doc = row["doc"]
        segments = [f"US-FL {doc}"]
        form = filing_type_label(row.get("filing_type"))
        if form:
            segments.append(form)
        if (row.get("status") or "") != "A":
            segments.append("inactive")
        return SourceHit(
            source_id="sunbiz_florida",
            hit_id=doc,
            kind=SearchKind.ENTITY,
            name=row.get("name") or f"US-FL {doc}",
            summary=" · ".join(segments),
            identifiers={"us_fl_doc": doc},
            raw={"doc": doc},
            is_stub=False,
            liveness="snapshot",
        )

    def _stub(self, doc: str, legal_name: str) -> dict[str, Any]:
        """Nothing was read: no index, or not a number. Dropped by the pipeline."""
        return {
            "source_id": self.id,
            "doc": doc,
            "name": legal_name or "",
            "legal_name": legal_name,
            "officers": [],
            "events": [],
            "link": DATA_DOWNLOADS_URL,
            "is_stub": True,
        }

    def _note(self, doc: str, legal_name: str, meta: dict[str, str], *,
              note: str, registered_name: str | None = None,
              name_mismatch: bool = False) -> dict[str, Any]:
        """The index answered and holds no record we may show — a note card
        (``is_stub`` False, ``not_found`` True). Not a degradation."""
        bundle = self._stub(doc, legal_name)
        bundle.update(
            {
                "is_stub": False,
                "not_found": True,
                "coverage_note": note,
                "cut": meta.get("cut"),
            }
        )
        if name_mismatch:
            bundle["name_mismatch"] = True
        if registered_name:
            bundle["registered_name"] = registered_name
        return bundle

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the index's record for one document number.

        ``hit_id`` is GLEIF's ``registeredAs`` (``L22000385605``), a bare
        document number, or ``FEI:<nine digits>`` for an LEI that filed the
        company's EIN; ``legal_name`` is GLEIF's name, checked against the
        register's current and former names.
        """
        raw = str(hit_id or "").strip()
        try:
            key = normalise_doc(raw)
        except ValueError:
            logger.warning("sunbiz_florida: not a document number: %r", hit_id)
            return self._stub(raw, legal_name)
        conn = _shared_conn()
        if conn is None:
            return self._stub(key, legal_name)
        try:
            meta = read_meta()
            if key.startswith("FEI:"):
                row = entity_by_fei(key[4:])
            else:
                row = entity_record(key)
            if row is None:
                return self._note(key, legal_name, meta, note=COVERAGE_NOT_IN_INDEX)
            doc = row["doc"]
            events = events_of(doc)
            check = name_check(legal_name, row.get("name"), events)
            if check == "differs":
                logger.info("sunbiz_florida: %s names a different company", doc)
                return self._note(
                    doc, legal_name, meta, note=COVERAGE_NAME_MISMATCH,
                    registered_name=row.get("name"), name_mismatch=True,
                )
            officers = officers_of(doc)
            linked = sorted({
                (e.get("other_doc") or "").strip().upper()
                for e in events
                if (e.get("code") or "") in LINK_EVENTS and (e.get("other_doc") or "").strip()
            } - {doc})
            partner_names = names_of(linked)
        except sqlite3.Error as exc:
            degradation.record(
                self.id,
                "The Florida register index could not be read; the card shows "
                "nothing from the register. This is not a confirmation that "
                "the register holds nothing.",
                reason=degradation.reason_for_failure(type(exc).__name__),
            )
            return self._stub(key, legal_name)

        declare_snapshot(meta)
        reading = status_reading(row.get("status"), row.get("filing_type"), events)
        principal = {
            k: row.get(f"p_{k}") for k in ("addr1", "addr2", "city", "state", "zip", "country")
            if row.get(f"p_{k}")
        }
        mailing = {
            k: row.get(f"m_{k}") for k in ("addr1", "addr2", "city", "state", "zip", "country")
            if row.get(f"m_{k}")
        }
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "doc": doc,
            "name": row.get("name") or legal_name or f"US-FL {doc}",
            "legal_name": legal_name,
            "status": row.get("status"),
            "status_class": reading["class"],
            "status_words": reading["words"],
            "status_since": reading["since"],
            "status_event": reading["event"],
            "filing_type": row.get("filing_type"),
            "file_date": row.get("file_date"),
            "last_transaction": row.get("last_tx"),
            "fei": clean_fei(row.get("fei")),
            "state_country": row.get("state_country"),
            "principal_address": principal or None,
            "mailing_address": mailing or None,
            "registered_agent": (
                {
                    "name": row.get("agent_name"),
                    "type": row.get("agent_type"),
                    "city": row.get("agent_city"),
                    "state": row.get("agent_state"),
                }
                if row.get("agent_name") else None
            ),
            "more_than_six_officers": (row.get("more_than_six") or "") == "Y",
            "annual_reports": parse_reports(row.get("reports")),
            "officers": officers,
            "events": events,
            "former_names": [
                {"name": n, "since": d} for n, d in former_names(row.get("name"), events)
            ],
            "linked_records": [
                {"doc": d, "name": partner_names.get(d)} for d in linked
            ],
            "name_check": check,
            "matched_via": "fei" if key.startswith("FEI:") else "document_number",
            "cut": meta.get("cut"),
            "link": record_url(doc),
            "is_stub": False,
        }
        validate_raw("sunbiz_florida", SunbizFloridaBundle, bundle)
        return bundle


__all__ = [
    "COVERAGE_NAME_MISMATCH",
    "COVERAGE_NOT_IN_INDEX",
    "EIN_SCHEME",
    "FILING_TYPES",
    "FL_RA_CODE",
    "FL_SCHEME",
    "FL_SCHEME_NAME",
    "INDEX_SCHEMA_VERSION",
    "KEPT_EVENTS",
    "SunbizFloridaAdapter",
    "clean_fei",
    "filing_type_label",
    "name_check",
    "normalise_doc",
    "status_reading",
    "title_label",
]
