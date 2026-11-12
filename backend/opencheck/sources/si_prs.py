"""Slovenia — PRS, the Slovenian Business Register (Poslovni register
Slovenije), kept by AJPES and published as an open CSV on podatki.gov.si.

An **index-tier** register source, the ``jar_lithuania`` / ``cyprus_drcor``
shape: the lookup reads a SQLite index of the register's open file,
downloaded at boot from the ``si-prs-index`` release asset
(``SI_PRS_DB_URL``) and replaced when the asset changes. Dispatch is gated
on the index being present (``covers_lei``); a number the index does not
hold is a note card, never a failure.

Why the index is built by hand
------------------------------
podatki.gov.si answers **HTTP 407** to every client that is not a browser:
the cloud, a GitHub Actions runner and ``curl`` on a home connection alike
(measured 12 Nov 2026). A browser download works. So the file is saved from
a browser and ``scripts/refresh_si_prs_index.sh`` builds and uploads the
index from it — no User-Agent games, no workflow.

What the file holds (measured 12 Nov 2026)
------------------------------------------
``opsiprs.csv``, 128 MB, **UTF-16LE with a BOM**, CRLF, every field quoted,
twelve columns: the ten-digit registration number (*matična številka*), the
full name, a house identifier (``HSEID``), the legal form in words, the
registering authority (an AJPES branch office or a district court), the
registered address (street, house number and suffix, settlement, postcode,
post office, country). **No status, no dates, no people** — no officers,
members or beneficial owners. Slovenia's BO register (RDL) is a separate,
restricted system. Licence CC BY 4.0, updated fortnightly.

What the index keeps (Stephen, 12 Nov 2026)
-------------------------------------------
* Every unit, sole traders included, so a sole trader's LEI (GLEIF files 21
  ``SOLE_PROPRIETOR`` records in Slovenia) still joins.
* A natural person's street and house number are **never** stored — a sole
  trader's registered address is usually their home. The settlement and
  postcode are kept. ``HSEID`` is never stored for anyone.
* Name search covers legal persons only.
* Which forms are a legal person is decided by the form's own words
  (``form_is_legal_person``): a form with no legal-person marker is treated
  as a natural person's, so an unforeseen form loses its street rather than
  publishing one.

No status is inferred: the file states none, and appearing in it says the
unit is on the register, nothing about whether it trades.

Licence: CC BY 4.0, AJPES (Agencija Republike Slovenije za javnopravne
evidence in storitve), via podatki.gov.si.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .. import degradation, provenance
from ..config import get_settings
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.si_prs import SiPrsBundle

logger = logging.getLogger(__name__)

#: GLEIF Registration Authority code for the Slovenian Business Register
#: (AJPES). 3,077 of 3,081 ISSUED Slovenian LEIs file under it, every one a
#: bare ten-digit registration number (12 Nov 2026). RA000997 (the ATVP funds
#: register, 4 records) files fund numbers and is not dispatched.
SI_PRS_RA_CODE: str = "RA000529"

#: ``meta.schema_version`` the reader accepts.
INDEX_SCHEMA_VERSION = "1"

DATASET_URL = "https://podatki.gov.si/dataset/poslovni-register-slovenije"
CSV_URL = (
    "https://podatki.gov.si/dataset/9ee1a9aa-c224-4995-b2ad-3760d7af0748/"
    "resource/beb70929-3d0d-41c6-9af2-25d525d906d3/download/opsiprs.csv"
)
#: AJPES's public register search (ePRS).
SEARCH_URL = "https://www.ajpes.si/prs/"

COVERAGE_NOT_IN_INDEX = (
    "This registration number is not in the Slovenian Business Register's "
    "open file as indexed. GLEIF may file a number the register does not use "
    "(a fund's, for instance), the unit may have been struck off, or it may "
    "be newer than the index."
)

#: Words in the form that make it a natural person's, matched before the
#: legal-person markers: a sole trader (*samostojni podjetnik posameznik
#: s.p.*), a registered room renter, a farmer, a private practitioner.
_NATURAL_PERSON_MARKERS = re.compile(
    r"(\bs\.\s?p\.|posameznik|sobodajal|\bkmet\b|zasebnik|fizičn|dopolnilne? dejavnost|na kmetiji)",
    re.IGNORECASE,
)
#: Words in the form that make it a legal person's.
_LEGAL_PERSON_MARKERS = re.compile(
    r"(d\.\s?o\.\s?o\.|\bd\.\s?d\.|\bk\.\s?d\.|d\.\s?n\.\s?o\.|z\.\s?o\.\s?o\.|z\.\s?b\.\s?o\."
    r"|družba|zadrug|zavod|društv|ustanov|podružnic|združenj|zvez|zbornic|sindikat"
    r"|strank|skupnost|agencij|sklad|občin|ministrstv|uprav|organ|\bjavn|evropsk|gospodarsk)",
    re.IGNORECASE,
)

#: Legal-form glosses, the first match wins (longer forms first).
_FORM_GLOSSES: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bs\.\s?p\.|posameznik", re.I), "sole trader"),
    (re.compile(r"sobodajal", re.I), "registered room renter"),
    (re.compile(r"k\.\s?d\.\s?d\.", re.I), "partnership limited by shares"),
    (re.compile(r"d\.\s?o\.\s?o\.", re.I), "private limited company"),
    (re.compile(r"evropsk\w* delniš", re.I), "European company (SE)"),
    (re.compile(r"\bd\.\s?d\.", re.I), "public limited company"),
    (re.compile(r"\bk\.\s?d\.", re.I), "limited partnership"),
    (re.compile(r"d\.\s?n\.\s?o\.", re.I), "general partnership"),
    (re.compile(r"podružnic", re.I), "branch"),
    (re.compile(r"zadrug|z\.\s?o\.\s?o\.|z\.\s?b\.\s?o\.", re.I), "cooperative"),
    (re.compile(r"gospodarsko interesno", re.I), "economic interest grouping"),
    (re.compile(r"zavod", re.I), "institute"),
    (re.compile(r"ustanov", re.I), "foundation"),
    (re.compile(r"društv", re.I), "association"),
)


def normalise_maticna(raw: str) -> str:
    """The ten-digit registration number, or "" when *raw* is not one.

    GLEIF files the bare ten digits. A seven-digit number is the unit's base
    (its check digit included) and names the main unit, ``000``.
    """
    digits = re.sub(r"\D", "", str(raw or ""))
    if len(digits) == 10:
        return digits
    if len(digits) == 7:
        return digits + "000"
    return ""


def form_is_legal_person(form: str | None) -> bool:
    """True when the form's own words name a legal person (see the module
    docstring): an unrecognised form is read as a natural person's."""
    text = form or ""
    if _NATURAL_PERSON_MARKERS.search(text):
        return False
    return bool(_LEGAL_PERSON_MARKERS.search(text))


def form_label(form: str | None) -> str | None:
    """English gloss of the register's legal-form words, or None."""
    text = form or ""
    for pattern, gloss in _FORM_GLOSSES:
        if pattern.search(text):
            return gloss
    return None


def organ_label(organ: str | None) -> str | None:
    """The registering authority in English: ``AJPES, izpostava Celje`` →
    "AJPES's Celje branch office"; a district court keeps its own words."""
    text = re.sub(r"\s+", " ", (organ or "")).strip()
    if not text:
        return None
    m = re.match(r"AJPES,?\s+izpostava\s+(.+)$", text, re.IGNORECASE)
    if m:
        return f"AJPES's {m.group(1)} branch office"
    if text.lower().startswith("okrožno sodišče"):
        return f"the district court ({text})"
    return text


# ---------------------------------------------------------------------------
# Index access
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``SI_PRS_DB_FILE`` or the data-root default — one function, so the
    reader, the boot download and the sweep's ``requires_files`` agree."""
    from ..cache import data_root

    configured = get_settings().si_prs_db_file
    return Path(configured) if configured else data_root() / "si_prs.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().si_prs_db_file:
            logger.warning("si_prs: index not found at %s", path)
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        version = conn.execute(
            "SELECT value FROM meta WHERE key = 'schema_version'"
        ).fetchone()
    except sqlite3.Error:
        version = None
    if not version or version[0] != INDEX_SCHEMA_VERSION:
        logger.warning(
            "si_prs: index at %s has an unexpected schema (want %s); ignoring",
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
    """True when a readable index of the expected schema is present."""
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
    """The day podatki.gov.si last changed the file the index was built from."""
    meta = read_meta() if meta is None else meta
    try:
        return datetime.strptime(meta.get("cut") or "", "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        return None


def declare_snapshot(meta: dict[str, str] | None = None) -> None:
    """Declare an index read as a snapshot: the portal's cut is the register's
    clock, ``built_at`` is ours (Phase 314)."""
    meta = read_meta() if meta is None else meta
    cut = meta.get("cut") or ""
    provenance.record_snapshot(
        retrieved_at=provenance.parse_moment(meta.get("built_at")),
        source_as_of=cut_date(meta),
        detail=(
            f"AJPES Slovenian Business Register open file, cut {cut}"
            if cut else "AJPES Slovenian Business Register open file"
        ),
    )


def warm_index() -> dict[str, Any]:
    """Download the index at boot when absent; replace it when the release
    asset changed; keep it otherwise. Never raises — no index is a state
    this module models (``covers_lei`` is False, the source not announced)."""
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
    url = settings.si_prs_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {"si_prs": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"si_prs": f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {"si_prs": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("si_prs: asset check/download failed: %s", exc)
        return {"si_prs": f"failed: {exc}"}


_ENTITY_SQL = (
    "SELECT number, name, form, organ, street, house, house_suffix, settlement,"
    " postcode, post_office, country, legal_person FROM entity"
)


def entity_record(number: str) -> dict[str, Any] | None:
    """The index's row for one registration number; None when the index does
    not hold it (or there is no index)."""
    conn = _shared_conn()
    if conn is None:
        return None
    row = conn.execute(_ENTITY_SQL + " WHERE number = ?", (number,)).fetchone()
    return dict(row) if row is not None else None


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class SiPrsAdapter(SourceAdapter):
    """Source adapter for the Slovenian Business Register (local SQLite index)."""

    id = "si_prs"

    lookup_derivers = (
        LookupDeriver(frozenset({SI_PRS_RA_CODE}), "si_maticna", normalise_maticna),
    )
    lookup_pass_legal_name = True

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="AJPES — Slovenian Business Register (PRS)",
            homepage="https://www.ajpes.si/",
            description=(
                "Slovenia's Business Register (Poslovni register Slovenije), "
                "kept by AJPES and read from an index of its open file on "
                "podatki.gov.si: name, ten-digit registration number, legal "
                "form, the registering authority and the registered address "
                "(a sole trader's settlement and postcode only). The file "
                "publishes no status, dates, officers, shareholders or "
                "beneficial owners."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Contains information from the Slovenian Business Register "
                "(Poslovni register Slovenije), AJPES, published on "
                "podatki.gov.si under CC BY 4.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=index_available(),
            is_national_register=True,
            country="SI",
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
        q = query.strip()
        try:
            # Legal persons only: a sole trader's name is a person's.
            rows = conn.execute(
                _ENTITY_SQL
                + " WHERE legal_person = 1 AND name LIKE ?"
                " ORDER BY upper(name) = upper(?) DESC, name LIMIT 10",
                (f"%{q}%", q),
            ).fetchall()
        except sqlite3.Error:
            return []
        if not rows:
            return []
        declare_snapshot()
        return [self._search_hit(dict(row)) for row in rows]

    @staticmethod
    def _search_hit(row: dict[str, Any]) -> SourceHit:
        segments = [f"SI {row['number']}"]
        form = form_label(row.get("form"))
        if form:
            segments.append(form)
        if row.get("settlement"):
            segments.append(str(row["settlement"]))
        return SourceHit(
            source_id="si_prs",
            hit_id=row["number"],
            kind=SearchKind.ENTITY,
            name=row.get("name") or f"SI {row['number']}",
            summary=" · ".join(segments),
            identifiers={"si_maticna": row["number"]},
            raw={"number": row["number"]},
            is_stub=False,
            liveness="snapshot",
        )

    def _stub(self, number: str, legal_name: str) -> dict[str, Any]:
        """Nothing was read: no index, or not a number. Dropped by the pipeline."""
        return {
            "source_id": self.id,
            "number": number,
            "name": legal_name or "",
            "legal_name": legal_name,
            "link": DATASET_URL,
            "is_stub": True,
        }

    def _not_in_index(self, number: str, legal_name: str, meta: dict[str, str]) -> dict[str, Any]:
        """The index answered and holds no unit of this number — a note card
        (``is_stub`` False, ``not_found`` True, the JAR shape). Not cached and
        not a degradation: nothing failed."""
        bundle = self._stub(number, legal_name)
        bundle.update(
            {
                "is_stub": False,
                "not_found": True,
                "coverage_note": COVERAGE_NOT_IN_INDEX,
                "cut": meta.get("cut"),
            }
        )
        return bundle

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the index's record for one registration number.

        ``hit_id`` is the number GLEIF files under ``registeredAs`` (ten
        digits); ``legal_name`` is GLEIF's name, used for the card when the
        register holds nothing.
        """
        number = normalise_maticna(hit_id)
        if not number:
            logger.warning("si_prs: not a registration number: %r", hit_id)
            return self._stub(str(hit_id or "").strip(), legal_name)
        if _shared_conn() is None:
            return self._stub(number, legal_name)
        try:
            row = entity_record(number)
            meta = read_meta()
        except sqlite3.Error as exc:
            degradation.record(
                self.id,
                "The Slovenian Business Register index could not be read; the "
                "card shows nothing from the register. This is not a "
                "confirmation that the register holds nothing.",
                reason=degradation.reason_for_failure(type(exc).__name__),
            )
            return self._stub(number, legal_name)
        if row is None:
            return self._not_in_index(number, legal_name, meta)

        # Declared only once a row is in hand: a miss is not a read of data.
        declare_snapshot(meta)
        address = {
            k: row.get(k)
            for k in ("street", "house", "house_suffix", "settlement", "postcode", "post_office", "country")
            if row.get(k)
        }
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "number": number,
            "name": row.get("name") or legal_name or f"SI {number}",
            "legal_name": legal_name,
            "form": row.get("form"),
            "organ": row.get("organ"),
            "legal_person": bool(row.get("legal_person")),
            "address": address or None,
            "cut": meta.get("cut"),
            "link": DATASET_URL,
            "is_stub": False,
        }
        validate_raw("si_prs", SiPrsBundle, bundle)
        return bundle
