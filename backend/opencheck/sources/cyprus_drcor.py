"""Cyprus — DRCOR, the Department of Registrar of Companies and Intellectual
Property (data.gov.cy open data).

An **index-tier** register source, the ``jar_lithuania`` shape: the lookup
reads a SQLite index of the register's three monthly CSV distributions,
downloaded at boot from the ``cyprus-drcor-index`` release asset
(``CYPRUS_DRCOR_DB_URL``) and replaced when the asset changes. Dispatch is
gated on the index being present (``covers_lei``); a number the index does
not hold is a note card, never a failure. The index is rebuilt monthly by
``.github/workflows/refresh-cyprus-drcor-index.yml`` running
``scripts/build_cyprus_drcor_index.py`` — data.gov.cy answers a datacentre
address, so unlike Lithuania's or Ukraine's this one is a workflow.

What the register publishes
---------------------------
The *Register of Registered Companies, Commercial Names and Cooperatives in
Cyprus*, an EU High-Value Dataset under CC BY 4.0: every organisation's name,
type (company, business name, partnership, overseas company), sub-type
(private, public, guarantee; general or limited partnership), registration
date, the register's status in Greek with the date it took effect, a
registered-office address, and its **officials** — directors, secretaries,
partners, the owner of a business name, alternate directors, authorised
persons of overseas companies. **No shareholders and no beneficial owners**;
Cyprus's BO register is a separate, restricted system. An official is a
name and a position: no person/organisation flag, no country, no date.

Why bulk and not live
---------------------
data.gov.cy is a Drupal/EKAN portal with no query API for these files —
``/api/1/datastore/query`` answers 404 — and the officials file alone is
127 MB. The three files are replaced monthly, with a counter in each file
name that moves on every release (``organisations_102.csv`` on 29 Sep 2026),
so the builder reads the dataset page for the current links.

Format traps, each measured against the 29 Sep 2026 release
-------------------------------------------------------------
* **The registration number is not a key on its own.** 574,132 organisations
  are keyed by ``(ORGANISATION_TYPE_CODE, REGISTRATION_NO)``: company 1
  (type ``C``) and overseas company 1 (type ``O``) are different. GLEIF
  files companies as ``ΗΕ 449149`` — **Greek** capital eta and epsilon — so
  ``normalise_he_number`` keeps the digits and the lookup reads type ``C``.
* Dates are ``DD/MM/YYYY``; the status date is the dissolution date where
  the status is terminal.
* 13 organisation rows and ~30 official rows are malformed (an unquoted
  newline in a name); the builder skips and counts them.

Licence: CC BY 4.0, Department of Registrar of Companies and Intellectual
Property (Republic of Cyprus), via data.gov.cy.
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
from .schemas.cyprus_drcor import CyprusBundle

logger = logging.getLogger(__name__)

#: GLEIF Registration Authority code for the Cyprus Companies Section (DRCOR).
CY_DRCOR_RA_CODE: str = "RA000161"

#: The register's organisation type codes, with an English gloss.
TYPE_CODES: dict[str, str] = {
    "C": "company",
    "B": "business name",
    "P": "partnership",
    "O": "overseas company",
    "N": "partnership (BN)",
}

#: The prefix the register writes before a number of each type — ``HE`` for
#: a company (``ΗΕ`` in Greek on GLEIF), ``S`` partnership, ``AE`` overseas
#: company, ``BN`` business name.
TYPE_PREFIXES: dict[str, str] = {"C": "HE", "P": "S", "O": "AE", "B": "BN", "N": "BN"}

#: Sub-types, glossed.
SUB_TYPE_GLOSSES: dict[str, str] = {
    "Ιδιωτική": "private",
    "Δημόσια": "public",
    "Δι΄ Εγγυήσεως": "limited by guarantee",
    "Δι΄ Εγγυήσεως Χωρίς Κεφάλαιο": "limited by guarantee without share capital",
    "Ομόρρυθμος": "general",
    "Ετερόρρυθμος": "limited",
}

#: The register's status vocabulary: class and English gloss. Every value
#: seen in the 29 Sep 2026 release is here; an unlisted one reads
#: ``unknown`` and is shown in the register's own words.
STATUSES: dict[str, tuple[str, str]] = {
    "Εγγεγραμμένη": ("live", "Registered"),
    "Στάληκε επιστολή Υπενθύμισης": ("live", "Registered — reminder letter sent"),
    "Δημοσιεύτηκε η Τρίμηνη": ("pending", "Three-month strike-off notice published"),
    "Εκούσια Εκκαθάριση από Μετόχους": ("pending", "Members' voluntary liquidation"),
    "Εκούσια Εκκαθάριση από Πιστωτές": ("pending", "Creditors' voluntary liquidation"),
    "Εκκαθάριση με Διάταγμα Δικαστηρίου": ("pending", "Liquidation by court order"),
    "Υπο Διαχείριση": ("pending", "Under administration"),
    "Υπο Διαχείριση & Υπο Εκκαθάριση": ("pending", "Under administration and liquidation"),
    "Υπο Εκκαθάριση": ("pending", "In liquidation"),
    "Διαγραμμένη": ("terminal", "Struck off"),
    "Διάλυση λόγω Ολοκλήρωσης Εκούσιας Εκκαθάρισης": ("terminal", "Dissolved after voluntary liquidation"),
    "Διάλυση λόγω Ολοκλήρωσης Εκκαθάρισης": ("terminal", "Dissolved after liquidation"),
    "Διαλύθηκε λόγω συγχώνευσης": ("terminal", "Dissolved by merger"),
    "Διορισμός Προσωρινού Εκκαθαριστή": ("pending", "Provisional liquidator appointed"),
    "Εκκαθάριση με Διάταγμα Δικαστηρίου& Διορ.Ειδικού Διαχειριστή": (
        "pending", "Liquidation by court order; special administrator appointed"
    ),
    "Αφαιρέθηκε από το Μητρώο ΕΕ με απόφαση Ανωτάτου Δικαστηρίου": (
        "terminal", "Removed from the register of business names by Supreme Court order"
    ),
    # "Ευρωπαϊκή Εταιρεία" (6 rows) is a form, not a status: unknown.
}

#: The nine official positions, glossed; the mapper decides the interest.
POSITIONS: dict[str, str] = {
    "Διευθυντής": "Director",
    "Αντικαταστάτης Διευθυντής": "Alternate director",
    "Γραμματέας": "Secretary",
    "Βοηθός Γραμματέας": "Assistant secretary",
    "Αναπληρωτής Γραμματέας": "Deputy secretary",
    "Ιδιοκτήτης": "Owner",
    "Ομόρρυθμος Συνέταιρος": "General partner",
    "Ετερόρρυθμος Συνέταιρος": "Limited partner",
    "Εξουσιοδοτημένο Πρόσωπο": "Authorised person",
}

#: ``meta.schema_version`` the reader accepts.
INDEX_SCHEMA_VERSION = "1"

DATASET_URL = (
    "https://www.data.gov.cy/el/dataset/"
    "mitroo-eggegrammenon-etaireion-emporikon-eponymion-kai-synetairismon-stin-kypro"
)
#: Public DRCOR search UI.
SEARCH_URL = "https://efiling.drcor.mcit.gov.cy/DrcorPublic/SearchForm.aspx?lang=EN"
_SEARCH_URL = SEARCH_URL

COVERAGE_NOT_IN_INDEX = (
    "This registration number is not in the Registrar of Companies' open data "
    "as indexed, among companies (HE numbers). GLEIF may hold a number the "
    "register does not use, or the company may be newer than the index."
)


def normalise_he_number(raw: str) -> str:
    """Return the numeric registration number from a Cyprus HE identifier.

    GLEIF files ``"ΗΕ 489243"`` (Greek capital eta + epsilon); a caller may
    pass Latin ``"HE489243"`` or a bare ``"489243"``. The digits are the
    index key, read as a company (type ``C``).
    """
    return re.sub(r"\D", "", str(raw or ""))


def he_type_code(raw: str) -> str:
    """The Latin prefix of a registration identifier, ``HE`` for ``ΗΕ``.

    Returns "" when there is none. Kept for callers that show the prefix;
    the lookup itself reads every HE number as a company.
    """
    s = str(raw or "").strip()
    m = re.match(r"\s*([A-Za-zΑ-Ωα-ω]{1,3})", s)
    if not m:
        return ""
    return m.group(1).upper().translate(str.maketrans("ΗΕΒΣΑΝ", "HEBSAN"))


def status_class(status: str | None) -> str:
    """``live`` / ``pending`` / ``terminal`` / ``unknown`` for a register status."""
    return STATUSES.get((status or "").strip(), ("unknown", ""))[0]


def status_label(status: str | None) -> str | None:
    text = (status or "").strip()
    if not text:
        return None
    return STATUSES.get(text, (None, text))[1] or text


def position_label(position: str | None) -> str | None:
    text = (position or "").strip()
    if not text:
        return None
    return POSITIONS.get(text, text)


def form_label(type_code: str | None, sub_type: str | None) -> str | None:
    """"Private company" / "general partnership" / "overseas company"…"""
    kind = TYPE_CODES.get((type_code or "").strip())
    if not kind:
        return None
    sub = SUB_TYPE_GLOSSES.get((sub_type or "").strip())
    if sub and kind == "partnership":
        return f"{sub} partnership"
    if sub:
        return f"{sub} {kind}"
    return kind


def display_number(type_code: str, reg_no: str) -> str:
    """``HE 449149`` — the register's own prefix for the type, in Latin."""
    prefix = TYPE_PREFIXES.get(type_code, type_code)
    return f"{prefix} {reg_no}" if prefix else reg_no


#: GLEIF files a Cypriot company's number as ``ΗΕ 449149`` — Greek capital
#: eta and epsilon, then a space (200 of 200 ISSUED records sampled, 29 Oct
#: 2026; one of 200 without the space), which is how the register itself
#: prints it. The BODS identifier is written the same way so the GLEIF and
#: register statements corroborate on ``CY-DRCOR`` (Phase 239: the scheme
#: follows the number, and the number is spelled as the register spells it).
REGISTER_PREFIXES: dict[str, str] = {"C": "ΗΕ", "P": "S", "O": "AE", "B": "BN", "N": "BN"}


def register_identifier(type_code: str, reg_no: str) -> str:
    """The number as the register (and GLEIF) spell it: ``ΗΕ 449149``."""
    prefix = REGISTER_PREFIXES.get(type_code, type_code)
    return f"{prefix} {reg_no}" if prefix else reg_no


def he_number_spellings(raw: str) -> tuple[str, ...]:
    """Every way an HE number is written, for GLEIF's exact-match filter.

    GLEIF files ``ΗΕ 449149`` (Greek eta/epsilon, a space), occasionally
    without the space; a caller types ``HE 449149`` (Latin) or ``449149``.
    ``registeredAs`` matches the stored string exactly, so the National ID
    picker's own placeholder found nothing (post-deploy test, 30 Oct 2026).
    Greek first, then Latin, then the bare digits; the input unchanged when
    it holds no digits.
    """
    digits = normalise_he_number(raw)
    if not digits:
        return ((raw or "").strip(),)
    return (f"ΗΕ {digits}", f"ΗΕ{digits}", f"HE {digits}", f"HE{digits}", digits)


# ---------------------------------------------------------------------------
# Index access
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``CYPRUS_DRCOR_DB_FILE`` or the data-root default — one function, so
    the reader, the boot download and the sweep's ``requires_files`` agree."""
    from ..cache import data_root

    configured = get_settings().cyprus_drcor_db_file
    return Path(configured) if configured else data_root() / "cyprus_drcor.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().cyprus_drcor_db_file:
            logger.warning("cyprus_drcor: index not found at %s", path)
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
            "cyprus_drcor: index at %s has an unexpected schema (want %s); ignoring",
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
    """The day data.gov.cy published the files the index was built from."""
    meta = read_meta() if meta is None else meta
    try:
        return datetime.strptime(meta.get("cut") or "", "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        return None


def declare_snapshot(meta: dict[str, str] | None = None) -> None:
    """Declare an index read as a snapshot: the portal's cut is the register's
    clock, ``built_at`` is ours (Phase 314). Without this the recorder
    resolves to ``stub`` and real rows badge as placeholder data."""
    meta = read_meta() if meta is None else meta
    cut = meta.get("cut") or ""
    provenance.record_snapshot(
        retrieved_at=provenance.parse_moment(meta.get("built_at")),
        source_as_of=cut_date(meta),
        detail=(
            f"data.gov.cy DRCOR register open data, cut {cut}"
            if cut else "data.gov.cy DRCOR register open data"
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
    url = settings.cyprus_drcor_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {"cyprus_drcor": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"cyprus_drcor": f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {"cyprus_drcor": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("cyprus_drcor: asset check/download failed: %s", exc)
        return {"cyprus_drcor": f"failed: {exc}"}


_ENTITY_SQL = (
    "SELECT e.type_code, e.reg_no, e.name, e.org_type, e.sub_type, e.registered_on,"
    " e.status, e.status_date, a.street, a.building, a.territory"
    " FROM entity e LEFT JOIN address a ON a.seq = e.address_seq"
)


def entity_record(reg_no: str, type_code: str = "C") -> dict[str, Any] | None:
    """The index's row for one registration number of one type; None when the
    index does not hold it (or there is no index)."""
    conn = _shared_conn()
    if conn is None:
        return None
    row = conn.execute(
        _ENTITY_SQL + " WHERE e.type_code = ? AND e.reg_no = ?", (type_code, reg_no)
    ).fetchone()
    return dict(row) if row is not None else None


def officials_of(reg_no: str, type_code: str = "C") -> list[dict[str, Any]]:
    conn = _shared_conn()
    if conn is None:
        return []
    rows = conn.execute(
        "SELECT seq, name, position FROM official WHERE type_code = ? AND reg_no = ? ORDER BY seq",
        (type_code, reg_no),
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class CyprusDrcorAdapter(SourceAdapter):
    """Source adapter for the Cyprus DRCOR register (local SQLite index)."""

    id = "cyprus_drcor"

    lookup_derivers = (
        LookupDeriver(frozenset({CY_DRCOR_RA_CODE}), "cy_he", normalise_he_number),
    )
    lookup_pass_legal_name = True

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="Cyprus DRCOR — Registrar of Companies",
            homepage="https://www.companies.gov.cy/",
            description=(
                "Cyprus's register of companies, business names, partnerships "
                "and overseas companies, published as open data by the "
                "Department of Registrar of Companies and Intellectual Property "
                "and read from a monthly index of its three CSV files: name, "
                "type and sub-type, registration date, the register's status "
                "with its date, the registered office, and the officials — "
                "directors, secretaries, partners, authorised persons. No "
                "shareholders or beneficial owners are published."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Contains information from the Department of Registrar of "
                "Companies and Intellectual Property (Republic of Cyprus), "
                "published on data.gov.cy under CC BY 4.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=index_available(),
            is_national_register=True,
            country="CY",
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
        try:
            # Companies first, then an exact name before a substring match.
            rows = conn.execute(
                _ENTITY_SQL
                + " WHERE e.name LIKE ? ORDER BY e.type_code = 'C' DESC,"
                " e.name = ? DESC, e.name LIMIT 10",
                (f"%{query.strip()}%", query.strip().upper()),
            ).fetchall()
        except sqlite3.Error:
            return []
        if not rows:
            return []
        declare_snapshot()
        return [self._search_hit(dict(row)) for row in rows]

    @staticmethod
    def _search_hit(row: dict[str, Any]) -> SourceHit:
        number = display_number(row["type_code"], row["reg_no"])
        segments = [f"CY {number}"]
        form = form_label(row.get("type_code"), row.get("sub_type"))
        if form:
            segments.append(form)
        cls = status_class(row.get("status"))
        if cls != "live":
            segments.append((status_label(row.get("status")) or cls).lower())
        identifiers = {"cy_he": row["reg_no"]} if row["type_code"] == "C" else {}
        return SourceHit(
            source_id="cyprus_drcor",
            hit_id=row["reg_no"] if row["type_code"] == "C" else f"{row['type_code']}:{row['reg_no']}",
            kind=SearchKind.ENTITY,
            name=row.get("name") or f"CY {number}",
            summary=" · ".join(segments),
            identifiers=identifiers,
            raw={"type_code": row["type_code"], "reg_no": row["reg_no"]},
            is_stub=False,
            liveness="snapshot",
        )

    def _stub(self, reg_no: str, legal_name: str) -> dict[str, Any]:
        """Nothing was read: no index, or not a number. Dropped by the pipeline."""
        return {
            "source_id": self.id,
            "reg_no": reg_no,
            "type_code": "C",
            "name": legal_name or "",
            "legal_name": legal_name,
            "officials": [],
            "link": SEARCH_URL,
            "is_stub": True,
        }

    def _not_in_index(self, reg_no: str, legal_name: str, meta: dict[str, str]) -> dict[str, Any]:
        """The index answered and holds no company of this number — a note
        card (``is_stub`` False, ``not_found`` True, the JAR / INPI shape).
        Not cached and not a degradation: nothing failed."""
        bundle = self._stub(reg_no, legal_name)
        bundle.update(
            {
                "link": DATASET_URL,
                "is_stub": False,
                "not_found": True,
                "coverage_note": COVERAGE_NOT_IN_INDEX,
                "cut": meta.get("cut"),
            }
        )
        return bundle

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the index's record for one HE number.

        ``hit_id`` is the number GLEIF files under ``registeredAs`` (``ΗΕ
        449149``, Greek letters), a bare number, or ``<type>:<number>`` from
        a search hit of another type; ``legal_name`` is GLEIF's name, used
        for the card when the register holds nothing.
        """
        raw = str(hit_id or "").strip()
        type_code = "C"
        if ":" in raw and raw.split(":", 1)[0] in TYPE_CODES:
            type_code, raw = raw.split(":", 1)
        reg_no = normalise_he_number(raw)
        if not reg_no:
            logger.warning("cyprus_drcor: not a registration number: %r", hit_id)
            return self._stub(raw, legal_name)
        conn = _shared_conn()
        if conn is None:
            return self._stub(reg_no, legal_name)
        try:
            row = entity_record(reg_no, type_code)
            meta = read_meta()
            if row is None:
                return self._not_in_index(reg_no, legal_name, meta)
            officials = officials_of(reg_no, type_code)
        except sqlite3.Error as exc:
            degradation.record(
                self.id,
                "The Cyprus register index could not be read; the card shows "
                "nothing from the register. This is not a confirmation that "
                "the register holds nothing.",
                reason=degradation.reason_for_failure(type(exc).__name__),
            )
            return self._stub(reg_no, legal_name)

        # Declared only once a row is in hand: a miss is not a read of data.
        declare_snapshot(meta)
        address = {
            k: row.get(k) for k in ("street", "building", "territory") if row.get(k)
        }
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "reg_no": reg_no,
            "type_code": row["type_code"],
            "name": row.get("name") or legal_name or f"CY {display_number(row['type_code'], reg_no)}",
            "legal_name": legal_name,
            "org_type": row.get("org_type"),
            "sub_type": row.get("sub_type"),
            "registered_on": row.get("registered_on"),
            "status": row.get("status"),
            "status_class": status_class(row.get("status")),
            "status_date": row.get("status_date"),
            "address": address or None,
            "officials": officials,
            "cut": meta.get("cut"),
            "link": DATASET_URL,
            "is_stub": False,
        }
        validate_raw("cyprus_drcor", CyprusBundle, bundle)
        return bundle
