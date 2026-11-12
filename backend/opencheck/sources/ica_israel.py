"""Israel — the Corporations Authority's registers of companies and
partnerships (רשות התאגידים), read from data.gov.il's open data.

An **index-tier** register source, the ``cyprus_drcor`` shape: the lookup
reads a SQLite index of two daily data.gov.il datasets, downloaded at boot
from the ``ica-israel-index`` release asset (``ICA_ISRAEL_DB_URL``) and
replaced when the asset changes. Dispatch is gated on the index being present
(``covers_lei``); a number the index does not hold is a note card, never a
failure. The index is rebuilt daily by
``.github/workflows/refresh-ica-israel-index.yml`` running
``scripts/build_ica_israel_index.py``.

What the registers publish
--------------------------
``ica_companies`` (מאגר חברות, "Other (Open)") and ``ica_partnerships``
(מאגר שותפויות, CC BY): every company and partnership on the register, with
its number, Hebrew name, English name where one was filed (46 % of companies),
type, status and sub-status, incorporation date and registered address; for a
company also the government-company flag, the limitation, the **violating
company** mark (חברה מפרה) and the last annual report filed. **Entity data
only**: no directors, no shareholders, no partners. Those are in the
Authority's paid extract.

Why an index and not live reads
-------------------------------
The CSV download answers 403 (CloudFront) to datacentre addresses, while the
CKAN datastore API answers everywhere: the builder pages ``datastore_search``
32,000 rows at a time (733,463 companies in about three minutes on 10 Nov
2026). A name search over the API is a full-text ``q=`` that takes up to
2.4 s; the index answers both kinds of read locally.

Format traps, each measured on 10 Nov 2026
------------------------------------------
* **``~`` stands for the Hebrew quotation mark** (בע~מ = בע"מ) in every text
  field, status words included. The builder writes ``"`` back, the way GLEIF
  files the names, so the two meet.
* The number is 9 digits on every row of both files (companies 51/52/56,
  partnerships 53/54/55), so one column keys both. GLEIF files it bare under
  RA000406.
* Dates are ``DD/MM/YYYY``. **No dissolution date** is published, so a
  terminal status is carried without one.
* No former names: GLEIF holding an earlier name is a rename, not a wrong
  number (1,194 of 1,230 ISSUED company LEIs agree on a name), so there is no
  name gate (the JAR / Taiwan rule).

Licence: companies "Other (Open)" (data.gov.il: free use, cite data.gov.il,
the dataset and its update date); partnerships CC BY. Israel Corporations
Authority, via data.gov.il.
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
from .schemas.ica_israel import IcaIsraelBundle

logger = logging.getLogger(__name__)

#: GLEIF Registration Authority code for the Israel Corporations Authority's
#: Registrar of Companies. It carries partnership numbers too (124 + 18 of the
#: 148 ISSUED records that are not companies, 10 Nov 2026).
IL_ICA_RA_CODE: str = "RA000406"

#: ``meta.schema_version`` the reader accepts.
INDEX_SCHEMA_VERSION = "1"

#: data.gov.il dataset pages (the register's open data, cited on statements).
COMPANIES_DATASET_URL = "https://data.gov.il/dataset/ica_companies"
PARTNERSHIPS_DATASET_URL = "https://data.gov.il/dataset/ica_partnerships"
#: The Corporations Authority's own public search (no deep link per number).
SEARCH_URL = "https://ica.justice.gov.il/GenericCorporarionInfo/SearchCorporation?unit=8"

#: Status classes by (kind, the register's status code). Every code seen on
#: 10 Nov 2026 is here; an unlisted one reads ``unknown`` and is shown in the
#: register's own words.
STATUS_CLASSES: dict[tuple[str, int], str] = {
    ("company", 0): "live",  # פעילה
    ("company", 78): "live",  # פעילה זמנית — temporarily active
    ("company", 4): "pending",  # active, in receivership
    ("company", 5): "pending",  # active, arrangement or temporary winding-up
    ("company", 15): "pending",  # in voluntary winding-up
    ("company", 16): "pending",  # in court winding-up
    ("company", 19): "terminal",  # struck off
    ("company", 35): "terminal",  # voluntarily liquidated
    ("company", 36): "terminal",  # liquidated by court
    ("company", 76): "terminal",  # dissolved by merger
    ("company", 80): "terminal",  # removed from the register
    ("partnership", 0): "live",
    ("partnership", 78): "live",
    ("partnership", 15): "pending",
    ("partnership", 16): "pending",
    ("partnership", 19): "terminal",
    ("partnership", 35): "terminal",
    ("partnership", 36): "terminal",
    ("partnership", 59): "terminal",  # struck off voluntarily
    ("partnership", 69): "terminal",  # struck off by court
    ("partnership", 76): "terminal",
    ("partnership", 80): "terminal",  # dissolved voluntarily
    ("partnership", 81): "terminal",  # dissolved by court
}

#: English glosses of the register's status words (after ``~`` → ``"``).
STATUS_GLOSSES: dict[str, str] = {
    "פעילה": "Active",
    "פעילה זמנית": "Temporarily active",
    "פעילה/בפירוק - בכינוס נכסים": "Active, in receivership",
    "פעילה/בפירוק - הליך פשרה או הסדר/פירוק זמני": (
        "Active, in a creditors' arrangement or temporary winding-up"
    ),
    "בפרוק מרצון": "In voluntary winding-up",
    'בפרוק ע"י בימ"ש': "In winding-up by court order",
    "מחוקה": "Struck off",
    "מחוסלת מרצון": "Voluntarily liquidated",
    'חיסול ע"י בימ"ש': "Liquidated by court order",
    "מחוסלת עקב מיזוג": "Dissolved by merger",
    "נגרעה מהמרשם": "Removed from the register",
    "שותפות מחוקה מרצון": "Struck off at the partners' request",
    "שותפות מחוקה על ידי בית משפט": "Struck off by court order",
    "פורקה מרצון": "Dissolved voluntarily",
    'פורקה ע"י בימ"ש': "Dissolved by court order",
}

#: Sub-statuses that make a live company's status pending: a strike-off
#: notice has been published, or an insolvency order made.
PENDING_SUB_STATUSES: tuple[str, ...] = ("בהליכי מחיקה", "צו לפתיחת הליכים")

#: English glosses of the sub-statuses seen on 10 Nov 2026.
SUB_STATUS_GLOSSES: dict[str, str] = {
    "בהליכי מחיקה": "strike-off proceedings under way",
    "מחוסלת עקב מחיקה": "liquidated by strike-off",
    "צו לפתיחת הליכים": "insolvency proceedings opened",
    "צו לפתיחת הליכים - תאגיד בשיקום": "insolvency proceedings opened, in rehabilitation",
    "בפרוק מרצון בהליך מזורז": "fast-track voluntary winding-up",
    "בקשת נושים בהחייאה": "creditors' application for revival",
    "בקשת חברה בהחייאה": "company's application for revival",
    "הוגשה בקשה להחייאה": "application for revival filed",
    "הוגשה בקשת מיזוג - מיועדת להתמזג": "merger application filed, to be merged",
    "הוגשה בקשת מיזוג - מיועדת לקלוט": "merger application filed, to absorb",
    "תצהיר כושר פירעון": "solvency declaration filed",
    "פעילה": "active",
}

#: The register's type words, glossed.
FORM_GLOSSES: dict[str, str] = {
    "ישראלית חברה פרטית": "Israeli private company",
    "ישראלית חברה ציבורית": "Israeli public company",
    "ישראלית חברה אגח": "Israeli bond company",
    "חוץ חברה פרטית": "foreign private company",
    "חוץ חברה ציבורית": "foreign public company",
    "ישראלית כללית": "Israeli general partnership",
    "ישראלית מוגבלת": "Israeli limited partnership",
    'חו"ל כללית': "foreign general partnership",
    'חו"ל מוגבלת': "foreign limited partnership",
    'חו"ל שותפות חו"ל מהסבה': "foreign partnership",
}

#: An Israeli private company's type words — the only type whose annual
#: report to the Registrar is said (a public company reports to the
#: Securities Authority, and its register year is often decades old).
PRIVATE_COMPANY_WORDS = "ישראלית חברה פרטית"

#: The violating-company mark, glossed (``קוד חברה מפרה`` 18 / 17).
VIOLATING_GLOSSES: dict[str, str] = {
    "מפרה": "violating company",
    "התראה": "violating-company warning",
}

COVERAGE_NOT_IN_INDEX = (
    "This number is not among the companies and partnerships in the Israel "
    "Corporations Authority's open data as indexed. GLEIF may hold a number "
    "the register does not use (an association or a cooperative is on "
    "another register), or the company may be newer than the index."
)


def normalise_il_number(raw: str) -> str:
    """The 9-digit corporate number, from any spelling (``51-339-1934``,
    ``513391934``). Anything that is not 9 digits once the rest goes is
    returned as its digits; the adapter refuses it."""
    return re.sub(r"\D", "", str(raw or ""))


def status_class(kind: str | None, code: Any, sub_status: str | None = None) -> str:
    """``live`` / ``pending`` / ``terminal`` / ``unknown`` for a register status.

    A live entity whose sub-status says a strike-off notice was published or
    an insolvency order made reads ``pending``.
    """
    try:
        key = (str(kind or "company"), int(code))
    except (TypeError, ValueError):
        return "unknown"
    cls = STATUS_CLASSES.get(key, "unknown")
    if cls == "live" and any(p in (sub_status or "") for p in PENDING_SUB_STATUSES):
        return "pending"
    return cls


def status_label(words: str | None) -> str | None:
    text = (words or "").strip()
    if not text:
        return None
    return STATUS_GLOSSES.get(text, text)


def sub_status_label(words: str | None) -> str | None:
    """Each ``; ``-joined sub-status, glossed, de-duplicated, in order."""
    out: list[str] = []
    for part in (words or "").split(";"):
        part = part.strip()
        if not part:
            continue
        gloss = SUB_STATUS_GLOSSES.get(part, part)
        if gloss not in out:
            out.append(gloss)
    return "; ".join(out) or None


def form_label(type_words: str | None) -> str | None:
    text = (type_words or "").strip()
    if not text:
        return None
    return FORM_GLOSSES.get(text, text)


def violating_label(words: str | None) -> str | None:
    text = (words or "").strip()
    if not text:
        return None
    return VIOLATING_GLOSSES.get(text, text)


def dataset_url(kind: str | None) -> str:
    return PARTNERSHIPS_DATASET_URL if kind == "partnership" else COMPANIES_DATASET_URL


# ---------------------------------------------------------------------------
# Index access
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``ICA_ISRAEL_DB_FILE`` or the data-root default — one function, so the
    reader, the boot download and the sweep's ``requires_files`` agree."""
    from ..cache import data_root

    configured = get_settings().ica_israel_db_file
    return Path(configured) if configured else data_root() / "ica_israel.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().ica_israel_db_file:
            logger.warning("ica_israel: index not found at %s", path)
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
            "ica_israel: index at %s has an unexpected schema (want %s); ignoring",
            path, INDEX_SCHEMA_VERSION,
        )
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
    conn = _shared_conn()
    if conn is None:
        return {}
    try:
        return {str(k): str(v) for k, v in conn.execute("SELECT key, value FROM meta")}
    except sqlite3.Error:  # pragma: no cover
        return {}


def cut_date(meta: dict[str, str] | None = None) -> datetime | None:
    """The day data.gov.il last regenerated the files the index was built from."""
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
            f"data.gov.il Israel Corporations Authority open data, cut {cut}"
            if cut else "data.gov.il Israel Corporations Authority open data"
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
    url = settings.ica_israel_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {"ica_israel": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"ica_israel": f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        # A replaced file may be in use by a lookup: drop the connection
        # without closing it, so a query in flight finishes on the old file.
        reset_connection(close=outcome == "downloaded")
        return {"ica_israel": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("ica_israel: asset check/download failed: %s", exc)
        return {"ica_israel": f"failed: {exc}"}


async def refresh_loop(interval_s: float) -> None:
    """Re-run :func:`warm_index` every ``interval_s`` seconds, so the daily
    index lands without a restart — the watchlist's register tier re-runs a
    changed company against the day it was told about (the Estonian
    arrangement). The first run is one interval after boot."""
    import asyncio

    while True:
        await asyncio.sleep(interval_s)
        try:
            result = await asyncio.to_thread(warm_index)
            if not str(result.get("ica_israel", "")).startswith("already present"):
                logger.info("ica_israel: refresh: %s", result)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning("ica_israel: refresh loop iteration failed: %s", exc)


_ENTITY_COLUMNS = (
    "number, kind, name_he, name_en, type_code, type_words, status_code,"
    " status_words, sub_status, incorporated, government, limitation,"
    " violating, last_report_year, city, street, house, postcode, po_box, country"
)


def entity_record(number: str) -> dict[str, Any] | None:
    """The index's row for one corporate number; None when the index does not
    hold it (or there is no index)."""
    conn = _shared_conn()
    if conn is None:
        return None
    row = conn.execute(
        f"SELECT {_ENTITY_COLUMNS} FROM entity WHERE number = ?", (number,)
    ).fetchone()
    return dict(row) if row is not None else None


def change_rows(number: str) -> list[dict[str, Any]] | None:
    """The change log's rows for one corporate number, oldest first:
    ``{"code", "words", "day", "charge_id", "first_seen", "retained"}``
    (``charge_id`` None for a row that is not about a charge). ``[]`` when the
    log holds none; None when there is no index or the index carries no change
    log (built before it, or without it — the Estonia rule, no schema bump)."""
    conn = _shared_conn()
    if conn is None:
        return None
    try:
        if conn.execute("SELECT 1 FROM meta WHERE key = 'changes_cut'").fetchone() is None:
            return None
        rows = conn.execute(
            "SELECT c.code, r.words, c.day, c.charge_id, c.first_seen, c.retained "
            "FROM change AS c LEFT JOIN request_type AS r ON r.code = c.code "
            "WHERE c.number = ? ORDER BY c.day, c.code, c.charge_id",
            (number,),
        ).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc):
            return None
        raise
    return [
        {
            "code": row["code"], "words": row["words"], "day": row["day"],
            "charge_id": row["charge_id"] or None, "first_seen": row["first_seen"],
            "retained": bool(row["retained"]),
        }
        for row in rows
    ]


def change_coverage(meta: dict[str, str] | None = None) -> dict[str, str] | None:
    """What the change log in the index covers: ``window_start`` (the
    register's own year), ``kept_since`` (the earliest day OpenCheck holds,
    retained rows included), ``first_cut`` (the day OpenCheck began keeping
    them) and ``cut``. None without a change log."""
    meta = read_meta() if meta is None else meta
    if not meta.get("changes_cut"):
        return None
    return {
        "cut": meta["changes_cut"],
        "window_start": meta.get("changes_window_start") or meta["changes_cut"],
        "kept_since": meta.get("changes_kept_since") or meta.get("changes_window_start") or meta["changes_cut"],
        "first_cut": meta.get("changes_first_cut") or meta["changes_cut"],
    }


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class IcaIsraelAdapter(SourceAdapter):
    """Source adapter for the Israel Corporations Authority (local SQLite index)."""

    id = "ica_israel"

    lookup_derivers = (
        LookupDeriver(frozenset({IL_ICA_RA_CODE}), "il_roc", normalise_il_number),
    )
    lookup_pass_legal_name = True

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="Israel Corporations Authority — Registrar of Companies and Partnerships",
            homepage="https://www.gov.il/en/departments/israeli_corporations_authority",
            description=(
                "Israel's registers of companies and partnerships, published "
                "daily as open data on data.gov.il by the Israel Corporations "
                "Authority and read from an index of the two files: Hebrew and "
                "English names, type, status and sub-status, incorporation "
                "date, registered address, and for a company the violating-"
                "company mark, the government-company flag and the last annual "
                "report filed. No directors, shareholders or partners are "
                "published."
            ),
            # Companies: "Other (Open)" — data.gov.il's terms allow free use
            # with the source, dataset and update date cited; partnerships:
            # CC BY. Both permit commercial re-use with attribution.
            license="data.gov.il open data (free use with attribution; partnerships CC BY)",
            attribution=(
                "Contains information from the Israel Corporations Authority "
                "(רשות התאגידים) — the companies and partnerships databases "
                "(מאגר חברות, מאגר שותפויות), published on data.gov.il."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=index_available(),
            is_national_register=True,
            country="IL",
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
        q = query.strip().replace("~", '"')
        try:
            # An exact name first, then a name that starts with the query,
            # then live before the rest, companies before partnerships.
            rows = conn.execute(
                f"SELECT {_ENTITY_COLUMNS} FROM entity"
                " WHERE name_he LIKE ? OR name_en LIKE ?"
                " ORDER BY (name_he = ? OR name_en = ?) DESC,"
                " (name_he LIKE ? OR name_en LIKE ?) DESC,"
                " status_code = 0 DESC, kind = 'company' DESC, name_he LIMIT 10",
                (f"%{q}%", f"%{q.upper()}%", q, q.upper(), f"{q}%", f"{q.upper()}%"),
            ).fetchall()
        except sqlite3.Error:
            return []
        if not rows:
            return []
        declare_snapshot()
        return [self._search_hit(dict(row)) for row in rows]

    @staticmethod
    def _search_hit(row: dict[str, Any]) -> SourceHit:
        number = row["number"]
        segments = [f"IL {number}"]
        form = form_label(row.get("type_words"))
        if form:
            segments.append(form)
        cls = status_class(row.get("kind"), row.get("status_code"), row.get("sub_status"))
        if cls != "live":
            segments.append((status_label(row.get("status_words")) or cls).lower())
        return SourceHit(
            source_id="ica_israel",
            hit_id=number,
            kind=SearchKind.ENTITY,
            name=row.get("name_he") or row.get("name_en") or f"IL {number}",
            summary=" · ".join(segments),
            identifiers={"il_roc": number},
            raw={"number": number, "kind": row.get("kind"), "name_en": row.get("name_en")},
            is_stub=False,
            liveness="snapshot",
        )

    def _stub(self, number: str, legal_name: str) -> dict[str, Any]:
        """Nothing was read: no index, or not a number. Dropped by the pipeline."""
        return {
            "source_id": self.id,
            "number": number,
            "kind": "company",
            "name": legal_name or "",
            "legal_name": legal_name,
            "link": COMPANIES_DATASET_URL,
            "is_stub": True,
        }

    def _not_in_index(self, number: str, legal_name: str, meta: dict[str, str]) -> dict[str, Any]:
        """The index answered and holds nothing under this number — a note
        card (``is_stub`` False, ``not_found`` True, the JAR / INPI shape).
        Not cached and not a degradation: nothing failed."""
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
        """Return the index's record for one corporate number.

        ``hit_id`` is the number GLEIF files under ``registeredAs`` (bare 9
        digits) or a search hit's id; ``legal_name`` is GLEIF's name, used for
        the card when the register holds nothing.
        """
        number = normalise_il_number(hit_id)
        if len(number) != 9:
            logger.warning("ica_israel: not a 9-digit corporate number: %r", hit_id)
            return self._stub(number or str(hit_id or ""), legal_name)
        conn = _shared_conn()
        if conn is None:
            return self._stub(number, legal_name)
        try:
            row = entity_record(number)
            meta = read_meta()
        except sqlite3.Error as exc:
            degradation.record(
                self.id,
                "The Israel register index could not be read; the card shows "
                "nothing from the register. This is not a confirmation that "
                "the register holds nothing.",
                reason=degradation.reason_for_failure(type(exc).__name__),
            )
            return self._stub(number, legal_name)
        if row is None:
            return self._not_in_index(number, legal_name, meta)

        # Declared only once a row is in hand: a miss is not a read of data.
        declare_snapshot(meta)
        kind = row.get("kind") or "company"
        address = {
            k: row.get(k)
            for k in ("street", "house", "city", "postcode", "po_box", "country")
            if row.get(k) not in (None, "")
        }
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "number": number,
            "kind": kind,
            "name": row.get("name_he") or row.get("name_en") or legal_name or f"IL {number}",
            "name_en": row.get("name_en") or None,
            "legal_name": legal_name,
            "type_code": row.get("type_code"),
            "type_words": row.get("type_words"),
            "status_code": row.get("status_code"),
            "status_words": row.get("status_words"),
            "sub_status": row.get("sub_status"),
            "status_class": status_class(kind, row.get("status_code"), row.get("sub_status")),
            "incorporated": row.get("incorporated"),
            "government": None if row.get("government") is None else bool(row["government"]),
            "limitation": row.get("limitation"),
            "violating": row.get("violating"),
            "last_report_year": row.get("last_report_year"),
            "address": address or None,
            "cut": meta.get("cut"),
            "link": dataset_url(kind),
            "is_stub": False,
        }
        validate_raw("ica_israel", IcaIsraelBundle, bundle)
        return bundle
