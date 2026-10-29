"""Belgian Crossroads Bank for Enterprises (BCE / KBO) — an index tier.

The Banque-Carrefour des Entreprises (Kruispuntbank van Ondernemingen) is
Belgium's central business register, run by the FPS Economy. Its open data
is one full ZIP a month (``KboOpenData_<n>_<yyyy>_<mm>_Full.zip``: the
**active** registered entities — legal persons and natural-person
enterprises — with names, status, juridical situation, form, start date
and registered office; plus the register's own wording for every code in
NL / FR / DE / EN) behind a free account, and interim update files. No
shareholders, no officers, no beneficial owners: the Belgian UBO register
is held separately by FPS Finance and is not open.

Phase 35 built this adapter against a hand-built local file and left it
env-gated; the Belgium wiring (29 Oct 2026) rebuilt it in the
``jar_lithuania`` / ``cyprus_drcor`` shape: ``scripts/build_bce_belgium_index.py``
logs in and builds the index from the monthly full file, a GitHub workflow
runs it monthly (``refresh-bce-belgium-index.yml`` — the portal answers a
datacentre address; the login is a plain form) and ships it as the
``bce-belgium-index`` release asset, which ``warm_index`` downloads at boot
(``BCE_BELGIUM_DB_URL``). Without an index ``covers_lei`` is False and the
source is not announced.

GLEIF files the enterprise number under ``RA000025`` as ``0202.239.951``
(dotted — 197 of 200 ISSUED records sampled on 29 Oct 2026; 41,351 ISSUED
Belgian LEIs carry one); ``normalise_enterprise_number`` gives the ten
digits that key the index, and the mapper writes the identifier **dotted**,
as the register and GLEIF spell it, so the two statements corroborate on
``BE-BCE_KBO``.

Licence: Reuse of KBO public data (FPS Economy) — free, attribution
required: https://kbopub.economie.fgov.be/kbo-open-data/static/doc/Licentie/Licentie.pdf
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
from .schemas.bce_belgium import BCEBundle

logger = logging.getLogger(__name__)

#: GLEIF Registration Authority code for the BCE/KBO.
BCE_RA_CODE: str = "RA000025"

#: ``meta.schema_version`` the reader accepts.
INDEX_SCHEMA_VERSION = "1"

PORTAL_URL = "https://kbopub.economie.fgov.be/kbo-open-data/"
#: KBO's public search, per enterprise.
_ENTITY_URL = (
    "https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html"
    "?ondernemingsnummer={enterprise_number}"
)

COVERAGE_NOT_IN_INDEX = (
    "This enterprise number is not in the KBO open data as indexed. The "
    "monthly file holds the register's active entities only, so a stopped "
    "enterprise is not in it; GLEIF may also file a number the register "
    "does not use, or the enterprise may be newer than the index."
)

#: KBO's ``Status``: the open data holds active entities; ``ST`` appears
#: only in update files, kept here in case a full file ever carries it.
STATUSES: dict[str, tuple[str, str]] = {
    "AC": ("live", "Active"),
    "ST": ("terminal", "Stopped"),
}

#: ``TypeOfEnterprise``.
ENTERPRISE_TYPES = {"1": "natural person", "2": "legal person"}


def normalise_enterprise_number(raw: str) -> str:
    """Ten digits, no dots — the index key.

    Accepts ``0433.795.975``, ``0433795975``, ``BE 0433.795.975`` (a VAT
    spelling); returns "" when the input has no digits.
    """
    digits = re.sub(r"[^0-9]", "", (raw or "").strip())
    if not digits:
        return ""
    return digits.zfill(10)


def format_enterprise_number(raw: str) -> str:
    """The canonical dotted form the register and GLEIF write: ``0433.795.975``."""
    digits = normalise_enterprise_number(raw)
    if len(digits) == 10:
        return f"{digits[:4]}.{digits[4:7]}.{digits[7:]}"
    return digits


def _kbo_date(raw: str | None) -> datetime | None:
    """KBO's ``SnapshotDate`` as an aware UTC datetime (day-first, or ISO)."""
    value = (raw or "").strip()
    if not value:
        return None
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    return provenance.parse_moment(value)


# ---------------------------------------------------------------------------
# Index access
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``BCE_BELGIUM_DB_FILE`` or the data-root default — one function, so the
    reader, the boot download and the sweep's ``requires_files`` agree."""
    from ..cache import data_root

    configured = get_settings().bce_belgium_db_file
    return Path(configured) if configured else data_root() / "bce_belgium.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().bce_belgium_db_file:
            logger.warning("bce_belgium: index not found at %s", path)
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        version = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    except sqlite3.Error:
        version = None
    if not version or version[0] != INDEX_SCHEMA_VERSION:
        logger.warning(
            "bce_belgium: index at %s has an unexpected schema (want %s); ignoring",
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
    """KBO's ``SnapshotDate`` — the day the register cut the monthly file."""
    meta = read_meta() if meta is None else meta
    return _kbo_date(meta.get("cut") or meta.get("kbo_SnapshotDate") or meta.get("SnapshotDate"))


def declare_snapshot(meta: dict[str, str] | None = None) -> None:
    """Declare an index read as a snapshot: KBO's cut is the register's clock,
    ``built_at`` is ours (Phase 314). Without this the recorder resolves to
    ``stub`` and real rows badge as placeholder data."""
    meta = read_meta() if meta is None else meta
    cut = cut_date(meta)
    provenance.record_snapshot(
        retrieved_at=provenance.parse_moment(meta.get("built_at")),
        source_as_of=cut,
        detail=(
            f"KBO Open Data monthly extract, cut {cut.date().isoformat()}"
            if cut else "KBO Open Data monthly extract"
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
    url = settings.bce_belgium_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {"bce_belgium": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"bce_belgium": f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {"bce_belgium": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("bce_belgium: asset check/download failed: %s", exc)
        return {"bce_belgium": f"failed: {exc}"}


def entity_record(number: str) -> dict[str, Any] | None:
    conn = _shared_conn()
    if conn is None:
        return None
    row = conn.execute("SELECT * FROM entity WHERE number = ?", (number,)).fetchone()
    return dict(row) if row is not None else None


def code_label(category: str, code: str | None, *languages: str) -> str | None:
    """The register's own wording for a code, in the first language it has
    it (English, then Dutch, then French by default)."""
    conn = _shared_conn()
    text = (code or "").strip()
    if conn is None or not text:
        return None
    order = languages or ("EN", "NL", "FR", "DE")
    rows = conn.execute(
        "SELECT language, description FROM code WHERE category = ? AND code = ?",
        (category, text),
    ).fetchall()
    found = {str(r["language"]): str(r["description"]) for r in rows}
    for lang in order:
        if found.get(lang):
            return found[lang]
    return None


def status_class(status: str | None) -> str:
    return STATUSES.get((status or "").strip().upper(), ("unknown", ""))[0]


def _display_name(row: dict[str, Any], legal_name: str = "") -> str:
    return (
        row.get("name_nl") or row.get("name_fr") or row.get("name_de")
        or row.get("name_en") or legal_name or ""
    )


def _address_text(row: dict[str, Any]) -> str | None:
    street = " ".join(x for x in (row.get("street"), row.get("house")) if x)
    if row.get("box"):
        street = f"{street} box {row['box']}".strip()
    locality = " ".join(x for x in (row.get("zipcode"), row.get("municipality")) if x)
    parts = [p for p in (street, locality, row.get("country")) if p]
    return ", ".join(parts) or None


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class BceBelgiumAdapter(SourceAdapter):
    """Source adapter for the Belgian Crossroads Bank for Enterprises (BCE/KBO)."""

    id = "bce_belgium"

    lookup_derivers = (
        LookupDeriver(frozenset({BCE_RA_CODE}), "be_enterprise_number", normalise_enterprise_number),
    )
    lookup_pass_legal_name = True

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="Belgian Crossroads Bank for Enterprises (BCE/KBO)",
            homepage=PORTAL_URL,
            description=(
                "Belgium's central business register, read from a monthly "
                "index of the KBO Open Data full file: name (NL/FR/DE), "
                "status, juridical situation and form, start date and "
                "registered office for every active registered entity. No "
                "shareholders, officers or beneficial owners — the Belgian "
                "UBO register is held separately and is not open."
            ),
            license="Custom-KBO-Reuse",
            attribution=(
                "Data from the Belgian Crossroads Bank for Enterprises (BCE/KBO), "
                "made available by the FPS Economy, SMEs, Self-Employed and "
                "Energy, Belgium."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=index_available(),
            is_national_register=True,
            country="BE",
        )

    def covers_lei(self, lei: str) -> bool:
        """Gate dispatch on the index file, not on the LEI (the JAR gate):
        without an index the source would be announced for every Belgian
        lookup and then answer nothing."""
        return index_available()

    # ------------------------------------------------------------------
    # Search — legal persons only (the FTS index is built over them)
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        text = " ".join((query or "").split())
        if kind != SearchKind.ENTITY or not text:
            return []
        conn = _shared_conn()
        if conn is None:
            return []
        # FTS5 MATCH syntax: quote the query so punctuation is literal.
        match = '"' + text.replace('"', '""') + '"'
        try:
            rows = conn.execute(
                "SELECT e.* FROM entity e WHERE e.number IN ("
                " SELECT number FROM entity_fts WHERE entity_fts MATCH ?"
                " GROUP BY number ORDER BY MIN(rank) LIMIT 10)",
                (match,),
            ).fetchall()
        except sqlite3.Error as exc:
            logger.warning("bce_belgium: name search failed: %s", exc)
            return []
        if not rows:
            return []
        declare_snapshot()
        return [self._search_hit(dict(r)) for r in rows]

    def _search_hit(self, row: dict[str, Any]) -> SourceHit:
        number = row["number"]
        segments = [f"BE {format_enterprise_number(number)}"]
        form = code_label("JuridicalForm", row.get("form"))
        if form:
            segments.append(form)
        cls = status_class(row.get("status"))
        if cls != "live":
            segments.append((STATUSES.get(row.get("status") or "", ("", cls))[1] or cls).lower())
        return SourceHit(
            source_id=self.id,
            hit_id=number,
            kind=SearchKind.ENTITY,
            name=_display_name(row) or f"BE {format_enterprise_number(number)}",
            summary=" · ".join(segments),
            identifiers={"be_enterprise_number": number},
            raw={"number": number},
            is_stub=False,
            liveness="snapshot",
        )

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    def _stub(self, number: str, legal_name: str) -> dict[str, Any]:
        """Nothing was read: no index, or not a number. Dropped by the pipeline."""
        return {
            "source_id": self.id,
            "enterprise_number": number,
            "dotted": format_enterprise_number(number) if number else "",
            "name": legal_name or "",
            "link": _ENTITY_URL.format(enterprise_number=number),
            "is_stub": True,
        }

    def _not_in_index(self, number: str, legal_name: str, meta: dict[str, str]) -> dict[str, Any]:
        """A number the monthly file does not hold: a note card, not a stub
        and not a degradation — the read succeeded and found nothing."""
        cut = cut_date(meta)
        return {
            "source_id": self.id,
            "enterprise_number": number,
            "dotted": format_enterprise_number(number),
            "name": legal_name or "",
            "link": _ENTITY_URL.format(enterprise_number=number),
            "is_stub": False,
            "not_found": True,
            "coverage_note": COVERAGE_NOT_IN_INDEX,
            "cut": cut.date().isoformat() if cut else None,
        }

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """The index's record for one enterprise number (any spelling)."""
        number = normalise_enterprise_number(hit_id)
        if len(number) != 10:
            logger.warning("bce_belgium: not an enterprise number: %r", hit_id)
            return self._stub(number, legal_name)
        if _shared_conn() is None:
            return self._stub(number, legal_name)
        try:
            meta = read_meta()
            row = entity_record(number)
            if row is None:
                return self._not_in_index(number, legal_name, meta)
            form = code_label("JuridicalForm", row.get("form"))
            form_native = code_label("JuridicalForm", row.get("form"), "NL", "FR")
            situation = code_label("JuridicalSituation", row.get("situation"))
            status_text = code_label("Status", row.get("status"))
        except sqlite3.Error as exc:
            degradation.record(
                self.id,
                "The KBO open-data index could not be read; the card shows "
                "nothing from the register. This is not a confirmation that "
                "the register holds nothing.",
                reason=degradation.reason_for_failure(type(exc).__name__),
            )
            return self._stub(number, legal_name)

        # Declared only once a row is in hand: a miss is not a read of data.
        declare_snapshot(meta)
        cut = cut_date(meta)
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "enterprise_number": number,
            "dotted": format_enterprise_number(number),
            "name": _display_name(row, legal_name),
            "name_nl": row.get("name_nl") or "",
            "name_fr": row.get("name_fr") or "",
            "name_de": row.get("name_de") or "",
            "name_en": row.get("name_en") or "",
            "name_type": row.get("name_type"),
            "status": row.get("status"),
            "status_label": status_text or STATUSES.get(row.get("status") or "", ("", ""))[1] or None,
            "status_class": status_class(row.get("status")),
            "juridical_situation_code": row.get("situation"),
            "juridical_situation": situation,
            "enterprise_type_code": row.get("type"),
            "enterprise_type": ENTERPRISE_TYPES.get(row.get("type") or ""),
            "juridical_form_code": row.get("form"),
            "juridical_form": form,
            "juridical_form_native": form_native,
            "juridical_form_cac": row.get("form_cac"),
            "start_date": row.get("start_date"),
            "address": _address_text(row),
            "registered_office": {
                k: row.get(k) for k in ("street", "house", "box", "zipcode", "municipality", "country")
                if row.get(k)
            },
            "cut": cut.date().isoformat() if cut else None,
            "link": _ENTITY_URL.format(enterprise_number=number),
            "is_stub": False,
        }
        validate_raw("bce_belgium", BCEBundle, bundle)
        return bundle
