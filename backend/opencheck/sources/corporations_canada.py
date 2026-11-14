"""Corporations Canada adapter.

Corporations Canada is the federal registry for companies incorporated under
Canadian federal statutes (Canada Business Corporations Act and others). It
is operated by Innovation, Science and Economic Development Canada (ISED).

Two tiers, one source id (the Canada index ticket, 9 Nov 2026)
--------------------------------------------------------------
1. **A daily index of the Federal Corporations open data** — the four English
   CSVs on open.canada.ca (active / inactive-or-dissolved × CBCA / other
   Acts), built by ``scripts/build_corporations_canada_index.py`` in
   ``.github/workflows/refresh-corporations-canada-index.yml`` and downloaded
   at boot (``warm_index``) from the ``corporations-canada-index`` release
   asset. Every federal corporation on file, 1.58M on 8 Nov 2026, dissolved
   ones included: name in both forms, BN, governing Act, status and status
   detail ("Active - Dissolution Pending (non-compliance)"), anniversary
   (incorporation) date, last annual filing, registered office, the
   director limits. It answers name search and BN → corporation number.
2. **The ISED API on every lookup**, for what the CSVs do not carry:

   * ``GET /v1/corporations/<corpId>.json?lang=eng`` — the record, with its
     dated activities (incorporation, amalgamation partners, name changes,
     dissolution). On the ISED API Gateway with our ``user-key``, and —
     when the key is missing or the gateway fails — on the keyless legacy
     endpoint ``ised-isde.canada.ca/cc/lgcy/api``, which returns the same
     body (checked 9 Nov 2026). Cached a day.
   * ``GET /v2/corporations/<corpId>/directors.json?lang=eng`` — current
     directors, gateway only (the legacy endpoint has none). Cached a week.

   A number the index lacks (newer than the cut) is read from the API alone;
   one neither holds is a note card. An index hit whose API read failed keeps
   the index answer and records a ``source_read`` degradation; a failure with
   nothing in hand is a ``source_fetch`` degradation, never "no record".

Numbers
-------
The corporation number **includes its check digit**: GLEIF files
``728613-9`` under RA000072 and the register writes ``7286139``. GLEIF also
writes leading zeros (``035818-5``, FCA Canada) that the register never
does, so ``normalise_corp_id`` strips them. Nine in 2,000 sampled GLEIF
records file a Business Number with its program account
(``123456789RC0001``) instead: those read as ``BN<9 digits>`` and resolve
through the index to the one corporation with that BN — or, where several
share it (amalgamations keep the BN), to the one active one; otherwise
nothing is guessed. There is no name gate: the 45 GLEIF / register name
differences in that sample were renames.

License: Open Government Licence – Canada (OGL-Canada 2.0).
  https://open.canada.ca/en/open-government-licence-canada
Attribution: Contains information licensed under the Open Government Licence
  – Canada. Source: Corporations Canada, Innovation, Science and Economic
  Development Canada.
API reference: https://api.ised-isde.canada.ca/corporations/api
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

from .. import degradation, provenance
from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.corporations_canada import CorpCanadaBundle

_API_BASE = "https://apigateway-passerelledapi.ised-isde.canada.ca/corporations/api"
#: The keyless legacy JSON endpoint (the 2020 docs' ``www.ic.gc.ca`` URL
#: 301s here). Same record body as the gateway's v1; no directors, no search.
LEGACY_API_BASE = "https://ised-isde.canada.ca/cc/lgcy/api"
_CACHE_NS = "corporations_canada"
_LOG = logging.getLogger(__name__)
logger = _LOG

#: The record (status, activities) is re-read after a day; directors after a
#: week. Before the index ticket neither expired, so a status read once was
#: served until the cache was cleared.
RECORD_MAX_AGE_DAYS = 1.0
DIRECTORS_MAX_AGE_DAYS = 7.0

# GLEIF Registration Authority code for Corporations Canada.
# All Canadian federal entities in GLEIF carry registeredAt.id == "RA000072".
CA_CORP_RA_CODE: str = "RA000072"

#: ``meta.schema_version`` the reader accepts.
INDEX_SCHEMA_VERSION = "1"

DATASET_URL = "https://open.canada.ca/data/en/dataset/0032ce54-c5dd-4b66-99a0-320a7b5e99f2"
#: The four English CSVs, by the short name the builder and meta use.
CSV_BASE = "https://d4bf66bykfyaf.cloudfront.net"
CSV_FILES: dict[str, str] = {
    "active_cbca": "corporations-active-cbca-en.csv",
    "active_other": "corporations-active-non-cbca-en.csv",
    "inactive_cbca": "corporations-inactive-or-dissolved-cbca-en.csv",
    "inactive_other": "corporations-inactive-or-dissolved-non-cbca-en.csv",
}

COVERAGE_NOT_FOUND = (
    "This corporation number is not on Corporations Canada's federal register "
    "— neither in the daily open data nor in its API. GLEIF may file a number "
    "the federal register does not use, such as a provincial one."
)
COVERAGE_BN_UNRESOLVED = (
    "GLEIF files a Business Number here, not a corporation number, and the "
    "federal register's open data does not tie it to exactly one corporation, "
    "so no record is shown rather than a guess."
)

_CORP_ID_RE = re.compile(r"^\d+$")
#: A Business Number with its program account: ``123456789RC0001``.
_BN_FILING_RE = re.compile(r"^\s*(\d{9})\s*[A-Z]{2}\s*\d{4}\s*$")
#: The key ``normalise_corp_id`` gives a Business Number filing. The lookup
#: normalises GLEIF's filing once in the deriver and ``fetch`` normalises its
#: ``hit_id`` again, so the key must survive a second pass.
_BN_KEY_RE = re.compile(r"^\s*BN\s*(\d{9})\s*$")


def is_bn_filing(raw: str) -> bool:
    """True when ``raw`` is a Business Number with a program account."""
    return bool(_BN_FILING_RE.match(str(raw or "").upper()))


def normalise_corp_id(corp_id: str) -> str:
    """The register's spelling of a corporation number.

    Digits only, leading zeros removed: ``"728613-9"`` → ``"7286139"``,
    ``"035818-5"`` → ``"358185"``, ``"CA-1007"`` → ``"1007"``. A Business
    Number filed with its program account (``"123456789RC0001"``) reads
    ``"BN123456789"`` for ``fetch`` to resolve through the index, and that
    key reads as itself (normalising twice must not drop the ``BN``).
    """
    text = str(corp_id or "").strip()
    m = _BN_FILING_RE.match(text.upper()) or _BN_KEY_RE.match(text.upper())
    if m:
        return f"BN{m.group(1)}"
    return re.sub(r"\D", "", text).lstrip("0")


def is_valid_corp_id(corp_id: str) -> bool:
    normalised = normalise_corp_id(corp_id)
    return bool(_CORP_ID_RE.match(normalised))


def _corp_url(corp_id: str) -> str:
    return (
        f"https://ised-isde.canada.ca/cc/lgcy/fdrlCrpDtls.html"
        f"?corpId={corp_id}&V_TOKEN=null&LANGUAGE_ID=1"
    )


def extract_current_name(corp: dict[str, Any]) -> str:
    """Return the current primary name from a corporation's ``corporationNames`` list."""
    names = corp.get("corporationNames") or []
    # First preference: current=True + nameType="Primary"
    for entry in names:
        cn = entry.get("CorporationName") or {}
        if cn.get("current") and (cn.get("nameType") or "").lower() == "primary":
            return (cn.get("name") or "").strip()
    # Second preference: any current name
    for entry in names:
        cn = entry.get("CorporationName") or {}
        if cn.get("current"):
            return (cn.get("name") or "").strip()
    # Fall back to the last entry in the list
    if names:
        cn = (names[-1].get("CorporationName") or {})
        return (cn.get("name") or "").strip()
    return ""


# ---------------------------------------------------------------------------
# Status vocabulary — the CSVs' Status / Status Detail and the API's status
# ---------------------------------------------------------------------------

#: Every Status Detail on the 8 Nov 2026 files, plus the API's and the bare
#: statuses. Matched whole (``liveness.classify``); an unlisted wording reads
#: ``unknown`` and is shown in the register's own words.
LIVE_STATUSES: tuple[str, ...] = ("Active",)
PENDING_STATUSES: tuple[str, ...] = (
    "Active - Dissolution Pending (non-compliance)",
    "Active - Intent to Dissolve Filed",
    "Active - Discontinuance Pending",
    "Dissolution pending",
    "Pending dissolution",
    "In liquidation",
)
TERMINAL_STATUSES: tuple[str, ...] = (
    "Dissolved",
    "Dissolved by Corporations Canada",
    "Dissolved by court order",
    "Dissolved by the corporation",
    "Dissolved for non-compliance",
    "Dissolved – Part II of CCA",
    "Dissolved - Part II of CCA",
    "Inactive",
    "Inactive - Amalgamated",
    "Inactive - Discontinued",
    "Amalgamated",
    "Discontinued",
    "Revoked",
)


if TYPE_CHECKING:
    from ..bods.liveness import LivenessClass


def status_class(status: str | None, detail: str | None = None) -> "LivenessClass":
    """``live`` / ``pending`` / ``terminal`` / ``unknown`` — the detail first."""
    from ..bods import liveness

    for text in (detail, status):
        cls = liveness.classify(
            text, live=LIVE_STATUSES, pending=PENDING_STATUSES, terminal=TERMINAL_STATUSES
        )
        if cls != liveness.UNKNOWN:
            return cls
    return liveness.UNKNOWN


def status_words(status: str | None, detail: str | None = None) -> str:
    """The register's own wording: the detail when it has one."""
    return " ".join(str(detail or status or "").split())


def display_status(text: str | None) -> str:
    """The status as shown to a reader: the register's words without the
    API's ``Active - `` / ``Inactive - `` prefix ("Inactive - Amalgamated"
    reads "Amalgamated", as the finding has always said it). Classify on
    :func:`status_words`, never on this."""
    return re.sub(r"^(Active|Inactive) - ", "", " ".join(str(text or "").split()))


# ---------------------------------------------------------------------------
# Index access
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``CORPORATIONS_CANADA_DB_FILE`` or the data-root default — one
    function, so the reader, the boot download and the sweep agree."""
    from ..cache import data_root

    configured = get_settings().corporations_canada_db_file
    return Path(configured) if configured else data_root() / "corporations_canada.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().corporations_canada_db_file:
            logger.warning("corporations_canada: index not found at %s", path)
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
            "corporations_canada: index at %s has an unexpected schema (want %s); ignoring",
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
    """The day ISED last modified the files the index was built from."""
    meta = read_meta() if meta is None else meta
    try:
        return datetime.strptime(meta.get("cut") or "", "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        return None


def declare_snapshot(meta: dict[str, str] | None = None) -> None:
    """Declare an index read as a snapshot: the files' cut is the register's
    clock, ``built_at`` is ours (Phase 314)."""
    meta = read_meta() if meta is None else meta
    cut = meta.get("cut") or ""
    provenance.record_snapshot(
        retrieved_at=provenance.parse_moment(meta.get("built_at")),
        source_as_of=cut_date(meta),
        detail=(
            f"Corporations Canada federal corporations open data, cut {cut}"
            if cut else "Corporations Canada federal corporations open data"
        ),
    )


def warm_index() -> dict[str, Any]:
    """Download the index at boot when absent; replace it when the release
    asset changed; keep it otherwise. Never raises — no index is a state
    this module models (the API answers alone, as before the index)."""
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
    url = settings.corporations_canada_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {"corporations_canada": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"corporations_canada": f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {
            "corporations_canada": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"
        }
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("corporations_canada: asset check/download failed: %s", exc)
        return {"corporations_canada": f"failed: {exc}"}


#: The index's columns, in the order the builder writes them.
INDEX_COLUMNS: tuple[str, ...] = (
    "id", "bn", "name", "name_other", "act", "status", "detail", "anniversary",
    "last_filing_year", "last_agm", "street", "street2", "city", "province",
    "country", "postal_code", "directors_min", "directors_max",
)


def index_record(corp_id: str) -> dict[str, Any] | None:
    """The index's row for one corporation number; None when the index does
    not hold it (or there is no index). Raises ``sqlite3.Error``."""
    conn = _shared_conn()
    if conn is None or not corp_id.isdigit():
        return None
    row = conn.execute("SELECT * FROM corp WHERE id = ?", (int(corp_id),)).fetchone()
    if row is None:
        return None
    record = dict(row)
    record["id"] = str(record["id"])
    return record


def resolve_bn(bn: str) -> str | None:
    """The corporation number a Business Number names: the only corporation
    with it, else the only *active* one; None when there is none or the
    choice would be a guess. Raises ``sqlite3.Error``."""
    conn = _shared_conn()
    if conn is None:
        return None
    rows = conn.execute("SELECT id, status FROM corp WHERE bn = ?", (bn,)).fetchall()
    if len(rows) == 1:
        return str(rows[0]["id"])
    active = [r for r in rows if (r["status"] or "") == "Active"]
    if len(active) == 1:
        return str(active[0]["id"])
    return None


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class CorporationsCanadaAdapter(SourceAdapter):
    """Source adapter for Corporations Canada — ISED federal register."""

    id = "corporations_canada"

    lookup_derivers = (
        LookupDeriver(frozenset({CA_CORP_RA_CODE}), "ca_corp_id", normalise_corp_id),
    )
    lookup_pass_legal_name = True

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="Corporations Canada — ISED federal register",
            homepage="https://ised-isde.canada.ca/site/corporations-canada/en",
            description=(
                "Federal Canadian company data from Corporations Canada "
                "(Innovation, Science and Economic Development Canada), "
                "covering companies incorporated under federal statutes "
                "including the Canada Business Corporations Act: a daily index "
                "of the Federal Corporations open data (every corporation, "
                "dissolved ones included, with its status, governing Act and "
                "registered office), and the ISED API for each lookup's dated "
                "activities and current directors."
            ),
            license="OGL-Canada-2.0",
            attribution=(
                "Contains information licensed under the Open Government "
                "Licence – Canada. Source: Corporations Canada, Innovation, "
                "Science and Economic Development Canada."
            ),
            supports=[SearchKind.ENTITY],
            # The key adds directors; the record is read keyless when it is
            # missing, and the index needs none.
            requires_api_key=False,
            live_available=bool(settings.allow_live) or index_available(),
            is_national_register=True,
            country="CA",
        )

    # ------------------------------------------------------------------
    # Search — an FTS5 search over both name forms in the index. Without an
    # index there is none (the gateway plan has no name search).
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        text = " ".join((query or "").split())
        if kind != SearchKind.ENTITY or not text:
            return []
        conn = _shared_conn()
        if conn is None:
            return []
        match = '"' + text.replace('"', '""') + '"'
        try:
            rows = conn.execute(
                "SELECT c.* FROM corp c WHERE c.id IN ("
                " SELECT rowid FROM corp_fts WHERE corp_fts MATCH ?"
                " ORDER BY rank LIMIT 50)",
                (match,),
            ).fetchall()
        except sqlite3.Error as exc:
            logger.warning("corporations_canada: name search failed: %s", exc)
            return []
        if not rows:
            return []
        upper = text.upper()
        ranked = sorted(
            (dict(r) for r in rows),
            key=lambda r: (
                (r.get("status") or "") != "Active",
                (r.get("name") or "").upper() != upper,
            ),
        )[:10]
        declare_snapshot()
        return [self._search_hit(r) for r in ranked]

    def _search_hit(self, row: dict[str, Any]) -> SourceHit:
        corp_id = str(row["id"])
        segments = [f"CA-CC {corp_id}"]
        if row.get("act"):
            segments.append(row["act"])
        if status_class(row.get("status"), row.get("detail")) != "live":
            segments.append(display_status(status_words(row.get("status"), row.get("detail"))).lower())
        return SourceHit(
            source_id=self.id,
            hit_id=corp_id,
            kind=SearchKind.ENTITY,
            name=row.get("name") or f"CA-CC {corp_id}",
            summary=" · ".join(s for s in segments if s),
            identifiers={"ca_corp_id": corp_id},
            raw={"corporationId": corp_id},
            is_stub=False,
            liveness="snapshot",
        )

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    def _stub(self, corp_id: str, legal_name: str) -> dict[str, Any]:
        """Nothing was read. Dropped by the pipeline."""
        return {
            "source_id": self.id,
            "corp_id": corp_id,
            "corporation": None,
            "directors": [],
            "legal_name": legal_name,
            "is_stub": True,
        }

    def _note_card(
        self, corp_id: str, legal_name: str, note: str, meta: dict[str, str]
    ) -> dict[str, Any]:
        """The register answered and holds no such corporation — a note card
        (``is_stub`` False, ``not_found`` True). Not cached, not a
        degradation: nothing failed."""
        bundle = self._stub(corp_id, legal_name)
        bundle.update(
            {"is_stub": False, "not_found": True, "coverage_note": note, "cut": meta.get("cut")}
        )
        return bundle

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the index row, the API record and the directors for one
        corporation.

        ``hit_id`` is the corporation number (any spelling GLEIF files,
        check digit included) or ``BN<9 digits>`` for a Business Number
        filing. ``legal_name`` is GLEIF's name, used when nothing names it.
        """
        key = normalise_corp_id(hit_id)
        if not key:
            return self._stub(key, legal_name)

        meta: dict[str, str] = {}
        index_row: dict[str, Any] | None = None
        index_read = False
        try:
            if index_available():
                meta = read_meta()
                index_read = True
            if key.startswith("BN"):
                corp_id = resolve_bn(key[2:]) if index_read else None
                if corp_id is None:
                    if index_read:
                        return self._note_card(key, legal_name, COVERAGE_BN_UNRESOLVED, meta)
                    return self._stub(key, legal_name)
            else:
                corp_id = key
            if index_read:
                index_row = index_record(corp_id)
        except sqlite3.Error as exc:
            degradation.record(
                self.id,
                "The Corporations Canada open-data index could not be read; the "
                "card shows only what the ISED API returned.",
                check=degradation.CHECK_SOURCE_READ,
                reason=degradation.reason_for_failure(type(exc).__name__),
            )
            index_row, index_read = None, False
            if key.startswith("BN"):
                return self._stub(key, legal_name)
            corp_id = key

        corporation, record_outcome, record_via = await self._get_corporation(corp_id)

        if index_row is None and corporation is None:
            if record_outcome == "not_found":
                return self._note_card(corp_id, legal_name, COVERAGE_NOT_FOUND, meta)
            if record_outcome == "failed":
                degradation.record(
                    self.id,
                    "The ISED API did not answer and the corporation is not in "
                    "the daily open-data index, so nothing was read from the "
                    "federal register.",
                    reason=degradation.REASON_UPSTREAM_ERROR,
                )
            return self._stub(corp_id, legal_name)

        if index_row is not None and record_outcome == "failed":
            degradation.record(
                self.id,
                "The ISED API did not answer: the record comes from the daily "
                "open-data index, without its dated activities or directors.",
                check=degradation.CHECK_SOURCE_READ,
                reason=degradation.REASON_UPSTREAM_ERROR,
            )

        directors, directors_read = await self._get_directors(corp_id)

        if index_row is not None:
            # Declared only once a row is in hand: a miss is not a read of data.
            declare_snapshot(meta)

        bundle: dict[str, Any] = {
            "source_id": self.id,
            "corp_id": corp_id,
            "corporation": corporation,
            "record_via": record_via,
            "directors": directors,
            "directors_read": directors_read,
            "index": index_row,
            "tier": "index" if index_row is not None else "api",
            "cut": meta.get("cut") if index_row is not None else None,
            "legal_name": legal_name,
            "is_stub": False,
        }
        if key.startswith("BN"):
            bundle["resolved_from_bn"] = key[2:]
        validate_raw("corporations_canada", CorpCanadaBundle, bundle)
        return bundle

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _auth_headers(self) -> dict[str, str]:
        settings = get_settings()
        key = settings.corporations_canada_api_key or ""
        return {"user-key": key, "Accept": "application/json"}

    @staticmethod
    def _parse_record(data: Any) -> tuple[dict[str, Any] | None, str]:
        """``(record, outcome)`` from a v1 body.

        Success: ``[corpObject, null]``; not found: ``["error string",
        "error en français"]``, always with HTTP 200 — a definitive answer.
        Anything else is a failure, never "no record".
        """
        if isinstance(data, list) and data:
            first = data[0]
            if isinstance(first, dict):
                return first, "found"
            if isinstance(first, str):
                return None, "not_found"
        return None, "failed"

    async def _read_record(
        self, client: httpx.AsyncClient, url: str, headers: dict[str, str]
    ) -> tuple[dict[str, Any] | None, str]:
        try:
            response = await client.get(url, params={"lang": "eng"}, headers=headers)
        except httpx.HTTPError as exc:
            _LOG.warning("Corporations Canada: %s from %s", type(exc).__name__,
                         httpx.URL(url).host)
            return None, "failed"
        if not response.is_success:
            _LOG.warning("Corporations Canada: HTTP %s from %s", response.status_code,
                         httpx.URL(url).host)
            return None, "failed"
        try:
            data = response.json()
        except ValueError:
            return None, "failed"
        return self._parse_record(data)

    async def _get_corporation(
        self, corp_id: str
    ) -> tuple[dict[str, Any] | None, str, str | None]:
        """``(record, outcome, via)``. Outcome ``found`` / ``not_found`` /
        ``failed`` / ``skipped`` (live calls off). The gateway first when the
        key is set; the keyless legacy endpoint when it is not, or when the
        gateway fails."""
        cache_key = f"{_CACHE_NS}/v1/{corp_id}"
        cached = self._cache.get_payload(cache_key, max_age_days=RECORD_MAX_AGE_DAYS)
        if cached is not None:
            return cached[0], "found", "cache"
        settings = get_settings()
        if not settings.allow_live:
            return None, "skipped", None

        record: dict[str, Any] | None = None
        outcome = "failed"
        via: str | None = None
        async with build_client() as client:
            if settings.corporations_canada_api_key:
                record, outcome = await self._read_record(
                    client,
                    f"{_API_BASE}/v1/corporations/{corp_id}.json",
                    self._auth_headers(),
                )
                via = "gateway"
            if outcome == "failed":
                record, outcome = await self._read_record(
                    client,
                    f"{LEGACY_API_BASE}/corporations/{corp_id}.json",
                    {"Accept": "application/json"},
                )
                via = "legacy"
        if record is not None:
            self._cache.put(cache_key, record)
        return record, outcome, via if outcome != "failed" else None

    async def _get_directors(self, corp_id: str) -> tuple[list[dict[str, Any]], bool]:
        """``(directors, read)`` — current directors, gateway only.

        ``read`` is False when they could not be asked (no key, live calls
        off) or the call failed; a failure with a key is a ``source_read``
        degradation. A 404 is a definitive "none on file".
        """
        cache_key = f"{_CACHE_NS}/v2/directors/{corp_id}"
        cached = self._cache.get_payload(cache_key, max_age_days=DIRECTORS_MAX_AGE_DAYS)
        if cached is not None:
            return _directors_of(cached[0]), True
        settings = get_settings()
        if not (settings.allow_live and settings.corporations_canada_api_key):
            return [], False
        failure: str | None = None
        payload: Any = None
        try:
            async with build_client() as client:
                response = await client.get(
                    f"{_API_BASE}/v2/corporations/{corp_id}/directors.json",
                    params={"lang": "eng"},
                    headers=self._auth_headers(),
                )
            if response.status_code == 404:
                self._cache.put_absent(cache_key)
                return [], True
            if response.is_success:
                payload = response.json()
            else:
                failure = f"HTTP {response.status_code}"
        except (httpx.HTTPError, ValueError) as exc:
            failure = type(exc).__name__
        if failure is not None or not isinstance(payload, dict):
            degradation.record(
                self.id,
                "The ISED API did not return the corporation's directors; "
                "none are shown, which is not a confirmation that there are none.",
                check=degradation.CHECK_SOURCE_READ,
                reason=degradation.reason_for_failure(failure or "unreadable body"),
            )
            return [], False
        self._cache.put(cache_key, payload)
        return _directors_of(payload), True

    # ------------------------------------------------------------------
    # Hit factory
    # ------------------------------------------------------------------

    @staticmethod
    def _entity_hit(corp: dict[str, Any], corp_id: str) -> SourceHit:
        name = extract_current_name(corp) or corp_id or "Unknown"
        status = display_status(corp.get("status"))
        act = (corp.get("act") or "").strip()
        summary = f"CA-CC {corp_id}"
        if status:
            summary += f" · {status}"
        if act:
            summary += f" · {act}"

        return SourceHit(
            source_id="corporations_canada",
            hit_id=corp_id,
            kind=SearchKind.ENTITY,
            name=name,
            summary=summary,
            identifiers={"ca_corp_id": corp_id},
            raw=corp,
            is_stub=False,
        )


def _directors_of(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    return list((payload.get("_embedded") or {}).get("directors") or [])
