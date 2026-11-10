"""South Korea — Open DART (Financial Supervisory Service) disclosure data.

The Financial Supervisory Service's DART system (전자공시시스템) holds every
filing a Korean disclosure filer makes: listed companies (KOSPI, KOSDAQ,
KONEX) and the unlisted companies that must file audited accounts or a
securities report. **Open DART** (``opendart.fss.or.kr``) serves those
filings as a free REST API behind a personal key (``OPENDART_API_KEY``).
Investigated on 10 Nov 2026; the measurements, the terms and Stephen's
decisions A–D are on the Notion ticket "South Korea / Open DART disclosure
data (largest shareholders, executives)".

What a lookup reads (decision B), five calls:

* ``company.json`` — the filer's profile: Korean and English names, listing
  class (``corp_cls``), stock code, **both** registration numbers
  (``bizr_no`` business registration, ``jurir_no`` corporate registration),
  address, founding date.
* ``list.json`` — the filer's periodic reports (``pblntf_ty=A``) over the last
  eighteen months, to find the latest one: its business year and report code.
* ``hyslrSttus.json`` — the **largest shareholder and its related persons**
  (최대주주 및 특수관계인) as filed in that report, with each holder's shares
  and percentage of the class at the period end.
* ``exctvSttus.json`` — the **executives** (임원 현황) in that report.
* ``majorstock.json`` — the **5% holding reports** (주식등의 대량보유상황보고서).

Korea has **no public beneficial-ownership register**: beneficial owners are
identified only in financial institutions' customer due diligence. So nothing
here is ever "a beneficial owner" or "the owner", and
``beneficialOwnershipOrControl`` is never set.

The join: LEI → ``corp_code`` (decision A)
------------------------------------------

DART keys everything on its own eight-digit ``corp_code``. GLEIF files the
business registration number under ``RA000657`` (National Tax Service;
``124-81-00998``) and the corporate registration number under ``RA000419``
(the Supreme Court's registry; ``110111-6981206``). Neither number is in
DART's bulk filer list (``corpCode.xml``), so ``scripts/build_dart_korea_index.py``
builds a crosswalk weekly from a GitHub workflow: every Korean LEI under those
two authorities is name-matched against the filer list, each candidate's
``company.json`` is read, and a filer joins only when **its own number equals
the number GLEIF files** — a name alone never joins. The index holds the
filers it checked (their numbers and names), never the filings; a lookup reads
the filings live. A number the index does not hold is a note card: the
company is not a DART filer, or its name did not match.

API behaviour (measured 10 Nov 2026)
------------------------------------

* The answer's ``status`` is in the JSON body, and an HTTP 200 can carry any
  of them: ``000`` ok; ``013`` no data (an answer — nothing filed); ``010`` /
  ``011`` / ``901`` the key is unknown, disabled or expired; ``012`` the IP is
  not allowed; ``020`` the key's daily limit (20,000 requests) is spent;
  ``021`` too many companies; ``100`` / ``101`` a bad request; ``800``
  maintenance; ``900`` undefined error.
* The key travels in the ``crtfc_key`` query parameter, so httpx puts it in
  every error's URL; :mod:`opencheck.secret_scrub` scrubs the parameter by name
  and the configured value by value. It must never be shared (terms art. 19②).
* A process-wide daily allowance (:data:`DAILY_BUDGET_DEFAULT`, the KST day)
  keeps the weekly index build's headroom; a spent allowance refuses before
  sending.

Licence: the Open DART terms (art. 16④) defer to the Public Data Act, and
data.go.kr lists the FSS's DART APIs with 이용허락범위 제한 없음 — no restriction
on the scope of use. Attribution is given anyway.
"""

from __future__ import annotations

import asyncio
import logging
import re
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from .. import degradation, provenance
from ..cache import Cache
from ..config import get_settings
from ..host_throttle import HostRateLimitedError
from ..http import build_client
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.dart_korea import DartKoreaBundle

logger = logging.getLogger(__name__)

API_BASE = "https://opendart.fss.or.kr/api"
HOMEPAGE = "https://opendart.fss.or.kr/"

#: GLEIF Registration Authority codes (verified live, 10 Nov 2026).
#: ``RA000657`` National Tax Service — business registration (사업자등록번호).
#: ``RA000419`` Supreme Court Registry — corporate registration (법인등록번호).
#: ``RA000923`` (FSC fund codes) is not a company register and not dispatched.
KR_BRN_RA_CODE = "RA000657"
KR_CRN_RA_CODE = "RA000419"

KR_BRN_SCHEME = "KR-BRN"
KR_BRN_SCHEME_NAME = "Business registration number (사업자등록번호), National Tax Service"
KR_CRN_SCHEME = "KR-CRN"
KR_CRN_SCHEME_NAME = "Corporate registration number (법인등록번호), court registry"

#: The table layout this adapter reads; the builder writes the same value
#: into ``meta`` (a test pins the two equal).
INDEX_SCHEMA_VERSION = "1"

#: The FSS's daily cap is 20,000 requests a key. Lookups take at most this
#: many, so the weekly crosswalk build (same key) always has room.
DAILY_BUDGET_DEFAULT = 15_000
_KST = timezone(timedelta(hours=9))

#: How far back ``list.json`` is searched for the latest periodic report.
REPORT_LOOKBACK_DAYS = 550

#: reprt_code values (Open DART's codes for the four periodic reports).
REPORT_CODES = {
    "11011": "annual report",
    "11012": "half-year report",
    "11013": "first-quarter report",
    "11014": "third-quarter report",
}

#: A periodic report's name, as DART lists it: "사업보고서 (2025.12)",
#: "[기재정정]반기보고서 (2026.06)".
_REPORT_NAME = re.compile(r"(사업|반기|분기)보고서\s*\((\d{4})\.(\d{2})\)")

#: Open DART statuses.
STATUS_OK = "000"
STATUS_NO_DATA = "013"
_STATUS_RATE_LIMITED = frozenset({"020"})
_STATUS_KEY_REFUSED = frozenset({"010", "011", "012", "901"})

_STATUS_WORDS = {
    "010": "the API key is not registered",
    "011": "the API key is disabled",
    "012": "this server's IP address is not allowed for the key",
    "020": "the key's daily request limit is spent",
    "021": "too many companies were requested",
    "100": "a request field was refused",
    "101": "the request was refused",
    "800": "the service is under maintenance",
    "900": "an undefined error",
    "901": "the key's account has expired",
}

#: ``corp_cls`` → listing words.
LISTING_CLASSES = {
    "Y": "listed on KOSPI",
    "K": "listed on KOSDAQ",
    "N": "listed on KONEX",
    "E": "unlisted disclosure filer",
}

_CACHE_NS = "dart_korea"
#: Bump when the cached payload's shape changes.
_CACHE_VERSION = "v1"
_REQUEST_TIMEOUT = httpx.Timeout(25.0, connect=10.0)

COVERAGE_NOT_IN_INDEX = (
    "This registration number is not in OpenCheck's crosswalk of DART filers. "
    "DART covers listed companies and companies that must file audited "
    "accounts or securities reports; this company may not be a filer, or its "
    "name in DART did not match the LEI record's."
)


# ---------------------------------------------------------------------------
# Numbers
# ---------------------------------------------------------------------------


def _digits(value: Any) -> str:
    return re.sub(r"\D", "", str(value or ""))


def normalise_bizr_no(value: str) -> str:
    """A business registration number (사업자등록번호): ten digits, separators
    dropped. ``ValueError`` otherwise — the source is then skipped."""
    text = _digits(value)
    if len(text) != 10:
        raise ValueError(f"not a Korean business registration number: {value!r}")
    return text


def normalise_jurir_no(value: str) -> str:
    """A corporate registration number (법인등록번호): thirteen digits."""
    text = _digits(value)
    if len(text) != 13:
        raise ValueError(f"not a Korean corporate registration number: {value!r}")
    return text


def format_bizr_no(digits: str) -> str:
    """``1248100998`` → ``124-81-00998``, the way the NTS and GLEIF write it."""
    d = _digits(digits)
    return f"{d[:3]}-{d[3:5]}-{d[5:]}" if len(d) == 10 else d


def format_jurir_no(digits: str) -> str:
    """``1301110006246`` → ``130111-0006246``."""
    d = _digits(digits)
    return f"{d[:6]}-{d[6:]}" if len(d) == 13 else d


def number_kind(value: str) -> str | None:
    """``"bizr"`` for ten digits, ``"jurir"`` for thirteen, else None."""
    n = len(_digits(value))
    return "bizr" if n == 10 else "jurir" if n == 13 else None


def parse_ymd(value: Any) -> str | None:
    """``19690113`` / ``2026-06-30`` / ``2026.06.30`` → ISO day, else None."""
    text = _digits(value)
    if len(text) != 8:
        return None
    try:
        return date(int(text[:4]), int(text[4:6]), int(text[6:])).isoformat()
    except ValueError:
        return None


def report_of(report_nm: str) -> tuple[str, str, str] | None:
    """A periodic report's ``(bsns_year, reprt_code, period end YYYY-MM)``
    from its listed name, or None. A business report (사업보고서) is the
    annual report; a quarterly report (분기보고서) ending in March is the
    first-quarter report, any other the third."""
    match = _REPORT_NAME.search(str(report_nm or ""))
    if not match:
        return None
    kind, year, month = match.groups()
    if kind == "사업":
        code = "11011"
    elif kind == "반기":
        code = "11012"
    else:
        code = "11013" if month == "03" else "11014"
    return year, code, f"{year}-{month}"


def filing_url(rcept_no: str) -> str:
    """DART's own page for one filing."""
    return f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"


def api_url(endpoint: str, **params: str) -> str:
    """An Open DART URL **without** the key — the statements' source URL."""
    query = "&".join(f"{k}={v}" for k, v in params.items() if v)
    return f"{API_BASE}/{endpoint}.json" + (f"?{query}" if query else "")


# ---------------------------------------------------------------------------
# The crosswalk index
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``DART_KOREA_DB_FILE`` or the data-root default."""
    from ..cache import data_root

    configured = get_settings().dart_korea_db_file
    return Path(configured) if configured else data_root() / "dart_korea.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().dart_korea_db_file:
            logger.warning("dart_korea: index not found at %s", path)
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        version = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    except sqlite3.Error:
        version = None
    if not version or version[0] != INDEX_SCHEMA_VERSION:
        logger.warning("dart_korea: index at %s has an unexpected schema; ignoring", path)
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


def filer_for_number(number: str) -> dict[str, Any] | None:
    """The filer the index holds under a business or corporate registration
    number (digits), or None. Raises ``sqlite3.Error`` on a broken index."""
    conn = _shared_conn()
    if conn is None:
        return None
    kind = number_kind(number)
    if kind is None:
        return None
    column = "bizr_no" if kind == "bizr" else "jurir_no"
    rows = conn.execute(
        f"SELECT * FROM filer WHERE {column} = ? ORDER BY corp_cls IS NULL, modify_date DESC",  # noqa: S608 — fixed column
        (_digits(number),),
    ).fetchall()
    if not rows:
        return None
    return dict(rows[0])


def warm_index() -> dict[str, Any]:
    """Download the crosswalk at boot when absent; replace it when the release
    asset changed. Never raises — no index is a state (the source is not
    announced)."""
    from ..entity_pages import (
        ASSET_STAMP_KEY,
        asset_check,
        download_db,
        read_meta as read_file_meta,
        record_asset_stamp,
    )

    settings = get_settings()
    path = db_path()
    url = settings.dart_korea_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {"dart_korea": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"dart_korea": f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {"dart_korea": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("dart_korea: asset check/download failed: %s", type(exc).__name__)
        return {"dart_korea": f"failed: {type(exc).__name__}"}


# ---------------------------------------------------------------------------
# The client: one key, one daily allowance
# ---------------------------------------------------------------------------


class DartUnanswered(Exception):
    """A request that did not answer: the degradation it would record."""

    def __init__(self, what: str, detail: str, reason: str = degradation.REASON_UPSTREAM_ERROR):
        super().__init__(what)
        self.what = what
        self.detail = detail
        self.reason = reason


_BUDGET: dict[str, Any] = {"day": None, "sent": 0, "refused": 0, "outcomes": {}}


def _kst_day() -> str:
    return datetime.now(_KST).date().isoformat()


def _budget_take() -> bool:
    """Charge one request against today's allowance (the KST day); False
    when it is spent — nothing is sent."""
    today = _kst_day()
    if _BUDGET["day"] != today:
        _BUDGET.update({"day": today, "sent": 0, "refused": 0, "outcomes": {}})
    cap = max(0, min(int(get_settings().opendart_daily_budget or 0), 20_000))
    if _BUDGET["sent"] >= cap:
        _BUDGET["refused"] += 1
        return False
    _BUDGET["sent"] += 1
    return True


def _count(outcome: str) -> None:
    outcomes = _BUDGET.setdefault("outcomes", {})
    outcomes[outcome] = outcomes.get(outcome, 0) + 1


def stats() -> dict[str, Any]:
    """Today's requests against the allowance — counts only (``/signalstats``)."""
    cap = max(0, min(int(get_settings().opendart_daily_budget or 0), 20_000))
    today = _kst_day()
    same_day = _BUDGET["day"] == today
    return {
        "day": today,
        "allowance": cap,
        "sent_today": _BUDGET["sent"] if same_day else 0,
        "refused_today": _BUDGET["refused"] if same_day else 0,
        "outcomes": dict(_BUDGET["outcomes"]) if same_day else {},
        "index": index_available(),
    }


def reset_budget() -> None:
    """Forget today's counts (tests)."""
    _BUDGET.update({"day": None, "sent": 0, "refused": 0, "outcomes": {}})


async def dart_get(client: httpx.AsyncClient, endpoint: str, params: dict[str, str], *, what: str) -> dict[str, Any]:
    """One Open DART call: the body for ``000``, ``{"list": []}`` for ``013``
    (no data — an answer). Anything else raises :class:`DartUnanswered`."""
    key = get_settings().opendart_api_key or ""
    if not key:
        raise DartUnanswered(what, "Open DART: no API key is configured", degradation.REASON_NOT_CONFIGURED)
    if not _budget_take():
        _count("refused_budget")
        raise DartUnanswered(
            what,
            f"Open DART {what}: not sent — OpenCheck's daily allowance for the key is spent",
            degradation.REASON_RATE_LIMITED,
        )
    try:
        response = await client.get(
            f"{API_BASE}/{endpoint}.json",
            params={"crtfc_key": key, **params},
            timeout=_REQUEST_TIMEOUT,
        )
    except HostRateLimitedError:
        _count("refused_pacing")
        raise DartUnanswered(
            what, f"Open DART {what}: not sent — request pacing spent", degradation.REASON_RATE_LIMITED
        ) from None
    except httpx.HTTPError as exc:
        kind = type(exc).__name__  # never the URL: it carries the key
        _count("network")
        raise DartUnanswered(
            what, f"Open DART {what} did not answer ({kind})", degradation.reason_for_failure(kind)
        ) from None
    if not response.is_success:
        _count(f"http_{response.status_code}")
        raise DartUnanswered(
            what,
            f"Open DART {what} returned HTTP {response.status_code}",
            degradation.reason_for_failure(f"HTTP {response.status_code}"),
        )
    try:
        body = response.json()
    except ValueError:
        _count("bad_body")
        raise DartUnanswered(what, f"Open DART {what} returned a body that is not JSON") from None
    if not isinstance(body, dict):
        _count("bad_body")
        raise DartUnanswered(what, f"Open DART {what} returned an unexpected shape")
    status = str(body.get("status") or "")
    if status == STATUS_OK:
        _count("ok")
        return body
    if status == STATUS_NO_DATA:
        _count("no_data")
        return {"status": status, "list": []}
    _count(f"status_{status or 'missing'}")
    words = _STATUS_WORDS.get(status, f"status {status or 'missing'}")
    if status in _STATUS_RATE_LIMITED:
        reason = degradation.REASON_RATE_LIMITED
    elif status in _STATUS_KEY_REFUSED:
        reason = degradation.REASON_NOT_CONFIGURED
    else:
        reason = degradation.REASON_UPSTREAM_ERROR
    raise DartUnanswered(what, f"Open DART {what}: {words}", reason)


def latest_periodic_report(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """The newest periodic report in a ``list.json`` answer: the latest
    period, then the latest receipt (a correction of the same period wins)."""
    best: tuple[str, str] | None = None
    chosen: dict[str, Any] | None = None
    for row in rows:
        parsed = report_of(str(row.get("report_nm") or ""))
        rcept_no = str(row.get("rcept_no") or "").strip()
        if not parsed or not rcept_no:
            continue
        year, code, period = parsed
        key = (period, rcept_no)
        if best is None or key > best:
            best = key
            chosen = {
                "rcept_no": rcept_no,
                "rcept_dt": parse_ymd(row.get("rcept_dt")),
                "report_nm": " ".join(str(row.get("report_nm") or "").split()),
                "bsns_year": year,
                "reprt_code": code,
                "period": period,
            }
    return chosen


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class DartKoreaAdapter(SourceAdapter):
    """Source adapter for Open DART (FSS disclosure filings, South Korea)."""

    id = "dart_korea"

    lookup_derivers = (
        LookupDeriver(frozenset({KR_BRN_RA_CODE}), "kr_bizr_no", normalise_bizr_no),
        LookupDeriver(frozenset({KR_CRN_RA_CODE}), "kr_jurir_no", normalise_jurir_no),
    )
    lookup_pass_legal_name = True

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="Open DART — Korean disclosure filings (FSS)",
            homepage=HOMEPAGE,
            description=(
                "South Korea's DART disclosure system, run by the Financial "
                "Supervisory Service, read through the Open DART API: a "
                "filer's profile and registration numbers, the largest "
                "shareholder and its related persons and the executives from "
                "its latest periodic report, and its 5% holding reports. "
                "Covers listed companies and other disclosure filers only. "
                "Korea has no public beneficial-ownership register."
            ),
            license="KR-Public-Data",
            attribution=(
                "Source: Financial Supervisory Service (금융감독원), Open DART "
                "(opendart.fss.or.kr). Public data, no restriction on the scope "
                "of use (data.go.kr)."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=True,
            live_available=bool(settings.allow_live and settings.opendart_api_key),
            is_national_register=True,
            country="KR",
        )

    def covers_lei(self, lei: str) -> bool:
        """Announced only with a crosswalk and a key: without either every
        Korean lookup would announce a source that answers nothing."""
        return index_available() and bool(get_settings().opendart_api_key)

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Name search over the crosswalk's filers (no API call)."""
        if kind != SearchKind.ENTITY or not (query or "").strip():
            return []
        conn = _shared_conn()
        if conn is None:
            return []
        text = query.strip()
        try:
            if number_kind(text):
                column = "bizr_no" if number_kind(text) == "bizr" else "jurir_no"
                rows = conn.execute(
                    f"SELECT * FROM filer WHERE {column} = ? LIMIT 10",  # noqa: S608 — fixed column
                    (_digits(text),),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM filer WHERE corp_name LIKE ? OR corp_name_eng LIKE ? LIMIT 10",
                    (f"%{text}%", f"%{text}%"),
                ).fetchall()
        except sqlite3.Error:
            return []
        hits: list[SourceHit] = []
        for row in rows:
            r = dict(row)
            number = r.get("bizr_no") or r.get("jurir_no") or r["corp_code"]
            segments = []
            if r.get("bizr_no"):
                segments.append(f"{KR_BRN_SCHEME} {format_bizr_no(r['bizr_no'])}")
            listing = LISTING_CLASSES.get(str(r.get("corp_cls") or ""))
            if listing:
                segments.append(listing)
            identifiers = {}
            if r.get("bizr_no"):
                identifiers["kr_bizr_no"] = r["bizr_no"]
            if r.get("jurir_no"):
                identifiers["kr_jurir_no"] = r["jurir_no"]
            hits.append(
                SourceHit(
                    source_id=self.id,
                    hit_id=str(number),
                    kind=SearchKind.ENTITY,
                    name=r.get("corp_name") or r.get("corp_name_eng") or str(number),
                    summary=" · ".join(segments) or "DART filer",
                    identifiers=identifiers,
                    raw={"corp_code": r["corp_code"]},
                    is_stub=False,
                    liveness="snapshot",
                )
            )
        return hits

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """The filer's profile, latest periodic report's shareholder and
        executive tables, and its 5% reports, by business (10 digits) or
        corporate (13 digits) registration number."""
        number = _digits(hit_id)
        if number_kind(number) is None or not index_available():
            return self._stub(number or str(hit_id or ""), legal_name)
        try:
            filer = filer_for_number(number)
        except sqlite3.Error as exc:
            degradation.record(
                self.id,
                "The DART crosswalk index could not be read; nothing from DART is shown. "
                "This is not a confirmation that DART holds nothing.",
                reason=degradation.reason_for_failure(type(exc).__name__),
            )
            return self._stub(number, legal_name)
        if filer is None:
            return self._not_in_index(number, legal_name)

        corp_code = str(filer["corp_code"])
        cache_key = f"{_CACHE_NS}/filer-{_CACHE_VERSION}/{corp_code}"
        cached = self._cache.get_payload(cache_key, max_age_days=1)
        if cached is not None and isinstance(cached[0], dict):
            return self._bundle(number, legal_name, filer, cached[0])

        settings = get_settings()
        if not (settings.allow_live and settings.opendart_api_key):
            if not settings.opendart_api_key:
                degradation.record(
                    self.id,
                    "Open DART: no API key is configured, so the filings were not read",
                    reason=degradation.REASON_NOT_CONFIGURED,
                )
            return self._stub(number, legal_name)

        parts, complete = await self._read_live(corp_code)
        if parts.get("company") is None:
            # The profile did not answer: the index's own row stands in, said.
            degradation.record(
                self.id,
                "Open DART did not answer for the company profile; the card shows the "
                "crosswalk's record of the filer only",
                check=degradation.CHECK_SOURCE_READ,
                reason=parts.pop("_company_reason", degradation.REASON_UPSTREAM_ERROR),
            )
        parts.pop("_company_reason", None)
        if complete:
            self._cache.put(cache_key, parts)
        return self._bundle(number, legal_name, filer, parts)

    async def _read_live(self, corp_code: str) -> tuple[dict[str, Any], bool]:
        """The five reads. A part that did not answer is ``None`` and its
        ``*_read`` flag False; ``complete`` is True only when everything
        answered (only then is the result cached — the Phase 262 rule)."""
        today = datetime.now(_KST).date()
        parts: dict[str, Any] = {
            "company": None,
            "report": None,
            "shareholders": [],
            "executives": [],
            "major_holders": [],
            "reports_read": False,
            "shareholders_read": False,
            "executives_read": False,
            "major_holders_read": False,
        }
        reasons: list[str] = []
        unanswered: list[str] = []

        async with build_client() as client:
            async def _get(endpoint: str, what: str, **params: str) -> dict[str, Any] | None:
                try:
                    return await dart_get(client, endpoint, {"corp_code": corp_code, **params}, what=what)
                except DartUnanswered as exc:
                    reasons.append(exc.reason)
                    unanswered.append(exc.what)
                    return None

            company, listing, major = await asyncio.gather(
                _get("company", "company profile"),
                _get(
                    "list",
                    "periodic report list",
                    bgn_de=(today - timedelta(days=REPORT_LOOKBACK_DAYS)).strftime("%Y%m%d"),
                    end_de=today.strftime("%Y%m%d"),
                    pblntf_ty="A",
                    page_count="100",
                ),
                _get("majorstock", "5% holding reports"),
            )
            if company is not None and company.get("status") == STATUS_OK:
                parts["company"] = {k: v for k, v in company.items() if k not in ("status", "message")}
            elif company is None and reasons:
                parts["_company_reason"] = reasons[0]
            if major is not None:
                parts["major_holders"] = [r for r in major.get("list") or [] if isinstance(r, dict)]
                parts["major_holders_read"] = True
            if listing is not None:
                parts["reports_read"] = True
                report = latest_periodic_report(
                    [r for r in listing.get("list") or [] if isinstance(r, dict)]
                )
                parts["report"] = report
                if report is not None:
                    holders, executives = await asyncio.gather(
                        _get("hyslrSttus", "largest shareholder table",
                             bsns_year=report["bsns_year"], reprt_code=report["reprt_code"]),
                        _get("exctvSttus", "executives table",
                             bsns_year=report["bsns_year"], reprt_code=report["reprt_code"]),
                    )
                    if holders is not None:
                        parts["shareholders"] = [r for r in holders.get("list") or [] if isinstance(r, dict)]
                        parts["shareholders_read"] = True
                    if executives is not None:
                        parts["executives"] = [r for r in executives.get("list") or [] if isinstance(r, dict)]
                        parts["executives_read"] = True

        missing = [w for w in unanswered if w != "company profile"]
        if missing:
            degradation.record(
                self.id,
                "Open DART answered only in part: " + ", ".join(missing) + " did not answer",
                check=degradation.CHECK_SOURCE_READ,
                reason=_worst_reason(reasons),
            )
        complete = not unanswered
        return parts, complete

    # ------------------------------------------------------------------
    # Bundles
    # ------------------------------------------------------------------

    def _stub(self, number: str, legal_name: str) -> dict[str, Any]:
        return {
            "source_id": self.id,
            "hit_id": number,
            "kr_number": number,
            "name": legal_name or "",
            "link": HOMEPAGE,
            "is_stub": True,
        }

    def _not_in_index(self, number: str, legal_name: str) -> dict[str, Any]:
        """A note card: the crosswalk answered and does not hold the number.
        Not cached, not a degradation — nothing failed."""
        provenance.record_snapshot(
            retrieved_at=provenance.parse_moment(read_meta().get("built_at")),
            source_as_of=None,
            detail="OpenCheck's crosswalk of Open DART filers",
        )
        return {
            "source_id": self.id,
            "hit_id": number,
            "kr_number": number,
            "name": legal_name or "",
            "link": HOMEPAGE,
            "is_stub": False,
            "not_found": True,
            "coverage_note": COVERAGE_NOT_IN_INDEX,
        }

    def _bundle(
        self, number: str, legal_name: str, filer: dict[str, Any], parts: dict[str, Any]
    ) -> dict[str, Any]:
        company = parts.get("company")
        profile_read = isinstance(company, dict)
        if not profile_read:
            company = {
                "corp_code": filer.get("corp_code"),
                "corp_name": filer.get("corp_name"),
                "corp_name_eng": filer.get("corp_name_eng"),
                "stock_code": filer.get("stock_code"),
                "corp_cls": filer.get("corp_cls"),
                "bizr_no": filer.get("bizr_no"),
                "jurir_no": filer.get("jurir_no"),
            }
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "hit_id": number,
            "kr_number": number,
            "corp_code": str(filer.get("corp_code") or ""),
            "legal_name": legal_name or "",
            "company": company,
            "profile_read": profile_read,
            "report": parts.get("report"),
            "reports_read": bool(parts.get("reports_read")),
            "shareholders": list(parts.get("shareholders") or []),
            "shareholders_read": bool(parts.get("shareholders_read")),
            "executives": list(parts.get("executives") or []),
            "executives_read": bool(parts.get("executives_read")),
            "major_holders": list(parts.get("major_holders") or []),
            "major_holders_read": bool(parts.get("major_holders_read")),
            "link": filing_url(parts["report"]["rcept_no"]) if parts.get("report") else HOMEPAGE,
            "is_stub": False,
        }
        validate_raw(self.id, DartKoreaBundle, bundle)
        return bundle


_REASON_ORDER = (
    degradation.REASON_NOT_CONFIGURED,
    degradation.REASON_RATE_LIMITED,
    degradation.REASON_TIMEOUT,
    degradation.REASON_UPSTREAM_ERROR,
)


def _worst_reason(reasons: list[str]) -> str:
    for reason in _REASON_ORDER:
        if reason in reasons:
            return reason
    return reasons[0] if reasons else degradation.REASON_UPSTREAM_ERROR


__all__ = [
    "DartKoreaAdapter",
    "KR_BRN_RA_CODE",
    "KR_CRN_RA_CODE",
    "KR_BRN_SCHEME",
    "KR_CRN_SCHEME",
]
