"""Taiwan company register adapter — MOEA GCIS open data (directors and supervisors).

The Ministry of Economic Affairs' Administration of Commerce (經濟部商業發展署)
publishes the company register through the GCIS open-data platform
(商工行政資料開放平臺, ``data.gcis.nat.gov.tw``) as key-less REST endpoints, one
GUID per dataset, updated hourly:

* ``5F64D864-…`` 公司登記基本資料-應用一 — the company: name (Chinese only),
  status wording, authorised and paid-in capital, par value and issued shares
  (``Share_Val`` / ``Equity_Amt``, absent from the Swagger schema), the
  representative, the registered address, the registering authority, setup and
  last-change dates, revocation and suspension dates.
* ``4E5F7653-…`` 公司登記董監事資料集 — directors and supervisors: position,
  name, the legal person a director represents (``Juristic_Person_Name``) and a
  holding (``Person_Shareholding``).
* ``673F0FC0-…`` 統編查是否為公司、分公司及商業 — whether a UBN is a company,
  a branch or a sole-trader business; read only when the company endpoint has
  no record, to say what the number is instead.
* ``6BBA2268-…`` 公司登記關鍵字查詢 — name search, for ``search()`` and, since
  the Taiwan GCIS v2 ticket, to find a legal-person director's own UBN by
  exact name (see "Legal-person holders" below).

Everything below was measured on 29 Oct 2026 against every ISSUED LEI GLEIF
files under ``RA000551`` (504; 1,512 calls, all HTTP 200). The findings and
the decisions taken on them are on the Notion ticket "Taiwan / MOEA GCIS
company registration open data (directors and supervisors)".

The identifier
--------------

The Unified Business Number (統一編號) — eight digits with a check digit,
the company's registration, tax and customs number. GLEIF's ``RA000551``
(Ministry of Economic Affairs) files it bare in ``registeredAs`` on every
record but three. ``RA001009`` (Financial Supervisory Commission) files funds'
tax numbers, which the company register does not hold, so it is **not**
dispatched. ``RA000551`` also sits on records whose jurisdiction is HK, SG,
CH, GB or US-DE — foreign companies' Taiwan branches — so the deriver keys on
the RA code, never the country (the ``ny_dos`` rule). Scheme ``TW-CR``
(org-id.guide). There is **no name gate** on the UBN join (Stephen, 29 Oct
2026): no wrong number in the 504, and a name that differs is GLEIF holding
a former name — the record-consistency layer's to report.

What the holding field means
----------------------------

Company Act art. 393 makes directors' and supervisors' names **and holdings**
public. The figure is a share count for a 股份有限公司 (company limited by
shares; denominator ``Equity_Amt``, the issued shares) and a capital
contribution in NT$ for a 有限公司 (limited company; denominator
``Capital_Stock_Amount``). When ``Juristic_Person_Name`` is set, the legal
person holds the seat and the shares (法人董事 / 法人代表) and the named
individual is its representative — and the legal person's holding is repeated
on every one of its representatives' rows. See ``bods/mappers/taiwan.py``.
These are directors' and supervisors' holdings only: never "the
shareholders", never beneficial ownership (the 10%-shareholder filings of
Company Act art. 22-1 are restricted to the authorities).

API behaviour
-------------

* **No record is HTTP 200 with an empty body** — not ``[]`` — and a malformed
  UBN gets the same answer, so a number is read only after its check digit
  passes. A record of the wrong kind (a branch, a limited partnership — a
  separate register) is a note card saying what the number is.
* Dates are the ROC calendar, ``YYYMMDD`` (``0760221`` = 21 Feb 1987).
* The platform answers a refusal with a Chinese sentence, not a status code:
  ``非授權介接之IP`` (this IP is not on the whitelist), ``超出…`` (a
  concurrency or daily cap). Both are degradations, never cached.
* The developer guide (``/od/rule``) asks API users to file a usage
  notification form (使用告知書, to opendata.gcis@gmail.com) and whitelists
  the caller's IP, with per-user concurrency and daily caps it does not
  quantify. Not enforced on 29 Oct 2026; the pacing in ``host_throttle``
  keeps a burst of lookups polite either way.
* **TLS.** The certificate chain (TWCA) has an intermediate without a Subject
  Key Identifier, which Python 3.13's default ``VERIFY_X509_STRICT`` refuses;
  curl and Python 3.11 accept it. The adapter's client clears that one flag for
  this host only (:func:`gcis_ssl_context`) and verifies everything else.

License: Open Government Data License, version 1.0 (政府資料開放授權條款－第1版)
— any purpose, commercial included, compatible with CC BY 4.0; the
attribution format is the licence's Exhibit. https://data.gov.tw/license
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import ssl
import unicodedata
from functools import lru_cache
from typing import Any
from urllib.parse import urlencode

import httpx

from .. import degradation
from ..cache import Cache
from ..config import get_settings
from ..host_throttle import HostRateLimitedError
from ..http import build_client
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .cr_hongkong import to_traditional
from .schemas import validate_raw
from .schemas.gcis_taiwan import GcisTaiwanBundle

_LOG = logging.getLogger(__name__)

_API_BASE = "https://data.gcis.nat.gov.tw/od/data/api"
_CACHE_NS = "gcis_taiwan"
#: Bump when the cached payload's shape changes.
_CACHE_VERSION = "v1"

#: The datasets read, by the platform's GUID.
COMPANY_DATASET = "5F64D864-61CB-4D0D-8AD9-492047CC1EA6"  # 公司登記基本資料-應用一
DIRECTORS_DATASET = "4E5F7653-1B91-4DDC-99D5-468530FAE396"  # 公司登記董監事資料集
KIND_DATASET = "673F0FC0-B3A7-429F-9041-E9866836B66D"  # 統編查是否為公司、分公司及商業
SEARCH_DATASET = "6BBA2268-1367-4B42-9CCA-BC17499EBE8C"  # 公司登記關鍵字查詢

#: The platform's page size ceiling; a page this long may be truncated.
_TOP = 1000

#: GLEIF Registration Authority codes whose ``registeredAs`` is a company UBN.
#: Verified 29 Oct 2026: ``RA000551`` Ministry of Economic Affairs. ``RA001009``
#: (Financial Supervisory Commission) files funds, which this register lacks.
TW_RA_CODES: frozenset[str] = frozenset({"RA000551"})

#: The BODS identifier scheme for the UBN (org-id.guide).
TW_CR_SCHEME = "TW-CR"
TW_CR_SCHEME_NAME = "Unified Business Number (統一編號), Ministry of Economic Affairs company register"

#: How many name-search rows become hits.
_SEARCH_RESULT_CAP = 20

#: Per-request timeout: the platform answered in 0.94 s at the median, 6 s at
#: worst, on 29 Oct 2026.
_REQUEST_TIMEOUT = httpx.Timeout(20.0, connect=10.0)

#: The weights of the UBN check digit (Ministry of Finance rule).
_UBN_WEIGHTS = (1, 2, 1, 2, 1, 2, 4, 1)

#: The platform's refusals, said in a sentence on a 200 rather than a status.
_REFUSAL_NOT_WHITELISTED = "非授權介接之IP"
_REFUSAL_OVER_LIMIT = "超出"
_REFUSAL_PLATFORM_BUSY = "介接服務達上限"
_REFUSAL_MAINTENANCE = "資料庫維護中"

#: 統編查是否為公司、分公司及商業 TYPE values, as the platform writes them.
KIND_COMPANY = "公司"
KIND_BRANCH = "分公司"
KIND_BUSINESS = "商業"

COVERAGE_NOT_FOUND = (
    "The Ministry of Economic Affairs' company register holds no company under "
    "this Unified Business Number."
)
COVERAGE_BRANCH = (
    "This Unified Business Number is a branch office (分公司) in the Ministry of "
    "Economic Affairs' register, not a company; the open data publishes no "
    "directors for a branch."
)
COVERAGE_BUSINESS = (
    "This Unified Business Number is a sole-trader or partnership business "
    "(商業) registered by a local government, not a company; the company "
    "register's open data does not cover it."
)
COVERAGE_OTHER = (
    "The Ministry of Economic Affairs' company register holds no company under "
    "this Unified Business Number. Limited partnerships (有限合夥) are kept on a "
    "separate register this open data does not serve."
)


def ubn_check_digit_ok(digits: str) -> bool:
    """The Unified Business Number check: the weighted digit sum is a multiple
    of five. Each product's digits are summed (14 → 1 + 4); when the seventh
    digit is 7 its product (28) may count as 10 or 1, so either sum passing is
    enough. Since 2023 the Ministry of Finance accepts multiples of five; the
    older multiple-of-ten rule is a subset of it.
    """
    if not re.fullmatch(r"\d{8}", digits or ""):
        return False
    total = 0
    for digit, weight in zip(map(int, digits), _UBN_WEIGHTS):
        product = digit * weight
        total += product // 10 + product % 10
    if total % 5 == 0:
        return True
    return digits[6] == "7" and (total + 1) % 5 == 0


def normalise_ubn(value: str) -> str:
    """Canonicalise a Unified Business Number: eight digits with a valid check
    digit, whitespace removed. Raises ``ValueError`` otherwise, so the lookup
    pipeline skips the adapter rather than asking the register about a number
    that cannot be one (the register answers a malformed number exactly as it
    answers an unknown one)."""
    text = "".join(str(value or "").split())
    if not re.fullmatch(r"\d{8}", text):
        raise ValueError(f"not a Unified Business Number: {value!r}")
    if not ubn_check_digit_ok(text):
        raise ValueError(f"Unified Business Number check digit fails: {value!r}")
    return text


def parse_roc_date(value: Any) -> str | None:
    """A Republic of China calendar date (``YYYMMDD``, or ``YYMMDD`` before
    ROC year 100) → ISO ``YYYY-MM-DD``; anything else → None. The ROC year
    plus 1911 is the Gregorian year: ``0760221`` → ``1987-02-21``."""
    text = str(value or "").strip()
    match = re.fullmatch(r"(\d{2,3})(\d{2})(\d{2})", text)
    if not match:
        return None
    year, month, day = (int(part) for part in match.groups())
    if year < 1 or not 1 <= month <= 12 or not 1 <= day <= 31:
        return None
    import datetime as _dt

    try:
        return _dt.date(year + 1911, month, day).isoformat()
    except ValueError:
        return None


def int_field(value: Any) -> int | None:
    """A numeric field the platform sends as a JSON number (and the Swagger
    calls a string). ``None`` for anything that is not a whole number."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    text = str(value or "").strip().replace(",", "")
    return int(text) if re.fullmatch(r"-?\d+", text) else None


def text_field(value: Any) -> str:
    return str(value or "").strip()


@lru_cache(maxsize=1)
def gcis_ssl_context() -> ssl.SSLContext:
    """The TLS context for this host only: httpx's default (same CA bundle,
    hostname check and certificate verification) with Python 3.13's
    ``VERIFY_X509_STRICT`` cleared, because GCIS's TWCA intermediate carries
    no Subject Key Identifier. Nothing else is relaxed."""
    context = httpx.create_ssl_context()
    strict = getattr(ssl, "VERIFY_X509_STRICT", 0)
    if strict:
        context.verify_flags &= ~strict
    return context


def search_spellings(query: str) -> list[str]:
    """The spellings a name search tries, in order.

    A simplified-Chinese query is converted to traditional, the register's
    script — but OpenCC writes 台 as 臺 under every traditional profile, and
    the register uses both (台灣積體電路製造, 臺北市政府). So the query's own 台
    is kept, and the 臺 spelling is tried second, only when the first finds
    nothing. Quotes are dropped: the platform's filter grammar has none.
    """
    text = query.replace("'", "").replace('"', "").strip()
    converted = to_traditional(text)
    if len(converted) == len(text):
        converted = "".join(
            original if original == "台" else new for original, new in zip(text, converted)
        )
    spellings = [converted]
    if "台" in converted:
        spellings.append(converted.replace("台", "臺"))
    elif "臺" in converted:
        spellings.append(converted.replace("臺", "台"))
    return list(dict.fromkeys(s for s in spellings if s))


def dataset_url(dataset: str, filter_: str, *, top: int = _TOP) -> str:
    """The platform URL for one dataset query (also the statements' source
    URL, so a reader can re-run it)."""
    query = urlencode(
        {"$format": "json", "$filter": filter_, "$skip": 0, "$top": top}, safe="$"
    )
    return f"{_API_BASE}/{dataset}?{query}"


def company_url(ubn: str) -> str:
    return dataset_url(COMPANY_DATASET, f"Business_Accounting_NO eq {ubn}", top=1)


def findbiz_url(ubn: str) -> str:
    """The register's own public search page for the number (findbiz)."""
    return (
        "https://findbiz.nat.gov.tw/fts/query/QueryList/queryList.do?"
        + urlencode({"qryCond": ubn, "infoType": "D", "qryType": "cmpyType"})
    )


class _Unanswered(Exception):
    """A dataset query that did not answer. ``detail`` and ``reason`` are the
    degradation it records (or, for a holder resolution, would have)."""

    def __init__(self, what: str, detail: str = "", reason: str = degradation.REASON_UPSTREAM_ERROR):
        super().__init__(what)
        self.detail = detail or f"GCIS {what} did not answer"
        self.reason = reason


# ---------------------------------------------------------------------------
# Legal-person holders → their UBN (the Taiwan GCIS v2 ticket, 4 Nov 2026)
# ---------------------------------------------------------------------------
#
# The register of directors and supervisors names a legal person holding a seat
# by name only. The keyword dataset (``SEARCH_DATASET``) can find its number,
# but it has **no exact operator**: ``like`` and ``eq`` on ``Company_Name`` are
# both a token search (``eq 台灣積體電路`` returns TSMC), it answers nothing
# without a ``Company_Status`` filter, it folds width and case but not 台/臺, a
# space in the value breaks the filter, and an answer is cut at ``$top``. So the
# exact match is OpenCheck's, on the rows returned, after :func:`fold_name`.
#
# Measured on 4 Nov 2026 over the 503 ISSUED RA000551 LEIs: 563 distinct
# legal-person holders; 365 resolve to exactly one live company, none to
# several, 42 to none (ministries, funds, foundations, associations, foreign
# companies); 156 carry a foreign prefix. Stephen's decisions on the ticket:
# several matches are refused silently (A); a resolution that could not be read
# is a ``source_read`` degradation (B); live companies (status 01) only (C); no
# foreign-prefixed name is sent (D); the finding sentence is unchanged (E).

#: At most this many holders are resolved per fetch; the measured maximum was
#: 14. Any beyond it are ``not_read`` and said (decision B).
HOLDER_RESOLVE_CAP = 15
#: Concurrent keyword queries per fetch; ``host_throttle`` paces the host.
_HOLDER_CONCURRENCY = 4
#: Wall-clock budget for the whole holder phase. The company and its directors
#: are already read; a slow name search must never spend the source's
#: ``lookup_timeout_s`` and cost the company card. Holders still unanswered
#: when it runs out are ``not_read`` (timeout) and said.
HOLDER_PHASE_BUDGET_S = 12.0
#: A resolution (or a refused several-match answer) is cached this long; a
#: definitive none is a ``put_absent`` (``cache.ABSENT_TTL_DAYS``).
_HOLDER_CACHE_DAYS = 7.0
_HOLDER_CACHE_VERSION = "v1"

HOLDER_RESOLVED = "resolved"
HOLDER_NONE = "none"
HOLDER_SEVERAL = "several"
HOLDER_NOT_READ = "not_read"
HOLDER_OUTCOMES = frozenset({HOLDER_RESOLVED, HOLDER_NONE, HOLDER_SEVERAL, HOLDER_NOT_READ})

#: The live status, the only one a holder is resolved against (decision C).
_LIVE_STATUS = "01"


def fold_name(name: str) -> str:
    """A company name as the comparison reads it: NFKC (full-width letters and
    digits to their ordinary forms — the platform folds width too), casefold,
    whitespace collapsed, and 臺 read as 台 (the register uses both, and the
    platform does not fold them)."""
    text = unicodedata.normalize("NFKC", str(name or "")).casefold()
    return " ".join(text.split()).replace("臺", "台")


def holder_is_resolvable(name: str) -> bool:
    """Whether a legal-person holder's name is sent to the keyword search.

    Only a domestic company: a name ending 公司, with no whitespace (a space
    breaks the platform's filter, and every holder written with one is a
    foreign company under a Latin name) and no foreign nationality prefix
    (decision D). Ministries, government funds, foundations and associations
    never end 公司, so a state body is never matched or classified by name
    (Phase 240, decision E of the v1 ticket).
    """
    from ..bods.mappers.taiwan import tw_foreign_prefix

    text = str(name or "").strip()
    if not text or not text.endswith("公司") or re.search(r"\s", text):
        return False
    return tw_foreign_prefix(text) is None


def _holder_cache_key(name: str) -> str:
    digest = hashlib.sha256(fold_name(name).encode("utf-8")).hexdigest()[:24]
    return f"{_CACHE_NS}/holder-ubn-{_HOLDER_CACHE_VERSION}/{digest}"


class GcisTaiwanAdapter(SourceAdapter):
    """Source adapter for Taiwan's company register (MOEA GCIS open data)."""

    id = "gcis_taiwan"

    lookup_derivers = (LookupDeriver(TW_RA_CODES, "tw_ubn", normalise_ubn),)
    lookup_pass_legal_name = True

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="Taiwan company register (MOEA GCIS)",
            homepage="https://data.gcis.nat.gov.tw/",
            description=(
                "Taiwan company data from the Ministry of Economic Affairs' GCIS "
                "open data platform: the company's name, status, capital and "
                "registered address, and its directors and supervisors with the "
                "holdings the Company Act requires them to disclose — including "
                "the companies that hold board seats through representatives."
            ),
            license="OGDL-Taiwan-1.0",
            attribution=(
                "Ministry of Economic Affairs, Administration of Commerce "
                "(經濟部商業發展署) [2026] [公司登記基本資料-應用一; "
                "公司登記董監事資料集]. Released under the Open Government Data "
                "License, version 1.0 (https://data.gov.tw/license); users may "
                "use it subject to the licence's terms."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
            is_national_register=True,
            country="TW",
        )

    # ------------------------------------------------------------------
    # HTTP — every outbound call funnels through here
    # ------------------------------------------------------------------

    async def _query(
        self, dataset: str, filter_: str, *, what: str, record: bool = True
    ) -> list[dict[str, Any]]:
        """One dataset query: the rows, ``[]`` for the platform's empty answer.

        Raises :class:`_Unanswered` for anything that is not an answer: a
        refusal by the pacing, a network failure, a non-2xx, the platform's
        refusal sentences, a body that is not a JSON list. With ``record`` (the
        default) the degradation is recorded here; a holder resolution passes
        ``record=False`` and says its failures once, together (decision B).
        """
        try:
            return await self._query_rows(dataset, filter_, what=what)
        except _Unanswered as exc:
            if record:
                degradation.record(self.id, exc.detail, reason=exc.reason)
            raise

    async def _query_rows(self, dataset: str, filter_: str, *, what: str) -> list[dict[str, Any]]:
        params = {"$format": "json", "$filter": filter_, "$skip": "0", "$top": str(_TOP)}
        try:
            async with build_client(verify=gcis_ssl_context()) as client:
                response = await client.get(
                    f"{_API_BASE}/{dataset}", params=params, timeout=_REQUEST_TIMEOUT
                )
        except HostRateLimitedError:
            raise _Unanswered(
                what,
                f"GCIS {what}: not sent — OpenCheck's request pacing for the platform was spent",
                degradation.REASON_RATE_LIMITED,
            ) from None
        except httpx.HTTPError as exc:
            # The detail names the exception type only; the URL carries the UBN.
            kind = type(exc).__name__
            _LOG.warning("GCIS %s did not answer: %s", what, kind)
            raise _Unanswered(
                what, f"GCIS {what} did not answer ({kind})", degradation.reason_for_failure(kind)
            ) from None

        if not response.is_success:
            raise _Unanswered(
                what,
                f"GCIS {what} returned HTTP {response.status_code}",
                degradation.reason_for_failure(f"HTTP {response.status_code}"),
            )

        body = response.text.strip()
        if not body:
            return []  # the platform's "no record"
        if not body.startswith(("[", "{")):
            raise _Unanswered(what, f"GCIS {what}: {_refusal_detail(body)}", _refusal_reason(body))
        try:
            payload = json.loads(body)
        except ValueError:
            raise _Unanswered(what, f"GCIS {what} returned a body that is not JSON") from None
        if isinstance(payload, dict):
            payload = [payload]
        if not isinstance(payload, list):
            raise _Unanswered(what, f"GCIS {what} returned an unexpected shape")
        return [row for row in payload if isinstance(row, dict)]

    # ------------------------------------------------------------------
    # Search — company name, or a UBN typed as the query
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Search companies by name (the platform's ``like``), approved
        companies only. A simplified-Chinese query is converted to traditional
        first, because that is how the register writes names. A query that is
        a valid UBN is looked up exactly instead."""
        if kind != SearchKind.ENTITY:
            return []
        cleaned = " ".join(str(query or "").split())
        if not cleaned or not self.info.live_available:
            return []
        try:
            ubn = normalise_ubn(cleaned)
        except ValueError:
            ubn = None
        try:
            if ubn:
                rows = await self._query(
                    COMPANY_DATASET, f"Business_Accounting_NO eq {ubn}", what="company read"
                )
            else:
                rows = []
                for name in search_spellings(cleaned):
                    rows = await self._query(
                        SEARCH_DATASET,
                        f"Company_Name like {name} and Company_Status eq 01",
                        what="name search",
                    )
                    if rows:
                        break
        except _Unanswered:
            return []

        hits: list[SourceHit] = []
        for company in rows[:_SEARCH_RESULT_CAP]:
            number = text_field(company.get("Business_Accounting_NO"))
            name = text_field(company.get("Company_Name"))
            if not number or not name:
                continue
            status = text_field(company.get("Company_Status_Desc"))
            hits.append(
                SourceHit(
                    source_id=self.id,
                    hit_id=number,
                    kind=SearchKind.ENTITY,
                    name=name,
                    summary=f"{TW_CR_SCHEME} {number}" + (f" · {status}" if status else ""),
                    identifiers={"tw_ubn": number},
                    raw={"company": company},
                    is_stub=False,
                )
            )
        return hits

    # ------------------------------------------------------------------
    # Fetch — the company and its directors and supervisors by UBN
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        try:
            ubn = normalise_ubn(hit_id)
        except ValueError:
            return self._bundle(None, None, str(hit_id or ""), legal_name)

        cache_key = f"{_CACHE_NS}/company-{_CACHE_VERSION}/{ubn}"
        cached = self._cache.get_payload(cache_key, max_age_days=1)
        if cached is not None and isinstance(cached[0], dict):
            directors = cached[0].get("directors")
            return self._bundle(
                cached[0].get("company"), directors, ubn, legal_name,
                holder_ubns=await self._resolve_holders(directors, ubn),
            )
        if not self.info.live_available:
            return self._bundle(None, None, ubn, legal_name)

        filter_ = f"Business_Accounting_NO eq {ubn}"
        company_task = asyncio.create_task(
            self._query(COMPANY_DATASET, filter_, what="company read")
        )
        directors_task = asyncio.create_task(
            self._query(DIRECTORS_DATASET, filter_, what="directors read")
        )
        results = await asyncio.gather(company_task, directors_task, return_exceptions=True)
        company_rows, director_rows = results
        if isinstance(company_rows, BaseException):
            if not isinstance(company_rows, _Unanswered):
                raise company_rows
            # The company read failed: nothing to show, nothing cached.
            return self._bundle(None, None, ubn, legal_name)

        company = next(
            (r for r in company_rows if text_field(r.get("Business_Accounting_NO")) == ubn),
            None,
        )
        if company is None:
            return await self._not_found(ubn, legal_name)

        if isinstance(director_rows, BaseException):
            if not isinstance(director_rows, _Unanswered):
                raise director_rows
            # A record with no directors read: shown, said, never cached.
            degradation.record(
                self.id,
                "GCIS answered for the company but not for its directors and supervisors",
                check=degradation.CHECK_SOURCE_READ,
            )
            return self._bundle(company, None, ubn, legal_name, directors_read=False)

        if len(director_rows) >= _TOP:
            degradation.record(
                self.id,
                f"GCIS returned a full page of {_TOP} director rows; the list may be cut",
                check=degradation.CHECK_SOURCE_READ,
                reason=degradation.REASON_TRUNCATED,
            )
            return self._bundle(
                company, director_rows, ubn, legal_name,
                holder_ubns=await self._resolve_holders(director_rows, ubn),
            )

        self._cache.put(cache_key, {"company": company, "directors": director_rows})
        return self._bundle(
            company, director_rows, ubn, legal_name,
            holder_ubns=await self._resolve_holders(director_rows, ubn),
        )

    # ------------------------------------------------------------------
    # Legal-person holders → UBN
    # ------------------------------------------------------------------

    async def _resolve_holders(
        self, directors: list[dict[str, Any]] | None, ubn: str
    ) -> dict[str, dict[str, Any]]:
        """Each sendable legal-person holder's outcome, keyed on its name as
        filed. Cached outcomes answer without a call; offline, an uncached
        holder is left out and nothing is said. A holder that could not be
        read is ``not_read`` and the lot are said in one ``source_read``
        degradation (decision B)."""
        names: list[str] = []
        for row in directors or []:
            name = str(row.get("Juristic_Person_Name") or "").strip()
            if name and name not in names and holder_is_resolvable(name):
                names.append(name)
        if not names:
            return {}

        outcomes: dict[str, dict[str, Any]] = {}
        to_read: list[str] = []
        for name in names:
            cached = self._cached_holder(name)
            if cached is not None:
                outcomes[name] = cached
            else:
                to_read.append(name)

        live = self.info.live_available
        reasons: list[str] = []
        if to_read and live:
            sent, over_cap = to_read[:HOLDER_RESOLVE_CAP], to_read[HOLDER_RESOLVE_CAP:]
            gate = asyncio.Semaphore(_HOLDER_CONCURRENCY)

            async def _one(name: str) -> tuple[str, dict[str, Any], str | None]:
                async with gate:
                    return name, *(await self._read_holder(name))

            tasks = {asyncio.ensure_future(_one(n)): n for n in sent}
            done, pending = await asyncio.wait(tasks, timeout=HOLDER_PHASE_BUDGET_S)
            for task in pending:
                task.cancel()
                outcomes[tasks[task]] = {"outcome": HOLDER_NOT_READ}
                reasons.append(degradation.REASON_TIMEOUT)
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                name, outcome, reason = task.result()
                outcomes[name] = outcome
                if reason:
                    reasons.append(reason)
            for name in over_cap:
                outcomes[name] = {"outcome": HOLDER_NOT_READ}
                reasons.append(degradation.REASON_TRUNCATED)

        # Never point a holder at the company itself.
        for outcome in outcomes.values():
            if outcome.get("ubn") == ubn:
                outcome.clear()
                outcome["outcome"] = HOLDER_NONE

        if reasons:
            degradation.record(
                self.id,
                f"GCIS: {len(reasons)} of {len(names)} legal-person directors could not be "
                "linked to their company-register record (the name search did not answer "
                "in full)",
                check=degradation.CHECK_SOURCE_READ,
                reason=_worst_reason(reasons),
            )
        return {name: outcomes[name] for name in names if name in outcomes}

    def _cached_holder(self, name: str) -> dict[str, Any] | None:
        hit = self._cache.get_payload(_holder_cache_key(name), max_age_days=_HOLDER_CACHE_DAYS)
        if hit is None:
            return None
        payload = hit[0]
        if payload is None:
            return {"outcome": HOLDER_NONE}
        if isinstance(payload, dict) and payload.get("outcome") in HOLDER_OUTCOMES:
            return dict(payload)
        return None

    async def _read_holder(self, name: str) -> tuple[dict[str, Any], str | None]:
        """One holder against live companies: ``(outcome, degradation reason)``.

        Exact means ``fold_name`` equal. One company → ``resolved``; several →
        ``several`` (refused, never picked — decision A); a full answer with
        none → ``none``. An answer cut at ``$top`` or a query that did not
        answer → ``not_read``, never cached (the Phase 262 rule)."""
        key = fold_name(name)
        matches: dict[str, str] = {}
        for spelling in search_spellings(name):
            try:
                rows = await self._query(
                    SEARCH_DATASET,
                    f"Company_Name like {spelling} and Company_Status eq {_LIVE_STATUS}",
                    what="legal-person name search",
                    record=False,
                )
            except _Unanswered as exc:
                return {"outcome": HOLDER_NOT_READ}, exc.reason
            if len(rows) >= _TOP:
                return {"outcome": HOLDER_NOT_READ}, degradation.REASON_TRUNCATED
            for row in rows:
                number = text_field(row.get("Business_Accounting_NO"))
                register_name = text_field(row.get("Company_Name"))
                if fold_name(register_name) == key and _is_ubn(number):
                    matches[number] = register_name
            if matches:
                break

        cache_key = _holder_cache_key(name)
        if not matches:
            self._cache.put_absent(cache_key)
            return {"outcome": HOLDER_NONE}, None
        if len(matches) > 1:
            outcome: dict[str, Any] = {"outcome": HOLDER_SEVERAL, "ubns": sorted(matches)}
        else:
            number, register_name = next(iter(matches.items()))
            outcome = {"outcome": HOLDER_RESOLVED, "ubn": number, "register_name": register_name}
        self._cache.put(cache_key, outcome)
        return dict(outcome), None

    async def _not_found(self, ubn: str, legal_name: str) -> dict[str, Any]:
        """The company endpoint has no record: say what the number is, if the
        kind endpoint answers. Not cached — a miss is rare (4 of 504) and a
        number can become a company."""
        kinds: list[str] = []
        try:
            rows = await self._query(KIND_DATASET, f"NO eq {ubn}", what="number-kind read")
            kinds = [
                text_field(r.get("TYPE"))
                for r in rows
                if text_field(r.get("exist")).upper() == "Y" and text_field(r.get("TYPE"))
            ]
        except _Unanswered:
            kinds = []
        if KIND_BRANCH in kinds and KIND_COMPANY not in kinds:
            note = COVERAGE_BRANCH
        elif KIND_BUSINESS in kinds and KIND_COMPANY not in kinds:
            note = COVERAGE_BUSINESS
        elif kinds:
            note = COVERAGE_NOT_FOUND
        else:
            note = COVERAGE_OTHER
        bundle = self._bundle(None, None, ubn, legal_name)
        bundle.update(
            {"is_stub": False, "not_found": True, "register_kinds": kinds, "coverage_note": note}
        )
        return bundle

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _bundle(
        self,
        company: dict[str, Any] | None,
        directors: list[dict[str, Any]] | None,
        ubn: str,
        legal_name: str,
        *,
        directors_read: bool = True,
        holder_ubns: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "tw_ubn": ubn,
            "company": company,
            "directors": list(directors or []),
            "directors_read": directors_read if company is not None else False,
            "legal_name": legal_name,
            "holder_ubns": dict(holder_ubns or {}),
            "is_stub": company is None,
        }
        if company is not None:
            validate_raw(self.id, GcisTaiwanBundle, bundle)
        return bundle


def _is_ubn(value: str) -> bool:
    try:
        return normalise_ubn(value) == value
    except ValueError:
        return False


#: Worst first: a refusal by the pacing says the most about what to do next.
_REASON_ORDER = (
    degradation.REASON_RATE_LIMITED,
    degradation.REASON_TIMEOUT,
    degradation.REASON_UPSTREAM_ERROR,
    degradation.REASON_TRUNCATED,
)


def _worst_reason(reasons: list[str]) -> str:
    for reason in _REASON_ORDER:
        if reason in reasons:
            return reason
    return reasons[0]


def _refusal_reason(body: str) -> str:
    if _REFUSAL_OVER_LIMIT in body or _REFUSAL_PLATFORM_BUSY in body:
        return degradation.REASON_RATE_LIMITED
    return degradation.REASON_UPSTREAM_ERROR


def _refusal_detail(body: str) -> str:
    """The platform's refusal, in words. Never the body itself: a refusal can
    quote the caller's IP address."""
    if _REFUSAL_NOT_WHITELISTED in body:
        return "the platform refused this server's IP address (not registered with it)"
    if _REFUSAL_PLATFORM_BUSY in body:
        return "the platform is at its connection limit"
    if _REFUSAL_OVER_LIMIT in body:
        return "the platform's per-user connection or daily limit was reached"
    if _REFUSAL_MAINTENANCE in body:
        return "the platform's database is under maintenance"
    return "returned a body that is neither JSON nor empty"
