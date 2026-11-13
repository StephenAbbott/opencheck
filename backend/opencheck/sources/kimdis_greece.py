"""Greece — ΚΗΜΔΗΣ, the Central Electronic Registry of Public Contracts.

Every Greek contracting authority must post each public contract it signs
(and each amendment) to ΚΗΜΔΗΣ, the Central Electronic Registry of Public
Contracts run by the General Secretariat of Commerce. Its Open Data API
answers one question for OpenCheck: **which public contracts has this
company signed, with which authorities** — contracts won, never ownership.

Why the API, not the Diavgis file
---------------------------------
The Open Contracting Partnership's Data Registry carries Diavgis, a
third-party OCDS compilation of ΚΗΜΔΗΣ, Diavgeia and TED (publication 161).
Measured on its 3 Nov 2026 file it starts in March 2026 and two in three of
its records are payment orders that merge unrelated payees on one award.
ΚΗΜΔΗΣ itself goes back to 2017, says which record amends which, and names
the contractor on every contract. Stephen chose the government source, read
live (decision A, 12 Nov 2026), contracts only — no payment orders (B).

The API
-------
``POST https://cerpp.eprocurement.gov.gr/khmdhs-opendata/contract?page=N``
with ``{"vatNumber": <ΑΦΜ>, "dateFrom": …, "dateTo": …}``. Keyless, answers a
datacentre, CC BY 4.0, 350 requests a minute. Measured 12–13 Nov 2026:

* **The window is at most 180 days**: a longer one is cut to the 180 days
  before ``dateTo``, and no dates at all means the last 180 days. So a
  company's history is read in 180-day windows from ``DATA_FROM`` (the first
  contracts are January 2017; every earlier window answers 404).
* **The window filters on ``submissionDate``** — when the record was posted,
  not when the contract was signed: an amendment posted in 2026 carries a
  2023 signing date.
* **No contracts is HTTP 404** with ``{"message": "No contracts found for the
  given criteria"}`` — an answer, not a failure.
* **Pages are 50**, whatever ``size`` asks; ``sort`` is ignored.
* **Each amendment is its own record** whose ``prevReferenceNo`` names the
  version it replaces (OTE's "2134 (5Η ΤΡΟΠ)" is a fifth amendment). A
  contract is counted once, at its latest version read; summing every record
  would count a five-times-amended contract six times.
* **The server drops connections** (``RemoteDisconnected``), retried once;
  and **some windows never answer**: for ΑΚΤΩΡ's ΑΦΜ, 23 Sep – 6 Nov 2018
  hangs every time while the days either side answer in a second. A timeout
  is not retried; that window becomes a stated gap (``gaps``, a
  ``source_read`` degradation) and the other windows are still read.

A long-standing supplier needs many calls (OTE: 21 windows, 2,970 records,
69 pages). A lookup reads at most ``MAX_CALLS``, newest windows first; when
that stops the read, the totals cover the windows read in full and the card
and the finding name the day they start from (``read_from``). Never a silent
cap.

The join is the ΑΦΜ, from ΓΕΜΗ
------------------------------
ΚΗΜΔΗΣ is searched by the contractor's tax number (ΑΦΜ). GLEIF never files a
Greek company's ΑΦΜ: RA000685 (ΓΕΜΗ, 3,348 ISSUED LEIs) and RA000376 (Athens
Chamber, 525) both file the twelve-digit ΓΕΜΗ number. The ΑΦΜ comes from the
ΓΕΜΗ record ``gemi_greece`` already reads, through its cache (decision C, no
crosswalk). ΓΕΜΗ not answering is a degradation of this source, never "no
contracts"; ΓΕΜΗ holding no ΑΦΜ for the company is an answer with no card.

What is kept
------------
Per contract: ΑΔΑΜ, title, posting day, signing day, period, authority,
value without VAT, procedure, type, bids, whether it amends an earlier
version, whether it is marked cancelled, and how many other contractors it
names. **Never** ``authorEmail``, ``signers`` or ``unitsOperator`` (people at
the authority), and never another contractor's name or ΑΦΜ (a joint
contractor can be a sole trader). Per supplier: totals, the five largest
authorities, the ``LATEST_KEPT`` latest and ``LARGEST_KEPT`` largest
contracts.

Shown beside TED, never added to it (decision D): a contract above the EU
thresholds can be in both, under different identifiers.

Licence: CC BY 4.0 (the Greek open-data licence the portal names, under
Directive 2019/1024).
"""

from __future__ import annotations

import asyncio
import logging
import re
import weakref
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, cast

import httpx

from .. import degradation, provenance
from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from ..outbound_rate import TokenBucket
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.kimdis_greece import KimdisGreeceBundle

if TYPE_CHECKING:
    from .gemi_greece import GemiGreeceAdapter

_LOG = logging.getLogger(__name__)

_CACHE_NS = "kimdis_greece"
_CACHE_MAX_AGE_DAYS = 1.0

API_BASE = "https://cerpp.eprocurement.gov.gr/khmdhs-opendata"
CONTRACT_URL = f"{API_BASE}/contract"
ATTACHMENT_URL_TEMPLATE = f"{API_BASE}/contract/attachment/{{adam}}"
HOMEPAGE = "https://cerpp.eprocurement.gov.gr/khmdhs-opendata/help"
LICENSE_ID = "CC-BY-4.0"

#: ΓΕΜΗ (RA000685) and the Athens Chamber of Commerce (RA000376) both file
#: the twelve-digit ΓΕΜΗ number in ``registeredAs`` (12 Nov 2026).
GR_GEMI_RA_CODES: frozenset[str] = frozenset({"RA000685", "RA000376"})

#: The first window: ΚΗΜΔΗΣ holds no contract posted before 2017.
DATA_FROM = date(2017, 1, 1)
#: The API refuses a longer window (it cuts it to the last 180 days).
WINDOW_DAYS = 180
#: The page size the API answers with, whatever is asked.
PAGE_SIZE = 50
#: Calls one read may spend: 21 windows' first pages plus room for the pages
#: of a supplier with a few hundred contracts a year.
MAX_CALLS = 60
_CONCURRENCY = 3
#: 350 a minute is the published limit, shared by every caller; pace under it.
_RATE_PER_MINUTE = 300.0
_TIMEOUT_S = 10.0

TOP_BUYERS = 5
LATEST_KEPT = 8
LARGEST_KEPT = 4
TITLE_CHARS = 160

#: One bucket per running event loop (the ny_dos rule).
_BUCKETS: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, TokenBucket]" = (
    weakref.WeakKeyDictionary()
)


def _bucket() -> TokenBucket:
    loop = asyncio.get_running_loop()
    bucket = _BUCKETS.get(loop)
    if bucket is None:
        bucket = TokenBucket(_RATE_PER_MINUTE, capacity=_CONCURRENCY, name="kimdis_greece")
        _BUCKETS[loop] = bucket
    return bucket


# ---------------------------------------------------------------------------
# The ΑΦΜ
# ---------------------------------------------------------------------------


def afm_valid(afm: str) -> bool:
    """The Greek tax number's check digit (weights 2^8 … 2^1, mod 11, mod 10)."""
    if not re.fullmatch(r"\d{9}", afm or "") or afm == "000000000":
        return False
    total = sum(int(afm[i]) * 2 ** (8 - i) for i in range(8))
    return total % 11 % 10 == int(afm[8])


def normalise_afm(value: Any) -> str:
    """``EL094019245`` / ``94019245`` → ``094019245``; '' when not an ΑΦΜ."""
    text = re.sub(r"\s", "", str(value or "")).upper()
    text = re.sub(r"^(EL|GR)", "", text)
    if not text.isdigit() or not 8 <= len(text) <= 9:
        return ""
    text = text.zfill(9)
    return text if afm_valid(text) else ""


def gemi_number(registered_at: str, registered_as: str) -> str:
    """The ΓΕΜΗ number GLEIF files, or '' when this is not a ΓΕΜΗ filing."""
    if registered_at not in GR_GEMI_RA_CODES:
        return ""
    cleaned = re.sub(r"[\s./-]", "", str(registered_as or ""))
    return cleaned if re.fullmatch(r"\d{1,12}", cleaned) else ""


# ---------------------------------------------------------------------------
# Reading a record
# ---------------------------------------------------------------------------


def windows(today: date) -> list[tuple[date, date]]:
    """180-day windows from ``DATA_FROM`` to ``today``, newest first."""
    out: list[tuple[date, date]] = []
    start = DATA_FROM
    while start <= today:
        end = min(start + timedelta(days=WINDOW_DAYS - 1), today)
        out.append((start, end))
        start = end + timedelta(days=1)
    out.reverse()
    return out


def _amount(value: Any) -> int | None:
    """``14826766.72`` → whole euros, half up. Unreadable → None."""
    if value is None or value == "":
        return None
    try:
        return int(Decimal(str(value)).quantize(Decimal(1), rounding=ROUND_HALF_UP))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _day(value: Any) -> str | None:
    """An ISO date or local timestamp → its day. ΚΗΜΔΗΣ writes Athens time
    with no offset, so the day as written is the day in Greece."""
    text = str(value or "").strip()
    return text[:10] if re.match(r"^\d{4}-\d{2}-\d{2}", text) else None


def _label(obj: Any) -> str:
    return str(obj.get("value") or "").strip() if isinstance(obj, dict) else ""


def reduce_record(record: dict[str, Any], afm: str) -> dict[str, Any] | None:
    """One ΚΗΜΔΗΣ contract record → the fields OpenCheck keeps (an allowlist).

    None when the record names no ΑΔΑΜ, or does not name ``afm`` among its
    contractors (the search is the API's; the check is ours).
    """
    adam = str(record.get("referenceNumber") or "").strip()
    if not adam:
        return None
    members = (record.get("contractingDataDetails") or {}).get("contractingMembersDataList") or []
    member_afms = [normalise_afm(m.get("vatNumber")) for m in members if isinstance(m, dict)]
    if afm not in member_afms:
        return None
    own_names = [
        str(m.get("name") or "").strip()
        for m in members
        if isinstance(m, dict) and normalise_afm(m.get("vatNumber")) == afm
    ]
    organisation = record.get("organization") or {}
    value = _amount(record.get("totalCostWithoutVAT"))
    if value is None:
        value = _amount(record.get("contractBudget"))
    return {
        "adam": adam,
        "previous": str(record.get("prevReferenceNo") or "").strip() or None,
        "published": _day(record.get("submissionDate")),
        "signed": _day(record.get("contractSignedDate")),
        "start_date": _day(record.get("startDate")),
        "end_date": None if record.get("noEndDate") else _day(record.get("endDate")),
        "buyer": _label(organisation),
        "buyer_key": str(organisation.get("key") or "") if isinstance(organisation, dict) else "",
        "title": str(record.get("title") or "").strip()[:TITLE_CHARS],
        "value_eur": value,
        "procedure": _label(record.get("procedureType")),
        "contract_type": _label(record.get("contractType")),
        "bids": record.get("bidsSubmitted") if isinstance(record.get("bidsSubmitted"), int) else None,
        "marked_cancelled": bool(record.get("cancelled")),
        "other_contractors": max(0, len(members) - 1),
        "name": own_names[0] if own_names else "",
    }


def summarise(records: list[dict[str, Any]], afm: str) -> dict[str, Any]:
    """Totals, the largest authorities and the kept contracts.

    A record another record amends is an earlier version and is dropped, so a
    contract is counted once, at its latest version read.
    """
    by_adam: dict[str, dict[str, Any]] = {}
    for r in records:
        by_adam.setdefault(r["adam"], r)
    replaced = {r["previous"] for r in by_adam.values() if r.get("previous")}
    current = [r for adam, r in by_adam.items() if adam not in replaced]
    if not current:
        return {"supplier": None, "buyers": [], "contracts": []}

    days = sorted(r["published"] for r in current if r["published"])
    latest_named = max(
        (r for r in current if r.get("name")),
        key=lambda r: (r["published"] or "", r["adam"]),
        default=None,
    )
    buyers: dict[str, dict[str, Any]] = {}
    for r in current:
        key = r["buyer_key"] or r["buyer"]
        tally = buyers.setdefault(key, {"name": r["buyer"], "contracts": 0, "value_eur": 0})
        tally["contracts"] += 1
        tally["value_eur"] += r["value_eur"] or 0
    ranked = sorted(
        buyers.values(), key=lambda b: (-b["value_eur"], -b["contracts"], b["name"])
    )
    supplier = {
        "afm": afm,
        "name": latest_named["name"] if latest_named else "",
        "contracts": len(current),
        "value_eur": sum(r["value_eur"] or 0 for r in current),
        "buyers": len(buyers),
        "first_date": days[0] if days else None,
        "last_date": days[-1] if days else None,
        "marked_cancelled": sum(1 for r in current if r["marked_cancelled"]),
        "amended": sum(1 for r in current if r.get("previous")),
        "shared": sum(1 for r in current if r["other_contractors"]),
        "records_read": len(by_adam),
    }
    newest = sorted(current, key=lambda r: (r["published"] or "", r["adam"]), reverse=True)
    largest = sorted(current, key=lambda r: (r["value_eur"] or 0, r["adam"]), reverse=True)
    kept: dict[str, dict[str, Any]] = {}
    for r in (*newest[:LATEST_KEPT], *largest[:LARGEST_KEPT]):
        kept.setdefault(r["adam"], r)
    contracts = [
        {
            "adam": r["adam"],
            "published": r["published"],
            "signed": r["signed"],
            "buyer": r["buyer"],
            "title": r["title"],
            "value_eur": r["value_eur"],
            "start_date": r["start_date"],
            "end_date": r["end_date"],
            "procedure": r["procedure"],
            "contract_type": r["contract_type"],
            "bids": r["bids"],
            "amends_earlier": bool(r.get("previous")),
            "marked_cancelled": r["marked_cancelled"],
            "other_contractors": r["other_contractors"],
            "url": ATTACHMENT_URL_TEMPLATE.format(adam=r["adam"]),
        }
        for r in kept.values()
    ]
    contracts.sort(key=lambda c: (c["published"] or "", c["adam"]), reverse=True)
    return {
        "supplier": supplier,
        "buyers": ranked[:TOP_BUYERS],
        "contracts": contracts,
    }


class _Unanswered(Exception):
    """One ΚΗΜΔΗΣ call that did not answer, after its retry."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


class _CallsSpent(Exception):
    """The read reached ``MAX_CALLS``."""


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class KimdisGreeceAdapter(SourceAdapter):
    """ΚΗΜΔΗΣ public contracts, by the ΑΦΜ ΓΕΜΗ publishes for the company."""

    id = "kimdis_greece"
    # Reached from ``_dispatch`` via ``fetch_by_identifiers`` (the AusTender
    # shape): ``gemi_greece`` owns the Greek RA codes as a register.
    lookup_timeout_s = 60.0

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="ΚΗΜΔΗΣ — Greek public contracts",
            homepage=HOMEPAGE,
            description=(
                "Public contracts Greek contracting authorities post to ΚΗΜΔΗΣ, "
                "the Central Electronic Registry of Public Contracts, read live "
                "from its Open Data API (from 2017). For a supplier: its latest "
                "and largest contracts with authority, value, period, procedure "
                "and a link to each contract document, plus totals. Matched on "
                "the tax number (ΑΦΜ) the company's ΓΕΜΗ record publishes. "
                "Contracts won, not ownership."
            ),
            license=LICENSE_ID,
            attribution=(
                "Contains public contract data from ΚΗΜΔΗΣ (Central Electronic "
                "Registry of Public Contracts), General Secretariat of Commerce, "
                "Hellenic Republic, licensed under CC BY 4.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=self._live(),
            is_national_register=False,
            country=None,
        )

    @staticmethod
    def _live() -> bool:
        """Live reads allowed, and ΓΕΜΗ able to give the ΑΦΜ."""
        settings = get_settings()
        return bool(settings.allow_live and settings.gemi_api_key)

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Not name-searchable: reached from the ΓΕΜΗ number GLEIF files."""
        return []

    @staticmethod
    def applies_to(registered_at: str, registered_as: str) -> bool:
        """A ΓΕΜΗ number under a Greek authority. Asked before dispatch."""
        return bool(gemi_number(registered_at, registered_as))

    # ------------------------------------------------------------------
    # Bundles
    # ------------------------------------------------------------------

    def _bundle(self, afm: str, legal_name: str, **extra: Any) -> dict[str, Any]:
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "afm": afm,
            "gr_argemi": "",
            "matched_via": "gemi",
            "legal_name": legal_name or "",
            "supplier": None,
            "buyers": [],
            "contracts": [],
            "read_from": None,
            "read_to": None,
            "truncated": False,
            "incomplete": False,
            "gaps": [],
            "link": HOMEPAGE,
            "not_found": False,
            "no_afm": False,
            "is_stub": True,
        }
        bundle.update(extra)
        return bundle

    # ------------------------------------------------------------------
    # The pipeline's entry point
    # ------------------------------------------------------------------

    async def fetch_by_identifiers(
        self,
        lei: str,
        registered_as: str,
        registered_at: str,
        *,
        legal_name: str = "",
    ) -> dict[str, Any] | None:
        """ΓΕΜΗ number → ΑΦΜ (from ``gemi_greece``) → ΚΗΜΔΗΣ contracts."""
        argemi = gemi_number(registered_at, registered_as)
        if not argemi:
            return None
        if not self._live():
            return self._bundle("", legal_name, gr_argemi=argemi)
        afm, answered = await self._afm_from_gemi(argemi, legal_name)
        if not answered:
            degradation.record(
                self.id,
                "ΓΕΜΗ did not answer, so the tax number ΚΗΜΔΗΣ is searched by "
                "was not read and no contracts were looked up",
            )
            return self._bundle("", legal_name, gr_argemi=argemi)
        if not afm:
            # ΓΕΜΗ answered and publishes no valid ΑΦΜ: nothing to search by.
            return self._bundle(
                "", legal_name, gr_argemi=argemi, no_afm=True, is_stub=False
            )
        bundle = await self.fetch(afm, legal_name=legal_name)
        bundle["gr_argemi"] = argemi
        return bundle

    async def _afm_from_gemi(self, argemi: str, legal_name: str) -> tuple[str, bool]:
        """(ΑΦΜ, whether ΓΕΜΗ answered).

        Through the ΓΕΜΗ adapter's own cache and per-lookup budget, so the
        lookup's ΓΕΜΗ card and this read cost one request between them. Read
        in its own provenance scope: a cached ΓΕΜΗ record must not date
        ΚΗΜΔΗΣ's live answer.
        """
        from . import REGISTRY

        gemi = cast("GemiGreeceAdapter | None", REGISTRY.get("gemi_greece"))
        if gemi is None:
            return "", False
        with provenance.recording():
            bundle = await gemi.fetch(argemi, legal_name=legal_name)
        company = bundle.get("company") if isinstance(bundle, dict) else None
        if isinstance(company, dict):
            return normalise_afm(company.get("afm")), True
        # No company: a refusal or failure records a ΓΕΜΗ degradation; a
        # ΓΕΜΗ that answered "no such company" records none.
        return "", degradation.unanswered_fetch("gemi_greece") is None

    # ------------------------------------------------------------------
    # Fetch — one ΑΦΜ (the deepen and retry path too)
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """What ΚΗΜΔΗΣ holds for one ΑΦΜ."""
        afm = normalise_afm(hit_id)
        if not afm:
            return self._bundle("", legal_name)
        cache_key = f"{_CACHE_NS}/contracts-v1/{afm}"
        cached = self._cache.get_payload(cache_key, max_age_days=_CACHE_MAX_AGE_DAYS)
        if cached is not None and isinstance(cached[0], dict):
            return self._finish(afm, legal_name, cached[0])
        if not _live_calls_allowed():
            return self._bundle(afm, legal_name)

        read = await self._read(afm)
        if read is None:
            return self._bundle(afm, legal_name)
        if not read["incomplete"]:
            # A read cut by the call cap is deterministic and kept; one cut by
            # a failure is not (a failure is never cached).
            self._cache.put(cache_key, read)
        return self._finish(afm, legal_name, read)

    def _finish(self, afm: str, legal_name: str, read: dict[str, Any]) -> dict[str, Any]:
        summary = summarise(read.get("records") or [], afm)
        extra = {
            "read_from": read.get("read_from"),
            "read_to": read.get("read_to"),
            "truncated": bool(read.get("truncated")),
            "incomplete": bool(read.get("incomplete")),
            "gaps": list(read.get("gaps") or []),
            "is_stub": False,
        }
        if summary["supplier"] is None:
            return self._bundle(afm, legal_name, not_found=True, **extra)
        bundle = self._bundle(afm, legal_name, **extra, **summary)
        validate_raw(self.id, KimdisGreeceBundle, bundle)
        return bundle

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    async def _read(self, afm: str, today: date | None = None) -> dict[str, Any] | None:
        """Every window, newest first, until ``MAX_CALLS``.

        Returns the reduced records, the span read and the windows that did
        not answer (``gaps``, each a ``source_read`` degradation), or None
        when no window answered at all (a ``source_fetch`` degradation).
        One window can fail on every attempt: 23 Sep – 6 Nov 2018 hangs for
        ΑΚΤΩΡ's ΑΦΜ (13 Nov 2026), so one bad window must not cost the rest.
        """
        today = today or date.today()
        spans = windows(today)
        calls = {"n": 0}
        semaphore = asyncio.Semaphore(_CONCURRENCY)

        async with build_client() as client:

            async def page(window: tuple[date, date], number: int) -> dict[str, Any] | None:
                if calls["n"] >= MAX_CALLS:
                    raise _CallsSpent
                calls["n"] += 1
                async with semaphore:
                    return await self._post(client, afm, window, number)

            async def read_window(window: tuple[date, date]) -> list[dict[str, Any]]:
                first = await page(window, 0)
                if first is None:
                    return []
                rows = list(first.get("content") or [])
                pages = int(first.get("totalPages") or 1)
                if pages > 1:
                    rest = await asyncio.gather(
                        *(page(window, n) for n in range(1, pages)),
                        return_exceptions=True,
                    )
                    for result in rest:
                        if isinstance(result, BaseException):
                            raise result
                        rows.extend((result or {}).get("content") or [])
                return rows

            results = await asyncio.gather(
                *(read_window(w) for w in spans), return_exceptions=True
            )

        records: list[dict[str, Any]] = []
        read_from: date | None = None
        gaps: list[dict[str, str]] = []
        failures: list[_Unanswered] = []
        truncated = False
        for window, result in zip(spans, results):
            if isinstance(result, _CallsSpent):
                # The cap stops the read: older windows are dropped too, so
                # what was read is one unbroken span ending today.
                truncated = True
                break
            if isinstance(result, _Unanswered):
                gaps.append({"from": window[0].isoformat(), "to": window[1].isoformat()})
                failures.append(result)
                read_from = window[0]
                continue
            if isinstance(result, BaseException):
                raise result
            for row in result:
                if isinstance(row, dict):
                    reduced = reduce_record(row, afm)
                    if reduced is not None:
                        records.append(reduced)
            read_from = window[0]

        answered = any(not isinstance(r, BaseException) for r in results)
        if failures:
            first = failures[0]
            if not answered:
                degradation.record(self.id, first.detail, reason=first.reason)
                return None
            degradation.record(
                self.id,
                (
                    f"{first.detail} for {len(gaps)} of the 180-day periods read; "
                    "contracts posted in them are not counted"
                ),
                check=degradation.CHECK_SOURCE_READ,
                reason=first.reason,
            )
        if read_from is None:
            return None
        return {
            "records": records,
            "read_from": read_from.isoformat(),
            "read_to": today.isoformat(),
            "truncated": truncated,
            "incomplete": bool(gaps),
            "gaps": gaps,
        }

    async def _post(
        self,
        client: httpx.AsyncClient,
        afm: str,
        window: tuple[date, date],
        number: int,
    ) -> dict[str, Any] | None:
        """One page of one window; None when ΚΗΜΔΗΣ holds no contracts there.

        Retried once on a dropped connection or a 5xx; a timeout is not.
        """
        body = {
            "vatNumber": afm,
            "dateFrom": window[0].isoformat(),
            "dateTo": window[1].isoformat(),
        }
        failure = ("", "")
        for _attempt in range(2):
            await _bucket().acquire()
            try:
                response = await client.post(
                    CONTRACT_URL,
                    params={"page": str(number)},
                    json=body,
                    headers={"Accept": "application/json"},
                    timeout=_TIMEOUT_S,
                )
            except httpx.TimeoutException:
                # Not retried: a window that hangs hangs again (measured), and
                # a second wait would double the lookup's cost for nothing.
                raise _Unanswered(degradation.REASON_TIMEOUT, "ΚΗΜΔΗΣ timed out") from None
            except httpx.HTTPError:
                failure = (degradation.REASON_UPSTREAM_ERROR, "ΚΗΜΔΗΣ dropped the connection")
                continue
            if response.status_code == 404:
                return None
            if response.status_code == 429:
                raise _Unanswered(degradation.REASON_RATE_LIMITED, "ΚΗΜΔΗΣ returned HTTP 429")
            if response.status_code >= 500:
                failure = (
                    degradation.REASON_UPSTREAM_ERROR,
                    f"ΚΗΜΔΗΣ returned HTTP {response.status_code}",
                )
                continue
            if not response.is_success:
                raise _Unanswered(
                    degradation.REASON_UPSTREAM_ERROR,
                    f"ΚΗΜΔΗΣ returned HTTP {response.status_code}",
                )
            try:
                payload = response.json()
            except ValueError:
                raise _Unanswered(
                    degradation.REASON_UPSTREAM_ERROR, "ΚΗΜΔΗΣ returned a non-JSON body"
                ) from None
            if not isinstance(payload, dict) or not isinstance(payload.get("content"), list):
                raise _Unanswered(
                    degradation.REASON_UPSTREAM_ERROR, "ΚΗΜΔΗΣ returned an unexpected shape"
                )
            return payload
        raise _Unanswered(*failure)


def _live_calls_allowed() -> bool:
    """Live calls are on (``OPENCHECK_ALLOW_LIVE``)."""
    return bool(get_settings().allow_live)
